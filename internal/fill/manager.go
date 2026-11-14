package fill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Options configures a LocalManager.
type Options struct {
	// Workers bounds concurrent ingest of spool files into the CAS.
	// Default 4.
	Workers int
	// Logf receives background errors. Default log.Printf.
	Logf func(string, ...any)
	// HoldIngest leaves spooled units in the spool until ResumeIngest is
	// called. For tests, and for inspecting the spool.
	HoldIngest bool
	// Shadow, if set, turns on the debug savings mode: after ingest each
	// resident unit is also kept unpacked as a plain tree under
	// Shadow/<subsystem>/<unit id>, hard-linked from its spool files, so
	// disk use can be compared with the CAS (see internal/savings). Shadow
	// bytes are not counted by the quota and are removed on eviction.
	Shadow string
}

// LocalManager is the Manager for one cache directory.
type LocalManager struct {
	Pins

	db    *meta.DB
	store *cas.Store
	spool *DiskSpool
	quota *LocalQuota
	opts  Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	ingest chan int64
	sem    chan struct{}
	// ingesting counts ingestUnit calls in progress, so Wait covers the
	// gap between the resident commit and the spool removal.
	ingesting atomic.Int64
	shadowMu  sync.Mutex // serializes shadow tree writes and SyncShadow
	resume    chan struct{}

	mu       sync.Mutex
	changed  *sync.Cond // broadcast on spool file creation and fill end
	subs     map[string]subsys.Subsystem
	flights  map[string]*flight
	watchers []func(UnitEvent)
	openers  []func(OpenEvent)
}

var _ Manager = (*LocalManager)(nil)

type flight struct {
	started chan struct{}
	done    chan struct{}
	once    sync.Once
	unit    meta.Unit
	err     error
}

func (f *flight) start() { f.once.Do(func() { close(f.started) }) }

// NewManager recovers state left by a previous run and starts the ingest
// workers. Units caught mid-fetch are dropped (they refetch on next use);
// spooled units are queued for ingest.
func NewManager(db *meta.DB, store *cas.Store, spool *DiskSpool, quota *LocalQuota, opts Options) (*LocalManager, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &LocalManager{db: db, store: store, spool: spool, quota: quota, opts: opts,
		ctx: ctx, cancel: cancel, ingest: make(chan int64, 1024), sem: make(chan struct{}, opts.Workers),
		subs: map[string]subsys.Subsystem{}, flights: map[string]*flight{}, resume: make(chan struct{})}
	if !opts.HoldIngest {
		close(m.resume)
	}
	m.changed = sync.NewCond(&m.mu)
	if err := m.recover(); err != nil {
		cancel()
		return nil, err
	}
	m.wg.Add(1)
	go m.ingestLoop()
	return m, nil
}

// Close stops ingest. Fills in progress are cancelled.
func (m *LocalManager) Close() error {
	m.cancel()
	m.wg.Wait()
	return nil
}

func (m *LocalManager) recover() error {
	ctx := m.ctx
	fetching, err := m.db.UnitsInState(ctx, meta.UnitFetching)
	if err != nil {
		return err
	}
	for _, u := range fetching {
		tx, err := m.db.Begin(ctx)
		if err != nil {
			return err
		}
		if err := tx.DeleteUnitTree(u.ID); err == nil {
			err = tx.SetUnitManifest(u.ID, 0, 0)
		}
		if err == nil {
			err = tx.SetUnitState(u.ID, meta.UnitFetching, meta.UnitAbsent)
		}
		if err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	spooled, err := m.db.UnitsInState(ctx, meta.UnitSpooled)
	if err != nil {
		return err
	}
	keep := map[int64]bool{}
	for _, u := range spooled {
		keep[u.ID] = true
	}
	ids, err := m.spool.Units()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !keep[id] {
			if err := m.spool.Remove(id); err != nil {
				return err
			}
		}
	}
	for _, u := range spooled {
		if err := m.spool.Recover(u.ID); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		m.ingest <- u.ID
	}
	return nil
}

// Register implements Manager.
func (m *LocalManager) Register(ctx context.Context, s subsys.Subsystem) error {
	name := s.Name()
	if name == "" || strings.ContainsAny(name, "/ ") {
		return fmt.Errorf("fill: bad subsystem name %q", name)
	}
	if err := m.db.Migrate(ctx, name, s.Migrations()); err != nil {
		return err
	}
	tx, err := m.db.Begin(ctx)
	if err != nil {
		return err
	}
	dirs := []string{""}
	if sc, ok := s.(subsys.Scaffolder); ok {
		dirs = append(dirs, sc.Dirs()...)
	}
	for _, d := range dirs {
		if _, err := tx.MkdirAll(path.Join("/"+name, d)); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.subs[name]; dup {
		return fmt.Errorf("fill: subsystem %q registered twice", name)
	}
	m.subs[name] = s
	return nil
}

// Subsystem implements Manager.
func (m *LocalManager) Subsystem(name string) (subsys.Subsystem, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.subs[name]
	return s, ok
}

// Watch implements Manager.
func (m *LocalManager) Watch(fn func(UnitEvent)) {
	m.mu.Lock()
	m.watchers = append(m.watchers, fn)
	m.mu.Unlock()
}

// WatchOpens implements Manager.
func (m *LocalManager) WatchOpens(fn func(OpenEvent)) {
	m.mu.Lock()
	m.openers = append(m.openers, fn)
	m.mu.Unlock()
}

func (m *LocalManager) emitOpen(ev OpenEvent) {
	m.mu.Lock()
	ws := m.openers
	m.mu.Unlock()
	for _, w := range ws {
		w(ev)
	}
}

// SetReclaimer implements Manager.
func (m *LocalManager) SetReclaimer(r Reclaimer) { m.quota.setReclaimer(r) }

func (m *LocalManager) emit(ev UnitEvent) {
	m.mu.Lock()
	ws := slices.Clone(m.watchers)
	m.mu.Unlock()
	for _, w := range ws {
		w(ev)
	}
}

func flightKey(sub string, key subsys.Key) string { return sub + "\x00" + string(key) }

// Ensure implements Manager.
func (m *LocalManager) Ensure(ctx context.Context, sub string, key subsys.Key, wait bool) (meta.Unit, error) {
	if key == "" || strings.ContainsRune(string(key), '/') {
		return meta.Unit{}, fmt.Errorf("fill: bad unit key %q", key)
	}
	m.mu.Lock()
	s, ok := m.subs[sub]
	if !ok {
		m.mu.Unlock()
		return meta.Unit{}, fmt.Errorf("fill: unknown subsystem %q: %w", sub, fs.ErrNotExist)
	}
	fk := flightKey(sub, key)
	f := m.flights[fk]
	if f == nil {
		u, err := m.db.Unit(ctx, sub, string(key))
		if err == nil && (u.State == meta.UnitSpooled || u.State == meta.UnitResident) {
			m.mu.Unlock()
			return u, nil
		}
		if err != nil && !errors.Is(err, meta.ErrNotExist) {
			m.mu.Unlock()
			return meta.Unit{}, err
		}
		f = &flight{started: make(chan struct{}), done: make(chan struct{})}
		m.flights[fk] = f
		m.wg.Add(1)
		go m.fill(s, key, fk, f)
	}
	m.mu.Unlock()

	ready := f.done
	if !wait {
		ready = f.started
	}
	select {
	case <-ready:
	case <-f.done:
	case <-ctx.Done():
		return meta.Unit{}, ctx.Err()
	}
	select {
	case <-f.done:
		if f.err != nil {
			return meta.Unit{}, f.err
		}
		return m.db.Unit(ctx, sub, string(key))
	default:
		return f.unit, nil
	}
}

func (m *LocalManager) fill(s subsys.Subsystem, key subsys.Key, fk string, f *flight) {
	defer m.wg.Done()
	err := m.runFill(s, key, f)
	f.err = err
	// Close done before broadcasting, under the lock: waiters check done
	// under m.mu before they Wait, so a broadcast that precedes the close
	// would be lost and leave them waiting forever.
	m.mu.Lock()
	delete(m.flights, fk)
	f.start()
	close(f.done)
	m.changed.Broadcast()
	m.mu.Unlock()
}

func (m *LocalManager) runFill(s subsys.Subsystem, key subsys.Key, f *flight) (err error) {
	ctx := m.ctx
	sub := s.Name()
	info, err := s.Describe(ctx, key)
	if err != nil {
		return err
	}
	res, err := m.quota.Reserve(ctx, info.Size)
	if err != nil {
		return err
	}
	defer res.Release()

	// absent|evicted|new -> fetching, with a root directory.
	tx, err := m.db.Begin(ctx)
	if err != nil {
		return err
	}
	from := meta.UnitAbsent
	u, err := tx.Unit(sub, string(key))
	var id int64
	switch {
	case errors.Is(err, meta.ErrNotExist):
		id, err = tx.CreateUnit(sub, string(key))
	case err == nil:
		id, from = u.ID, u.State
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.SetUnitState(id, from, meta.UnitFetching); err != nil {
		tx.Rollback()
		return err
	}
	mount := path.Clean("/" + info.Mount)
	if info.Mount == "" {
		mount = path.Clean("/" + string(key))
	}
	if mount == "/" {
		tx.Rollback()
		return fmt.Errorf("fill: %s/%s: empty mount", sub, key)
	}
	rootParent, err := tx.MkdirAll("/" + sub + path.Dir(mount))
	if err != nil {
		tx.Rollback()
		return err
	}
	rootName := path.Base(mount)
	root := u.Root
	if root == 0 {
		root, err = tx.Mkdir(rootParent, rootName, 0o555, time.Now(), id)
		if err == nil {
			err = tx.SetUnitManifest(id, root, 0)
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	release := m.Acquire(id)
	defer release()
	f.unit, _ = m.db.UnitByID(ctx, id)
	m.emit(UnitEvent{Unit: f.unit, From: from, To: meta.UnitFetching})
	f.start()

	sink := &fillSink{m: m, ctx: ctx, unit: id, refault: from == meta.UnitEvicted, res: res.(*reservation),
		rootParent: rootParent, rootName: rootName, root: root, dirs: map[string]uint64{}}
	if rin, err := m.db.Get(ctx, root); err == nil && rin.Kind == meta.KindDir {
		sink.dirs["."] = root
	}
	err = s.Fetch(ctx, key, sink)
	if err == nil {
		err = sink.finish()
	}
	if err == nil && info.Digest != "" && sink.digest != info.Digest {
		err = fmt.Errorf("%w: %s/%s: want %s, got %s", ErrDigestMismatch, sub, key, info.Digest, sink.digest)
	}
	if err == nil {
		err = m.spool.Sync(id)
	}
	if err != nil {
		sink.failAll(err)
		m.failFill(id, from, err)
		return err
	}

	tx, err = m.db.Begin(ctx)
	if err != nil {
		return err
	}
	if err := tx.SetUnitManifest(id, sink.root, sink.logical); err == nil {
		err = tx.SetUnitState(id, meta.UnitFetching, meta.UnitSpooled)
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	u, _ = m.db.UnitByID(ctx, id)
	m.emit(UnitEvent{Unit: u, From: meta.UnitFetching, To: meta.UnitSpooled})
	select {
	case m.ingest <- id:
	case <-ctx.Done():
	}
	return nil
}

// failFill undoes a failed fill: a first fill leaves nothing behind; a
// refault keeps the manifest and goes back to evicted.
func (m *LocalManager) failFill(id int64, from meta.UnitState, cause error) {
	ctx := context.Background()
	m.spool.Remove(id)
	to := meta.UnitAbsent
	if from == meta.UnitEvicted {
		to = meta.UnitEvicted
	}
	tx, err := m.db.Begin(ctx)
	if err != nil {
		m.opts.Logf("fill: unit %d: rollback: %v", id, err)
		return
	}
	if to == meta.UnitAbsent {
		if err = tx.DeleteUnitTree(id); err == nil {
			err = tx.SetUnitManifest(id, 0, 0)
		}
	}
	if err == nil {
		err = tx.SetUnitState(id, meta.UnitFetching, to)
	}
	if err == nil {
		err = tx.Commit()
	} else {
		tx.Rollback()
	}
	if err != nil {
		m.opts.Logf("fill: unit %d: rollback: %v", id, err)
	}
	u, _ := m.db.UnitByID(ctx, id)
	m.emit(UnitEvent{Unit: u, From: meta.UnitFetching, To: to, Err: cause})
}

// Open implements Manager.
func (m *LocalManager) Open(ctx context.Context, in meta.Inode) (cas.File, error) {
	if in.Kind != meta.KindFile {
		return nil, fmt.Errorf("fill: inode %d is not a regular file", in.Ino)
	}
	release := m.Acquire(in.Unit)
	f, err := m.open(ctx, in)
	if err != nil {
		release()
		return nil, err
	}
	internal := IsInternal(ctx)
	m.emitOpen(OpenEvent{Inode: in, Internal: internal})
	return &pinnedFile{File: f, m: m, in: in, release: release, internal: internal}, nil
}

// OpenPath implements Manager.
func (m *LocalManager) OpenPath(ctx context.Context, sub string, key subsys.Key, rel string) (cas.File, meta.Inode, error) {
	u, err := m.Ensure(ctx, sub, key, false)
	if err != nil {
		return nil, meta.Inode{}, err
	}
	dir, err := m.db.PathOf(ctx, u.Root)
	if err != nil {
		return nil, meta.Inode{}, err
	}
	ino := u.Root
	var in meta.Inode
	for _, name := range strings.Split(strings.Trim(path.Clean("/"+rel), "/"), "/") {
		in, err = m.db.Lookup(ctx, ino, name)
		if errors.Is(err, meta.ErrNotExist) {
			var retry bool
			if retry, err = m.Miss(ctx, dir, name); err == nil && retry {
				in, err = m.db.Lookup(ctx, ino, name)
			} else if err == nil {
				err = meta.ErrNotExist
			}
		}
		if err != nil {
			return nil, meta.Inode{}, err
		}
		ino, dir = in.Ino, path.Join(dir, name)
	}
	f, err := m.Open(ctx, in)
	return f, in, err
}

func (m *LocalManager) open(ctx context.Context, in meta.Inode) (cas.File, error) {
	// Re-read first: the caller's inode may predate an ingest or an
	// eviction, and eviction may race with the pin taken by Open.
	if fresh, err := m.db.Get(ctx, in.Ino); err == nil {
		in = fresh
	}
	for attempt := 0; attempt < 4; attempt++ {
		if in.Content != "" {
			f, err := m.store.Open(ctx, in.Content)
			if err == nil {
				return f, nil
			}
			// A blob can vanish if eviction committed between our pin and
			// this open. If the inode no longer has that ref, treat it as a
			// refault and go round again; otherwise it is a real error.
			fresh, gerr := m.db.Get(ctx, in.Ino)
			if gerr != nil || fresh.Content == in.Content {
				return nil, err
			}
			in = fresh
			continue
		}
		if f, err := m.spool.Open(in.Ino); err == nil {
			return f, nil
		}
		if in.Size == 0 {
			return emptyFile{}, nil
		}
		// No bytes anywhere: the unit is fetching and has not reached this
		// file yet, or it was evicted. Fault it in and wait for the file.
		u, err := m.db.UnitByID(ctx, in.Unit)
		if err != nil {
			return nil, err
		}
		fk := flightKey(u.Subsystem, subsys.Key(u.Key))
		m.mu.Lock()
		f := m.flights[fk]
		m.mu.Unlock()
		if f == nil && (u.State == meta.UnitAbsent || u.State == meta.UnitEvicted) {
			if _, err := m.Ensure(ctx, u.Subsystem, subsys.Key(u.Key), false); err != nil {
				return nil, err
			}
			m.mu.Lock()
			f = m.flights[fk]
			m.mu.Unlock()
		}
		if f != nil {
			if err := m.waitChange(ctx, in.Ino, f); err != nil {
				return nil, err
			}
		}
		if in, err = m.db.Get(ctx, in.Ino); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("fill: inode %d: no data in the spool or the CAS", in.Ino)
}

// waitChange blocks until a spool file for ino exists or the flight ends.
func (m *LocalManager) waitChange(ctx context.Context, ino uint64, f *flight) error {
	stop := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.changed.Broadcast()
		m.mu.Unlock()
	})
	defer stop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-f.done:
			return nil
		default:
		}
		m.spool.mu.Lock()
		_, ok := m.spool.files[ino]
		m.spool.mu.Unlock()
		if ok {
			return nil
		}
		m.changed.Wait()
	}
}

type pinnedFile struct {
	cas.File
	m        *LocalManager
	in       meta.Inode
	read     atomic.Int64
	once     sync.Once
	release  func()
	internal bool
}

func (p *pinnedFile) ReadAt(b []byte, off int64) (int, error) {
	n, err := p.File.ReadAt(b, off)
	p.read.Add(int64(n))
	return n, err
}

func (p *pinnedFile) Close() error {
	err := p.File.Close()
	p.once.Do(func() {
		p.release()
		p.m.emitOpen(OpenEvent{Inode: p.in, Closed: true, BytesRead: p.read.Load(), Internal: p.internal})
	})
	return err
}

type emptyFile struct{}

func (emptyFile) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (emptyFile) Close() error                      { return nil }

// Miss is called by the namespace when a name is not found in directory
// dir (an absolute path). If dir is under a registered subsystem and the
// subsystem resolves the name to a unit, the unit is faulted in and Miss
// reports true so the caller retries the lookup.
func (m *LocalManager) Miss(ctx context.Context, dir, name string) (bool, error) {
	rest := strings.TrimPrefix(path.Join(dir, name), "/")
	sub, rel, _ := strings.Cut(rest, "/")
	s, ok := m.Subsystem(sub)
	if !ok || rel == "" {
		return false, nil
	}
	key, _, err := s.Resolve(ctx, rel)
	if errors.Is(err, subsys.ErrDir) {
		return true, m.mkdirs(ctx, "/"+rest)
	}
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if _, err := m.Ensure(ctx, sub, key, false); err != nil {
		return false, err
	}
	// The unit is filling (or done). Wait until the name appears or the
	// fill ends, so a lookup inside a unit being fetched does not fail
	// just because the fetch has not reached it yet.
	m.mu.Lock()
	f := m.flights[flightKey(sub, key)]
	m.mu.Unlock()
	if f == nil {
		return true, nil
	}
	full := "/" + rest
	stop := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.changed.Broadcast()
		m.mu.Unlock()
	})
	defer stop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		select {
		case <-f.done:
			return true, f.err
		default:
		}
		m.mu.Unlock()
		found := m.exists(ctx, full)
		m.mu.Lock()
		if found {
			return true, nil
		}
		m.changed.Wait()
	}
}

// mkdirs creates the scaffolding directories of absolute path p, for
// Resolve's ErrDir.
func (m *LocalManager) mkdirs(ctx context.Context, p string) error {
	tx, err := m.db.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.MkdirAll(p); err != nil {
		tx.Rollback()
		if errors.Is(err, meta.ErrExist) {
			return nil // a concurrent miss made it
		}
		return err
	}
	if err := tx.Commit(); err != nil && !strings.Contains(err.Error(), "UNIQUE") {
		return err
	}
	return nil
}

// exists reports whether an absolute path resolves in the namespace.
func (m *LocalManager) exists(ctx context.Context, p string) bool {
	ino := meta.RootIno
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		in, err := m.db.Lookup(ctx, ino, name)
		if err != nil {
			return false
		}
		ino = in.Ino
	}
	return true
}

// Wait blocks until no fills or ingests are pending, for tests and
// orderly shutdown.
func (m *LocalManager) Wait(ctx context.Context) error {
	for {
		busy := m.ingesting.Load()
		m.mu.Lock()
		n := len(m.flights)
		m.mu.Unlock()
		spooled, err := m.db.UnitsInState(ctx, meta.UnitSpooled)
		if err != nil {
			return err
		}
		if n == 0 && busy == 0 && len(spooled) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// ResumeIngest starts ingest after Options.HoldIngest.
func (m *LocalManager) ResumeIngest() {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.resume:
	default:
		close(m.resume)
	}
}

func (m *LocalManager) ingestLoop() {
	defer m.wg.Done()
	select {
	case <-m.resume:
	case <-m.ctx.Done():
		return
	}
	for {
		select {
		case <-m.ctx.Done():
			return
		case id := <-m.ingest:
			if err := m.ingestUnit(id); err != nil {
				if m.ctx.Err() != nil {
					return
				}
				m.opts.Logf("fill: ingest unit %d: %v (will retry)", id, err)
				time.AfterFunc(5*time.Second, func() {
					select {
					case m.ingest <- id:
					case <-m.ctx.Done():
					}
				})
			}
		}
	}
}

// ingestUnit chunks a spooled unit's files into the CAS, with up to
// Options.Workers files in flight, then records every content ref and moves
// the unit to resident in one transaction, and drops its spool. No write
// transaction is held across CAS puts.
func (m *LocalManager) ingestUnit(id int64) error {
	m.ingesting.Add(1)
	defer m.ingesting.Add(-1)
	ctx := m.ctx
	// Every blob put or found stays safe from a GC sweep until the content
	// refs that keep it are committed.
	sess := m.store.Session()
	defer sess.Close()
	u, err := m.db.UnitByID(ctx, id)
	if err != nil || u.State != meta.UnitSpooled {
		return err
	}
	files, err := m.db.UnitFiles(ctx, id)
	if err != nil {
		return err
	}
	type result struct {
		ino         uint64
		digest, ref string
		size        int64
	}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		results  []result
		firstErr error
	)
	for _, in := range files {
		if in.Content != "" || in.Size == 0 {
			continue
		}
		in := in
		m.sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-m.sem; wg.Done() }()
			digest, ref, err := m.ingestFile(ctx, sess, in)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("inode %d: %w", in.Ino, err)
				}
				return
			}
			results = append(results, result{in.Ino, digest, ref, in.Size})
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	// The bytes are in the CAS now: stop counting the spool copy so the
	// quota does not count the unit twice until the spool is removed.
	released := m.spool.Release(id)
	tx, err := m.db.Begin(ctx)
	if err != nil {
		m.spool.Unrelease(id, released)
		return err
	}
	for _, r := range results {
		if err = tx.RefContent(r.digest, r.ref, r.size); err == nil {
			err = tx.SetContent(r.ino, r.ref)
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = tx.SetUnitState(id, meta.UnitSpooled, meta.UnitResident)
	}
	if err == nil {
		err = tx.Commit()
	} else {
		tx.Rollback()
	}
	if err != nil {
		m.spool.Unrelease(id, released)
		return err
	}
	u, _ = m.db.UnitByID(ctx, id)
	if m.opts.Shadow != "" {
		if err := m.shadowUnit(ctx, u); err != nil {
			m.opts.Logf("fill: unit %d: shadow tree: %v", id, err)
		}
	}
	if err := m.spool.Remove(id); err != nil {
		m.opts.Logf("fill: unit %d: removing spool: %v", id, err)
	}
	m.emit(UnitEvent{Unit: u, From: meta.UnitSpooled, To: meta.UnitResident})
	return nil
}

// ingestFile stores one spool file in the CAS, skipping the chunker when
// its whole-file digest is already known.
func (m *LocalManager) ingestFile(ctx context.Context, sess *cas.Session, in meta.Inode) (digest, ref string, err error) {
	f, err := m.spool.Open(in.Ino)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, in.Size)); err != nil {
		return "", "", err
	}
	digest = "sha256-" + hex.EncodeToString(h.Sum(nil))
	ref, ok, err := m.db.ContentRef(ctx, digest)
	if err != nil {
		return "", "", err
	}
	if ok {
		// Hold fails if a sweep removed a blob since the ref was read;
		// put the bytes again then.
		if _, err := sess.Hold(ctx, ref); err == nil {
			return digest, ref, nil
		}
	}
	if ref, err = sess.Put(ctx, io.NewSectionReader(f, 0, in.Size)); err != nil {
		return "", "", err
	}
	return digest, ref, nil
}

// fillSink writes a fetch into the namespace and the spool.
type fillSink struct {
	m       *LocalManager
	ctx     context.Context
	unit    int64
	refault bool
	res     *reservation

	// The unit root's place in the namespace. A Fetch whose first entry is
	// File("") or Symlink("", ...) replaces the empty root directory.
	rootParent uint64
	rootName   string

	mu      sync.Mutex
	root    uint64
	dirs    map[string]uint64 // unit-relative dir path -> ino
	open    []*sinkFile
	logical int64
	digest  string
	started bool // an entry was added, so the root can no longer change
}

func clean(p string) (dir, name string, err error) {
	p = path.Clean("/" + p)
	if p == "/" {
		return "", "", errors.New("fill: empty path")
	}
	d := strings.TrimPrefix(path.Dir(p), "/")
	if d == "" {
		d = "."
	}
	return d, path.Base(p), nil
}

func (s *fillSink) parent(dir string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
	ino, ok := s.dirs[dir]
	if !ok {
		return 0, fmt.Errorf("fill: parent directory %q not created", dir)
	}
	return ino, nil
}

// do runs fn in its own transaction, so each entry is visible on return.
func (s *fillSink) do(fn func(tx *meta.Tx) error) error {
	tx, err := s.m.db.Begin(s.ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.m.mu.Lock()
	s.m.changed.Broadcast()
	s.m.mu.Unlock()
	return nil
}

// existing returns the entry a refault finds in the kept manifest.
func (s *fillSink) existing(tx *meta.Tx, parent uint64, name string) (meta.Inode, bool, error) {
	if !s.refault {
		return meta.Inode{}, false, nil
	}
	in, err := tx.Lookup(parent, name)
	if errors.Is(err, meta.ErrNotExist) {
		return in, false, nil
	}
	return in, err == nil, err
}

func (s *fillSink) Dir(p string, mode fs.FileMode, mtime time.Time) error {
	dir, name, err := clean(p)
	if err != nil {
		return err
	}
	parent, err := s.parent(dir)
	if err != nil {
		return err
	}
	var ino uint64
	err = s.do(func(tx *meta.Tx) error {
		old, ok, err := s.existing(tx, parent, name)
		if err != nil {
			return err
		}
		if ok && old.Kind == meta.KindDir {
			ino = old.Ino
			return nil
		}
		ino, err = tx.Mkdir(parent, name, mode, mtime, s.unit)
		return err
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.dirs[path.Join(dir, name)] = ino
	s.mu.Unlock()
	return nil
}

func (s *fillSink) File(p string, mode fs.FileMode, size int64, mtime time.Time) (io.WriteCloser, error) {
	parent, name, isRoot, err := s.place(p)
	if err != nil {
		return nil, err
	}
	var ino uint64
	err = s.do(func(tx *meta.Tx) error {
		if isRoot {
			if err := s.replaceRoot(tx, meta.KindFile); err != nil {
				return err
			}
		}
		old, ok, err := s.existing(tx, parent, name)
		if err != nil {
			return err
		}
		if ok && old.Kind == meta.KindFile {
			ino = old.Ino
			return tx.UpdateFile(ino, uint32(mode.Perm()), max(size, 0), mtime)
		}
		ino, err = tx.File(parent, name, mode, max(size, 0), mtime, "", s.unit)
		if err == nil && isRoot {
			err = s.setRoot(tx, ino)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	w, err := s.m.spool.Create(s.unit, ino)
	if err != nil {
		return nil, err
	}
	s.m.mu.Lock()
	s.m.changed.Broadcast()
	s.m.mu.Unlock()
	f := &sinkFile{s: s, ino: ino, want: size, mode: uint32(mode.Perm()), mtime: mtime, w: w}
	s.mu.Lock()
	s.open = append(s.open, f)
	s.mu.Unlock()
	return f, nil
}

func (s *fillSink) Symlink(p, target string, mtime time.Time) error {
	parent, name, isRoot, err := s.place(p)
	if err != nil {
		return err
	}
	return s.do(func(tx *meta.Tx) error {
		if isRoot {
			if err := s.replaceRoot(tx, meta.KindSymlink); err != nil {
				return err
			}
		}
		if _, ok, err := s.existing(tx, parent, name); ok || err != nil {
			return err
		}
		ino, err := tx.Symlink(parent, name, target, mtime, s.unit)
		if err == nil && isRoot {
			err = s.setRoot(tx, ino)
		}
		return err
	})
}

// place resolves p to its parent directory and name. The empty path is the
// unit root itself, allowed only as the first entry of a fill.
func (s *fillSink) place(p string) (parent uint64, name string, isRoot bool, err error) {
	if strings.Trim(p, "/.") == "" {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.started {
			return 0, "", false, errors.New("fill: a non-directory unit root must be the only entry")
		}
		s.started = true
		delete(s.dirs, ".")
		return s.rootParent, s.rootName, true, nil
	}
	dir, name, err := clean(p)
	if err != nil {
		return 0, "", false, err
	}
	parent, err = s.parent(dir)
	return parent, name, false, err
}

// replaceRoot drops the placeholder root directory, unless the root is
// already of kind k (a refault of a non-directory unit).
func (s *fillSink) replaceRoot(tx *meta.Tx, k meta.Kind) error {
	in, err := tx.Lookup(s.rootParent, s.rootName)
	if err != nil {
		return err
	}
	if in.Kind == k {
		return nil
	}
	return tx.ReplaceEmptyDir(s.rootParent, s.rootName)
}

func (s *fillSink) setRoot(tx *meta.Tx, ino uint64) error {
	if err := tx.SetUnitManifest(s.unit, ino, 0); err != nil {
		return err
	}
	s.mu.Lock()
	s.root = ino
	s.mu.Unlock()
	return nil
}

func (s *fillSink) Hardlink(p, target string) error {
	dir, name, err := clean(p)
	if err != nil {
		return err
	}
	tdir, tname, err := clean(target)
	if err != nil {
		return err
	}
	parent, err := s.parent(dir)
	if err != nil {
		return err
	}
	tparent, err := s.parent(tdir)
	if err != nil {
		return err
	}
	return s.do(func(tx *meta.Tx) error {
		if _, ok, err := s.existing(tx, parent, name); ok || err != nil {
			return err
		}
		t, err := tx.Lookup(tparent, tname)
		if err != nil {
			return fmt.Errorf("fill: hard link target %q: %w", target, err)
		}
		return tx.Link(parent, name, t.Ino)
	})
}

func (s *fillSink) Whiteout(p string) error {
	dir, name, err := clean(p)
	if err != nil {
		return err
	}
	parent, err := s.parent(dir)
	if err != nil {
		return err
	}
	return s.do(func(tx *meta.Tx) error {
		if _, ok, err := s.existing(tx, parent, name); ok || err != nil {
			return err
		}
		_, err := tx.Whiteout(parent, name, s.unit)
		return err
	})
}

func (s *fillSink) Opaque(p string) error {
	p = strings.Trim(path.Clean("/"+p), "/")
	if p == "" {
		p = "."
	}
	ino, err := s.parent(p)
	if err != nil {
		return err
	}
	return s.do(func(tx *meta.Tx) error { return tx.SetFlags(ino, meta.FlagOpaque) })
}

func (s *fillSink) Digest(d string) {
	s.mu.Lock()
	s.digest = d
	s.mu.Unlock()
}

// finish checks every file was closed.
func (s *fillSink) finish() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		if !f.closed {
			return fmt.Errorf("fill: file inode %d not closed by Fetch", f.ino)
		}
	}
	return nil
}

func (s *fillSink) failAll(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		f.w.Fail(err)
	}
}

type sinkFile struct {
	s      *fillSink
	ino    uint64
	want   int64
	mode   uint32
	mtime  time.Time
	w      SpoolFile
	n      int64
	closed bool
}

func (f *sinkFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, errors.New("fill: write after close")
	}
	if err := f.s.res.use(f.s.ctx, int64(len(p))); err != nil {
		return 0, err
	}
	n, err := f.w.Write(p)
	f.n += int64(n)
	return n, err
}

func (f *sinkFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if f.want >= 0 && f.n != f.want {
		err := fmt.Errorf("fill: inode %d: wrote %d bytes, expected %d", f.ino, f.n, f.want)
		f.w.Fail(err)
		return err
	}
	if f.want < 0 {
		if err := f.s.do(func(tx *meta.Tx) error { return tx.UpdateFile(f.ino, f.mode, f.n, f.mtime) }); err != nil {
			f.w.Fail(err)
			return err
		}
	}
	f.s.mu.Lock()
	f.s.logical += f.n
	f.s.mu.Unlock()
	return f.w.Finish()
}
