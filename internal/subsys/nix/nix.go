// Package nix is the Nix subsystem: read-through /nix/store paths filled
// from binary caches (cache.nixos.org by default), closures usable as H9
// roots or layers with no Nix installed, and a binary cache endpoint so
// Nix inside a guest can substitute from the cache.
//
// Namespace, under /nix/:
//
//	store/<hash>-<name>/...   one store path, unit key "<hash>-<name>"
//	closure/<hash>-<name>/    a closure as a root: nix/store/<member>/... for
//	                          every path in the closure, unit key
//	                          "closure:<hash>-<name>"
//
// A store path's NAR is fetched, decompressed and unpacked into the spool as
// it streams, so the tree is stored as files (which dedupe in the CAS) and
// never as a compressed archive. The NarHash is checked over the stream.
// NARs served to guests are rebuilt from the tree; NAR serialization is
// deterministic, so they hash to the same NarHash and upstream signatures
// stay valid.
package nix

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Name is the subsystem name and namespace root.
const Name = "nix"

// closurePrefix marks closure unit keys.
const closurePrefix = "closure:"

// rootEntry holds a store path whose NAR root is a file or symlink, inside
// the unit's root directory, until the fill path supports non-directory
// unit roots.
const rootEntry = ".nar-root"

// storeMtime is the mtime Nix gives everything in the store.
var storeMtime = time.Unix(1, 0)

// Options configures the subsystem.
type Options struct {
	// Substituters are binary cache URLs, tried in order.
	// Default: DefaultSubstituter.
	Substituters []string
	// Client fetches from substituters. Default: http.DefaultClient.
	Client *http.Client
	// OnEvict, if set, runs before a store path is evicted, for example
	// NixStoreDelete for a guest store that tracks the mounted paths. An
	// error keeps the path resident.
	OnEvict func(ctx context.Context, p StorePath) error
	// NegativeTTL is how long a path no substituter has is remembered as
	// missing. Default 10 minutes.
	NegativeTTL time.Duration
	// Indexer, if set, gets IndexRoot("/nix/closure/<path>") each time a
	// closure's manifest completes, so H9 sees its /.h9index.
	Indexer subsys.RootIndexer
	// Logf receives background errors. Default: discarded.
	Logf func(string, ...any)
	// Now is the clock, for tests.
	Now func() time.Time
}

// Subsystem implements subsys.Subsystem for Nix.
type Subsystem struct {
	db   *meta.DB
	opts Options
	subs []*substituter

	mu      sync.Mutex
	missing map[string]time.Time // hash -> expiry
	flights map[string]*niFlight
	mgr     fill.Manager
}

type niFlight struct {
	done chan struct{}
	ni   *NarInfo
	err  error
}

var (
	_ subsys.Subsystem       = (*Subsystem)(nil)
	_ subsys.Evicter         = (*Subsystem)(nil)
	_ subsys.EvictionOrderer = (*Subsystem)(nil)
)

// New returns the Nix subsystem using db for its tables.
func New(db *meta.DB, opts Options) (*Subsystem, error) {
	if len(opts.Substituters) == 0 {
		opts.Substituters = []string{DefaultSubstituter}
	}
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
	if opts.NegativeTTL == 0 {
		opts.NegativeTTL = 10 * time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Subsystem{db: db, opts: opts, missing: map[string]time.Time{}, flights: map[string]*niFlight{}}
	for _, raw := range opts.Substituters {
		u, err := url.Parse(strings.TrimSuffix(raw, "/") + "/")
		if err != nil {
			return nil, fmt.Errorf("nix: substituter %q: %w", raw, err)
		}
		s.subs = append(s.subs, &substituter{base: u, client: opts.Client})
	}
	return s, nil
}

// Attach gives the subsystem the fill manager it is registered with, so
// closures and the binary cache read member paths from the cache (faulting
// them in as units) instead of straight from upstream. It also creates the
// empty /nix/store and /nix/closure directories, so a guest can walk into
// them (and a miss inside them can fault) before anything is cached. Call
// it after Register.
func (s *Subsystem) Attach(ctx context.Context, m fill.Manager) error {
	s.mu.Lock()
	s.mgr = m
	s.mu.Unlock()
	if s.opts.Indexer != nil {
		m.Watch(s.indexClosure)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, d := range []string{"/nix/store", "/nix/closure"} {
		if _, err := tx.MkdirAll(d); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// indexClosure indexes a closure root once its manifest is complete. Its
// members were all filled (spooled or resident) before it, by Fetch.
func (s *Subsystem) indexClosure(ev fill.UnitEvent) {
	sp, ok := strings.CutPrefix(ev.Unit.Key, closurePrefix)
	if ev.Unit.Subsystem != Name || !ok || ev.To != meta.UnitSpooled {
		return
	}
	go func() {
		if err := s.opts.Indexer.IndexRoot(context.Background(), "/nix/closure/"+sp); err != nil && s.opts.Logf != nil {
			s.opts.Logf("nix: indexing closure %s: %v", sp, err)
		}
	}()
}

func (s *Subsystem) manager() fill.Manager {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mgr
}

func (s *Subsystem) Name() string { return Name }

func (s *Subsystem) Migrations() []meta.Migration {
	return []meta.Migration{
		{Version: 1, SQL: `
CREATE TABLE nix_narinfo (
	hash        TEXT    PRIMARY KEY,
	path        TEXT    NOT NULL,
	substituter TEXT    NOT NULL,
	narinfo     TEXT    NOT NULL,
	fetched     INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE nix_ref (
	referrer  TEXT NOT NULL,
	reference TEXT NOT NULL,
	PRIMARY KEY (referrer, reference)
) WITHOUT ROWID;
CREATE INDEX nix_ref_reference ON nix_ref (reference);`},
	}
}

// Resolve implements subsys.Subsystem.
func (s *Subsystem) Resolve(ctx context.Context, p string) (subsys.Key, string, error) {
	top, rest, _ := strings.Cut(p, "/")
	base, rel, _ := strings.Cut(rest, "/")
	sp, err := ParseStorePath(base)
	if err != nil {
		return "", "", fs.ErrNotExist
	}
	switch top {
	case "store":
		return subsys.Key(sp), rel, nil
	case "closure":
		return subsys.Key(closurePrefix + string(sp)), rel, nil
	}
	return "", "", fs.ErrNotExist
}

func parseKey(key subsys.Key) (sp StorePath, closure bool, err error) {
	k := string(key)
	if c, ok := strings.CutPrefix(k, closurePrefix); ok {
		k, closure = c, true
	}
	sp, err = ParseStorePath(k)
	if err != nil {
		return "", false, fmt.Errorf("nix: bad unit key %q: %w", key, fs.ErrNotExist)
	}
	return sp, closure, nil
}

// Describe implements subsys.Subsystem.
func (s *Subsystem) Describe(ctx context.Context, key subsys.Key) (subsys.Info, error) {
	sp, closure, err := parseKey(key)
	if err != nil {
		return subsys.Info{}, err
	}
	name, version := sp.Package()
	if closure {
		members, err := s.Closure(ctx, sp)
		if err != nil {
			return subsys.Info{}, err
		}
		info := subsys.Info{Mount: "closure/" + string(sp), Name: name, Version: version}
		var refs []StorePath
		for _, m := range members {
			ni, err := s.NarInfo(ctx, m)
			if err != nil {
				return subsys.Info{}, err
			}
			info.Size += ni.NarSize
			info.Deps = append(info.Deps, subsys.Key(m))
			refs = append(refs, m)
		}
		if err := s.recordRefs(ctx, string(key), refs); err != nil {
			return subsys.Info{}, err
		}
		return info, nil
	}
	ni, err := s.NarInfo(ctx, sp)
	if err != nil {
		return subsys.Info{}, err
	}
	info := subsys.Info{
		Size:    ni.NarSize,
		Digest:  ni.NarHash,
		Mount:   "store/" + string(sp),
		Name:    name,
		Version: version,
	}
	for _, d := range ni.Deps() {
		info.Deps = append(info.Deps, subsys.Key(d))
	}
	return info, nil
}

// Fetch implements subsys.Subsystem.
func (s *Subsystem) Fetch(ctx context.Context, key subsys.Key, sink subsys.Sink) error {
	sp, closure, err := parseKey(key)
	if err != nil {
		return err
	}
	if !closure {
		ni, err := s.NarInfo(ctx, sp)
		if err != nil {
			return err
		}
		digest, err := s.unpack(ctx, ni, sink, "", s.upstreamNAR)
		if err != nil {
			return err
		}
		sink.Digest(digest)
		return nil
	}
	members, err := s.Closure(ctx, sp)
	if err != nil {
		return err
	}
	if err := sink.Dir("nix", 0o555, storeMtime); err != nil {
		return err
	}
	if err := sink.Dir("nix/store", 0o555, storeMtime); err != nil {
		return err
	}
	open := s.upstreamNAR
	if s.manager() != nil {
		open = s.localNAR
	}
	for _, m := range members {
		ni, err := s.NarInfo(ctx, m)
		if err != nil {
			return err
		}
		digest, err := s.unpack(ctx, ni, sink, "nix/store/"+string(m), open)
		if err != nil {
			return fmt.Errorf("nix: closure %s: %s: %w", sp, m, err)
		}
		if digest != ni.NarHash {
			return fmt.Errorf("%w: %s: want %s, got %s", fill.ErrDigestMismatch, m, ni.NarHash, digest)
		}
	}
	return nil
}

type narOpener func(ctx context.Context, ni *NarInfo) (io.ReadCloser, error)

// unpack streams ni's NAR from open into sink under prefix, and returns the
// NarHash computed over the stream.
func (s *Subsystem) unpack(ctx context.Context, ni *NarInfo, sink subsys.Sink, prefix string, open narOpener) (string, error) {
	r, err := open(ctx, ni)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	cr := &countReader{r: io.TeeReader(r, h)}
	if err := ReadNAR(cr, &sinkHandler{sink: sink, prefix: prefix}); err != nil {
		return "", err
	}
	// Anything after the archive is not part of it.
	if n, _ := io.Copy(io.Discard, io.LimitReader(r, 1)); n != 0 {
		return "", fmt.Errorf("nix: %s: trailing bytes after NAR", ni.StorePath)
	}
	if cr.n != ni.NarSize {
		return "", fmt.Errorf("%w: %s: NAR is %d bytes, narinfo says %d", fill.ErrDigestMismatch, ni.StorePath, cr.n, ni.NarSize)
	}
	return FormatHash(h.Sum(nil)), nil
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// sinkHandler writes NAR entries into a fill sink under prefix.
type sinkHandler struct {
	sink   subsys.Sink
	prefix string
}

func (h *sinkHandler) path(p string) string {
	if p == "" && h.prefix == "" {
		return rootEntry
	}
	return path.Join(h.prefix, p)
}

func (h *sinkHandler) Dir(p string) error {
	if p == "" && h.prefix == "" {
		return nil // the unit root, made by the fill path
	}
	return h.sink.Dir(h.path(p), 0o555, storeMtime)
}

func (h *sinkHandler) File(p string, exec bool, size int64, r io.Reader) error {
	mode := fs.FileMode(0o444)
	if exec {
		mode = 0o555
	}
	w, err := h.sink.File(h.path(p), mode, size, storeMtime)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (h *sinkHandler) Symlink(p, target string) error {
	return h.sink.Symlink(h.path(p), target, storeMtime)
}

func (s *Subsystem) upstreamNAR(ctx context.Context, ni *NarInfo) (io.ReadCloser, error) {
	sub, err := s.substituterFor(ctx, ni.StorePath)
	if err != nil {
		return nil, err
	}
	return sub.nar(ctx, ni)
}

// localNAR serializes a store path from its unit in the cache, faulting it
// in first if needed.
func (s *Subsystem) localNAR(ctx context.Context, ni *NarInfo) (io.ReadCloser, error) {
	m := s.manager()
	// The closure reads its members' files for itself, not for a client.
	ctx = fill.Internal(ctx)
	u, err := m.Ensure(ctx, Name, subsys.Key(ni.StorePath), true)
	if err != nil {
		return nil, err
	}
	release := m.Acquire(u.ID)
	pr, pw := io.Pipe()
	go func() {
		defer release()
		_, err := WriteNAR(pw, &unitTree{ctx: ctx, db: s.db, mgr: m, root: u.Root})
		pw.CloseWithError(err)
	}()
	return pr, nil
}

// NAR writes a store path's NAR, rebuilt from the cache, to w. It faults
// the path in if needed and needs Attach.
func (s *Subsystem) NAR(ctx context.Context, sp StorePath, w io.Writer) (int64, error) {
	m := s.manager()
	if m == nil {
		return 0, errors.New("nix: NAR needs a fill manager (Attach)")
	}
	u, err := m.Ensure(ctx, Name, subsys.Key(sp), true)
	if err != nil {
		return 0, err
	}
	release := m.Acquire(u.ID)
	defer release()
	return WriteNAR(w, &unitTree{ctx: ctx, db: s.db, mgr: m, root: u.Root})
}

// unitTree is a unit's manifest as a NAR Tree, with bytes from the fill
// manager (spool or CAS).
type unitTree struct {
	ctx  context.Context
	db   *meta.DB
	mgr  fill.Manager
	root uint64
}

func nodeOf(in meta.Inode) (Node, error) {
	switch in.Kind {
	case meta.KindDir:
		return Node{Kind: KindDir, ID: in}, nil
	case meta.KindFile:
		return Node{Kind: KindFile, Executable: in.Mode&0o111 != 0, Size: in.Size, ID: in}, nil
	case meta.KindSymlink:
		return Node{Kind: KindSymlink, Target: in.Target, ID: in}, nil
	}
	return Node{}, fmt.Errorf("nix: inode %d: %v has no NAR form", in.Ino, in.Kind)
}

func (t *unitTree) Root() (Node, error) {
	in, err := t.db.Get(t.ctx, t.root)
	if err != nil {
		return Node{}, err
	}
	if in.Kind == meta.KindDir {
		des, err := t.db.ReadDir(t.ctx, in.Ino, "", 2)
		if err != nil {
			return Node{}, err
		}
		if len(des) == 1 && des[0].Name == rootEntry {
			in = des[0].Inode
		}
	}
	return nodeOf(in)
}

func (t *unitTree) Children(dir Node) ([]string, []Node, error) {
	in := dir.ID.(meta.Inode)
	var names []string
	var nodes []Node
	after := ""
	for {
		des, err := t.db.ReadDir(t.ctx, in.Ino, after, 1024)
		if err != nil {
			return nil, nil, err
		}
		for _, de := range des {
			nd, err := nodeOf(de.Inode)
			if err != nil {
				return nil, nil, err
			}
			names = append(names, de.Name)
			nodes = append(nodes, nd)
			after = de.Name
		}
		if len(des) < 1024 {
			return names, nodes, nil
		}
	}
}

func (t *unitTree) Open(file Node) (io.ReadCloser, error) {
	f, err := t.mgr.Open(t.ctx, file.ID.(meta.Inode))
	if err != nil {
		return nil, err
	}
	return readCloser{io.NewSectionReader(f, 0, file.Size), f.Close}, nil
}

// NarInfo returns a store path's narinfo, from the database or upstream.
// It returns an error wrapping fs.ErrNotExist when no substituter has it.
func (s *Subsystem) NarInfo(ctx context.Context, sp StorePath) (*NarInfo, error) {
	ni, err := s.NarInfoByHash(ctx, sp.Hash())
	if err != nil {
		return nil, err
	}
	if ni.StorePath != sp {
		return nil, fmt.Errorf("nix: %s: hash belongs to %s: %w", sp, ni.StorePath, fs.ErrNotExist)
	}
	return ni, nil
}

// NarInfoByHash is NarInfo by the 32-character hash part alone, which is
// how Nix asks a binary cache.
func (s *Subsystem) NarInfoByHash(ctx context.Context, hash string) (*NarInfo, error) {
	if ni, _, err := s.storedNarInfo(ctx, hash); err == nil {
		return ni, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	s.mu.Lock()
	if exp, ok := s.missing[hash]; ok {
		if s.opts.Now().Before(exp) {
			s.mu.Unlock()
			return nil, fmt.Errorf("nix: %s: %w", hash, fs.ErrNotExist)
		}
		delete(s.missing, hash)
	}
	f := s.flights[hash]
	if f == nil {
		f = &niFlight{done: make(chan struct{})}
		s.flights[hash] = f
		go func() {
			f.ni, f.err = s.fetchNarInfo(context.WithoutCancel(ctx), hash)
			s.mu.Lock()
			delete(s.flights, hash)
			if errors.Is(f.err, fs.ErrNotExist) {
				s.missing[hash] = s.opts.Now().Add(s.opts.NegativeTTL)
			}
			s.mu.Unlock()
			close(f.done)
		}()
	}
	s.mu.Unlock()
	select {
	case <-f.done:
		return f.ni, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Subsystem) storedNarInfo(ctx context.Context, hash string) (*NarInfo, string, error) {
	var text, sub string
	err := s.db.SQL().QueryRowContext(ctx, `SELECT narinfo, substituter FROM nix_narinfo WHERE hash = ?`, hash).Scan(&text, &sub)
	if err != nil {
		return nil, "", err
	}
	ni, err := ParseNarInfo(text)
	return ni, sub, err
}

func (s *Subsystem) fetchNarInfo(ctx context.Context, hash string) (*NarInfo, error) {
	var errs []error
	for _, sub := range s.subs {
		ni, text, err := sub.narInfo(ctx, hash)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := s.storeNarInfo(ctx, ni, text, sub.base.String()); err != nil {
			return nil, err
		}
		return ni, nil
	}
	allMissing := true
	for _, e := range errs {
		allMissing = allMissing && errors.Is(e, fs.ErrNotExist)
	}
	if allMissing {
		return nil, fmt.Errorf("nix: no substituter has %s: %w", hash, fs.ErrNotExist)
	}
	return nil, errors.Join(errs...)
}

func (s *Subsystem) storeNarInfo(ctx context.Context, ni *NarInfo, text, sub string) error {
	tx, err := s.db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO nix_narinfo (hash, path, substituter, narinfo, fetched) VALUES (?, ?, ?, ?, ?)`,
		ni.StorePath.Hash(), string(ni.StorePath), sub, text, s.opts.Now().UnixNano()); err != nil {
		return err
	}
	for _, r := range ni.Deps() {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO nix_ref (referrer, reference) VALUES (?, ?)`, string(ni.StorePath), string(r)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Subsystem) recordRefs(ctx context.Context, referrer string, refs []StorePath) error {
	tx, err := s.db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, r := range refs {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO nix_ref (referrer, reference) VALUES (?, ?)`, referrer, string(r)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// substituterFor returns the substituter a path's narinfo came from.
func (s *Subsystem) substituterFor(ctx context.Context, sp StorePath) (*substituter, error) {
	_, base, err := s.storedNarInfo(ctx, sp.Hash())
	if err != nil {
		return nil, err
	}
	for _, sub := range s.subs {
		if sub.base.String() == base {
			return sub, nil
		}
	}
	// The substituter list changed since; fetch the narinfo afresh.
	if _, err := s.db.SQL().ExecContext(ctx, `DELETE FROM nix_narinfo WHERE hash = ?`, sp.Hash()); err != nil {
		return nil, err
	}
	if _, err := s.NarInfo(ctx, sp); err != nil {
		return nil, err
	}
	return s.substituterFor(ctx, sp)
}

// Closure returns every store path reachable from sp through narinfo
// references, sp included, in name order. It needs no Nix: only narinfos.
func (s *Subsystem) Closure(ctx context.Context, sp StorePath) ([]StorePath, error) {
	seen := map[StorePath]bool{sp: true}
	frontier := []StorePath{sp}
	for len(frontier) > 0 {
		type result struct {
			ni  *NarInfo
			err error
		}
		results := make([]result, len(frontier))
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16)
		for i, p := range frontier {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer func() { <-sem; wg.Done() }()
				ni, err := s.NarInfo(ctx, p)
				results[i] = result{ni, err}
			}()
		}
		wg.Wait()
		var next []StorePath
		for i, r := range results {
			if r.err != nil {
				return nil, fmt.Errorf("nix: closure of %s: %s: %w", sp, frontier[i], r.err)
			}
			for _, d := range r.ni.Deps() {
				if !seen[d] {
					seen[d] = true
					next = append(next, d)
				}
			}
		}
		frontier = next
	}
	out := make([]StorePath, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// DerivedUnit implements usage.Derived: a closure is built from its member
// units, so filling it is not an upstream fetch.
func (s *Subsystem) DerivedUnit(key subsys.Key) bool {
	return strings.HasPrefix(string(key), closurePrefix)
}

// OnEvict implements subsys.Evicter.
func (s *Subsystem) OnEvict(ctx context.Context, key subsys.Key) error {
	sp, closure, err := parseKey(key)
	if err != nil || closure || s.opts.OnEvict == nil {
		return nil
	}
	return s.opts.OnEvict(ctx, sp)
}

// EvictionOrder implements subsys.EvictionOrderer: Nix evicts from the
// leaves inward. A store path is a candidate only when nothing that is
// still live (fetching, spooled or resident) refers to it, whether another
// store path or a closure. Candidates keep the caller's (LRU) order.
func (s *Subsystem) EvictionOrder(ctx context.Context, units []meta.Unit) ([]meta.Unit, error) {
	rows, err := s.db.SQL().QueryContext(ctx, `SELECT r.reference FROM nix_ref r
		JOIN unit u ON u.subsystem = ? AND u.key = r.referrer
		WHERE u.state IN (?, ?, ?) AND r.referrer != r.reference`,
		Name, meta.UnitFetching, meta.UnitSpooled, meta.UnitResident)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	referenced := map[string]bool{}
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		referenced[ref] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Units being evicted together in this batch still count as live
	// referrers; the next round picks up what they referenced.
	var out []meta.Unit
	for _, u := range units {
		if u.Subsystem == Name && !referenced[u.Key] {
			out = append(out, u)
		}
	}
	return out, nil
}
