package fill

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// fakeSub serves units from memory. A unit is a map of path -> contents;
// paths ending in "/" are directories, "->x" values are symlinks.
type fakeSub struct {
	units   map[subsys.Key]map[string]string
	order   map[subsys.Key][]string
	digest  map[subsys.Key]string
	fetches atomic.Int32
	gate    chan struct{} // if set, Fetch pauses mid-file until closed
	fail    error         // if set, Fetch fails after writing
}

func (f *fakeSub) Name() string { return "fake" }
func (f *fakeSub) Migrations() []meta.Migration {
	return []meta.Migration{{Version: 1, SQL: `CREATE TABLE fake_x (a)`}}
}
func (f *fakeSub) Resolve(_ context.Context, p string) (subsys.Key, string, error) {
	k, rest, _ := strings.Cut(p, "/")
	if _, ok := f.units[subsys.Key(k)]; !ok {
		return "", "", fs.ErrNotExist
	}
	return subsys.Key(k), rest, nil
}
func (f *fakeSub) Describe(_ context.Context, k subsys.Key) (subsys.Info, error) {
	var n int64
	for _, v := range f.units[k] {
		if !strings.HasPrefix(v, "->") {
			n += int64(len(v))
		}
	}
	return subsys.Info{Size: n, Digest: f.digest[k]}, nil
}
func (f *fakeSub) Fetch(ctx context.Context, k subsys.Key, s subsys.Sink) error {
	f.fetches.Add(1)
	now := time.Unix(1_700_000_000, 0)
	for _, p := range f.order[k] {
		v := f.units[k][p]
		switch {
		case strings.HasSuffix(p, "/"):
			if err := s.Dir(strings.TrimSuffix(p, "/"), 0o755, now); err != nil {
				return err
			}
		case strings.HasPrefix(v, "->"):
			if err := s.Symlink(p, v[2:], now); err != nil {
				return err
			}
		default:
			w, err := s.File(p, 0o644, int64(len(v)), now)
			if err != nil {
				return err
			}
			half := len(v) / 2
			if _, err := io.WriteString(w, v[:half]); err != nil {
				return err
			}
			if f.gate != nil {
				select {
				case <-f.gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if _, err := io.WriteString(w, v[half:]); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
		}
	}
	s.Digest("d-" + string(k))
	return f.fail
}

func (f *fakeSub) add(k subsys.Key, kv ...string) {
	if f.units == nil {
		f.units, f.order = map[subsys.Key]map[string]string{}, map[subsys.Key][]string{}
	}
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
		f.order[k] = append(f.order[k], kv[i])
	}
	f.units[k] = m
}

type env struct {
	dir   string
	db    *meta.DB
	store *cas.Store
	spool *DiskSpool
	quota *LocalQuota
	m     *LocalManager
}

func newEnv(t *testing.T, dir string, max int64, opts Options) *env {
	t.Helper()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := cas.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := OpenDiskSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	q := &LocalQuota{Max: max, CAS: func() int64 { u, _ := store.Usage(); return u }, Spool: sp.Bytes}
	opts.Logf = t.Logf
	m, err := NewManager(db, store, sp, q, opts)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{dir: dir, db: db, store: store, spool: sp, quota: q, m: m}
	t.Cleanup(func() { m.Close(); db.Close() })
	return e
}

func (e *env) read(t *testing.T, p string) string {
	t.Helper()
	ctx := context.Background()
	ino := meta.RootIno
	var in meta.Inode
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		var err error
		in, err = e.db.Lookup(ctx, ino, name)
		if err != nil {
			t.Fatalf("lookup %s in %s: %v", name, p, err)
		}
		ino = in.Ino
	}
	f, err := e.m.Open(ctx, in)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.NewSectionReader(f, 0, in.Size))
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(b)
}

func TestFillIngest(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{})
	s := &fakeSub{}
	big := string(randBytes(300 << 10))
	s.add("u1", "bin/", "", "bin/tool", big, "bin/alias", "->tool", "etc/", "", "etc/conf", "x=1\n")
	s.digest = map[subsys.Key]string{"u1": "d-u1"}
	if err := e.m.Register(ctx, s); err != nil {
		t.Fatal(err)
	}
	var events []string
	var mu sync.Mutex
	e.m.Watch(func(ev UnitEvent) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%v>%v", ev.From, ev.To))
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.m.Ensure(ctx, "fake", "u1", true); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := s.fetches.Load(); n != 1 {
		t.Fatalf("%d fetches for 8 concurrent Ensures", n)
	}
	if got := e.read(t, "/fake/u1/bin/tool"); got != big {
		t.Fatal("tool bytes differ (from spool or CAS)")
	}
	if err := e.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	u, _ := e.db.Unit(ctx, "fake", "u1")
	if u.State != meta.UnitResident || u.LogicalBytes != int64(len(big)+4) {
		t.Fatalf("unit after ingest: %+v", u)
	}
	if e.spool.Bytes() != 0 {
		t.Errorf("spool holds %d bytes after ingest", e.spool.Bytes())
	}
	if got := e.read(t, "/fake/u1/etc/conf"); got != "x=1\n" {
		t.Fatalf("conf from CAS = %q", got)
	}
	bin, _ := e.db.Lookup(ctx, u.Root, "bin")
	if l, err := e.db.Lookup(ctx, bin.Ino, "alias"); err != nil || l.Target != "tool" {
		t.Fatalf("symlink: %+v, %v", l, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(events) != "[absent>fetching fetching>spooled spooled>resident]" {
		t.Errorf("events = %v", events)
	}
	if _, err := e.m.Ensure(ctx, "fake", "u1", true); err != nil || s.fetches.Load() != 1 {
		t.Errorf("resident unit refetched: %v", err)
	}
}

// TestServeWhileFilling reads a file whose fetch is paused halfway: the
// first half is readable at once, and a read past it blocks until the
// fetch continues.
func TestServeWhileFilling(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{HoldIngest: true})
	s := &fakeSub{gate: make(chan struct{})}
	s.add("u", "f", "0123456789")
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "u", false); err != nil {
		t.Fatal(err)
	}
	var in meta.Inode
	deadline := time.Now().Add(5 * time.Second)
	for {
		u, _ := e.db.Unit(ctx, "fake", "u")
		var err error
		if in, err = e.db.Lookup(ctx, u.Root, "f"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("file never appeared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	f, err := e.m.Open(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 5)
	if n, err := f.ReadAt(buf, 0); n != 5 || err != nil || string(buf) != "01234" {
		t.Fatalf("first half: %d %v %q", n, err, buf)
	}
	got := make(chan string, 1)
	go func() {
		b := make([]byte, 10)
		n, _ := f.ReadAt(b, 0)
		got <- string(b[:n])
	}()
	select {
	case v := <-got:
		t.Fatalf("read past the written bytes returned early: %q", v)
	case <-time.After(50 * time.Millisecond):
	}
	close(s.gate)
	if v := <-got; v != "0123456789" {
		t.Fatalf("full read = %q", v)
	}
}

func TestFailedFillRollsBack(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{})
	s := &fakeSub{fail: errors.New("upstream reset")}
	s.add("u", "a", "aaaa")
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "u", true); err == nil {
		t.Fatal("failed fetch succeeded")
	}
	u, err := e.db.Unit(ctx, "fake", "u")
	if err != nil || u.State != meta.UnitAbsent || u.Root != 0 {
		t.Fatalf("unit after failure: %+v, %v", u, err)
	}
	dir, _ := e.db.Lookup(ctx, meta.RootIno, "fake")
	if _, err := e.db.Lookup(ctx, dir.Ino, "u"); !errors.Is(err, meta.ErrNotExist) {
		t.Fatalf("partial tree left behind: %v", err)
	}
	if e.spool.Bytes() != 0 || e.quota.Usage().Reserved != 0 {
		t.Fatalf("leaked spool %d / reserved %d", e.spool.Bytes(), e.quota.Usage().Reserved)
	}
	s.fail = nil
	if _, err := e.m.Ensure(ctx, "fake", "u", true); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
}

func TestDigestMismatch(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{})
	s := &fakeSub{digest: map[subsys.Key]string{"u": "something-else"}}
	s.add("u", "a", "aaaa")
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "u", true); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("got %v, want ErrDigestMismatch", err)
	}
}

// TestRefault evicts a resident unit the way phase 5 will (content refs
// cleared, manifest kept, state evicted) and reads a file again.
func TestRefault(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{})
	s := &fakeSub{}
	s.add("u", "d/", "", "d/f", "hello")
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "u", true); err != nil {
		t.Fatal(err)
	}
	e.m.Wait(ctx)
	u, _ := e.db.Unit(ctx, "fake", "u")
	d, _ := e.db.Lookup(ctx, u.Root, "d")
	before, _ := e.db.Lookup(ctx, d.Ino, "f")

	tx, _ := e.db.Begin(ctx)
	files, _ := e.db.UnitFiles(ctx, u.ID)
	for _, f := range files {
		tx.SetContent(f.Ino, "")
	}
	tx.SetUnitState(u.ID, meta.UnitResident, meta.UnitEvicted)
	tx.Commit()

	if got := e.read(t, "/fake/u/d/f"); got != "hello" {
		t.Fatalf("after refault = %q", got)
	}
	after, _ := e.db.Lookup(ctx, d.Ino, "f")
	if after.Ino != before.Ino {
		t.Errorf("refault changed the inode: %d -> %d", before.Ino, after.Ino)
	}
	if s.fetches.Load() != 2 {
		t.Errorf("fetches = %d, want 2", s.fetches.Load())
	}
}

// TestRecovery restarts with a unit spooled but not ingested.
func TestRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := &fakeSub{}
	s.add("u", "f", "persist me")
	e := newEnv(t, dir, 0, Options{HoldIngest: true})
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "u", true); err != nil {
		t.Fatal(err)
	}
	e.m.Close()
	e.db.Close()

	e2 := newEnv(t, dir, 0, Options{})
	e2.m.Register(ctx, s)
	if got := e2.read(t, "/fake/u/f"); got != "persist me" {
		t.Fatalf("read after restart = %q", got)
	}
	if err := e2.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	u, _ := e2.db.Unit(ctx, "fake", "u")
	if u.State != meta.UnitResident || s.fetches.Load() != 1 {
		t.Fatalf("unit %+v after recovery, %d fetches", u, s.fetches.Load())
	}
}

type reclaimFunc func(context.Context, int64) (int64, error)

func (f reclaimFunc) Reclaim(ctx context.Context, n int64) (int64, error) { return f(ctx, n) }

func TestQuota(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 1000, Options{})
	s := &fakeSub{}
	s.add("big", "f", string(make([]byte, 2000)))
	s.add("small", "f", "tiny")
	e.m.Register(ctx, s)
	if _, err := e.m.Ensure(ctx, "fake", "big", true); !errors.Is(err, cas.ErrFull) {
		t.Fatalf("over quota: got %v", err)
	}
	var asked int64
	e.m.SetReclaimer(reclaimFunc(func(_ context.Context, n int64) (int64, error) { asked = n; return 0, nil }))
	if _, err := e.m.Ensure(ctx, "fake", "big", true); !errors.Is(err, cas.ErrFull) || asked == 0 {
		t.Fatalf("reclaimer not asked (%d) or quota not enforced (%v)", asked, err)
	}
	if _, err := e.m.Ensure(ctx, "fake", "small", true); err != nil {
		t.Fatal(err)
	}
	if r := e.quota.Usage().Reserved; r != 0 {
		t.Errorf("reserved = %d after fills", r)
	}
}

func TestMissFaultsIn(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, t.TempDir(), 0, Options{})
	s := &fakeSub{}
	s.add("pkg", "f", "x")
	e.m.Register(ctx, s)
	if ok, err := e.m.Miss(ctx, "/fake", "nope"); ok || err != nil {
		t.Fatalf("unknown name: %v %v", ok, err)
	}
	if ok, err := e.m.Miss(ctx, "/other", "pkg"); ok || err != nil {
		t.Fatalf("unknown subsystem: %v %v", ok, err)
	}
	ok, err := e.m.Miss(ctx, "/fake", "pkg")
	if !ok || err != nil {
		t.Fatalf("Miss = %v, %v", ok, err)
	}
	// A lookup inside the unit waits for the entry if the fetch has not
	// reached it yet.
	if ok, err := e.m.Miss(ctx, "/fake/pkg", "f"); !ok || err != nil {
		t.Fatalf("Miss inside unit = %v, %v", ok, err)
	}
	if got := e.read(t, "/fake/pkg/f"); got != "x" {
		t.Fatalf("read = %q", got)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

var _ = bytes.Equal
