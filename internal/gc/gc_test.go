package gc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
	"github.com/progrium/wanix-cache/internal/subsys"
)

type env struct {
	t   *testing.T
	ctx context.Context
	dir string
	db  *meta.DB
	st  *store.Store
	c   *Collector
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
	}
	c, err := New(context.Background(), db, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, ctx: context.Background(), dir: dir, db: db, st: st, c: c}
}

// tree writes files (path -> bytes) under a new source directory.
func (e *env) tree(name string, files map[string][]byte) string {
	src := filepath.Join(e.dir, "src", name)
	for p, b := range files {
		fp := filepath.Join(src, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
			e.t.Fatal(err)
		}
		if err := os.WriteFile(fp, b, 0o644); err != nil {
			e.t.Fatal(err)
		}
	}
	return src
}

func (e *env) importDir(key, src string) meta.Unit {
	e.t.Helper()
	res, err := ingest.ImportDir(e.ctx, e.db, e.st, "local", key, src)
	if err != nil {
		e.t.Fatalf("import %s: %v", key, err)
	}
	return res.Unit
}

func (e *env) unit(key string) meta.Unit {
	e.t.Helper()
	u, err := e.db.Unit(e.ctx, "local", key)
	if err != nil {
		e.t.Fatal(err)
	}
	return u
}

func (e *env) lookup(p string) meta.Inode {
	e.t.Helper()
	ino := meta.RootIno
	var in meta.Inode
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		var err error
		if in, err = e.db.Lookup(e.ctx, ino, name); err != nil {
			e.t.Fatalf("lookup %s: %v", p, err)
		}
		ino = in.Ino
	}
	return in
}

func (e *env) read(p string) ([]byte, error) {
	in := e.lookup(p)
	if in.Content == "" {
		return nil, errors.New("no content (evicted)")
	}
	f, err := e.st.Open(in.Unit, in.Ino)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	n, err := f.ReadAt(buf, 0)
	if err == io.EOF && int64(n) == in.Size {
		err = nil
	}
	return buf[:n], err
}

func (e *env) mustRead(p string, want []byte) {
	e.t.Helper()
	got, err := e.read(p)
	if err != nil {
		e.t.Fatalf("read %s: %v", p, err)
	}
	if !bytes.Equal(got, want) {
		e.t.Fatalf("read %s: wrong bytes", p)
	}
}

func (e *env) audit() AuditReport {
	e.t.Helper()
	rep, err := e.c.Audit(e.ctx, AuditOptions{Deep: true})
	if err != nil {
		e.t.Fatal(err)
	}
	if !rep.Clean() {
		e.t.Fatalf("audit not clean:\n%s", rep)
	}
	return rep
}

func (e *env) used() int64 { return e.st.Used() }

// importFit imports a unit and then, like `wanix-cache import -max-gib`,
// evicts least recently used units (not the new one) until usage fits
// the collector's cap.
func (e *env) importFit(key, src string) meta.Unit {
	e.t.Helper()
	u := e.importDir(key, src)
	release := e.c.Acquire(u.ID)
	defer release()
	if _, err := e.c.Collect(e.ctx); err != nil {
		e.t.Fatal(err)
	}
	return u
}

// sameFile reports whether two namespace paths are one file on disk.
func (e *env) sameFile(p, q string) bool {
	a, b := e.lookup(p), e.lookup(q)
	fa, err := os.Stat(e.st.Path(a.Unit, a.Ino))
	if err != nil {
		e.t.Fatal(err)
	}
	fb, err := os.Stat(e.st.Path(b.Unit, b.Ino))
	if err != nil {
		e.t.Fatal(err)
	}
	return os.SameFile(fa, fb)
}

func (e *env) touch(key string, at time.Time) {
	e.t.Helper()
	if err := e.db.TouchUnits(e.ctx, map[int64]time.Time{e.unit(key).ID: at}); err != nil {
		e.t.Fatal(err)
	}
}

func random(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func TestEvictRefault(t *testing.T) {
	e := newEnv(t, Config{})
	shared := random(1, 300<<10)
	onlyA := random(2, 200<<10)
	onlyB := random(3, 100<<10)
	srcA := e.tree("a", map[string][]byte{"shared.bin": shared, "a.bin": onlyA, "dir/a-again.bin": onlyA})
	srcB := e.tree("b", map[string][]byte{"shared.bin": shared, "b.bin": onlyB})
	e.importDir("a", srcA)
	e.importDir("b", srcB)
	e.audit()
	// Identical files are one file on disk, within a unit and across units.
	if !e.sameFile("/local/a/shared.bin", "/local/b/shared.bin") || !e.sameFile("/local/a/a.bin", "/local/a/dir/a-again.bin") {
		t.Fatal("identical files are not hard-linked")
	}

	full := e.used()
	if want := int64(len(shared) + len(onlyA) + len(onlyB)); full != want {
		t.Fatalf("store holds %d bytes, want %d (each distinct file once)", full, want)
	}
	ex, err := e.c.Exclusive(e.ctx, e.unit("a").ID)
	if err != nil {
		t.Fatal(err)
	}
	if ex < int64(len(onlyA)) || ex >= int64(len(onlyA)+len(shared)) {
		t.Fatalf("exclusive(a) = %d, want about %d (its own file, not the shared one)", ex, len(onlyA))
	}

	freed, err := e.c.Evict(e.ctx, e.unit("a").ID)
	if err != nil {
		t.Fatal(err)
	}
	if freed != ex {
		t.Errorf("evict freed %d, exclusive said %d", freed, ex)
	}
	if got := e.used(); got != full-freed {
		t.Errorf("store usage %d after freeing %d of %d", got, freed, full)
	}
	if s := e.unit("a").State; s != meta.UnitEvicted {
		t.Fatalf("a is %v", s)
	}
	// The manifest survives eviction.
	if in := e.lookup("/local/a/dir/a-again.bin"); in.Size != int64(len(onlyA)) || in.Content != "" {
		t.Fatalf("evicted inode = %+v", in)
	}
	// B still reads the file it shared with A.
	e.mustRead("/local/b/shared.bin", shared)
	e.mustRead("/local/b/b.bin", onlyB)
	e.audit()

	if _, err := ingest.RefaultDir(e.ctx, e.db, e.st, "local", "a", srcA); err != nil {
		t.Fatal(err)
	}
	e.mustRead("/local/a/a.bin", onlyA)
	e.mustRead("/local/a/dir/a-again.bin", onlyA)
	e.mustRead("/local/a/shared.bin", shared)
	if got := e.used(); got != full {
		t.Errorf("store usage %d after refault, was %d", got, full)
	}
	e.audit()

	// Evict both: everything goes.
	for _, k := range []string{"a", "b"} {
		if _, err := e.c.Evict(e.ctx, e.unit(k).ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.used(); got != 0 {
		t.Errorf("store still holds %d bytes with every unit evicted", got)
	}
	rep := e.audit()
	if rep.DiskFiles != 0 || rep.Contents != 0 {
		t.Errorf("left: %d files, %d contents", rep.DiskFiles, rep.Contents)
	}
}

// Fill past the cap: older units are evicted to admit newer ones.
func TestFillPastCap(t *testing.T) {
	const unitSize = 256 << 10
	e := newEnv(t, Config{Max: unitSize * 7 / 2, High: 1, Low: 1})

	srcs := map[string]string{}
	for i := 0; i < 8; i++ {
		k := string(rune('a' + i))
		srcs[k] = e.tree(k, map[string][]byte{"f": random(int64(i), unitSize)})
		e.importFit(k, srcs[k])
		e.touch(k, time.Unix(int64(1000+i), 0))
		if used := e.used(); used > e.c.Max() {
			t.Fatalf("after %s: usage %d over the cap %d", k, used, e.c.Max())
		}
	}
	resident := 0
	for i := 0; i < 8; i++ {
		k := string(rune('a' + i))
		u := e.unit(k)
		if u.State == meta.UnitResident {
			resident++
		}
		if i >= 5 && u.State != meta.UnitResident {
			t.Errorf("recent unit %s was evicted", k)
		}
	}
	if resident != 3 {
		t.Errorf("%d units resident, want 3", resident)
	}
	e.audit()
	e.mustRead("/local/h/f", random(7, unitSize))

	// Refault the oldest: it evicts the least recently used resident unit.
	u, err := ingest.RefaultDir(e.ctx, e.db, e.st, "local", "a", srcs["a"])
	if err != nil {
		t.Fatal(err)
	}
	release := e.c.Acquire(u.Unit.ID)
	if _, err := e.c.Collect(e.ctx); err != nil {
		t.Fatal(err)
	}
	release()
	e.mustRead("/local/a/f", random(0, unitSize))
	if s := e.unit("f").State; s != meta.UnitEvicted {
		t.Errorf("f (least recently used) is %v, want evicted", s)
	}
	e.audit()

	// Pin everything resident: now there is nothing to evict.
	us, _ := e.db.Units(e.ctx, "local")
	for _, u := range us {
		if u.State == meta.UnitResident {
			if err := e.c.Pin(e.ctx, u.ID, "test"); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := e.used()
	e.importFit("big", e.tree("big", map[string][]byte{"f": random(50, unitSize)}))
	if e.used() != before+unitSize {
		t.Fatalf("with everything pinned, usage went from %d to %d", before, e.used())
	}
	e.audit()
}

func TestWatermarks(t *testing.T) {
	e := newEnv(t, Config{})
	for i := 0; i < 10; i++ {
		k := string(rune('a' + i))
		e.importDir(k, e.tree(k, map[string][]byte{"f": random(int64(i), 100<<10)}))
		e.touch(k, time.Unix(int64(1000+i), 0))
	}
	total := e.used()
	e.c.cfg.Max = total * 100 / 96 // usage is 96% of the cap
	freed, err := e.c.Collect(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if freed == 0 {
		t.Fatal("over the high watermark and nothing evicted")
	}
	if got := e.used(); float64(got) > 0.85*float64(e.c.cfg.Max) {
		t.Fatalf("usage %d still over the low watermark of %d", got, int64(0.85*float64(e.c.cfg.Max)))
	}
	if s := e.unit("a").State; s != meta.UnitEvicted {
		t.Errorf("oldest unit is %v", s)
	}
	if s := e.unit("j").State; s != meta.UnitResident {
		t.Errorf("newest unit is %v", s)
	}
	// Under the high watermark: nothing more happens.
	before := e.used()
	if freed, _ := e.c.Collect(e.ctx); freed != 0 || e.used() != before {
		t.Errorf("collect under the watermark freed %d", freed)
	}
	e.audit()
}

// A unit whose bytes are all shared frees nothing, so a unit that frees
// bytes goes first even if it was used more recently.
func TestSharedUnitsGoLast(t *testing.T) {
	e := newEnv(t, Config{})
	shared := random(1, 200<<10)
	e.importDir("old-shared", e.tree("old-shared", map[string][]byte{"s": shared}))
	e.importDir("holder", e.tree("holder", map[string][]byte{"s": shared}))
	e.importDir("own", e.tree("own", map[string][]byte{"o": random(2, 200<<10)}))
	e.touch("old-shared", time.Unix(1, 0))
	e.touch("own", time.Unix(2, 0))
	e.touch("holder", time.Unix(3, 0))
	if _, err := e.c.Reclaim(e.ctx, 1); err != nil {
		t.Fatal(err)
	}
	if s := e.unit("old-shared").State; s != meta.UnitResident {
		t.Errorf("old-shared is %v; evicting it frees nothing", s)
	}
	if s := e.unit("own").State; s != meta.UnitEvicted {
		t.Errorf("own is %v", s)
	}
	// Asking for more than the exclusive bytes: the shared pair goes in LRU
	// order, and the second one frees the shared blob.
	freed, err := e.c.Reclaim(e.ctx, int64(len(shared)))
	if err != nil {
		t.Fatal(err)
	}
	if freed < int64(len(shared)) {
		t.Errorf("freed %d", freed)
	}
	e.audit()
}

func TestPins(t *testing.T) {
	var pinned sync.Map
	e := newEnv(t, Config{Pinned: func(id int64) bool { _, ok := pinned.Load(id); return ok }})
	for _, k := range []string{"index", "handle", "mem", "free"} {
		e.importDir(k, e.tree(k, map[string][]byte{"f": random(int64(len(k)), 64<<10)}))
	}
	if err := e.c.Pin(e.ctx, e.unit("index").ID, "apk-index"); err != nil {
		t.Fatal(err)
	}
	pinned.Store(e.unit("handle").ID, true) // e.g. fill.Manager: an open handle
	release := e.c.Acquire(e.unit("mem").ID)

	if _, err := e.c.Evict(e.ctx, e.unit("index").ID); !errors.Is(err, ErrPinned) {
		t.Fatalf("evict durable-pinned: %v", err)
	}
	if _, err := e.c.Reclaim(e.ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]meta.UnitState{"index": meta.UnitResident, "handle": meta.UnitResident, "mem": meta.UnitResident, "free": meta.UnitEvicted} {
		if s := e.unit(k).State; s != want {
			t.Errorf("%s is %v, want %v", k, s, want)
		}
	}
	pins, _ := e.c.Pins(e.ctx)
	if len(pins) != 1 || pins[0].Reason != "apk-index" {
		t.Errorf("pins = %+v", pins)
	}
	release()
	pinned.Delete(e.unit("handle").ID)
	e.c.Unpin(e.ctx, e.unit("index").ID, "apk-index")
	if _, err := e.c.Reclaim(e.ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	us, _ := e.db.Units(e.ctx, "")
	for _, u := range us {
		if u.State != meta.UnitResident {
			continue
		}
		t.Errorf("%s still resident after unpinning", u.Key)
	}
	e.audit()
}

// An ingest that finds an identical file whose unit has just been evicted
// stores its own copy.
func TestLinkSourceGone(t *testing.T) {
	e := newEnv(t, Config{})
	data := random(5, 300<<10)
	e.importDir("a", e.tree("a", map[string][]byte{"f": data}))
	e.importDir("b", e.tree("b", map[string][]byte{"f": data}))
	if !e.sameFile("/local/a/f", "/local/b/f") {
		t.Fatal("not linked")
	}
	// Evicting a frees nothing: b still links the bytes.
	if freed, err := e.c.Evict(e.ctx, e.unit("a").ID); err != nil || freed != 0 {
		t.Fatalf("evict a freed %d, %v", freed, err)
	}
	e.mustRead("/local/b/f", data)
	if e.used() != int64(len(data)) {
		t.Fatalf("usage %d", e.used())
	}
	// b's file vanishes behind the metadata's back (as if b were being
	// evicted while c looked it up): c stores its own copy.
	in := e.lookup("/local/b/f")
	if _, err := e.st.RemoveFile(in.Unit, in.Ino); err != nil {
		t.Fatal(err)
	}
	e.importDir("c", e.tree("c", map[string][]byte{"f": data}))
	e.mustRead("/local/c/f", data)
	rep, err := e.c.Audit(e.ctx, AuditOptions{Fix: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.BrokenUnits) != 1 || rep.BrokenUnits[0].Key != "b" {
		t.Fatalf("audit:\n%s", rep)
	}
	e.audit()
}

// Concurrent imports and reclaims under a small cap never lose bytes a
// resident unit needs.
func TestConcurrentImportAndReclaim(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	common := random(77, 128<<10)
	e := newEnv(t, Config{Max: 2 << 20})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 6; i++ {
				k := string(rune('a'+w)) + string(rune('0'+i))
				src := e.tree(k, map[string][]byte{"common": common, "own": random(int64(w*100+i), 160<<10)})
				if _, err := ingest.ImportDir(e.ctx, e.db, e.st, "local", k, src); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := e.c.Reclaim(e.ctx, 256<<10); err != nil {
				errs <- err
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	us, _ := e.db.Units(e.ctx, "local")
	for _, u := range us {
		if u.State != meta.UnitResident {
			continue
		}
		e.mustRead(path.Join("/local", u.Key, "common"), common)
	}
	e.audit()
}

func TestAuditRepairs(t *testing.T) {
	e := newEnv(t, Config{})
	a := random(1, 200<<10)
	b := random(2, 200<<10)
	e.importDir("a", e.tree("a", map[string][]byte{"f": a}))
	e.importDir("b", e.tree("b", map[string][]byte{"f": b}))
	e.importDir("c", e.tree("c", map[string][]byte{"f": a}))
	e.audit()
	sqldb := e.db.SQL()

	// Drift: wrong refcounts.
	sqldb.Exec(`UPDATE content SET refs = refs + 5`)
	// Leftovers: a directory of a unit that is not resident, and a file in
	// a resident unit's directory that no inode uses.
	if err := e.st.Write(999, 1, bytes.NewReader(random(3, 50<<10))); err != nil {
		t.Fatal(err)
	}
	if err := e.st.Write(e.unit("a").ID, 999, bytes.NewReader(random(4, 10<<10))); err != nil {
		t.Fatal(err)
	}
	// Corruption: b's file deleted behind the GC's back.
	in := e.lookup("/local/b/f")
	if _, err := e.st.RemoveFile(in.Unit, in.Ino); err != nil {
		t.Fatal(err)
	}
	// A copy: c's file stored separately from a's identical one.
	in = e.lookup("/local/c/f")
	if err := e.st.Write(in.Unit, in.Ino, bytes.NewReader(a)); err != nil {
		t.Fatal(err)
	}
	if e.sameFile("/local/a/f", "/local/c/f") {
		t.Fatal("still linked")
	}

	rep, err := e.c.Audit(e.ctx, AuditOptions{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("report:\n%s", rep)
	if rep.ContentRefsFixed != 2 || rep.OrphanUnits != 1 || rep.OrphanFiles != 1 || rep.MissingFiles != 1 ||
		rep.Duplicates != 1 || len(rep.BrokenUnits) != 1 || rep.BrokenUnits[0].Key != "b" {
		t.Fatalf("audit missed something")
	}
	if _, err := os.Stat(e.st.Path(999, 1)); err != nil {
		t.Fatal("report-only audit deleted the orphan")
	}

	before := e.used()
	rep, err = e.c.Audit(e.ctx, AuditOptions{Fix: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.BrokenEvicted != 1 || rep.OrphansRemoved != 2 || rep.Relinked != 1 {
		t.Fatalf("fix did not repair:\n%s", rep)
	}
	if s := e.unit("b").State; s != meta.UnitEvicted {
		t.Errorf("broken unit b is %v, want evicted for refetch", s)
	}
	if !e.sameFile("/local/a/f", "/local/c/f") || e.used() != before-int64(len(a))-60<<10 {
		t.Errorf("after fix: usage %d, was %d", e.used(), before)
	}
	e.mustRead("/local/a/f", a)
	e.mustRead("/local/c/f", a)
	e.audit()
}

type hookSub struct {
	subsys.Subsystem // nil; only the hooks are called
	name             string
	veto             map[string]bool
	evicted          []string
	order            func([]meta.Unit) []meta.Unit
}

func (h *hookSub) Name() string { return h.name }
func (h *hookSub) OnEvict(_ context.Context, k subsys.Key) error {
	if h.veto[string(k)] {
		return errors.New("in use by the guest store")
	}
	h.evicted = append(h.evicted, string(k))
	return nil
}
func (h *hookSub) EvictionOrder(_ context.Context, us []meta.Unit) ([]meta.Unit, error) {
	return h.order(us), nil
}

func TestHooksAndOrder(t *testing.T) {
	// "Nix": c references b references a. Only units nothing resident
	// references may go, so the order is c, b, a whatever LRU says.
	refs := map[string]string{"c": "b", "b": "a"}
	var e *env
	h := &hookSub{name: "local", veto: map[string]bool{}}
	h.order = func(us []meta.Unit) []meta.Unit {
		var out []meta.Unit
		for _, u := range us {
			referenced := false
			for from, to := range refs {
				if to == u.Key && e.unit(from).State == meta.UnitResident {
					referenced = true
				}
			}
			if !referenced {
				out = append(out, u)
			}
		}
		return out
	}
	e = newEnv(t, Config{Subsystem: func(name string) (subsys.Subsystem, bool) { return h, name == "local" }})
	for i, k := range []string{"a", "b", "c"} {
		e.importDir(k, e.tree(k, map[string][]byte{"f": random(int64(i), 64<<10)}))
	}
	e.touch("a", time.Unix(1, 0)) // LRU order is a, b, c
	e.touch("b", time.Unix(2, 0))
	e.touch("c", time.Unix(3, 0))
	if _, err := e.c.Reclaim(e.ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(h.evicted, ","); got != "c,b,a" {
		t.Errorf("evicted %s, want c,b,a", got)
	}

	// A veto keeps the unit resident.
	h.evicted = nil
	h.order = func(us []meta.Unit) []meta.Unit { return us }
	e.importDir("d", e.tree("d", map[string][]byte{"f": random(9, 64<<10)}))
	h.veto["d"] = true
	if _, err := e.c.Evict(e.ctx, e.unit("d").ID); err == nil {
		t.Fatal("vetoed eviction succeeded")
	}
	if _, err := e.c.Reclaim(e.ctx, 1<<40); err != nil {
		t.Fatal(err)
	}
	if s := e.unit("d").State; s != meta.UnitResident {
		t.Errorf("vetoed unit is %v", s)
	}
	e.audit()
}
