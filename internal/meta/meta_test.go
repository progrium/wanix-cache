package meta

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t testing.TB) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestNamespace(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	dir, err := tx.MkdirAll("/a/b")
	if err != nil {
		t.Fatal(err)
	}
	f, err := tx.File(dir, "f", 0o644, 3, now, "sha224-x", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Link(dir, "g", f); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Symlink(dir, "s", "f", now, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.File(dir, "f", 0o644, 0, now, "", 0); !errors.Is(err, ErrExist) {
		t.Fatalf("duplicate name: got %v, want ErrExist", err)
	}
	if err := tx.Link(dir, "d", dir); err == nil {
		t.Fatal("hard link to a directory succeeded")
	}
	if _, err := tx.File(f, "x", 0o644, 0, now, "", 0); !errors.Is(err, ErrNotDir) {
		t.Fatalf("create under a file: got %v, want ErrNotDir", err)
	}
	if again, err := tx.MkdirAll("/a/b"); err != nil || again != dir {
		t.Fatalf("MkdirAll again = %d, %v; want %d", again, err, dir)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	a, err := db.Lookup(ctx, RootIno, "a")
	if err != nil || a.Kind != KindDir || a.Parent != RootIno {
		t.Fatalf("lookup a = %+v, %v", a, err)
	}
	g, err := db.Lookup(ctx, dir, "g")
	if err != nil || g.Ino != f || g.Content != "sha224-x" {
		t.Fatalf("hard link g = %+v, %v; want ino %d", g, err, f)
	}
	s, err := db.Lookup(ctx, dir, "s")
	if err != nil || s.Kind != KindSymlink || s.Target != "f" {
		t.Fatalf("symlink = %+v, %v", s, err)
	}
	if _, err := db.Lookup(ctx, dir, "nope"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("missing: got %v", err)
	}
	ents, err := db.ReadDir(ctx, dir, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name)
	}
	if fmt.Sprint(names) != "[f g s]" {
		t.Fatalf("ReadDir = %v", names)
	}
	page, err := db.ReadDir(ctx, dir, "f", 1)
	if err != nil || len(page) != 1 || page[0].Name != "g" {
		t.Fatalf("ReadDir after f = %+v, %v", page, err)
	}
}

func TestRollback(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	tx, _ := db.Begin(ctx)
	if _, err := tx.MkdirAll("/gone"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if _, err := db.Lookup(ctx, RootIno, "gone"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("rolled-back dir visible: %v", err)
	}
}

func TestUnitStates(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	tx, _ := db.Begin(ctx)
	id, err := tx.CreateUnit("apk", "musl-1.2.5-r0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateUnit("apk", "musl-1.2.5-r0"); !errors.Is(err, ErrExist) {
		t.Fatalf("duplicate unit: %v", err)
	}
	steps := []UnitState{UnitFetching, UnitSpooled, UnitResident, UnitEvicted, UnitFetching}
	from := UnitAbsent
	for _, to := range steps {
		if err := tx.SetUnitState(id, from, to); err != nil {
			t.Fatalf("%v -> %v: %v", from, to, err)
		}
		from = to
	}
	if err := tx.SetUnitState(id, UnitFetching, UnitResident); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("fetching -> resident allowed: %v", err)
	}
	if err := tx.SetUnitState(id, UnitAbsent, UnitFetching); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("stale from-state accepted: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	u, err := db.Unit(ctx, "apk", "musl-1.2.5-r0")
	if err != nil || u.State != UnitFetching {
		t.Fatalf("unit = %+v, %v", u, err)
	}
	at := time.Unix(1_700_000_000, 0)
	if err := db.TouchUnits(ctx, map[int64]time.Time{id: at}); err != nil {
		t.Fatal(err)
	}
	if u, _ = db.Unit(ctx, "apk", "musl-1.2.5-r0"); !u.LastAccess.Equal(at) {
		t.Fatalf("last access = %v", u.LastAccess)
	}
}

// buildWide creates /d<i>/f<j> for dirs*files files.
func buildWide(t testing.TB, db *DB, dirs, files int) []uint64 {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	var ds []uint64
	for i := 0; i < dirs; i++ {
		d, err := tx.Mkdir(RootIno, fmt.Sprintf("d%04d", i), 0o755, now, 0)
		if err != nil {
			t.Fatal(err)
		}
		ds = append(ds, d)
		for j := 0; j < files; j++ {
			if _, err := tx.File(d, fmt.Sprintf("f%05d", j), 0o644, 1, now, "", 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return ds
}

// TestLookupScales checks that lookups do not slow down with store size,
// which was the prototype's main problem (37x slower creates at 11.6k
// entries).
func TestLookupScales(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	ctx := context.Background()
	per := func(db *DB, ds []uint64) time.Duration {
		const n = 2000
		start := time.Now()
		for i := 0; i < n; i++ {
			if _, err := db.Lookup(ctx, ds[i%len(ds)], fmt.Sprintf("f%05d", i%100)); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start) / n
	}
	small := openTest(t)
	smallT := per(small, buildWide(t, small, 10, 100)) // 1k files
	big := openTest(t)
	start := time.Now()
	bigDirs := buildWide(t, big, 100, 1000) // 100k files
	build := time.Since(start)
	bigT := per(big, bigDirs)
	t.Logf("build 100k entries: %v (%.0f entries/s)", build, 100_000/build.Seconds())
	t.Logf("lookup: %v/op at 1k entries, %v/op at 100k entries", smallT, bigT)
	if bigT > 5*smallT+50*time.Microsecond {
		t.Errorf("lookup at 100k entries is %v, vs %v at 1k", bigT, smallT)
	}
}

func BenchmarkLookup100k(b *testing.B) {
	ctx := context.Background()
	db := openTest(b)
	ds := buildWide(b, db, 100, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Lookup(ctx, ds[i%len(ds)], fmt.Sprintf("f%05d", i%1000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDir1k(b *testing.B) {
	ctx := context.Background()
	db := openTest(b)
	ds := buildWide(b, db, 100, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		after := ""
		for {
			ents, err := db.ReadDir(ctx, ds[i%len(ds)], after, 256)
			if err != nil {
				b.Fatal(err)
			}
			if len(ents) == 0 {
				break
			}
			after = ents[len(ents)-1].Name
		}
	}
}

func TestGen(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	g := db.Gen()
	tx, _ := db.Begin(ctx)
	if _, err := tx.MkdirAll("/gone"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if db.Gen() != g {
		t.Fatal("rollback bumped the generation")
	}
	tx, _ = db.Begin(ctx)
	if _, err := tx.CreateUnit("apk", "x"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if db.Gen() != g {
		t.Fatal("a unit-only commit bumped the generation")
	}
	tx, _ = db.Begin(ctx)
	ino, err := tx.File(RootIno, "f", 0o644, 1, time.Now(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if db.Gen() == g {
		t.Fatal("creating a file did not bump the generation")
	}
	g = db.Gen()
	tx, _ = db.Begin(ctx)
	if err := tx.SetContent(ino, "sha224-x"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if db.Gen() == g {
		t.Fatal("SetContent did not bump the generation")
	}
}
