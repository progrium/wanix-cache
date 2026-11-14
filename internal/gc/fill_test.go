package gc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// memSub serves units of one file each from memory.
type memSub struct {
	files   map[subsys.Key][]byte
	fetches map[subsys.Key]int
}

func (s *memSub) Name() string                 { return "mem" }
func (s *memSub) Migrations() []meta.Migration { return nil }
func (s *memSub) Resolve(_ context.Context, p string) (subsys.Key, string, error) {
	k, rest, _ := strings.Cut(p, "/")
	if _, ok := s.files[subsys.Key(k)]; !ok {
		return "", "", fs.ErrNotExist
	}
	return subsys.Key(k), rest, nil
}
func (s *memSub) Describe(_ context.Context, k subsys.Key) (subsys.Info, error) {
	return subsys.Info{Size: int64(len(s.files[k]))}, nil
}
func (s *memSub) Fetch(_ context.Context, k subsys.Key, sink subsys.Sink) error {
	s.fetches[k]++
	w, err := sink.File("f", 0o644, int64(len(s.files[k])), time.Unix(1, 0))
	if err != nil {
		return err
	}
	if _, err := w.Write(s.files[k]); err != nil {
		return err
	}
	return w.Close()
}

// The phase 5 exit test through the real fill path: fill past the cap,
// evict, refault, and the audit finds no orphans.
func TestFillManagerPastCap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	spool, err := fill.OpenDiskSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	const size = 200 << 10
	quota := &fill.LocalQuota{Max: 5 * size, Store: st.Used, Spool: spool.Bytes}
	m, err := fill.NewManager(db, st, spool, quota, fill.Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	sub := &memSub{files: map[subsys.Key][]byte{}, fetches: map[subsys.Key]int{}}
	for i := 0; i < 12; i++ {
		sub.files[subsys.Key(rune('a'+i))] = random(int64(i), size)
	}
	if err := m.Register(ctx, sub); err != nil {
		t.Fatal(err)
	}
	c, err := New(ctx, db, st, Config{
		Max:   quota.Max,
		Usage: func() int64 { u := quota.Usage(); return u.Store + u.Spool + u.Reserved },
		Logf:  t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Attach(m)

	resident := func(k subsys.Key) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			// The manager commits resident before it removes the spool, so
			// wait for both or the spool bytes are briefly counted twice.
			u, err := db.Unit(ctx, "mem", string(k))
			if err == nil && u.State == meta.UnitResident && spool.Bytes() == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never became resident (%v, %v)", k, u.State, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	read := func(k subsys.Key) {
		t.Helper()
		f, _, err := m.OpenPath(ctx, "mem", k, "f")
		if err != nil {
			t.Fatalf("open %s: %v", k, err)
		}
		defer f.Close()
		got, err := io.ReadAll(io.NewSectionReader(f, 0, size))
		if err != nil || !bytes.Equal(got, sub.files[k]) {
			t.Fatalf("read %s: %v", k, err)
		}
	}

	for i := 0; i < 12; i++ {
		k := subsys.Key(rune('a' + i))
		if _, err := m.Ensure(ctx, "mem", k, true); err != nil {
			t.Fatalf("ensure %s: %v", k, err)
		}
		resident(k)
		db.TouchUnits(ctx, map[int64]time.Time{mustUnit(t, db, k).ID: time.Unix(int64(100+i), 0)})
		if u := quota.Usage(); u.Store+u.Spool > u.Max {
			t.Fatalf("after %s: %d bytes over the cap of %d", k, u.Store+u.Spool, u.Max)
		}
	}
	if s := mustUnit(t, db, "a").State; s != meta.UnitEvicted {
		t.Fatalf("oldest unit is %v after filling past the cap", s)
	}
	rep := (&env{t: t, ctx: ctx, db: db, st: st, c: c}).audit()
	t.Logf("after fill:\n%s", rep)

	// Refault the evicted unit by reading it.
	read("a")
	resident("a")
	if sub.fetches["a"] != 2 {
		t.Errorf("a fetched %d times, want 2", sub.fetches["a"])
	}
	read("l")
	(&env{t: t, ctx: ctx, db: db, st: st, c: c}).audit()

	// Pin everything resident; a new fill no longer fits.
	us, _ := db.Units(ctx, "mem")
	for _, u := range us {
		if u.State == meta.UnitResident {
			c.Pin(ctx, u.ID, "test")
		}
	}
	sub.files["z"] = random(26, size)
	if _, err := m.Ensure(ctx, "mem", "z", true); !errors.Is(err, store.ErrFull) {
		t.Fatalf("ensure with everything pinned: %v, want ErrFull", err)
	}
}

func mustUnit(t *testing.T, db *meta.DB, k subsys.Key) meta.Unit {
	t.Helper()
	u, err := db.Unit(context.Background(), "mem", string(k))
	if err != nil {
		t.Fatal(err)
	}
	return u
}
