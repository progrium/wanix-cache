package tree_test

import (
	"context"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
	"github.com/progrium/wanix-cache/internal/tree"
)

// upstream is a one-unit subsystem: /up/<key>/hello and a symlink.
type upstream struct{ fetches atomic.Int32 }

func (*upstream) Name() string                 { return "up" }
func (*upstream) Migrations() []meta.Migration { return nil }
func (*upstream) Resolve(_ context.Context, p string) (subsys.Key, string, error) {
	k, rest, _ := strings.Cut(p, "/")
	if !strings.HasPrefix(k, "pkg-") {
		return "", "", fs.ErrNotExist
	}
	return subsys.Key(k), rest, nil
}
func (*upstream) Describe(context.Context, subsys.Key) (subsys.Info, error) {
	return subsys.Info{Size: 6}, nil
}
func (u *upstream) Fetch(_ context.Context, k subsys.Key, s subsys.Sink) error {
	u.fetches.Add(1)
	now := time.Now()
	w, err := s.File("hello", 0o644, 6, now)
	if err != nil {
		return err
	}
	io.WriteString(w, "hello\n")
	if err := w.Close(); err != nil {
		return err
	}
	return s.Symlink("greeting", "hello", now)
}

func TestReadThroughOverDotL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, _ := cas.Open(dir, 0)
	sp, _ := fill.OpenDiskSpool(filepath.Join(dir, "spool"))
	q := &fill.LocalQuota{CAS: func() int64 { u, _ := store.Usage(); return u }, Spool: sp.Bytes}
	m, err := fill.NewManager(db, store, sp, q, fill.Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	up := &upstream{}
	if err := m.Register(ctx, up); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go tree.NewDotLServer(&tree.FS{Meta: db, CAS: store, Files: m, Faults: m}).Serve(ln)
	t.Cleanup(func() { ln.Close(); m.Close(); db.Close() })
	c, _ := net.Dial("tcp", ln.Addr().String())
	cl, err := p9.NewClient(c)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	root, err := cl.Attach("")
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := root.Walk([]string{"up", "other"}); err == nil {
		t.Error("walk to a name the subsystem does not resolve succeeded")
	}
	// The first walk faults the unit in; nothing was cached before.
	if got := string(readL(t, root, "/up/pkg-a/hello")); got != "hello\n" {
		t.Fatalf("read-through = %q", got)
	}
	if target, err := walk(t, root, "/up/pkg-a/greeting").Readlink(); err != nil || target != "hello" {
		t.Fatalf("readlink = %q, %v", target, err)
	}
	if err := m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if got := string(readL(t, root, "/up/pkg-a/hello")); got != "hello\n" {
		t.Fatalf("read after ingest = %q", got)
	}
	if n := up.fetches.Load(); n != 1 {
		t.Errorf("%d fetches, want 1", n)
	}
	u, _ := db.Unit(ctx, "up", "pkg-a")
	if u.State != meta.UnitResident {
		t.Errorf("unit state %v", u.State)
	}
}
