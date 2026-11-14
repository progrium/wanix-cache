package tree_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"9fans.net/go/plan9"
	"9fans.net/go/plan9/client"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/tree"
)

type env struct {
	db    *meta.DB
	store *cas.Store
	fsys  *client.Fsys
}

func setup(t testing.TB) *env {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := cas.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &ninep.Server{FS: &tree.FS{Meta: db, CAS: store}}
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close(); db.Close() })
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := client.NewConn(c)
	if err != nil {
		t.Fatal(err)
	}
	fsys, err := conn.Attach(nil, "test", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &env{db: db, store: store, fsys: fsys}
}

func writeFile(t testing.TB, p string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func random(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func readAll(t testing.TB, fsys *client.Fsys, name string) []byte {
	t.Helper()
	fid, err := fsys.Open(name, plan9.OREAD)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer fid.Close()
	var out []byte
	buf := make([]byte, 64<<10)
	for off := int64(0); ; {
		n, err := fid.ReadAt(buf, off)
		out = append(out, buf[:n]...)
		off += int64(n)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
	}
	return out
}

func TestImportAndServe(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	src := t.TempDir()
	big := random(3 << 20)
	writeFile(t, filepath.Join(src, "bin", "tool"), big)
	writeFile(t, filepath.Join(src, "bin", "tool-copy"), big)
	writeFile(t, filepath.Join(src, "etc", "conf"), []byte("hello\n"))
	writeFile(t, filepath.Join(src, "empty"), nil)
	if err := os.Symlink("tool", filepath.Join(src, "bin", "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "etc", "conf"), filepath.Join(src, "etc", "conf-link")); err != nil {
		t.Fatal(err)
	}

	res, err := ingest.ImportDir(ctx, e.db, e.store, "local", "demo", src)
	if err != nil {
		t.Fatal(err)
	}
	if res.Files != 4 || res.Hardlinks != 1 || res.Symlinks != 1 || res.NewContents != 2 {
		t.Fatalf("import result %+v", res)
	}
	if res.Unit.State != meta.UnitResident || res.Unit.Root == 0 {
		t.Fatalf("unit %+v", res.Unit)
	}
	used, _ := e.store.Usage()
	if used > int64(len(big))+64<<10 {
		t.Errorf("CAS holds %d bytes for one 3 MiB file stored twice", used)
	}
	if _, err := ingest.ImportDir(ctx, e.db, e.store, "local", "demo", src); err == nil {
		t.Error("second import with the same key succeeded")
	}

	if got := readAll(t, e.fsys, "/local/demo/bin/tool"); !bytes.Equal(got, big) {
		t.Fatalf("tool: got %d bytes, want %d", len(got), len(big))
	}
	if got := readAll(t, e.fsys, "/local/demo/bin/tool-copy"); !bytes.Equal(got, big) {
		t.Fatal("tool-copy differs")
	}
	if got := string(readAll(t, e.fsys, "/local/demo/etc/conf-link")); got != "hello\n" {
		t.Fatalf("conf-link = %q", got)
	}
	if got := readAll(t, e.fsys, "/local/demo/empty"); len(got) != 0 {
		t.Fatalf("empty = %q", got)
	}

	a, err := e.fsys.Stat("/local/demo/etc/conf")
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.fsys.Stat("/local/demo/etc/conf-link")
	if err != nil {
		t.Fatal(err)
	}
	if a.Qid != b.Qid || a.Length != 6 || a.Name != "conf" || b.Name != "conf-link" {
		t.Fatalf("hard link stats: %v / %v", a, b)
	}
	if d, err := e.fsys.Stat("/local/demo/bin/../etc"); err != nil || d.Qid.Type != plan9.QTDIR || d.Name != "etc" {
		t.Fatalf("stat via ..: %v, %v", d, err)
	}

	fid, err := e.fsys.Open("/local/demo/bin", plan9.OREAD)
	if err != nil {
		t.Fatal(err)
	}
	dirs, err := fid.Dirreadall()
	fid.Close()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range dirs {
		names = append(names, d.Name)
	}
	if fmt.Sprint(names) != "[tool tool-copy]" {
		t.Errorf("bin listing = %v (symlink alias should be hidden)", names)
	}
	if _, err := e.fsys.Stat("/local/demo/bin/alias"); err == nil {
		t.Error("symlink resolved over plain 9P2000")
	}

	if _, err := e.fsys.Open("/local/demo/etc/conf", plan9.OWRITE); err == nil {
		t.Error("open for write succeeded")
	}
	if _, err := e.fsys.Create("/local/demo/new", plan9.OWRITE, 0o644); err == nil {
		t.Error("create succeeded")
	}
	if err := e.fsys.Remove("/local/demo/etc/conf"); err == nil {
		t.Error("remove succeeded")
	}
}

// TestLargeDirectory lists a 5,000-entry directory over 9P, which needs
// several paged reads, and checks the listing is complete and ordered.
func TestLargeDirectory(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	tx, err := e.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d, err := tx.MkdirAll("/wide")
	if err != nil {
		t.Fatal(err)
	}
	const n = 5000
	now := time.Now()
	for i := 0; i < n; i++ {
		if _, err := tx.File(d, fmt.Sprintf("file-%05d", i), 0o644, 0, now, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	fid, err := e.fsys.Open("/wide", plan9.OREAD)
	if err != nil {
		t.Fatal(err)
	}
	dirs, err := fid.Dirreadall()
	fid.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("listed %d entries over 9P in %v", len(dirs), time.Since(start))
	if len(dirs) != n {
		t.Fatalf("got %d entries, want %d", len(dirs), n)
	}
	if !sort.SliceIsSorted(dirs, func(i, j int) bool { return dirs[i].Name < dirs[j].Name }) {
		t.Error("listing not sorted")
	}
	start = time.Now()
	for i := 0; i < 1000; i++ {
		if _, err := e.fsys.Stat(fmt.Sprintf("/wide/file-%05d", i*5)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("1000 walk+stat round trips: %v", time.Since(start))
}
