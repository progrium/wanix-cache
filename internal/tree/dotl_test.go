package tree_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/tree"
)

func setupL(t testing.TB) (*meta.DB, *cas.Store, p9.File) {
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
	go tree.NewDotLServer(&tree.FS{Meta: db, CAS: store}).Serve(ln)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cl, err := p9.NewClient(c)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cl.Attach("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close(); ln.Close(); db.Close() })
	return db, store, root
}

func walk(t testing.TB, root p9.File, p string) p9.File {
	t.Helper()
	_, f, err := root.Walk(strings.Split(strings.Trim(p, "/"), "/"))
	if err != nil {
		t.Fatalf("walk %s: %v", p, err)
	}
	return f
}

func readL(t testing.TB, root p9.File, p string) []byte {
	t.Helper()
	f := walk(t, root, p)
	defer f.Close()
	if _, _, err := f.Open(p9.ReadOnly); err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	_, _, attr, err := f.GetAttr(p9.AttrMaskAll)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, attr.Size)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n != len(buf) {
		t.Fatalf("read %s: %d, %v", p, n, err)
	}
	return buf[:n]
}

func TestDotL(t *testing.T) {
	ctx := context.Background()
	db, store, root := setupL(t)
	src := t.TempDir()
	big := random(2 << 20)
	writeFile(t, filepath.Join(src, "lib", "libc.so.6"), big)
	writeFile(t, filepath.Join(src, "etc", "os-release"), []byte("ID=test\n"))
	os.Symlink("libc.so.6", filepath.Join(src, "lib", "libc.so"))
	os.Symlink("../lib", filepath.Join(src, "etc", "lib"))
	os.Link(filepath.Join(src, "etc", "os-release"), filepath.Join(src, "etc", "os-release.hard"))
	if _, err := ingest.ImportDir(ctx, db, store, "local", "rootfs", src); err != nil {
		t.Fatal(err)
	}

	if got := readL(t, root, "/local/rootfs/lib/libc.so.6"); !bytes.Equal(got, big) {
		t.Fatalf("libc: %d bytes", len(got))
	}

	link := walk(t, root, "/local/rootfs/lib/libc.so")
	qid, _, attr, err := link.GetAttr(p9.AttrMaskAll)
	if err != nil {
		t.Fatal(err)
	}
	if qid.Type != p9.TypeSymlink || attr.Mode.FileType() != p9.ModeSymlink {
		t.Fatalf("symlink qid %v mode %o", qid, attr.Mode)
	}
	if target, err := link.Readlink(); err != nil || target != "libc.so.6" {
		t.Fatalf("readlink = %q, %v", target, err)
	}
	if target, err := walk(t, root, "/local/rootfs/etc/lib").Readlink(); err != nil || target != "../lib" {
		t.Fatalf("readlink dir link = %q, %v", target, err)
	}

	_, _, a1, err := walk(t, root, "/local/rootfs/etc/os-release").GetAttr(p9.AttrMaskAll)
	if err != nil {
		t.Fatal(err)
	}
	if a1.NLink != 2 || a1.Size != 8 {
		t.Fatalf("hard-linked file attr %v", a1)
	}

	d := walk(t, root, "/local/rootfs/lib")
	if _, _, err := d.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	ents, err := d.Readdir(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, fmt.Sprintf("%s:%v", e.Name, e.Type == p9.TypeSymlink))
	}
	if fmt.Sprint(names) != "[libc.so:true libc.so.6:false]" {
		t.Fatalf("lib listing = %v", names)
	}

	f := walk(t, root, "/local/rootfs/etc/os-release")
	if _, _, err := f.Open(p9.WriteOnly); err == nil {
		t.Error("open for write succeeded")
	}
	etc := walk(t, root, "/local/rootfs/etc")
	if _, _, _, err := etc.Create("x", p9.WriteOnly, 0o644, 0, 0); err == nil {
		t.Error("create succeeded")
	}
	if err := etc.UnlinkAt("os-release", 0); err == nil {
		t.Error("unlink succeeded")
	}
	if _, _, err := root.Walk([]string{"local", "rootfs", "missing"}); err == nil {
		t.Error("walk to a missing name succeeded")
	}
}

// TestDotLReaddirPaging lists a large directory in small reply buffers and
// also re-reads from an arbitrary offset, which takes the position path.
func TestDotLReaddirPaging(t *testing.T) {
	ctx := context.Background()
	db, _, root := setupL(t)
	tx, _ := db.Begin(ctx)
	d, err := tx.MkdirAll("/wide")
	if err != nil {
		t.Fatal(err)
	}
	const n = 3000
	now := time.Now()
	for i := 0; i < n; i++ {
		if _, err := tx.File(d, fmt.Sprintf("entry-%05d", i), 0o644, 0, now, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	dir := walk(t, root, "/wide")
	if _, _, err := dir.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	var got []string
	var off uint64
	for {
		ents, err := dir.Readdir(off, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(ents) == 0 {
			break
		}
		for _, e := range ents {
			got = append(got, e.Name)
		}
		off = ents[len(ents)-1].Offset
	}
	if len(got) != n || got[0] != "entry-00000" || got[n-1] != fmt.Sprintf("entry-%05d", n-1) {
		t.Fatalf("listed %d entries, first %q last %q", len(got), got[0], got[len(got)-1])
	}
	ents, err := dir.Readdir(1234, 200)
	if err != nil || len(ents) == 0 || ents[0].Name != "entry-01234" {
		t.Fatalf("readdir at offset 1234: %v, %v", ents, err)
	}
}
