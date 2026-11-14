package tree_test

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"

	"9fans.net/go/plan9"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/tree"
)

func TestSynthetic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, _ := cas.Open(dir, 0)
	var calls atomic.Int32
	fs := &tree.FS{Meta: db, CAS: store, Synthetic: map[string]tree.SyntheticFunc{
		"usage": func(context.Context) ([]byte, error) {
			calls.Add(1)
			return []byte("usage report\n"), nil
		},
	}}
	if err := fs.InitSynthetic(ctx); err != nil {
		t.Fatal(err)
	}

	// Plain 9P2000.
	fsys := serve9(t, fs, "")
	rf, err := fsys.Open("/.cache/usage", plan9.OREAD)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rf)
	rf.Close()
	if err != nil || string(b) != "usage report\n" {
		t.Fatalf("9P2000 read: %q, %v", b, err)
	}
	d, err := fsys.Stat("/.cache/usage")
	if err != nil || d.Length != 13 || d.Mode != 0o444 {
		t.Fatalf("9P2000 stat: %+v, %v", d, err)
	}
	fid, err := fsys.Open("/.cache", plan9.OREAD)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := fid.Dirreadall()
	fid.Close()
	if err != nil || len(ds) != 1 || ds[0].Name != "usage" {
		t.Fatalf("9P2000 listing: %v, %v", ds, err)
	}
	if _, err := fsys.Open("/.cache/usage", plan9.OWRITE); err == nil {
		t.Error("9P2000 write open succeeded")
	}
	if _, err := fsys.Stat("/.cache/nope"); err == nil {
		t.Error("unknown synthetic name resolved")
	}

	// 9P2000.L.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go tree.NewDotLServer(fs).Serve(ln)
	t.Cleanup(func() { ln.Close() })
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
	if got := string(readL(t, root, "/.cache/usage")); got != "usage report\n" {
		t.Fatalf("9P2000.L read: %q", got)
	}
	_, f, err := root.Walk([]string{".cache"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	ents, err := f.Readdir(0, 4096)
	if err != nil || len(ents) != 1 || ents[0].Name != "usage" {
		t.Fatalf("9P2000.L listing: %v, %v", ents, err)
	}
	_, uf, _ := root.Walk([]string{".cache", "usage"})
	if _, _, err := uf.Open(p9.WriteOnly); err == nil {
		t.Error("9P2000.L write open succeeded")
	}
	_, _, attr, err := uf.GetAttr(p9.AttrMaskAll)
	if err != nil || attr.Size != 13 {
		t.Fatalf("9P2000.L getattr: %+v %v", attr, err)
	}
	// The root lists /.cache.
	_, r2, _ := root.Walk(nil)
	r2.Open(p9.ReadOnly)
	rents, _ := r2.Readdir(0, 4096)
	found := false
	for _, e := range rents {
		found = found || e.Name == ".cache"
	}
	if !found {
		t.Errorf("root listing lacks .cache: %v", rents)
	}
}
