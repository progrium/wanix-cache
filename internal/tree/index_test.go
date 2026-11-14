package tree_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"9fans.net/go/plan9"
	"9fans.net/go/plan9/client"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/tree"
)

type ixenv struct {
	db    *meta.DB
	store *cas.Store
	idx   *h9index.Store
	fs    *tree.FS
	ix    *h9index.Indexer
}

// setupIndexed imports a small rootfs as /local/root and indexes it.
func setupIndexed(t *testing.T) *ixenv {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := cas.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "src")
	writeFile(t, filepath.Join(src, "bin/ls"), []byte("ls binary"))
	os.Chmod(filepath.Join(src, "bin/ls"), 0o755)
	if err := os.Link(filepath.Join(src, "bin/ls"), filepath.Join(src, "bin/dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("ls", filepath.Join(src, "bin/sh")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(src, "etc/motd"), []byte("welcome\n"))
	writeFile(t, filepath.Join(src, "etc/empty"), nil)
	writeFile(t, filepath.Join(src, "a/x"), []byte("x"))
	writeFile(t, filepath.Join(src, "a-b"), []byte("y"))
	writeFile(t, filepath.Join(src, "docs/my notes.txt"), []byte("spaces"))
	writeFile(t, filepath.Join(src, "docs/ok"), []byte("ok"))
	writeFile(t, filepath.Join(src, ".h9index"), []byte("a stale index shipped in the tree\n"))
	os.MkdirAll(filepath.Join(src, "tmp"), 0o755)
	os.Chmod(filepath.Join(src, "tmp"), 0o777)
	if _, err := ingest.ImportDir(ctx, db, store, "local", "root", src); err != nil {
		t.Fatal(err)
	}
	idx, err := h9index.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	fs := &tree.FS{Meta: db, CAS: store, Index: idx}
	ix := fs.Indexer(idx, "")
	if _, err := ix.Index(ctx, "/local/root"); err != nil {
		t.Fatal(err)
	}
	return &ixenv{db, store, idx, fs, ix}
}

func serve9(t *testing.T, fs *tree.FS, aname string) *client.Fsys {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go (&ninep.Server{FS: fs}).Serve(ln)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := client.NewConn(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); ln.Close() })
	fsys, err := conn.Attach(nil, "test", aname)
	if err != nil {
		t.Fatal(err)
	}
	return fsys
}

// mkindex does what hyper9's mkindex does: walks a mounted tree over 9P
// and writes the index from the server's own stats.
func mkindex(t *testing.T, fsys *client.Fsys) []byte {
	t.Helper()
	root, err := fsys.Stat("/")
	if err != nil {
		t.Fatal(err)
	}
	entries := []h9index.Entry{{Path: "/", Dir: *root}}
	var unindexed []string
	queue := []string{"/"}
	for len(queue) > 0 {
		d := queue[0]
		queue = queue[1:]
		fid, err := fsys.Open(d, plan9.OREAD)
		if err != nil {
			t.Fatal(err)
		}
		ds, err := fid.Dirreadall()
		fid.Close()
		if err != nil {
			t.Fatal(err)
		}
		bad := false
		for _, c := range ds {
			if d == "/" && c.Name == h9index.Name {
				continue
			}
			if !h9index.ValidName(c.Name) {
				bad = true
				continue
			}
			p := path.Join(d, c.Name)
			// A stat by walk must agree with the directory read.
			st, err := fsys.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			if st.Qid != c.Qid || st.Mode != c.Mode || st.Length != c.Length || st.Mtime != c.Mtime {
				t.Fatalf("%s: stat %v and dirread %v disagree", p, st, c)
			}
			entries = append(entries, h9index.Entry{Path: p, Dir: *c})
			if c.Mode&plan9.DMDIR != 0 {
				queue = append(queue, p)
			}
		}
		if bad {
			unindexed = append(unindexed, d)
		}
	}
	return h9index.Encode(entries, unindexed)
}

func TestIndexMatchesMkindexOver9P(t *testing.T) {
	e := setupIndexed(t)
	fsys := serve9(t, e.fs, "local/root")

	served := readAll(t, fsys, "/"+h9index.Name)
	if want := mkindex(t, fsys); !bytes.Equal(served, want) {
		t.Fatalf("served index differs from mkindex's:\n%s\nmkindex:\n%s", served, want)
	}
	x, err := h9index.ParseBytes(served)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := x.Lookup("/bin/sh"); ok {
		t.Error("9P2000 index lists a symlink, which the 9P2000 server hides")
	}
	if !x.Unindexed["/docs"] {
		t.Error("/docs has a name with a space but is not unindexed")
	}
	if _, ok := x.Lookup("/docs/ok"); !ok {
		t.Error("/docs/ok missing")
	}
	ls, _ := x.Lookup("/bin/ls")
	dir, _ := x.Lookup("/bin/dir")
	if ls.Qid.Path != dir.Qid.Path || ls.Mode != 0o755 || ls.Length != 9 || ls.Uid != ninep.DefaultUser {
		t.Errorf("hard link or file fields wrong: %+v %+v", ls, dir)
	}
	if tmp, _ := x.Lookup("/tmp"); tmp.Mode&0o777 != 0o777 || tmp.Mode&plan9.DMDIR == 0 {
		t.Errorf("/tmp mode %o", tmp.Mode)
	}
	if _, ok := x.Lookup("/" + h9index.Name); ok {
		t.Error("index lists itself")
	}

	// The file itself: read-only, the right length, listed once, and the
	// generated index shadows the one shipped in the tree.
	st, err := fsys.Stat("/" + h9index.Name)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode != 0o444 || st.Length != uint64(len(served)) || st.Qid.Path>>63 != 1 {
		t.Errorf("index stat %+v", st)
	}
	fid, _ := fsys.Open("/", plan9.OREAD)
	ds, _ := fid.Dirreadall()
	fid.Close()
	n := 0
	for _, d := range ds {
		if d.Name == h9index.Name {
			n++
			if d.Qid != st.Qid {
				t.Errorf("listed qid %v, stat qid %v", d.Qid, st.Qid)
			}
		}
	}
	if n != 1 {
		t.Errorf(".h9index listed %d times", n)
	}

	// ".." at the attach root stays there.
	up, err := fsys.Stat("/..")
	if err != nil {
		t.Fatal(err)
	}
	if root, _ := fsys.Stat("/"); up.Qid != root.Qid {
		t.Errorf(".. escaped the attach root: %v vs %v", up.Qid, root.Qid)
	}

	// Regenerating an unchanged tree keeps the file's qid and mtime, so
	// h9's host cache keeps hitting.
	time.Sleep(1100 * time.Millisecond)
	r, err := e.ix.Index(context.Background(), "/local/root")
	if err != nil {
		t.Fatal(err)
	}
	if r.Changed {
		t.Error("reindexing an unchanged root changed it")
	}
	st2, _ := fsys.Stat("/" + h9index.Name)
	if st2.Qid != st.Qid || st2.Mtime != st.Mtime {
		t.Errorf("index stat changed: %+v -> %+v", st, st2)
	}
}

func TestIndexNamespaceRootHasNone(t *testing.T) {
	e := setupIndexed(t)
	fsys := serve9(t, e.fs, "")
	if _, err := fsys.Stat("/" + h9index.Name); err == nil {
		t.Error("the whole namespace has an index")
	}
	// The indexed root is still reachable under the whole namespace.
	if _, err := fsys.Stat("/local/root/" + h9index.Name); err != nil {
		t.Error(err)
	}
	if _, err := fsys.Stat("/local/root/bin/ls"); err != nil {
		t.Error(err)
	}
}

func TestAttachNameErrors(t *testing.T) {
	e := setupIndexed(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go (&ninep.Server{FS: e.fs}).Serve(ln)
	for _, aname := range []string{"local/nope", "local/root/etc/motd", "local/../local"} {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conn, err := client.NewConn(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Attach(nil, "test", aname); err == nil {
			t.Errorf("attach %q succeeded", aname)
		}
		conn.Close()
	}
}

func attachL(t *testing.T, fs *tree.FS, aname string) p9.File {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go tree.NewDotLServer(fs).Serve(ln)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cl, err := p9.NewClient(c)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cl.Attach(aname)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close(); ln.Close() })
	return root
}

func TestIndexOverDotL(t *testing.T) {
	e := setupIndexed(t)
	root := attachL(t, e.fs, "local/root")
	served := readL(t, root, h9index.Name)
	x, err := h9index.ParseBytes(served)
	if err != nil {
		t.Fatalf("%v\n%s", err, served)
	}
	sh, ok := x.Lookup("/bin/sh")
	if !ok || sh.Mode&plan9.DMSYMLINK == 0 || sh.Length != 2 {
		t.Errorf("symlink entry %+v %v", sh, ok)
	}
	// Every entry's qid and permissions match what the .L server reports.
	for _, en := range x.Entries {
		f := root
		if en.Path != "/" {
			f = walk(t, root, en.Path)
		}
		q, _, a, err := f.GetAttr(p9.AttrMaskAll)
		if err != nil {
			t.Fatal(err)
		}
		if q.Path != en.Qid.Path || q.Version != en.Qid.Vers || uint32(a.Mode.Permissions()) != uint32(en.Mode&0o7777) ||
			a.MTimeSeconds != uint64(en.Mtime) {
			t.Errorf("%s: index %+v, server %+v %+v", en.Path, en, q, a)
		}
		if !a.Mode.IsDir() && a.Size != en.Length {
			t.Errorf("%s: length %d, server %d", en.Path, en.Length, a.Size)
		}
	}
	// The 9P2000 and .L variants are different files.
	q9, _, err := e.idx.Get(context.Background(), x.Entries[0].Qid.Path, h9index.NineP)
	if err != nil {
		t.Fatal(err)
	}
	ql, _, _, err := walk(t, root, h9index.Name).GetAttr(p9.AttrMaskAll)
	if err != nil {
		t.Fatal(err)
	}
	if q9.Ino == ql.Path {
		t.Error("variants share a qid")
	}

	// Readdir lists the index once, first, and every real entry after it.
	if _, _, err := root.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	var names []string
	for off := uint64(0); ; {
		ds, err := root.Readdir(off, 64) // small replies, to page
		if err != nil {
			t.Fatal(err)
		}
		if len(ds) == 0 {
			break
		}
		for _, d := range ds {
			names = append(names, d.Name)
			off = d.Offset
		}
	}
	got := strings.Join(names, ",")
	if want := ".h9index,a,a-b,bin,docs,etc,tmp"; got != want {
		t.Errorf("readdir %s, want %s", got, want)
	}
}

func TestIndexRefusesRootBeingFilled(t *testing.T) {
	e := setupIndexed(t)
	ctx := context.Background()
	tx, err := e.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := tx.CreateUnit("local", "half")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetUnitState(id, meta.UnitAbsent, meta.UnitFetching); err != nil {
		t.Fatal(err)
	}
	parent, _ := tx.MkdirAll("/local")
	d, err := tx.Mkdir(parent, "half", 0o755, time.Now(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.File(d, "f", 0o644, 0, time.Now(), "", id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ix.Index(ctx, "/local/half"); !errors.Is(err, h9index.ErrNotReady) {
		t.Fatalf("indexing a fetching unit: %v", err)
	}
	// And /local as a whole contains it.
	if _, err := e.ix.Index(ctx, "/local"); !errors.Is(err, h9index.ErrNotReady) {
		t.Fatalf("indexing a parent of a fetching unit: %v", err)
	}
}
