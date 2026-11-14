package nix

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/subsys"
	"github.com/progrium/wanix-cache/internal/tree"
)

func TestBase32(t *testing.T) {
	sum := sha256.Sum256([]byte("hello"))
	// nix-hash --type sha256 --to-base32 2cf24dba...
	const want = "094qif9n4cq4fdg459qzbhg1c6wywawwaaivx0k0x8xhbyx4vwic"
	if got := EncodeBase32(sum[:]); got != want {
		t.Fatalf("EncodeBase32 = %s, want %s", got, want)
	}
	back, err := DecodeBase32(want, 32)
	if err != nil || !bytes.Equal(back, sum[:]) {
		t.Fatalf("DecodeBase32 = %x, %v", back, err)
	}
	for _, s := range []string{
		"sha256:" + want,
		"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		"sha256-LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=",
	} {
		if got, err := NormalizeHash(s); err != nil || got != "sha256:"+want {
			t.Errorf("NormalizeHash(%s) = %s, %v", s, got, err)
		}
	}
}

func TestStorePath(t *testing.T) {
	for _, c := range []struct{ in, name, version string }{
		{"18vypjxdydqyk36p0mi78jsq4i05br0s-hello-1.0.0.2", "hello", "1.0.0.2"},
		{"/nix/store/0p8b2lqk47fvxm9hc6c8mnln5l8x51q1-gcc-14.3.0-lib", "gcc", "14.3.0-lib"},
		{"08dpjm29ra7vprsjjgrs8mh0khzv1vr3-env-manifest.nix", "env-manifest.nix", ""},
		{"1fx0ggl781wbmspg609zj5m59sispjf5-akku-retropikzel-hello-1.2.3", "akku-retropikzel-hello", "1.2.3"},
	} {
		p, err := ParseStorePath(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if n, v := p.Package(); n != c.name || v != c.version {
			t.Errorf("%s: Package = %q, %q", c.in, n, v)
		}
	}
	for _, bad := range []string{"hello", "18vypjxdydqyk36p0mi78jsq4i05br0e-x", "18vypjxdydqyk36p0mi78jsq4i05br0s-a/b", "18vypjxdydqyk36p0mi78jsq4i05br0s-.x"} {
		if _, err := ParseStorePath(bad); err == nil {
			t.Errorf("ParseStorePath(%q) succeeded", bad)
		}
	}
}

const sampleNarInfo = `StorePath: /nix/store/18vypjxdydqyk36p0mi78jsq4i05br0s-hello-1.0.0.2
URL: nar/013h629j7fdcqwk8sm8h4g5a6vzgs5r0fknzhcqqaa22fd9q6r24.nar.zst
Compression: zstd
FileHash: sha256:0k9k1yqw2b71lqjvsmaz4wmvh927f11wd5srxy9prgs3qva9vryq
FileSize: 369321
NarHash: sha256:013h629j7fdcqwk8sm8h4g5a6vzgs5r0fknzhcqqaa22fd9q6r24
NarSize: 1143064
References: 1q857rs68rs3gijls9wdnsjqnwnv5zd8-glibc-2.44-57 3l8x8a3nimjhn2z0yyb87xll8fry0sss-libffi-3.8.0 4x2knmgc6k5i4j1l5gh0c14i22lx0mab-gmp-with-cxx-6.3.0 nccd0hyy08sdz7yb2ssxzylmalfbcz7g-numactl-2.0.18
Deriver: x80gdng7zi3800xbpqddx9mgga6l7rps-hello-1.0.0.2.drv
Sig: cache.nixos.org-1:R3onc6uNy58+vOFHDWo5zYgLcVSVRR7BgLmmZ/h0eWI6vxcnxwZam0a6nYQUnav1qzMXwxNpMQk81dOKV2vyDQ==
`

func TestNarInfo(t *testing.T) {
	ni, err := ParseNarInfo(sampleNarInfo)
	if err != nil {
		t.Fatal(err)
	}
	if ni.String() != sampleNarInfo {
		t.Errorf("round trip:\n%s", ni.String())
	}
	if len(ni.Deps()) != 4 || ni.NarSize != 1143064 || len(ni.Sigs) != 1 {
		t.Errorf("parsed %+v", ni)
	}
	u := ni.Uncompressed("nar/x.nar")
	if u.Compression != "none" || u.FileHash != ni.NarHash || u.FileSize != ni.NarSize || u.Sigs[0] != ni.Sigs[0] {
		t.Errorf("Uncompressed = %+v", u)
	}
	empty, err := ParseNarInfo(strings.Replace(sampleNarInfo, ni.String()[strings.Index(ni.String(), "References: "):strings.Index(ni.String(), "Deriver:")], "References: \n", 1))
	if err != nil || len(empty.References) != 0 {
		t.Errorf("empty references: %v, %v", empty, err)
	}
}

var fixtureTree = MemTree{
	"":                  nil,
	"bin":               nil,
	"bin/tool":          {Data: []byte("#!/bin/sh\necho tool\n"), Executable: true},
	"lib":               nil,
	"lib/libfoo.so":     {Data: bytes.Repeat([]byte("libfoo!"), 3000)},
	"lib/libfoo.so.1":   {Symlink: "libfoo.so"},
	"share":             nil,
	"share/empty":       {Data: nil},
	"share/Z-sorts-1st": {Data: []byte("x")},
}

// TestNARMatchesNix checks WriteNAR against nix-store --dump, when Nix is
// installed, and that ReadNAR round-trips.
func TestNARMatchesNix(t *testing.T) {
	nar := fixtureTree.NAR()
	if nixStore, err := exec.LookPath("nix-store"); err == nil {
		dir := filepath.Join(t.TempDir(), "tree")
		writeTree(t, dir, fixtureTree)
		out, err := exec.Command(nixStore, "--dump", dir).Output()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, nar) {
			t.Fatalf("WriteNAR differs from nix-store --dump (%d vs %d bytes)", len(nar), len(out))
		}
	} else {
		t.Log("nix-store not installed; skipping comparison with Nix")
	}
	got := MemTree{}
	if err := ReadNAR(bytes.NewReader(nar), memHandler(got)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.NAR(), nar) {
		t.Fatal("ReadNAR/WriteNAR does not round-trip")
	}
	for _, cut := range []int{8, 100, len(nar) - 8} {
		if err := ReadNAR(bytes.NewReader(nar[:cut]), memHandler(MemTree{})); err == nil {
			t.Errorf("truncated NAR at %d parsed", cut)
		}
	}
}

func writeTree(t *testing.T, dir string, m MemTree) {
	for _, p := range sortedKeys(m) {
		f, full := m[p], filepath.Join(dir, p)
		switch {
		case f == nil:
			os.MkdirAll(full, 0o755)
		case f.Symlink != "":
			os.Symlink(f.Symlink, full)
		default:
			mode := os.FileMode(0o644)
			if f.Executable {
				mode = 0o755
			}
			if err := os.WriteFile(full, f.Data, mode); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func sortedKeys(m MemTree) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	// Parents sort before children.
	sortStrings(ks)
	return ks
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

type memHandler MemTree

func (m memHandler) Dir(p string) error { m[p] = nil; return nil }
func (m memHandler) File(p string, exec bool, size int64, r io.Reader) error {
	b, err := io.ReadAll(r)
	m[p] = &MemFile{Data: b, Executable: exec}
	return err
}
func (m memHandler) Symlink(p, target string) error { m[p] = &MemFile{Symlink: target}; return nil }

// fixtures is a fake binary cache.
type fixtures struct {
	mu       sync.Mutex
	files    map[string][]byte
	requests map[string]int
	nars     map[StorePath][]byte
}

func (f *fixtures) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	p := strings.TrimPrefix(r.URL.Path, "/")
	f.requests[p]++
	b, ok := f.files[p]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Write(b)
}

func (f *fixtures) count(p string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[p]
}

func compress(t *testing.T, method string, b []byte) []byte {
	var buf bytes.Buffer
	switch method {
	case "none":
		return b
	case "xz":
		w, err := xz.NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		w.Write(b)
		w.Close()
	case "zstd":
		w, _ := zstd.NewWriter(&buf)
		w.Write(b)
		w.Close()
	}
	return buf.Bytes()
}

func (f *fixtures) add(t *testing.T, sp StorePath, tree MemTree, method string, refs ...StorePath) {
	nar := tree.NAR()
	sum := sha256.Sum256(nar)
	ni := &NarInfo{StorePath: sp, URL: "nar/" + sp.Hash() + ".nar." + method, Compression: method,
		NarHash: FormatHash(sum[:]), NarSize: int64(len(nar)), References: append([]StorePath{sp}, refs...),
		Sigs: []string{"test-1:c2ln"}}
	f.files[sp.Hash()+".narinfo"] = []byte(ni.String())
	f.files[ni.URL] = compress(t, method, nar)
	f.nars[sp] = nar
}

const (
	libPath  StorePath = "0p8b2lqk47fvxm9hc6c8mnln5l8x51q1-libfoo-1.0"
	appPath  StorePath = "18vypjxdydqyk36p0mi78jsq4i05br0s-app-2.1"
	filePath StorePath = "08dpjm29ra7vprsjjgrs8mh0khzv1vr3-config.json"
	linkPath StorePath = "0a0y1r2nhq7gxg00s9hm6h9ncv8qb38f-link"
	badPath  StorePath = "0y221n4x5cwrig34sq4i40n3lls0m2ac-tampered-1"
	nonePath StorePath = "3km45gzvnpin7xddlm9gpj5fac4wvjvn-missing-1"
)

type env struct {
	ctx   context.Context
	db    *meta.DB
	store *cas.Store
	m     *fill.LocalManager
	nix   *Subsystem
	fx    *fixtures
}

func newEnv(t *testing.T, opts Options) *env {
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
	fx := &fixtures{files: map[string][]byte{}, requests: map[string]int{}, nars: map[StorePath][]byte{}}
	fx.add(t, libPath, fixtureTree, "xz")
	app := MemTree{"": nil, "bin": nil,
		"bin/app":     {Data: []byte("#!" + libPath.Abs() + "/bin/tool\n"), Executable: true},
		"bin/libcopy": {Data: bytes.Repeat([]byte("libfoo!"), 3000)}, // same bytes as lib/libfoo.so
	}
	fx.add(t, appPath, app, "zstd", libPath, filePath, linkPath)
	fx.add(t, filePath, MemTree{"": {Data: []byte(`{"a":1}`)}}, "none")
	fx.add(t, linkPath, MemTree{"": {Symlink: libPath.Abs() + "/lib"}}, "none")
	fx.add(t, badPath, MemTree{"": nil, "f": {Data: []byte("good")}}, "none")
	// Tamper with the NAR bytes after the narinfo was written.
	fx.files["nar/"+badPath.Hash()+".nar.none"] = bytes.Replace(fx.files["nar/"+badPath.Hash()+".nar.none"], []byte("good"), []byte("evil"), 1)
	srv := httptest.NewServer(fx)
	opts.Substituters = append([]string{srv.URL + "/"}, opts.Substituters...)
	n, err := New(db, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Register(ctx, n); err != nil {
		t.Fatal(err)
	}
	if err := n.Attach(ctx, m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close(); m.Close(); db.Close() })
	return &env{ctx: ctx, db: db, store: store, m: m, nix: n, fx: fx}
}

func (e *env) read(t *testing.T, key subsys.Key, rel string) string {
	t.Helper()
	f, in, err := e.m.OpenPath(e.ctx, Name, key, rel)
	if err != nil {
		t.Fatalf("OpenPath %s %s: %v", key, rel, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.NewSectionReader(f, 0, in.Size))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestReadThrough(t *testing.T) {
	e := newEnv(t, Options{})
	ctx := e.ctx

	// A miss under /nix/store resolves and faults the path in.
	if retry, err := e.m.Miss(ctx, "/nix/store", string(appPath)); err != nil || !retry {
		t.Fatalf("Miss = %v, %v", retry, err)
	}
	if got := e.read(t, subsys.Key(appPath), "bin/app"); got != "#!"+libPath.Abs()+"/bin/tool\n" {
		t.Fatalf("bin/app = %q", got)
	}
	if got := e.read(t, subsys.Key(libPath), "lib/libfoo.so"); got != string(fixtureTree["lib/libfoo.so"].Data) {
		t.Fatal("lib/libfoo.so differs")
	}
	if err := e.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	u, err := e.db.Unit(ctx, Name, string(appPath))
	if err != nil || u.State != meta.UnitResident {
		t.Fatalf("unit %+v, %v", u, err)
	}
	if p, _ := e.db.PathOf(ctx, u.Root); p != "/nix/store/"+string(appPath) {
		t.Errorf("mounted at %s", p)
	}
	in, err := e.db.Lookup(ctx, u.Root, "bin")
	if err == nil {
		in, err = e.db.Lookup(ctx, in.Ino, "app")
	}
	if err != nil || in.Mode != 0o555 || in.Mtime.Unix() != 1 {
		t.Errorf("bin/app inode %+v, %v", in, err)
	}

	// File and symlink store paths are file and symlink roots.
	for sp, kind := range map[StorePath]meta.Kind{filePath: meta.KindFile, linkPath: meta.KindSymlink} {
		if _, err := e.m.Ensure(ctx, Name, subsys.Key(sp), true); err != nil {
			t.Fatal(err)
		}
		u, _ := e.db.Unit(ctx, Name, string(sp))
		in, _ := e.db.Lookup(ctx, mustLookup(t, e, "/nix/store"), string(sp))
		if in.Kind != kind || in.Ino != u.Root {
			t.Errorf("%s root is %v (ino %d, unit root %d), want %v", sp, in.Kind, in.Ino, u.Root, kind)
		}
	}

	// Identical bytes in two store paths are one content object.
	var n int
	e.db.SQL().QueryRow(`SELECT refs FROM content WHERE size = ?`, len(fixtureTree["lib/libfoo.so"].Data)).Scan(&n)
	if n != 2 {
		t.Errorf("libfoo content refs = %d, want 2 (deduped)", n)
	}

	// The rebuilt NARs are byte for byte the upstream NARs, for directory,
	// file and symlink roots.
	for _, sp := range []StorePath{libPath, appPath, filePath, linkPath} {
		var b bytes.Buffer
		if _, err := e.nix.NAR(ctx, sp, &b); err != nil {
			t.Fatalf("NAR %s: %v", sp, err)
		}
		if !bytes.Equal(b.Bytes(), e.fx.nars[sp]) {
			t.Errorf("NAR %s differs from upstream", sp)
		}
	}
	// Each narinfo was fetched from upstream once.
	if c := e.fx.count(appPath.Hash() + ".narinfo"); c != 1 {
		t.Errorf("app narinfo fetched %d times", c)
	}
}

func TestBadAndMissing(t *testing.T) {
	e := newEnv(t, Options{})
	ctx := e.ctx
	if _, err := e.m.Ensure(ctx, Name, subsys.Key(badPath), true); !errors.Is(err, fill.ErrDigestMismatch) {
		t.Errorf("tampered NAR: %v, want digest mismatch", err)
	}
	if u, err := e.db.Unit(ctx, Name, string(badPath)); err == nil && u.State != meta.UnitAbsent {
		t.Errorf("tampered unit left in state %v", u.State)
	}
	for i := 0; i < 3; i++ {
		if _, err := e.m.Ensure(ctx, Name, subsys.Key(nonePath), true); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("missing path: %v", err)
		}
	}
	if c := e.fx.count(nonePath.Hash() + ".narinfo"); c != 1 {
		t.Errorf("missing narinfo asked upstream %d times, want 1 (negative cache)", c)
	}
	// A real hash with the wrong name does not exist.
	wrong := subsys.Key(appPath.Hash() + "-not-app")
	if _, err := e.nix.Describe(ctx, wrong); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("wrong name: %v", err)
	}
	for _, p := range []string{"", "store", "foo/bar", "store/notapath"} {
		if _, _, err := e.nix.Resolve(ctx, p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Resolve(%q) = %v", p, err)
		}
	}
	if k, rel, err := e.nix.Resolve(ctx, "store/"+string(appPath)+"/bin/app"); k != subsys.Key(appPath) || rel != "bin/app" || err != nil {
		t.Errorf("Resolve store path = %q %q %v", k, rel, err)
	}
}

type fakeIndexer struct {
	mu    sync.Mutex
	roots []string
}

func (f *fakeIndexer) IndexRoot(_ context.Context, root string) error {
	f.mu.Lock()
	f.roots = append(f.roots, root)
	f.mu.Unlock()
	return nil
}

func TestClosure(t *testing.T) {
	idx := &fakeIndexer{}
	e := newEnv(t, Options{Indexer: idx})
	ctx := e.ctx
	got, err := e.nix.Closure(ctx, appPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []StorePath{filePath, linkPath, libPath, appPath}
	if strings.Join(keysOf(got), " ") != strings.Join(keysOf(want), " ") {
		t.Fatalf("closure = %v, want %v", got, want)
	}
	tfs := &tree.FS{Meta: e.db, CAS: e.store, Files: e.m, Faults: e.m}
	if err := tfs.AddViewer(ctx, Name, e.nix); err != nil {
		t.Fatal(err)
	}
	walk := func(p string) ninep.Node {
		t.Helper()
		n, err := tfs.Root(ctx)
		for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
			if err != nil {
				break
			}
			n, err = tfs.Lookup(ctx, n, name)
		}
		if err != nil {
			t.Fatalf("walk %s: %v", p, err)
		}
		return n
	}
	readAll := func(p string) string {
		t.Helper()
		n := walk(p)
		f, err := tfs.Open(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		b := make([]byte, n.Size)
		if _, err := f.ReadAt(b, 0); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		return string(b)
	}
	croot := "/nix/closure/" + string(appPath)
	if got := readAll(croot + "/nix/store/" + string(libPath) + "/lib/libfoo.so"); got != string(fixtureTree["lib/libfoo.so"].Data) {
		t.Fatal("closure file differs")
	}
	if got := readAll(croot + "/nix/store/" + string(filePath)); got != `{"a":1}` {
		t.Fatalf("file root in closure = %q", got)
	}
	// Plain 9P2000 hides the symlink member; the view itself has all four.
	ents, _, _, err := tfs.ReadDir(ctx, walk(croot+"/nix/store"), "", 100)
	if err != nil || len(ents) != 3 {
		t.Fatalf("closure store listing: %v, %v", ents, err)
	}
	des, err := e.nix.ViewReadDir(ctx, walk(croot+"/nix/store").Ino)
	if err != nil || len(des) != 4 || des[1].Kind != meta.KindSymlink {
		t.Fatalf("closure store view: %v, %v", des, err)
	}
	if _, err := tfs.Lookup(ctx, walk(croot+"/nix/store"), string(badPath)); err == nil {
		t.Error("a path outside the closure resolved in its view")
	}
	if _, err := tfs.Lookup(ctx, walk("/nix/closure"), string(nonePath)); err == nil {
		t.Error("closure of a missing path resolved")
	}
	// Members are units of their own, each downloaded once; the view
	// itself is metadata only.
	if err := e.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, sp := range want {
		u, err := e.db.Unit(ctx, Name, string(sp))
		if err != nil || u.State != meta.UnitResident {
			t.Errorf("member %s: %+v, %v", sp, u, err)
		}
	}
	if c := e.fx.count("nar/" + libPath.Hash() + ".nar.xz"); c != 1 {
		t.Errorf("lib NAR downloaded %d times", c)
	}
	units, _ := e.db.Units(ctx, Name)
	if len(units) != 4 {
		t.Errorf("%d units, want the 4 members only", len(units))
	}
	// While the view is in use, its members are pinned.
	u, _ := e.db.Unit(ctx, Name, string(libPath))
	if !e.m.Pinned(u.ID) {
		t.Error("member not pinned while the view is in use")
	}
	for i := 0; i < 200; i++ {
		idx.mu.Lock()
		n := len(idx.roots)
		idx.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if len(idx.roots) != 1 || idx.roots[0] != croot {
		t.Errorf("indexed roots = %v, want the closure once", idx.roots)
	}
}

func TestViewIdleUnpins(t *testing.T) {
	e := newEnv(t, Options{ViewIdle: 500 * time.Millisecond})
	ctx := e.ctx
	tfs := &tree.FS{Meta: e.db, CAS: e.store, Files: e.m, Faults: e.m}
	tfs.AddViewer(ctx, Name, e.nix)
	n, _ := tfs.Root(ctx)
	var err error
	for _, name := range []string{"nix", "closure", string(libPath), "nix", "store", string(libPath)} {
		if n, err = tfs.Lookup(ctx, n, name); err != nil {
			t.Fatal(err)
		}
	}
	e.m.Wait(ctx)
	u, _ := e.db.Unit(ctx, Name, string(libPath))
	if !e.m.Pinned(u.ID) {
		t.Fatal("not pinned in use")
	}
	time.Sleep(1500 * time.Millisecond)
	if e.m.Pinned(u.ID) {
		t.Fatal("still pinned after the view went idle")
	}
}

func TestEviction(t *testing.T) {
	var evicted []StorePath
	e := newEnv(t, Options{OnEvict: func(_ context.Context, p StorePath) error {
		evicted = append(evicted, p)
		if p == libPath {
			return errors.New("still alive")
		}
		return nil
	}})
	ctx := e.ctx
	for _, sp := range []StorePath{libPath, appPath, filePath} {
		if _, err := e.m.Ensure(ctx, Name, subsys.Key(sp), true); err != nil {
			t.Fatal(err)
		}
	}
	e.m.Wait(ctx)
	units, _ := e.db.Units(ctx, Name)
	byKey := map[string]meta.Unit{}
	for _, u := range units {
		byKey[u.Key] = u
	}
	lru := []meta.Unit{byKey[string(libPath)], byKey[string(filePath)], byKey[string(appPath)]}
	got, err := e.nix.EvictionOrder(ctx, lru)
	if err != nil {
		t.Fatal(err)
	}
	// lib and the file are referenced by the resident app: only app goes.
	if len(got) != 1 || got[0].Key != string(appPath) {
		t.Fatalf("EvictionOrder = %v, want only app", keys(got))
	}
	// Once app is evicted, its references become candidates, in LRU order.
	tx, _ := e.db.Begin(ctx)
	if err := tx.SetUnitState(byKey[string(appPath)].ID, meta.UnitResident, meta.UnitEvicted); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	got, _ = e.nix.EvictionOrder(ctx, lru[:2])
	if len(got) != 2 || got[0].Key != string(libPath) || got[1].Key != string(filePath) {
		t.Fatalf("EvictionOrder after app = %v", keys(got))
	}
	if err := e.nix.OnEvict(ctx, subsys.Key(filePath)); err != nil {
		t.Error(err)
	}
	if err := e.nix.OnEvict(ctx, subsys.Key(libPath)); err == nil {
		t.Error("hook error did not keep the path")
	}
	if err := e.nix.OnEvict(ctx, subsys.Key("closure:"+string(appPath))); err != nil || len(evicted) != 2 {
		t.Errorf("closure evict ran the hook: %v %v", err, evicted)
	}
}

func keysOf(ps []StorePath) []string {
	var out []string
	for _, p := range ps {
		out = append(out, string(p))
	}
	return out
}

func keys(us []meta.Unit) []string {
	var out []string
	for _, u := range us {
		out = append(out, path.Base(u.Key))
	}
	return out
}

func TestBinaryCache(t *testing.T) {
	e := newEnv(t, Options{})
	srv := httptest.NewServer(&BinaryCache{Nix: e.nix, Logf: t.Logf})
	defer srv.Close()
	get := func(p string) (int, []byte) {
		resp, err := http.Get(srv.URL + "/" + p)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	if code, b := get("nix-cache-info"); code != 200 || !strings.Contains(string(b), "StoreDir: /nix/store") {
		t.Fatalf("nix-cache-info: %d %s", code, b)
	}
	code, b := get(appPath.Hash() + ".narinfo")
	if code != 200 {
		t.Fatalf("narinfo: %d %s", code, b)
	}
	ni, err := ParseNarInfo(string(b))
	if err != nil {
		t.Fatal(err)
	}
	if ni.Compression != "none" || ni.URL != "nar/"+appPath.Hash()+".nar" || len(ni.Sigs) != 1 || len(ni.References) != 4 {
		t.Errorf("served narinfo %+v", ni)
	}
	code, b = get(ni.URL)
	if code != 200 || !bytes.Equal(b, e.fx.nars[appPath]) {
		t.Fatalf("NAR: %d, %d bytes, equal=%v", code, len(b), bytes.Equal(b, e.fx.nars[appPath]))
	}
	if sum := sha256.Sum256(b); FormatHash(sum[:]) != ni.NarHash {
		t.Error("served NAR does not match NarHash")
	}
	if code, _ := get(nonePath.Hash() + ".narinfo"); code != 404 {
		t.Errorf("missing narinfo: %d", code)
	}
	if code, _ := get("nar/" + nonePath.Hash() + ".nar"); code != 404 {
		t.Errorf("missing NAR: %d", code)
	}
	if code, _ := get("bogus.narinfo"); code != 404 {
		t.Errorf("bogus: %d", code)
	}
}

func mustLookup(t *testing.T, e *env, p string) uint64 {
	t.Helper()
	ino := meta.RootIno
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		in, err := e.db.Lookup(e.ctx, ino, name)
		if err != nil {
			t.Fatalf("lookup %s: %v", p, err)
		}
		ino = in.Ino
	}
	return ino
}
