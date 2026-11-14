package oci

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
	"github.com/progrium/wanix-cache/internal/tree"
)

var amd64 = Platform{OS: "linux", Architecture: "amd64"}

type env struct {
	db   *meta.DB
	m    *fill.LocalManager
	oci  *OCI
	fs   *tree.FS
	root p9.File
}

func newEnv(t *testing.T, opts Options) *env {
	t.Helper()
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
	if opts.Logf == nil {
		opts.Logf = t.Logf
	}
	if opts.Client == nil {
		opts.Client = &Client{PerRegistry: 4}
	}
	idx, err := h9index.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	fsys := &tree.FS{Meta: db, CAS: store, Files: m, Faults: m, Index: idx}
	opts.Indexer = fsys.Indexer(idx, "")
	o := New(db, m, opts)
	if err := m.Register(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := fsys.AddViewer(ctx, Name, o); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go tree.NewDotLServer(fsys).Serve(ln)
	c, _ := net.Dial("tcp", ln.Addr().String())
	cl, err := p9.NewClient(c)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cl.Attach("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close(); ln.Close(); o.Close(); m.Close(); db.Close() })
	return &env{db: db, m: m, oci: o, fs: fsys, root: root}
}

func (e *env) walk(p string) (p9.File, error) {
	_, f, err := e.root.Walk(strings.Split(strings.Trim(p, "/"), "/"))
	return f, err
}

func (e *env) read(t testing.TB, p string) string {
	t.Helper()
	f, err := e.walk(p)
	if err != nil {
		t.Fatalf("walk %s: %v", p, err)
	}
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
	return string(buf[:n])
}

func (e *env) ls(t testing.TB, p string) []string {
	t.Helper()
	f, err := e.walk(p)
	if err != nil {
		t.Fatalf("walk %s: %v", p, err)
	}
	defer f.Close()
	if _, _, err := f.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	var names []string
	for off := uint64(0); ; {
		des, err := f.Readdir(off, 64*1024)
		if err != nil {
			t.Fatalf("readdir %s: %v", p, err)
		}
		if len(des) == 0 {
			break
		}
		for _, d := range des {
			names = append(names, d.Name)
			off = d.Offset
		}
	}
	return names
}

func (e *env) missing(t testing.TB, p string) {
	t.Helper()
	if f, err := e.walk(p); err == nil {
		f.Close()
		t.Errorf("%s exists, want it hidden", p)
	}
}

// threeLayers is a base, a middle layer with whiteouts and an opaque
// directory, and a zstd top layer.
func threeLayers(t testing.TB) []layer {
	base := makeLayer(t, "gzip",
		dir("bin"), file("bin/sh", "#!sh"), hardlink("bin/sh2", "bin/sh"), symlink("bin/ash", "sh"),
		dir("etc"), file("etc/passwd", "root:x:0:0\n"), file("etc/hosts", "127.0.0.1 localhost\n"),
		dir("usr"), dir("usr/lib"), file("usr/lib/a", "a"), file("usr/lib/b", "b"),
		dir("opt"), dir("opt/x"), file("opt/x/y", "y"),
		dir("var"), dir("var/cache"), file("var/cache/c1", "c1"))
	mid := makeLayer(t, "gzip",
		dir("etc"), file("etc/.wh.hosts", ""), file("etc/passwd", "root:x:0:0:new\n"),
		dir("usr/lib"), file("usr/lib/.wh..wh..opq", ""), file("usr/lib/c", "c"),
		file("srv/data/f", "no parent entries")) // implicit directories
	top := makeLayer(t, "zstd",
		file(".wh.opt", ""), file("var/cache/c2", "c2"))
	return []layer{base, mid, top}
}

func TestMergedRootfs(t *testing.T) {
	reg := newRegistry(t)
	ls := threeLayers(t)
	img := reg.push("test/img", "", amd64, ls...)
	arm := reg.push("test/img", "", Platform{OS: "linux", Architecture: "arm64"}, makeLayer(t, "gzip", file("arch", "arm64")))
	reg.index("test/img", "v1", map[string]Platform{img: amd64, arm: {OS: "linux", Architecture: "arm64"}})
	e := newEnv(t, Options{})
	r := "/oci/" + reg.host() + "/test/img@v1"

	if got := e.read(t, r+"/etc/passwd"); got != "root:x:0:0:new\n" {
		t.Errorf("etc/passwd = %q, want the middle layer's", got)
	}
	if got := e.read(t, r+"/bin/sh"); got != "#!sh" {
		t.Errorf("bin/sh = %q", got)
	}
	if got := e.read(t, r+"/bin/sh2"); got != "#!sh" {
		t.Errorf("hard link bin/sh2 = %q", got)
	}
	f, err := e.walk(r + "/bin/ash")
	if err != nil {
		t.Fatal(err)
	}
	if target, err := f.Readlink(); err != nil || target != "sh" {
		t.Errorf("readlink bin/ash = %q, %v", target, err)
	}
	f.Close()
	e.missing(t, r+"/etc/hosts") // whited out
	e.missing(t, r+"/usr/lib/a") // below an opaque directory
	e.missing(t, r+"/opt")       // whited out directory
	e.missing(t, r+"/opt/x/y")   // and what was in it
	e.missing(t, r+"/etc/.wh.hosts")
	if got := e.read(t, r+"/srv/data/f"); got != "no parent entries" {
		t.Errorf("srv/data/f = %q", got)
	}

	check := func(p string, want ...string) {
		t.Helper()
		var got []string
		for _, n := range e.ls(t, r+p) {
			if p == "/" && n == h9index.Name {
				continue // generated once the layers finish; checked below
			}
			got = append(got, n)
		}
		sort.Strings(want)
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("ls %s = %v, want %v", p, got, want)
		}
	}
	check("/", "bin", "etc", "srv", "usr", "var")
	check("/etc", "passwd")
	check("/usr/lib", "c")
	check("/var/cache", "c1", "c2")
	check("/bin", "ash", "sh", "sh2")

	// The namespace above the image lists what is known.
	if got := e.ls(t, "/oci/"+reg.host()+"/test"); strings.Join(got, " ") != "img@"+img+" img@v1" {
		t.Errorf("ls repo dir = %v", got)
	}
	// A digest reference to the platform image is the same rootfs.
	if got := e.read(t, "/oci/"+reg.host()+"/test/img@"+img+"/etc/passwd"); got != "root:x:0:0:new\n" {
		t.Errorf("by digest: %q", got)
	}
	e.missing(t, "/oci/"+reg.host()+"/test/img@nope")
	e.missing(t, "/oci/"+reg.host()+"/test/img@v1/arch") // the arm64 image's file

	if err := e.m.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, l := range ls {
		if n := reg.gets(l.digest); n != 1 {
			t.Errorf("layer %s downloaded %d times, want 1", l.digest[:19], n)
		}
		u, err := e.db.Unit(context.Background(), Name, l.diffID)
		if err != nil || u.State != meta.UnitResident {
			t.Errorf("layer unit %s: %v %v", l.diffID[:19], u.State, err)
		}
	}
	if reg.tokens.Load() == 0 {
		t.Error("no token was requested")
	}
	// Layers are also plain units at /oci/layers/<diff_id>/.
	if got := e.read(t, "/oci/layers/"+ls[0].diffID+"/etc/hosts"); got != "127.0.0.1 localhost\n" {
		t.Errorf("base layer etc/hosts = %q", got)
	}
	// After ingest the merged view reads from the CAS just the same.
	if got := e.read(t, r+"/var/cache/c2"); got != "c2" {
		t.Errorf("var/cache/c2 after ingest = %q", got)
	}
	// The merged rootfs gets an index once its layers are final.
	deadline := time.Now().Add(10 * time.Second)
	for {
		f, err := e.walk(r + "/" + h9index.Name)
		if err == nil {
			f.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s in the rootfs: %v", h9index.Name, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if idx := e.read(t, r+"/"+h9index.Name); !strings.Contains(idx, "passwd") || strings.Contains(idx, "hosts") {
		t.Errorf("index does not match the merged tree:\n%s", idx)
	}
	// The compressed digest maps to the diff_id.
	if d, err := e.oci.st.diffID(context.Background(), ls[2].digest); err != nil || d != ls[2].diffID {
		t.Errorf("diffID(%s) = %s, %v", ls[2].digest[:19], d, err)
	}
}

// A name the upper layers answer is served while a lower layer is still
// downloading; a name only the lower layer has waits for it.
func TestLookupWaitsOnlyForUpperLayers(t *testing.T) {
	reg := newRegistry(t)
	ls := threeLayers(t)
	reg.push("test/img", "v1", amd64, ls...)
	open := reg.gate(ls[0].digest)
	e := newEnv(t, Options{})
	r := "/oci/" + reg.host() + "/test/img@v1"

	done := make(chan string, 1)
	go func() { done <- e.read(t, r+"/etc/passwd") }()
	select {
	case got := <-done:
		if got != "root:x:0:0:new\n" {
			t.Errorf("etc/passwd = %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lookup answered by the middle layer waited on the base layer")
	}

	base := make(chan string, 1)
	go func() { base <- e.read(t, r+"/bin/sh") }()
	select {
	case got := <-base:
		t.Fatalf("bin/sh read %q before the base layer finished", got)
	case <-time.After(300 * time.Millisecond):
	}
	close(open)
	select {
	case got := <-base:
		if got != "#!sh" {
			t.Errorf("bin/sh = %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("bin/sh never resolved")
	}
}

func TestDownloadsCappedPerRegistry(t *testing.T) {
	reg := newRegistry(t)
	reg.slow = 150 * time.Millisecond
	var ls []layer
	for i := 0; i < 6; i++ {
		ls = append(ls, makeLayer(t, "gzip", file("f"+string(rune('a'+i)), strings.Repeat("x", i+1))))
	}
	reg.push("test/many", "v1", amd64, ls...)
	e := newEnv(t, Options{Client: &Client{PerRegistry: 2}})
	var got []string
	for _, n := range e.ls(t, "/oci/"+reg.host()+"/test/many@v1") {
		if n != h9index.Name { // present once the rootfs has been indexed
			got = append(got, n)
		}
	}
	if len(got) != 6 {
		t.Fatalf("ls = %v", got)
	}
	if n := reg.maxFlight.Load(); n != 2 {
		t.Errorf("%d concurrent downloads, want exactly the cap of 2", n)
	}
}

func TestCorruptLayerFails(t *testing.T) {
	reg := newRegistry(t)
	l := makeLayer(t, "gzip", file("a", strings.Repeat("a", 4096)))
	reg.push("test/bad", "v1", amd64, l)
	reg.mu.Lock()
	reg.corrupt[l.digest] = true
	reg.mu.Unlock()
	e := newEnv(t, Options{})
	r := "/oci/" + reg.host() + "/test/bad@v1"
	if f, err := e.walk(r + "/b"); err == nil {
		f.Close()
		t.Fatal("lookup in a corrupt image succeeded")
	}
	u, err := e.db.Unit(context.Background(), Name, l.diffID)
	if err == nil && u.State != meta.UnitAbsent {
		t.Errorf("corrupt layer left in state %v", u.State)
	}
	// Fixed upstream: the next access rebuilds the view and fetches again.
	reg.mu.Lock()
	reg.corrupt[l.digest] = false
	reg.mu.Unlock()
	if got := e.read(t, r+"/a"); len(got) != 4096 {
		t.Errorf("after repair read %d bytes", len(got))
	}
}

func TestImageLevelEviction(t *testing.T) {
	ctx := context.Background()
	reg := newRegistry(t)
	shared := makeLayer(t, "gzip", file("etc/os-release", "ID=base\n"))
	a := makeLayer(t, "gzip", file("app-a", "a"))
	b := makeLayer(t, "gzip", file("app-b", "b"))
	imgA := reg.push("test/a", "v1", amd64, shared, a)
	imgB := reg.push("test/b", "v1", amd64, shared, b)
	e := newEnv(t, Options{IdleTimeout: time.Millisecond})
	e.read(t, "/oci/"+reg.host()+"/test/a@v1/app-a")
	time.Sleep(5 * time.Millisecond)
	e.read(t, "/oci/"+reg.host()+"/test/b@v1/app-b")

	if err := e.oci.OnEvict(ctx, subsys.Key(shared.diffID)); !errors.Is(err, ErrLayerInUse) {
		t.Errorf("OnEvict(shared) = %v, want ErrLayerInUse", err)
	}
	if _, err := e.oci.EvictImage(ctx, imgA); !errors.Is(err, ErrImageInUse) {
		t.Errorf("EvictImage while served = %v, want ErrImageInUse", err)
	}
	time.Sleep(5 * time.Millisecond)
	e.oci.releaseIdle(time.Now())

	cs, err := e.oci.Candidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 2 || cs[0].Image != imgA || cs[1].Image != imgB {
		t.Fatalf("candidates = %+v, want a then b", cs)
	}
	if len(cs[0].Layers) != 1 || cs[0].Layers[0] != subsys.Key(a.diffID) {
		t.Errorf("a frees %v, want only its own layer", cs[0].Layers)
	}
	freed, err := e.oci.EvictImage(ctx, imgA)
	if err != nil || len(freed) != 1 || freed[0] != subsys.Key(a.diffID) {
		t.Fatalf("EvictImage(a) = %v, %v", freed, err)
	}
	if err := e.oci.OnEvict(ctx, subsys.Key(a.diffID)); err != nil {
		t.Errorf("OnEvict(a's layer) = %v", err)
	}
	if err := e.oci.OnEvict(ctx, subsys.Key(shared.diffID)); !errors.Is(err, ErrLayerInUse) {
		t.Errorf("shared layer evictable while b is live: %v", err)
	}
	freed, err = e.oci.EvictImage(ctx, imgB)
	if err != nil || len(freed) != 2 {
		t.Fatalf("EvictImage(b) = %v, %v, want b's layer and the shared one", freed, err)
	}
	if err := e.oci.OnEvict(ctx, subsys.Key(shared.diffID)); err != nil {
		t.Errorf("OnEvict(shared) after both images = %v", err)
	}
	// Using an image again makes it live.
	e.read(t, "/oci/"+reg.host()+"/test/a@v1/app-a")
	if err := e.oci.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.oci.OnEvict(ctx, subsys.Key(shared.diffID)); !errors.Is(err, ErrLayerInUse) {
		t.Errorf("shared layer evictable after a was used again: %v", err)
	}
}

func TestTagFallsBackWhenRegistryDown(t *testing.T) {
	reg := newRegistry(t)
	reg.push("test/img", "v1", amd64, makeLayer(t, "gzip", file("x", "x")))
	e := newEnv(t, Options{TagTTL: time.Nanosecond})
	r := Ref{Registry: reg.host(), Repo: "test/img", Tag: "v1"}
	im, err := e.oci.ResolveImage(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	reg.srv.Close()
	im2, err := e.oci.ResolveImage(context.Background(), r)
	if err != nil || im2.Digest != im.Digest {
		t.Fatalf("offline resolve = %v, %v", im2.Digest, err)
	}
}

func TestParseRef(t *testing.T) {
	for in, want := range map[string]string{
		"docker.io/alpine@3.20":              "docker.io/library/alpine@3.20",
		"index.docker.io/library/alpine@3":   "docker.io/library/alpine@3",
		"ghcr.io/a/b/c@latest":               "ghcr.io/a/b/c@latest",
		"localhost:5000/x@sha256:" + hex64(): "localhost:5000/x@sha256:" + hex64(),
	} {
		r, err := ParseRef(in)
		if err != nil || r.String() != want {
			t.Errorf("ParseRef(%q) = %v, %v; want %s", in, r, err, want)
		}
	}
	for _, bad := range []string{"alpine@3", "docker.io/alpine", "docker.io/Alpine@3", "docker.io/a@", "docker.io/a@sha256:xyz", "docker.io/a@-x"} {
		if _, err := ParseRef(bad); err == nil {
			t.Errorf("ParseRef(%q) succeeded", bad)
		}
	}
	if h := (Ref{Registry: "docker.io"}).Host(); h != "registry-1.docker.io" {
		t.Errorf("docker.io host = %s", h)
	}
}

func hex64() string { return strings.Repeat("ab", 32) }

func TestParseChallenge(t *testing.T) {
	s, p := parseChallenge(`Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull"`)
	if s != "Bearer" || p["realm"] != "https://auth.docker.io/token" || p["service"] != "registry.docker.io" || p["scope"] != "repository:library/alpine:pull" {
		t.Errorf("got %s %v", s, p)
	}
	s, p = parseChallenge(`Bearer realm=https://x/token,service=svc`)
	if s != "Bearer" || p["realm"] != "https://x/token" || p["service"] != "svc" {
		t.Errorf("unquoted: %s %v", s, p)
	}
}

// TestLive pulls a real image from Docker Hub. Run with WANIX_OCI_LIVE=1.
func TestLive(t *testing.T) {
	if os.Getenv("WANIX_OCI_LIVE") == "" {
		t.Skip("set WANIX_OCI_LIVE=1 to pull from Docker Hub")
	}
	e := newEnv(t, Options{})
	r := "/oci/docker.io/library/alpine@3.20"
	got := e.read(t, r+"/etc/os-release")
	if !strings.Contains(got, "Alpine") {
		t.Errorf("os-release = %q", got)
	}
	t.Logf("ls / = %v", e.ls(t, r))
	f, err := e.walk(r + "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	target, _ := f.Readlink()
	t.Logf("/bin/sh -> %s", target)
}

func TestParentOfVirtualDirs(t *testing.T) {
	reg := newRegistry(t)
	reg.push("test/img", "v1", amd64, threeLayers(t)...)
	e := newEnv(t, Options{})
	ctx := context.Background()
	n, err := e.fs.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var chain []uint64
	for _, name := range []string{"oci", reg.host(), "test", "img@v1", "var", "cache"} {
		if n, err = e.fs.Lookup(ctx, n, name); err != nil {
			t.Fatalf("lookup %s: %v", name, err)
		}
		chain = append(chain, n.Ino)
	}
	cache := n
	for i := len(chain) - 2; i >= 0; i-- {
		if n, err = e.fs.Parent(ctx, n); err != nil || n.Ino != chain[i] {
			t.Fatalf("parent at depth %d = %d, %v; want %d", i, n.Ino, err, chain[i])
		}
	}
	n = cache
	ents, _, done, err := e.fs.ReadDir(ctx, n, "", 1)
	if err != nil || len(ents) != 1 || ents[0].Name != "c1" || done {
		t.Fatalf("plain ReadDir page 1 = %v, %v, %v", ents, done, err)
	}
	ents, _, done, err = e.fs.ReadDir(ctx, n, "c1", 10)
	if err != nil || len(ents) != 1 || ents[0].Name != "c2" || !done {
		t.Fatalf("plain ReadDir page 2 = %v, %v, %v", ents, done, err)
	}
}

func TestEvictionOrder(t *testing.T) {
	ctx := context.Background()
	reg := newRegistry(t)
	shared := makeLayer(t, "gzip", file("base", "base"))
	a := makeLayer(t, "gzip", file("a", "a"))
	b := makeLayer(t, "gzip", file("b", "b"))
	reg.push("test/a", "v1", amd64, shared, a)
	imgB := reg.push("test/b", "v1", amd64, shared, b)
	e := newEnv(t, Options{IdleTimeout: time.Millisecond})
	e.read(t, "/oci/"+reg.host()+"/test/a@v1/a")
	time.Sleep(5 * time.Millisecond)
	e.read(t, "/oci/"+reg.host()+"/test/b@v1/b")
	if err := e.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	e.oci.releaseIdle(time.Now())
	var units []meta.Unit
	for _, l := range []layer{shared, b, a} {
		u, err := e.db.Unit(ctx, Name, l.diffID)
		if err != nil {
			t.Fatal(err)
		}
		units = append(units, u)
	}
	// Nothing is orphaned, so the LRU image (a) is evicted and frees only
	// its own layer.
	got, err := e.oci.EvictionOrder(ctx, units)
	if err != nil || len(got) != 1 || got[0].Key != a.diffID {
		t.Fatalf("first order = %v, %v; want a's layer", got, err)
	}
	got, err = e.oci.EvictionOrder(ctx, units)
	if err != nil || len(got) != 1 || got[0].Key != a.diffID {
		t.Fatalf("second order = %v, %v; want a's layer again (still orphaned)", got, err)
	}
	if _, err := e.oci.EvictImage(ctx, imgB); err != nil {
		t.Fatal(err)
	}
	got, _ = e.oci.EvictionOrder(ctx, units)
	if len(got) != 3 {
		t.Errorf("after both images, order = %v, want all three layers", got)
	}
}
