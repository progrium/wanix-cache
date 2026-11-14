package apk

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/tree"
)

const repo = "v3.20/main/x86_64"

var hello = (&fixturePkg{name: "hello", version: "2.12-r1", arch: "x86_64", files: []fixtureEntry{
	{"usr/", ""},
	{"usr/bin/", ""},
	{"usr/bin/hello", "#!/bin/sh\necho hello\n"},
	{"usr/bin/hi", "->hello"},
	{"usr/share/doc/hello/README", "hello docs\n"}, // parents not listed
	{"usr/bin/hello2", "=>usr/bin/hello"},
	{"etc/", ""},
	{"etc/hello.conf", "greeting=hello\n"},
}}).build()

var busybox = (&fixturePkg{name: "busybox", version: "1.36.1-r29", arch: "x86_64", unsigned: true, files: []fixtureEntry{
	{"bin/", ""},
	{"bin/busybox", strings.Repeat("busybox!", 4096)},
	{"bin/sh", "->/bin/busybox"},
}}).build()

type harness struct {
	t      *testing.T
	db     *meta.DB
	store  *cas.Store
	fs     *tree.FS
	idx    *h9index.Store
	mgr    *fill.LocalManager
	apk    *APK
	up     *upstream
	mirror *httptest.Server
	clock  time.Time
	mu     sync.Mutex
}

func newHarness(t *testing.T, unpack bool) *harness {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := cas.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := fill.OpenDiskSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	q := &fill.LocalQuota{CAS: func() int64 { u, _ := store.Usage(); return u }, Spool: sp.Bytes}
	mgr, err := fill.NewManager(db, store, sp, q, fill.Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, db: db, store: store, mgr: mgr, up: newUpstream(), clock: time.Unix(1_800_000_000, 0)}
	upSrv := httptest.NewServer(h.up)
	h.up.set("/"+repo+"/APKINDEX.tar.gz", buildIndex(hello, busybox))
	h.up.set("/"+repo+"/"+hello.nv()+".apk", hello.apk)
	h.up.set("/"+repo+"/"+busybox.nv()+".apk", busybox.apk)
	idx, err := h9index.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	h.fs = &tree.FS{Meta: db, CAS: store, Files: mgr, Faults: mgr, Index: idx}
	h.idx = idx
	h.apk = New(db, Config{Upstream: upSrv.URL, Unpack: unpack, Logf: t.Logf, Indexer: h.fs.Indexer(idx, "")})
	h.apk.now = func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.clock }
	if err := mgr.Register(ctx, h.apk); err != nil {
		t.Fatal(err)
	}
	h.apk.Attach(mgr)
	mgr.Watch(func(ev fill.UnitEvent) {
		if ev.Err != nil {
			t.Logf("unit %s %v -> %v: %v", ev.Unit.Key, ev.From, ev.To, ev.Err)
		}
	})
	h.mirror = httptest.NewServer(h.apk)
	t.Cleanup(func() {
		h.mirror.Close()
		// Background unpacks and layer indexing log through t: let them
		// finish before the test does. Unpacks can start more indexing, so
		// wait for fills, then the subsystem, then fills again.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mgr.Wait(ctx)
		h.apk.Wait()
		mgr.Wait(ctx)
		h.apk.Wait()
		upSrv.Close()
		mgr.Close()
		db.Close()
	})
	return h
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.clock = h.clock.Add(d)
	h.mu.Unlock()
}

func (h *harness) get(p string, hdr ...string) (*http.Response, []byte) {
	h.t.Helper()
	req, _ := http.NewRequest("GET", h.mirror.URL+p, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, b
}

func (h *harness) wait() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.mgr.Wait(ctx); err != nil {
		h.t.Fatal(err)
	}
}

func TestParse(t *testing.T) {
	for _, p := range []*fixturePkg{hello, busybox} {
		var names []string
		got, err := Parse(bytes.NewReader(p.apk), func(h *tar.Header, r io.Reader) error {
			names = append(names, h.Name)
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", p.nv(), err)
		}
		if got.Checksum != p.checksum {
			t.Errorf("%s: checksum %s, want %s", p.nv(), got.Checksum, p.checksum)
		}
		if got.Signed == p.unsigned {
			t.Errorf("%s: signed = %v", p.nv(), got.Signed)
		}
		if got.Info["pkgname"] != p.name {
			t.Errorf("%s: .PKGINFO pkgname %q", p.nv(), got.Info["pkgname"])
		}
		if len(names) != len(p.files) {
			t.Errorf("%s: %d entries, want %d: %v", p.nv(), len(names), len(p.files), names)
		}
		// Hash-only mode must agree.
		got2, err := Parse(bytes.NewReader(p.apk), nil)
		if err != nil || got2.Checksum != p.checksum || got2.DataHash != got.DataHash {
			t.Errorf("%s: hash-only parse: %+v, %v", p.nv(), got2, err)
		}
	}
	// A flipped byte in the data stream fails datahash.
	bad := append([]byte(nil), hello.apk...)
	bad[len(bad)-20] ^= 0xff
	if _, err := Parse(bytes.NewReader(bad), nil); err == nil || !strings.Contains(err.Error(), "datahash") {
		t.Errorf("corrupt data stream: %v", err)
	}
}

func TestParseIndex(t *testing.T) {
	ents, err := ParseIndex(buildIndex(hello, busybox))
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 || ents[0].NV() != hello.nv() || ents[0].Checksum != hello.checksum ||
		ents[1].Size != int64(len(busybox.apk)) || ents[1].InstalledSize != busybox.installed {
		t.Fatalf("got %+v", ents)
	}
}

func TestMirror(t *testing.T) {
	h := newHarness(t, false)
	idxPath := "/" + repo + "/APKINDEX.tar.gz"
	apkPath := "/" + repo + "/" + hello.nv() + ".apk"

	resp, body := h.get(idxPath)
	if resp.StatusCode != 200 || !bytes.Equal(body, buildIndex(hello, busybox)) {
		t.Fatalf("index: %s, %d bytes", resp.Status, len(body))
	}
	etag := resp.Header.Get("ETag")
	if r, _ := h.get(idxPath, "If-None-Match", etag); r.StatusCode != http.StatusNotModified {
		t.Errorf("client revalidation: %s", r.Status)
	}
	if n := h.up.count(idxPath); n != 1 {
		t.Errorf("upstream index GETs = %d, want 1 inside the TTL", n)
	}

	// First .apk request streams from upstream through the spool.
	resp, body = h.get(apkPath)
	if resp.StatusCode != 200 || !bytes.Equal(body, hello.apk) {
		t.Fatalf("apk: %s, %d bytes, want %d", resp.Status, len(body), len(hello.apk))
	}
	h.wait()
	// Second comes from the cache.
	resp, body = h.get(apkPath)
	if resp.StatusCode != 200 || !bytes.Equal(body, hello.apk) {
		t.Fatalf("apk again: %s", resp.Status)
	}
	if n := h.up.count(apkPath); n != 1 {
		t.Errorf("upstream .apk GETs = %d, want 1", n)
	}
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		t.Errorf("Cache-Control %q", cc)
	}
	u, err := h.db.Unit(context.Background(), Name, string(rawKey(hello.sha1hex())))
	if err != nil || u.State != meta.UnitResident {
		t.Fatalf("raw unit: %+v, %v", u, err)
	}

	// After the TTL the index is revalidated by ETag: a 304 upstream.
	h.advance(2 * time.Minute)
	if r, b := h.get(idxPath); r.StatusCode != 200 || len(b) == 0 {
		t.Fatalf("index after TTL: %s", r.Status)
	}
	if n, nm := h.up.count(idxPath), h.up.notMod; n != 2 || nm != 1 {
		t.Errorf("after TTL: upstream GETs %d (want 2), 304s %d (want 1)", n, nm)
	}

	// Upstream down: a stale index is still served, and cached .apks too.
	h.up.mu.Lock()
	h.up.down = true
	h.up.mu.Unlock()
	h.advance(2 * time.Minute)
	if r, _ := h.get(idxPath); r.StatusCode != 200 {
		t.Errorf("stale index with upstream down: %s", r.Status)
	}
	if r, b := h.get(apkPath); r.StatusCode != 200 || !bytes.Equal(b, hello.apk) {
		t.Errorf("cached apk with upstream down: %s", r.Status)
	}

	// Unknown packages and paths 404.
	if r, _ := h.get("/" + repo + "/nope-1.0-r0.apk"); r.StatusCode != 404 {
		t.Errorf("unknown package: %s", r.Status)
	}
	if r, _ := h.get("/" + repo + "/../../etc/passwd"); r.StatusCode != 404 {
		t.Errorf("traversal: %s", r.Status)
	}
	req, _ := http.NewRequest("PUT", h.mirror.URL+apkPath, strings.NewReader("x"))
	if r, err := http.DefaultClient.Do(req); err != nil || r.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("PUT: %v %v", r.Status, err)
	}
}

func TestIndexChange(t *testing.T) {
	h := newHarness(t, false)
	idxPath := "/" + repo + "/APKINDEX.tar.gz"
	h.get(idxPath)
	// Upstream publishes an upgrade.
	hello2 := (&fixturePkg{name: "hello", version: "2.12-r2", arch: "x86_64", files: hello.files}).build()
	h.up.set(idxPath, buildIndex(hello2, busybox))
	h.up.set("/"+repo+"/"+hello2.nv()+".apk", hello2.apk)
	h.advance(2 * time.Minute)
	_, body := h.get(idxPath)
	if !bytes.Equal(body, buildIndex(hello2, busybox)) {
		t.Fatal("new index not served")
	}
	if r, b := h.get("/" + repo + "/" + hello2.nv() + ".apk"); r.StatusCode != 200 || !bytes.Equal(b, hello2.apk) {
		t.Fatalf("upgraded package: %s", r.Status)
	}
	// The superseded package is still described, so its unit can refetch.
	if _, err := h.apk.Describe(context.Background(), rawKey(hello.sha1hex())); err != nil {
		t.Errorf("superseded package: %v", err)
	}
}

func TestCorruptPackage(t *testing.T) {
	h := newHarness(t, false)
	h.get("/" + repo + "/APKINDEX.tar.gz")
	bad := append([]byte(nil), hello.apk...)
	bad[len(bad)-20] ^= 0xff
	h.up.mu.Lock()
	h.up.override["/"+repo+"/"+hello.nv()+".apk"] = bad
	h.up.mu.Unlock()
	h.get("/" + repo + "/" + hello.nv() + ".apk")
	h.wait()
	u, err := h.db.Unit(context.Background(), Name, string(rawKey(hello.sha1hex())))
	if err != nil || u.State != meta.UnitAbsent {
		t.Fatalf("corrupt fill should leave the unit absent: %+v, %v", u, err)
	}
	// Upstream fixed: the next request fetches a good copy.
	h.up.mu.Lock()
	delete(h.up.override, "/"+repo+"/"+hello.nv()+".apk")
	h.up.mu.Unlock()
	if r, b := h.get("/" + repo + "/" + hello.nv() + ".apk"); r.StatusCode != 200 || !bytes.Equal(b, hello.apk) {
		t.Fatalf("after fix: %s", r.Status)
	}
}

func TestOverlayLayer(t *testing.T) {
	h := newHarness(t, false)
	ctx := context.Background()
	h.get("/" + repo + "/APKINDEX.tar.gz")

	// Walk the namespace the way a 9P client does: every name is a lookup
	// that may fault in a unit.
	// (This is tree.FS's lookup, which is unexported.)
	lookup := func(dir uint64, name string) (meta.Inode, error) {
		in, err := h.db.Lookup(ctx, dir, name)
		if !errors.Is(err, meta.ErrNotExist) {
			return in, err
		}
		p, _ := h.db.PathOf(ctx, dir)
		retry, ferr := h.mgr.Miss(ctx, p, name)
		if ferr != nil || !retry {
			return in, err
		}
		return h.db.Lookup(ctx, dir, name)
	}
	walk := func(p string) (meta.Inode, error) {
		in, err := h.db.Get(ctx, meta.RootIno)
		for _, name := range strings.Split(p, "/") {
			if err != nil {
				return in, err
			}
			in, err = lookup(in.Ino, name)
		}
		return in, err
	}
	in, err := walk("apk/x86_64/" + hello.nv() + "/usr/bin/hello")
	if err != nil {
		t.Fatal(err)
	}
	f, err := h.mgr.Open(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(io.NewSectionReader(f, 0, in.Size))
	f.Close()
	if string(b) != "#!/bin/sh\necho hello\n" || in.Mode != 0o755 {
		t.Errorf("usr/bin/hello: %q mode %v", b, in.Mode)
	}
	h.wait()
	if l, err := walk("apk/x86_64/" + hello.nv() + "/usr/bin/hi"); err != nil || l.Kind != meta.KindSymlink || l.Target != "hello" {
		t.Errorf("symlink: %+v %v", l, err)
	}
	if l, err := walk("apk/x86_64/" + hello.nv() + "/usr/bin/hello2"); err != nil || l.Ino != in.Ino {
		t.Errorf("hard link: %+v %v, want ino %d", l, err, in.Ino)
	}
	if _, err := walk("apk/x86_64/" + hello.nv() + "/usr/share/doc/hello/README"); err != nil {
		t.Errorf("implicit parents: %v", err)
	}
	for _, p := range []string{".PKGINFO", ".post-install", ".SIGN.RSA.test.rsa.pub"} {
		if _, err := walk("apk/x86_64/" + hello.nv() + "/" + p); !errors.Is(err, meta.ErrNotExist) {
			t.Errorf("%s in the layer: %v", p, err)
		}
	}
	// The layer was built from the cached raw unit: one upstream GET total.
	if n := h.up.count("/" + repo + "/" + hello.nv() + ".apk"); n != 1 {
		t.Errorf("upstream .apk GETs = %d, want 1", n)
	}
	u, _ := h.db.Unit(ctx, Name, string(filesKey(hello.sha1hex())))
	if u.State != meta.UnitResident || u.LogicalBytes != hello.installed {
		t.Errorf("files unit: %+v, want resident with %d bytes", u, hello.installed)
	}
	// The layer root gets an /.h9index once its manifest is complete.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok, err := h.idx.Get(ctx, u.Root, h9index.DotL); err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("layer root has no /.h9index")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Unknown names under /apk do not fault anything.
	if _, err := walk("apk/x86_64/nope-1.0-r0"); !errors.Is(err, meta.ErrNotExist) {
		t.Errorf("unknown package: %v", err)
	}
	if _, err := walk("apk/riscv64"); !errors.Is(err, meta.ErrNotExist) {
		t.Errorf("unknown arch: %v", err)
	}
}

func TestUnpackAfterMirror(t *testing.T) {
	h := newHarness(t, true)
	h.get("/" + repo + "/APKINDEX.tar.gz")
	h.get("/" + repo + "/" + busybox.nv() + ".apk")
	deadline := time.Now().Add(5 * time.Second)
	for {
		u, err := h.db.Unit(context.Background(), Name, string(filesKey(busybox.sha1hex())))
		if err == nil && u.State == meta.UnitResident {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unpacked unit not resident: %+v %v", u, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := h.up.count("/" + repo + "/" + busybox.nv() + ".apk"); n != 1 {
		t.Errorf("upstream .apk GETs = %d, want 1", n)
	}
}
