package nix

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/tree"
)

// TestUpstream runs against cache.nixos.org: a 9P2000.L read of a store
// path and of a closure root with nothing cached, then real Nix
// substituting the closure from the binary cache endpoint (when Nix is
// installed), signatures checked. Set WANIX_NIX_UPSTREAM=1 to run it.
func TestUpstream(t *testing.T) {
	if os.Getenv("WANIX_NIX_UPSTREAM") == "" {
		t.Skip("set WANIX_NIX_UPSTREAM=1 to test against cache.nixos.org")
	}
	const hello StorePath = "18vypjxdydqyk36p0mi78jsq4i05br0s-hello-1.0.0.2"
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
	idx, err := h9index.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	tfs := &tree.FS{Meta: db, CAS: store, Files: m, Faults: m, Index: idx}
	n, _ := New(db, Options{Indexer: tfs.Indexer(idx, ""), Logf: t.Logf})
	if err := m.Register(ctx, n); err != nil {
		t.Fatal(err)
	}
	if err := n.Attach(ctx, m); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go tree.NewDotLServer(tfs).Serve(ln)
	bc := httptest.NewServer(&BinaryCache{Nix: n, Logf: t.Logf})
	t.Cleanup(func() { bc.Close(); ln.Close(); m.Close(); db.Close() })

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
	read := func(p string) []byte {
		t.Helper()
		_, f, err := root.Walk(strings.Split(strings.Trim(p, "/"), "/"))
		if err != nil {
			t.Fatalf("walk %s: %v", p, err)
		}
		defer f.Close()
		if _, _, err := f.Open(p9.ReadOnly); err != nil {
			t.Fatal(err)
		}
		_, _, attr, err := f.GetAttr(p9.AttrMaskAll)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, attr.Size)
		if _, err := f.ReadAt(b, 0); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		return b
	}
	start := time.Now()
	bin := read("/nix/store/" + string(hello) + "/bin/hello")
	t.Logf("cold 9P read of bin/hello: %d bytes in %v", len(bin), time.Since(start))
	if len(bin) < 4 || string(bin[1:4]) != "ELF" {
		t.Fatalf("bin/hello is not an ELF file")
	}
	start = time.Now()
	libc := read("/nix/closure/" + string(hello) + "/nix/store/1q857rs68rs3gijls9wdnsjqnwnv5zd8-glibc-2.44-57/lib/libc.so.6")
	t.Logf("closure root read of libc.so.6: %d bytes in %v", len(libc), time.Since(start))

	var index []byte
	for i := 0; i < 50 && len(index) == 0; i++ {
		if _, f, err := root.Walk([]string{"nix", "closure", string(hello), ".h9index"}); err == nil {
			f.Close()
			index = read("/nix/closure/" + string(hello) + "/.h9index")
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if len(index) == 0 {
		t.Fatal("closure root has no /.h9index")
	}
	t.Logf("closure /.h9index: %d bytes", len(index))

	nixBin, err := exec.LookPath("nix")
	if err != nil {
		t.Log("nix not installed; skipping substitution")
		return
	}
	dst := filepath.Join(t.TempDir(), "store")
	out, err := exec.Command(nixBin, "copy", "--from", bc.URL, "--to", dst, hello.Abs()).CombinedOutput()
	if err != nil {
		t.Fatalf("nix copy: %v\n%s", err, out)
	}
	out, err = exec.Command(nixBin, "store", "verify", "--store", dst, "-r", hello.Abs()).CombinedOutput()
	if err != nil {
		t.Fatalf("nix store verify: %v\n%s", err, out)
	}
}
