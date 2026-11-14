package apk

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/progrium/wanix-cache/internal/gc"
	"github.com/progrium/wanix-cache/internal/usage"
)

// TestUsage covers usage stats end to end on APK: mirror installs count
// per package, overlay opens count per path and object, an eviction and
// refault counts as a refetch, and the history survives the eviction.
func TestUsage(t *testing.T) {
	h := newHarness(t, false)
	ctx := context.Background()
	tr, err := usage.New(ctx, h.db, h.mgr)
	if err != nil {
		t.Fatal(err)
	}
	tr.Attach(h.mgr)

	h.get("/" + repo + "/APKINDEX.tar.gz")
	h.get("/" + repo + "/" + hello.nv() + ".apk")
	h.get("/" + repo + "/" + hello.nv() + ".apk")
	h.wait()

	read := func(rel string) {
		t.Helper()
		f, in, err := h.mgr.OpenPath(ctx, Name, filesKey(hello.sha1hex()), rel)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, io.NewSectionReader(f, 0, in.Size))
		f.Close()
	}
	read("usr/bin/hello")
	read("usr/bin/hello")
	read("usr/bin/hello2") // hard link: same inode, counted at one name
	h.wait()
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	r, err := usage.Build(ctx, h.db, usage.Query{Untouched: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Packages) != 1 {
		t.Fatalf("packages: %+v", r.Packages)
	}
	p := r.Packages[0]
	// 2 mirror GETs + 3 overlay opens; one upstream fetch (the unpacked
	// unit is derived from the raw one and does not count).
	if p.Name != "hello" || p.Version != "2.12-r1" || p.Opens != 5 || p.Fetches != 1 || p.Refetches != 0 || p.DaysUsed != 1 {
		t.Errorf("package: %+v", p)
	}
	paths := map[string]int64{}
	for _, ps := range r.Paths {
		paths[ps.Path] = ps.Opens
	}
	if paths["usr/bin/hello"]+paths["usr/bin/hello2"] != 3 || paths[hello.nv()+".apk"] != 2 {
		t.Errorf("paths: %v", paths)
	}
	var obj *usage.ObjectStat
	for i := range r.Objects {
		if r.Objects[i].Opens == 3 {
			obj = &r.Objects[i]
		}
	}
	if obj == nil || obj.Paths != 1 || obj.Size != 21 {
		t.Errorf("hello's object should have 3 opens: %+v", r.Objects)
	}
	// Never opened: README and hello.conf in the unpacked unit (hello2 is
	// the opened hello under another name).
	var un *usage.UnitUntouched
	for i := range r.Untouched {
		if r.Untouched[i].Key == string(filesKey(hello.sha1hex())) {
			un = &r.Untouched[i]
		}
	}
	if un == nil || un.Files != 4 || un.Untouched != 2 || len(un.List) != 2 || un.Package != "hello-2.12-r1" {
		t.Fatalf("untouched: %+v", un)
	}
	got := []string{un.List[0].Path, un.List[1].Path}
	if strings.Join(got, ",") != "etc/hello.conf,usr/share/doc/hello/README" {
		t.Errorf("untouched files: %v", got)
	}
	var text bytes.Buffer
	if err := r.WriteText(&text); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"PACKAGES", "hello", "NEVER OPENED", "usr/share/doc/hello/README"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, text.String())
		}
	}

	// Evict the raw unit and fetch it again through the mirror: a refetch,
	// and the history is still there.
	col, err := gc.New(ctx, h.db, h.st, gc.Config{})
	if err != nil {
		t.Fatal(err)
	}
	col.Attach(h.mgr)
	u, _ := h.db.Unit(ctx, Name, string(rawKey(hello.sha1hex())))
	if _, err := col.Evict(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	if r, b := h.get("/" + repo + "/" + hello.nv() + ".apk"); r.StatusCode != 200 || !bytes.Equal(b, hello.apk) {
		t.Fatalf("refetch: %s", r.Status)
	}
	h.wait()
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	r, _ = usage.Build(ctx, h.db, usage.Query{})
	p = r.Packages[0]
	if p.Opens != 6 || p.Fetches != 1 || p.Refetches != 1 {
		t.Errorf("after evict and refetch: %+v", p)
	}
}
