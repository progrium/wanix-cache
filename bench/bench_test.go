package bench

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadersAgree checks that all six readers return the same bytes for
// every file of a small tree, with paths deeper than one Twalk can carry,
// a hard link, a symlink and an empty file, and that each workload runs.
func TestReadersAgree(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	classes := []SizeClass{{"1K", 1 << 10, 3}, {"200K", 200 << 10, 2}, {"3M", 3 << 20, 1}}
	if err := GenSynthetic(filepath.Join(src, "syn"), classes, 1); err != nil {
		t.Fatal(err)
	}
	deep := filepath.Join(src, strings.Repeat("d/", 20))
	os.MkdirAll(deep, 0o755)
	os.WriteFile(filepath.Join(deep, "leaf"), []byte("deep leaf\n"), 0o644)
	os.WriteFile(filepath.Join(src, "empty"), nil, 0o644)
	os.Link(filepath.Join(src, "syn", "1K", "f0000"), filepath.Join(src, "hard"))
	os.Symlink("empty", filepath.Join(src, "link"))

	fx, err := Load(ctx, "t", src, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fx.Close()
	if len(fx.Files) != 3+2+1+4 { // syn files; leaf, empty, hard, syn.done
		t.Fatalf("collected %d files", len(fx.Files))
	}
	s, err := StartServers(fx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	want := map[string][32]byte{}
	for _, f := range fx.Files {
		b, err := os.ReadFile(f.Disk)
		if err != nil {
			t.Fatal(err)
		}
		want[f.Rel] = sha256.Sum256(b)
	}
	for _, name := range ReaderNames {
		r, err := NewReader(name, fx, s)
		if err != nil {
			t.Fatal(err)
		}
		for i := range fx.Files {
			f := &fx.Files[i]
			h, err := r.Open(f)
			if err != nil {
				t.Fatalf("%s: open %s: %v", name, f.Rel, err)
			}
			var buf bytes.Buffer
			b := make([]byte, 50<<10) // not a chunk or page multiple
			for off := int64(0); off < f.Size; {
				n, err := h.ReadAt(b[:min(int64(len(b)), f.Size-off)], off)
				if n == 0 {
					t.Fatalf("%s: %s: read at %d: %v", name, f.Rel, off, err)
				}
				buf.Write(b[:n])
				off += int64(n)
			}
			h.Close()
			if sha256.Sum256(buf.Bytes()) != want[f.Rel] {
				t.Errorf("%s: %s: bytes differ", name, f.Rel)
			}
		}
		if res, err := ReadAll(r, fx.Files, 3); err != nil || res.Files != len(fx.Files) {
			t.Errorf("%s: ReadAll = %+v, %v", name, res, err)
		}
		if _, err := Head(r, fx.Files, 4096, 1); err != nil {
			t.Errorf("%s: Head: %v", name, err)
		}
		big := []File{}
		for _, f := range fx.Files {
			if f.Size >= 1<<20 {
				big = append(big, f)
			}
		}
		if lat, err := Random(r, big, 50, 4096, 7); err != nil || len(lat) != 50 {
			t.Errorf("%s: Random: %d ops, %v", name, len(lat), err)
		}
		r.Close()
	}
	u, err := fx.DiskUsage()
	if err != nil || u.CASFiles == 0 || u.PlainBytes == 0 {
		t.Fatalf("usage %+v, %v", u, err)
	}
	fmt.Fprintf(os.Stderr, "usage: %+v\n", u)
}
