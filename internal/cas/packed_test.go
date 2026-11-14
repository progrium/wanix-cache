package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// packTests, when set, makes openStore turn small-blob packing on, so the
// store tests run again over the split store.
var packTests int

func openStore(t *testing.T, quota int64) (*Store, error) {
	dir := t.TempDir()
	if packTests > 0 {
		if err := EnablePacking(dir, packTests); err != nil {
			t.Fatal(err)
		}
	}
	return Open(dir, quota)
}

func TestPacked(t *testing.T) {
	packTests = 16 << 10
	defer func() { packTests = 0 }()
	t.Run("PutDeterministicAndDeduped", TestPutDeterministicAndDeduped)
	t.Run("Quota", TestQuota)
	t.Run("BlobsRemoveAndHolds", TestBlobsRemoveAndHolds)
	t.Run("ReclaimerBeforeFull", TestReclaimerBeforeFull)
	t.Run("CancelledStatsDoNotWedge", TestCancelledStatsDoNotWedge)
	t.Run("SmallFilesPacked", testSmallFilesPacked)
}

func testSmallFilesPacked(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := EnablePacking(dir, 16<<10); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	var datas [][]byte
	for i := range 50 {
		d := make([]byte, 100+i*37)
		rand.Read(d)
		ref, err := s.Put(ctx, bytes.NewReader(d))
		if err != nil {
			t.Fatal(err)
		}
		refs, datas = append(refs, ref), append(datas, d)
	}
	// Nothing went to localdisk: no .dat files outside blobs/packed.
	filepath.WalkDir(filepath.Join(dir, "blobs"), func(p string, d os.DirEntry, err error) error {
		if d != nil && d.IsDir() && d.Name() == packedDir {
			return filepath.SkipDir
		}
		if d != nil && filepath.Ext(p) == ".dat" && d.Name() != "GENERATION.dat" {
			t.Errorf("small blob in localdisk: %s", p)
		}
		return nil
	})
	// Reopen: everything reads back through the split store.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range refs {
		f, err := s.Open(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(io.NewSectionReader(f, 0, int64(len(datas[i]))))
		f.Close()
		if err != nil || !bytes.Equal(got, datas[i]) {
			t.Fatalf("%s: %d bytes, %v", ref, len(got), err)
		}
	}
	s.Close()
}
