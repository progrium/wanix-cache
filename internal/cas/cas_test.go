package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
)

func TestPutDeterministicAndDeduped(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1<<20)
	rand.Read(data)
	a, err := s.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	used, _ := s.Usage()
	b, err := s.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("same bytes, different refs: %s vs %s", a, b)
	}
	if again, _ := s.Usage(); again != used {
		t.Fatalf("second put grew the store from %d to %d bytes", used, again)
	}
	f, err := s.Open(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, len(data)+10)
	n, err := f.ReadAt(got, 0)
	if n != len(data) || err != io.EOF || !bytes.Equal(got[:n], data) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
}

func TestQuota(t *testing.T) {
	s, err := Open(t.TempDir(), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 512<<10)
	rand.Read(data)
	if _, err := s.Put(context.Background(), bytes.NewReader(data)); err == nil {
		t.Fatal("put over quota succeeded")
	}
}

func TestBlobsRemoveAndHolds(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 2<<20)
	rand.Read(data)
	sess := s.Session()
	ref, err := sess.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := s.Blobs(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	var refs []string
	for _, b := range blobs {
		total += b.Size
		refs = append(refs, b.Ref)
	}
	if used, _ := s.Usage(); used != total || len(blobs) < 3 {
		t.Fatalf("%d blobs totalling %d; store holds %d", len(blobs), total, used)
	}
	// Held by the open session: nothing is removed.
	if removed, _, err := s.Remove(ctx, refs, nil); err != nil || len(removed) != 0 {
		t.Fatalf("removed %d held blobs, %v", len(removed), err)
	}
	sess.Close()
	// confirm can veto.
	if removed, _, _ := s.Remove(ctx, refs, func([]string) ([]string, error) { return nil, nil }); len(removed) != 0 {
		t.Fatal("removed blobs confirm rejected")
	}
	removed, freed, err := s.Remove(ctx, refs, nil)
	if err != nil || len(removed) != len(refs) || freed != total {
		t.Fatalf("removed %d, freed %d of %d: %v", len(removed), freed, total, err)
	}
	if used, _ := s.Usage(); used != 0 {
		t.Fatalf("usage %d after removing everything", used)
	}
	if _, err := s.Blobs(ctx, ref); err == nil {
		t.Fatal("walk of removed content succeeded")
	}
	n := 0
	s.Enumerate(ctx, func(Blob) error { n++; return nil })
	if n != 0 {
		t.Fatalf("%d blobs left on disk", n)
	}
}

func TestReclaimerBeforeFull(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir(), 600<<10)
	if err != nil {
		t.Fatal(err)
	}
	a := make([]byte, 400<<10)
	rand.Read(a)
	refA, err := s.Put(ctx, bytes.NewReader(a))
	if err != nil {
		t.Fatal(err)
	}
	asked := int64(0)
	s.SetReclaimer(func(ctx context.Context, need int64) (int64, error) {
		asked = need
		bs, err := s.Blobs(ctx, refA)
		if err != nil {
			return 0, err
		}
		var refs []string
		for _, b := range bs {
			refs = append(refs, b.Ref)
		}
		_, freed, err := s.Remove(ctx, refs, nil)
		return freed, err
	})
	b := make([]byte, 400<<10)
	rand.Read(b)
	if _, err := s.Put(ctx, bytes.NewReader(b)); err != nil {
		t.Fatalf("put with a reclaimer: %v", err)
	}
	if asked == 0 {
		t.Fatal("reclaimer not asked")
	}
}
