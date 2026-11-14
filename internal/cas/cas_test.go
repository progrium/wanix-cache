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
