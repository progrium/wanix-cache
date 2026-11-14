package store

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func readAll(t *testing.T, s *Store, unit int64, ino uint64, n int) []byte {
	t.Helper()
	f, err := s.Open(unit, ino)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.NewSectionReader(f, 0, int64(n)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLinksAndAccounting(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("wanix "), 10000)
	spool := filepath.Join(dir, "spool-file")
	if err := os.WriteFile(spool, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(1, 10, spool); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(1, 10, spool); err != nil { // a retried ingest
		t.Fatal(err)
	}
	if err := s.Link(1, 10, 2, 20); err != nil {
		t.Fatal(err)
	}
	if err := s.Link(1, 10, 2, 21); err != nil {
		t.Fatal(err)
	}
	if got := s.Used(); got != int64(len(data)) {
		t.Fatalf("used %d after one file and two links, want %d", got, len(data))
	}
	if !bytes.Equal(readAll(t, s, 2, 21, len(data)), data) {
		t.Fatal("linked file reads wrong bytes")
	}
	os.Remove(spool)
	if freed, err := s.RemoveUnit(1); err != nil || freed != 0 {
		t.Fatalf("removing unit 1 freed %d, %v; unit 2 still links the file", freed, err)
	}
	if err := s.Link(1, 10, 3, 30); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("link from a removed unit: %v", err)
	}
	if freed, err := s.RemoveUnit(2); err != nil || freed != int64(len(data)) {
		t.Fatalf("removing unit 2 freed %d, %v", freed, err)
	}
	if s.Used() != 0 {
		t.Fatalf("used %d with nothing stored", s.Used())
	}

	// The count survives a reopen, each file once.
	if err := s.Write(4, 40, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	s.Link(4, 40, 5, 50)
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s2.Used() != int64(len(data)) {
		t.Fatalf("reopened: used %d", s2.Used())
	}
	units, _ := s2.Units()
	if len(units) != 2 {
		t.Fatalf("units %v", units)
	}
}

func TestCompression(t *testing.T) {
	dir := t.TempDir()
	if err := EnableCompression(dir, 2); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	text := bytes.Repeat([]byte("compressible "), 50000)
	random := make([]byte, 10000)
	for i := range random {
		random[i] = byte(i*7919 + i/13)
	}
	if err := s.Write(1, 1, bytes.NewReader(text)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(1, 2, bytes.NewReader(random)); err != nil {
		t.Fatal(err)
	}
	if s.Used() >= int64(len(text)/10) {
		t.Errorf("used %d for %d compressible bytes", s.Used(), len(text))
	}
	if !bytes.Equal(readAll(t, s, 1, 1, len(text)), text) || !bytes.Equal(readAll(t, s, 1, 2, len(random)), random) {
		t.Fatal("wrong bytes")
	}
	// Reads in any order.
	f, _ := s.Open(1, 1)
	defer f.Close()
	buf := make([]byte, 13)
	for _, off := range []int64{13 * 1000, 13 * 5, 13 * 40000, 0} {
		if _, err := f.ReadAt(buf, off); err != nil || string(buf) != "compressible " {
			t.Fatalf("read at %d: %q, %v", off, buf, err)
		}
	}
	if n, err := f.ReadAt(buf, int64(len(text))-5); n != 5 || err != io.EOF {
		t.Fatalf("read at the end: %d, %v", n, err)
	}
	if err := EnableCompression(t.TempDir(), 9); err == nil {
		t.Error("bad level accepted")
	}
	plain := t.TempDir()
	p, _ := Open(plain)
	p.Write(1, 1, bytes.NewReader(text))
	if err := EnableCompression(plain, 2); err == nil {
		t.Error("zstd turned on for a store that already holds files")
	}
}
