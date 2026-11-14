// Package cas stores file contents as Perkeep file schemas over a localdisk
// blobserver, so identical rolling-hash chunks are stored once.
//
// Ported from the perkeep-dualfs prototype. Changes: file schemas are
// written with no name and a zero mtime, so identical bytes always produce
// the identical file ref (the prototype embedded both, which wrote a new
// schema blob per save); calls take a context; Open returns an io.ReaderAt
// that reports a short read at EOF as io.EOF.
//
// The quota counts unique blob bytes. There is no delete or GC yet.
package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/localdisk"
	"perkeep.org/pkg/schema"
)

// ErrFull is returned when a new blob would exceed the quota.
var ErrFull = syscall.ENOSPC

// Store is a quota-limited Perkeep blob store.
type Store struct{ q *quotaStore }

type quotaStore struct {
	blobserver.Storage
	mu   sync.Mutex
	used int64
	max  int64
}

// Open opens or creates a store under dir/blobs. maxBytes 0 means no quota.
func Open(dir string, maxBytes int64) (*Store, error) {
	bdir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(bdir, 0o700); err != nil {
		return nil, err
	}
	raw, err := localdisk.New(bdir)
	if err != nil {
		return nil, err
	}
	q := &quotaStore{Storage: raw, max: maxBytes}
	err = blobserver.EnumerateAll(context.Background(), raw, func(sr blob.SizedRef) error {
		q.used += int64(sr.Size)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Store{q: q}, nil
}

// Usage returns unique blob bytes stored and the quota.
func (s *Store) Usage() (used, max int64) {
	s.q.mu.Lock()
	defer s.q.mu.Unlock()
	return s.q.used, s.q.max
}

// Put chunks r into the store and returns the file schema ref.
func (s *Store) Put(ctx context.Context, r io.Reader) (string, error) {
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, s.q, "", time.Time{}, r)
	if err != nil {
		return "", err
	}
	return ref.String(), nil
}

// File is an open CAS object.
type File interface {
	io.ReaderAt
	io.Closer
}

// Open opens the file schema ref for reading.
func (s *Store) Open(ctx context.Context, ref string) (File, error) {
	br, ok := blob.Parse(ref)
	if !ok {
		return nil, fmt.Errorf("cas: bad blob ref %q", ref)
	}
	fr, err := schema.NewFileReader(ctx, s.q, br)
	if err != nil {
		return nil, err
	}
	return eofFile{fr}, nil
}

// eofFile maps Perkeep's io.ErrUnexpectedEOF for a short read at the end of
// the file to io.EOF, as io.ReaderAt documents.
type eofFile struct{ *schema.FileReader }

func (f eofFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.FileReader.ReadAt(p, off)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

// ReceiveBlob enforces the quota. A blob that already exists is free.
//
// TODO(phase 3): the mutex is held across stat and write, which serializes
// all ingest. Reserve quota under the lock and write outside it.
func (q *quotaStore) ReceiveBlob(ctx context.Context, br blob.Ref, r io.Reader) (blob.SizedRef, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(blobserver.MaxBlobSize)+1))
	if err != nil {
		return blob.SizedRef{}, err
	}
	if len(data) > blobserver.MaxBlobSize {
		return blob.SizedRef{}, errors.New("cas: blob exceeds maximum size")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	old, err := blobserver.StatBlob(ctx, q.Storage, br)
	if err == nil {
		return old, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return blob.SizedRef{}, err
	}
	if q.max > 0 && q.used+int64(len(data)) > q.max {
		return blob.SizedRef{}, ErrFull
	}
	got, err := q.Storage.ReceiveBlob(ctx, br, bytes.NewReader(data))
	if err == nil {
		q.used += int64(got.Size)
	}
	return got, err
}
