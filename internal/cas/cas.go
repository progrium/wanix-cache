// Package cas stores file contents as Perkeep file schemas over a localdisk
// blobserver, so identical rolling-hash chunks are stored once.
//
// Ported from the perkeep-dualfs prototype. Changes: file schemas are
// written with no name and a zero mtime, so identical bytes always produce
// the identical file ref (the prototype embedded both, which wrote a new
// schema blob per save); calls take a context; Open returns an io.ReaderAt
// that reports a short read at EOF as io.EOF.
//
// The quota counts unique blob bytes. Blobs are deleted only through
// Remove, which the GC sweeper calls; see Session for how ingest keeps the
// blobs it is using safe from a concurrent sweep.
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

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/localdisk"
	"perkeep.org/pkg/schema"
)

// ErrFull is returned when a new blob would exceed the quota.
var ErrFull = syscall.ENOSPC

// Store is a quota-limited Perkeep blob store.
type Store struct {
	q *quotaStore

	// holds and deleting close the race between ingest and the sweeper:
	// an ingest that dedups against a blob the DB says is unreferenced must
	// not have that blob deleted under it before it commits its reference.
	// Sessions hold every blob they stat or write; Remove skips held blobs
	// and marks the ones it deletes so new holds wait for the delete.
	hmu      sync.Mutex
	hcond    *sync.Cond
	holds    map[blob.Ref]int
	deleting map[blob.Ref]bool
}

type quotaStore struct {
	blobserver.Storage
	mu      sync.Mutex
	used    int64
	max     int64
	reclaim func(ctx context.Context, need int64) (int64, error)
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
	st := &Store{q: q, holds: map[blob.Ref]int{}, deleting: map[blob.Ref]bool{}}
	st.hcond = sync.NewCond(&st.hmu)
	return st, nil
}

// SetReclaimer installs a function that frees space when a new blob would
// exceed the quota. It is called without the store's locks held, and the
// write fails with ErrFull only if the blob still does not fit afterwards.
// The reclaimer must not wait on an open transaction of the caller's, so
// ingest writes blobs before it opens the transaction that references them.
func (s *Store) SetReclaimer(fn func(ctx context.Context, need int64) (freed int64, err error)) {
	s.q.mu.Lock()
	s.q.reclaim = fn
	s.q.mu.Unlock()
}

// Usage returns unique blob bytes stored and the quota.
func (s *Store) Usage() (used, max int64) {
	s.q.mu.Lock()
	defer s.q.mu.Unlock()
	return s.q.used, s.q.max
}

// Put chunks r into the store and returns the file schema ref. It holds
// nothing: callers that record the ref in the metadata DB while a GC sweep
// may run use a Session instead.
func (s *Store) Put(ctx context.Context, r io.Reader) (string, error) {
	sess := s.Session()
	defer sess.Close()
	return sess.Put(ctx, r)
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

// ReceiveBlob enforces the quota. A blob that already exists is free. When
// a new blob does not fit, the reclaimer (if any) is asked for the space
// first, so a write fails only when nothing evictable remains.
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
	if q.max > 0 && q.used+int64(len(data)) > q.max && q.reclaim != nil {
		need, reclaim := q.used+int64(len(data))-q.max, q.reclaim
		q.mu.Unlock()
		_, rerr := reclaim(ctx, need)
		q.mu.Lock()
		if rerr != nil && !errors.Is(rerr, ErrFull) {
			return blob.SizedRef{}, rerr
		}
		// Another writer may have stored the blob while unlocked.
		if old, err := blobserver.StatBlob(ctx, q.Storage, br); err == nil {
			return old, nil
		}
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

// statNoCancel works around a Perkeep bug: localdisk's StatBlobs shares one
// process-wide gate of 20 slots, and blobserver.StatBlobsParallelHelper
// leaks a slot whenever its context is cancelled between taking the slot and
// starting the stat. Enough cancelled stats (for example fills or ingests
// cut short by shutdown) exhaust the gate and every later stat in the
// process blocks forever. Stats are local and fast, so they run uncancelled.
type statNoCancel struct{ blobserver.Storage }

func (s statNoCancel) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	return s.Storage.StatBlobs(context.WithoutCancel(ctx), blobs, fn)
}
