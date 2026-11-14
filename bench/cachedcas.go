package bench

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/localdisk"
	"perkeep.org/pkg/schema"
)

// CachedCAS is a prototype of a faster CAS read path, built only to see
// whether the gap to plain disk can be closed without keeping hot units on
// disk. It reads the same blobs/ directory as cas.Store and changes two
// things:
//
//   - Blobs (file schemas, bytes schemas and chunks) are kept in a shared
//     LRU of up to MaxBytes, so an open does not re-read and re-parse the
//     schema and a small read does not re-read its whole chunk from disk.
//   - A file read sequentially (a read that starts where the last one
//     ended, or at 0) fetches the chunks covering the next Readahead bytes
//     in parallel, so a cold large file is not read one chunk file at a
//     time.
//
// It is not wired into the servers outside this package.
type CachedCAS struct {
	src       blob.Fetcher
	MaxBytes  int64
	Readahead int64
	Workers   int

	mu       sync.Mutex
	lru      *list.List // of *cent, front = most recent
	m        map[blob.Ref]*list.Element
	used     int64
	inflight map[blob.Ref]*fetch
	sem      chan struct{}
}

type cent struct {
	ref blob.Ref
	b   []byte
}

type fetch struct {
	done chan struct{}
	b    []byte
	err  error
}

// OpenCachedCAS opens the blobs under dir/blobs (the layout cas.Open uses).
func OpenCachedCAS(dir string, maxBytes, readahead int64) (*CachedCAS, error) {
	raw, err := localdisk.New(filepath.Join(dir, "blobs"))
	if err != nil {
		return nil, err
	}
	const workers = 8
	return &CachedCAS{src: raw, MaxBytes: maxBytes, Readahead: readahead, Workers: workers,
		lru: list.New(), m: map[blob.Ref]*list.Element{}, inflight: map[blob.Ref]*fetch{},
		sem: make(chan struct{}, workers)}, nil
}

// Reset empties the cache, for cold runs.
func (c *CachedCAS) Reset() {
	c.mu.Lock()
	c.lru.Init()
	c.m = map[blob.Ref]*list.Element{}
	c.used = 0
	c.mu.Unlock()
}

// Fetch implements blob.Fetcher from the cache.
func (c *CachedCAS) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	b, err := c.get(ctx, br)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(b)), uint32(len(b)), nil
}

func (c *CachedCAS) get(ctx context.Context, br blob.Ref) ([]byte, error) {
	c.mu.Lock()
	if e, ok := c.m[br]; ok {
		c.lru.MoveToFront(e)
		b := e.Value.(*cent).b
		c.mu.Unlock()
		return b, nil
	}
	if f, ok := c.inflight[br]; ok {
		c.mu.Unlock()
		<-f.done
		return f.b, f.err
	}
	f := &fetch{done: make(chan struct{})}
	c.inflight[br] = f
	c.mu.Unlock()

	f.b, f.err = c.load(ctx, br)

	c.mu.Lock()
	delete(c.inflight, br)
	if f.err == nil && int64(len(f.b)) <= c.MaxBytes {
		c.m[br] = c.lru.PushFront(&cent{br, f.b})
		c.used += int64(len(f.b))
		for c.used > c.MaxBytes {
			e := c.lru.Back()
			ce := e.Value.(*cent)
			c.lru.Remove(e)
			delete(c.m, ce.ref)
			c.used -= int64(len(ce.b))
		}
	}
	c.mu.Unlock()
	close(f.done)
	return f.b, f.err
}

func (c *CachedCAS) load(ctx context.Context, br blob.Ref) ([]byte, error) {
	rc, size, err := c.src.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b := make([]byte, size)
	_, err = io.ReadFull(rc, b)
	return b, err
}

// Open opens a file schema ref.
func (c *CachedCAS) Open(ctx context.Context, ref string) (Handle, error) {
	br, ok := blob.Parse(ref)
	if !ok {
		return nil, errors.New("bad ref")
	}
	fr, err := schema.NewFileReader(ctx, c, br)
	if err != nil {
		return nil, err
	}
	return &cachedFile{c: c, fr: fr}, nil
}

type span struct {
	off, size int64
	ref       blob.Ref
}

type cachedFile struct {
	c  *CachedCAS
	fr *schema.FileReader

	mu     sync.Mutex
	next   int64  // where a sequential read would start
	spans  []span // chunk layout, built on first readahead
	listed bool
	ahead  int64 // readahead has been issued up to here
}

func (f *cachedFile) ReadAt(p []byte, off int64) (int, error) {
	if f.c.Readahead > 0 {
		f.mu.Lock()
		if off == f.next {
			f.readahead(off + int64(len(p)))
		}
		f.mu.Unlock()
	}
	n, err := f.fr.ReadAt(p, off)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	f.mu.Lock()
	f.next = off + int64(n)
	f.mu.Unlock()
	return n, err
}

// readahead starts fetches for the chunks in [from, from+Readahead).
// Called with f.mu held.
func (f *cachedFile) readahead(from int64) {
	if !f.listed {
		f.listed = true
		var off int64
		f.fr.ForeachChunk(context.Background(), func(_ []blob.Ref, p schema.BytesPart) error {
			f.spans = append(f.spans, span{off, int64(p.Size), p.BlobRef})
			off += int64(p.Size)
			return nil
		})
	}
	end := from + f.c.Readahead
	start := max(from, f.ahead)
	for _, s := range f.spans {
		if s.off+s.size <= start || s.off >= end || !s.ref.Valid() {
			continue
		}
		ref := s.ref
		go func() {
			f.c.sem <- struct{}{}
			f.c.get(context.Background(), ref)
			<-f.c.sem
		}()
	}
	if end > f.ahead {
		f.ahead = end
	}
}

func (f *cachedFile) Close() error { return nil }
