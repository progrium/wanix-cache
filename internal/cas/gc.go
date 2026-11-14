package cas

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/schema"
)

// Blob is one stored blob: a chunk or a schema blob.
type Blob struct {
	Ref  string
	Size int64
}

// Session is a run of puts whose blobs stay safe from Remove until Close.
//
// Ingest opens a session, puts files (or calls Hold on a content ref it
// found by whole-file digest), records the refs in the metadata DB, commits,
// and only then closes the session. Every blob the session wrote or found
// already present is held until Close, so a sweep that started from a DB
// snapshot where those blobs were unreferenced cannot delete them before the
// new reference is visible.
type Session struct {
	s    *Store
	mu   sync.Mutex // the schema writer uploads chunks concurrently
	held map[blob.Ref]bool
}

// Session starts a session. It is safe for concurrent use, but holds last
// until Close, so an ingest worker normally opens one per unit.
func (s *Store) Session() *Session {
	return &Session{s: s, held: map[blob.Ref]bool{}}
}

func (ss *Session) hold(br blob.Ref) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.held[br] {
		return
	}
	s := ss.s
	s.hmu.Lock()
	for s.deleting[br] {
		s.hcond.Wait()
	}
	s.holds[br]++
	s.hmu.Unlock()
	ss.held[br] = true
}

// Close releases the session's holds. It is safe to call more than once.
func (ss *Session) Close() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s := ss.s
	s.hmu.Lock()
	for br := range ss.held {
		if s.holds[br]--; s.holds[br] <= 0 {
			delete(s.holds, br)
		}
	}
	s.hmu.Unlock()
	clear(ss.held)
}

// Put chunks r into the store and returns the file schema ref.
func (ss *Session) Put(ctx context.Context, r io.Reader) (string, error) {
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, sessionStorage{ss.s.q, ss}, "", time.Time{}, r)
	if err != nil {
		return "", err
	}
	return ref.String(), nil
}

// Hold holds every blob of content ref and returns them. It fails if any
// blob is missing, for example because a sweep deleted it after the ref was
// read from the DB; the caller then puts the bytes again.
func (ss *Session) Hold(ctx context.Context, ref string) ([]Blob, error) {
	br, ok := blob.Parse(ref)
	if !ok {
		return nil, fmt.Errorf("cas: bad blob ref %q", ref)
	}
	ss.hold(br) // before reading it, so it cannot vanish mid-walk
	return ss.s.blobs(ctx, br, ss.hold)
}

// sessionStorage holds every blob the schema writer stats or receives. The
// writer stats each blob before uploading it and skips the upload when it is
// present, so holding on stat covers dedup hits.
type sessionStorage struct {
	*quotaStore
	ss *Session
}

func (st sessionStorage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	for _, br := range blobs {
		st.ss.hold(br)
	}
	return st.quotaStore.StatBlobs(ctx, blobs, fn)
}

func (st sessionStorage) ReceiveBlob(ctx context.Context, br blob.Ref, r io.Reader) (blob.SizedRef, error) {
	st.ss.hold(br)
	return st.quotaStore.ReceiveBlob(ctx, br, r)
}

// Blobs returns every blob that content ref is made of: the file schema,
// any nested bytes schemas, and the data chunks, each once, sorted by ref.
func (s *Store) Blobs(ctx context.Context, ref string) ([]Blob, error) {
	br, ok := blob.Parse(ref)
	if !ok {
		return nil, fmt.Errorf("cas: bad blob ref %q", ref)
	}
	return s.blobs(ctx, br, nil)
}

func (s *Store) blobs(ctx context.Context, br blob.Ref, visit func(blob.Ref)) ([]Blob, error) {
	fr, err := schema.NewFileReader(ctx, s.q, br)
	if err != nil {
		return nil, err
	}
	seen := map[blob.Ref]bool{br: true}
	err = fr.ForeachChunk(ctx, func(path []blob.Ref, p schema.BytesPart) error {
		for _, sr := range path {
			if !seen[sr] && visit != nil {
				visit(sr)
			}
			seen[sr] = true
		}
		if p.BlobRef.Valid() {
			if !seen[p.BlobRef] && visit != nil {
				visit(p.BlobRef)
			}
			seen[p.BlobRef] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	refs := make([]blob.Ref, 0, len(seen))
	for r := range seen {
		refs = append(refs, r)
	}
	out := make([]Blob, 0, len(refs))
	err = s.q.Storage.StatBlobs(ctx, refs, func(sb blob.SizedRef) error {
		out = append(out, Blob{sb.Ref.String(), int64(sb.Size)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(out) != len(refs) {
		return nil, fmt.Errorf("cas: %s: %d of %d blobs missing: %w", br, len(refs)-len(out), len(refs), ErrMissing)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out, nil
}

// ErrMissing reports a content object with blobs missing from the store.
var ErrMissing = fmt.Errorf("blob missing")

// Enumerate calls fn for every blob in the store.
func (s *Store) Enumerate(ctx context.Context, fn func(Blob) error) error {
	return blobserver.EnumerateAll(ctx, s.q.Storage, func(sb blob.SizedRef) error {
		return fn(Blob{sb.Ref.String(), int64(sb.Size)})
	})
}

// Remove deletes blobs from the store and returns the ones it deleted and
// their total size. Blobs held by an open Session are skipped. The others
// are marked as being deleted, so new holds on them wait, and then confirm
// is called with them; only the refs confirm returns are deleted. The GC
// passes a confirm that re-reads the DB, which closes the window between
// choosing candidates and deleting them. A nil confirm deletes them all.
func (s *Store) Remove(ctx context.Context, refs []string, confirm func(refs []string) ([]string, error)) (removed []string, freed int64, err error) {
	var cand []blob.Ref
	s.hmu.Lock()
	for _, r := range refs {
		br, ok := blob.Parse(r)
		if !ok || s.holds[br] > 0 || s.deleting[br] {
			continue
		}
		s.deleting[br] = true
		cand = append(cand, br)
	}
	s.hmu.Unlock()
	defer func() {
		s.hmu.Lock()
		for _, br := range cand {
			delete(s.deleting, br)
		}
		s.hcond.Broadcast()
		s.hmu.Unlock()
	}()
	if len(cand) == 0 {
		return nil, 0, nil
	}
	names := make([]string, len(cand))
	for i, br := range cand {
		names[i] = br.String()
	}
	if confirm != nil {
		if names, err = confirm(names); err != nil {
			return nil, 0, err
		}
	}
	if len(names) == 0 {
		return nil, 0, nil
	}
	del := make([]blob.Ref, 0, len(names))
	for _, n := range names {
		del = append(del, blob.MustParse(n))
	}

	s.q.mu.Lock()
	defer s.q.mu.Unlock()
	sizes := map[blob.Ref]int64{}
	if err := s.q.Storage.StatBlobs(ctx, del, func(sb blob.SizedRef) error {
		sizes[sb.Ref] = int64(sb.Size)
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if err := s.q.Storage.RemoveBlobs(ctx, del); err != nil {
		return nil, 0, err
	}
	for _, br := range del {
		if sz, ok := sizes[br]; ok {
			s.q.used -= sz
			freed += sz
		}
		removed = append(removed, br.String())
	}
	return removed, freed, nil
}
