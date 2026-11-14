package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/diskpacked"
	_ "perkeep.org/pkg/sorted/leveldb" // diskpacked's index
)

// Small-blob packing (experimental). localdisk keeps one file per blob in
// a two-level hash-prefix tree, which on a 4 KiB-block filesystem costs a
// partly empty block per blob plus a block per directory: about a quarter
// of the store on small-file trees. With packing on, blobs up to a
// threshold go to Perkeep's diskpacked storage (append-only pack files
// with a LevelDB index; removal punches holes) under blobs/packed, and
// larger blobs stay in localdisk.
//
// Packing is turned on once per store with EnablePacking and recorded in
// blobs/packed/THRESHOLD; every later Open of the store routes through it.
// The pack index is locked by one process at a time, so with packing on,
// commands that open the store (stats, gc, audit) need the server stopped.

const (
	packedDir     = "packed"
	thresholdFile = "THRESHOLD"
)

// EnablePacking turns small-blob packing on for the store under dir, for
// blobs of at most max bytes. Blobs already stored stay where they are.
func EnablePacking(dir string, max int) error {
	if max <= 0 {
		return fmt.Errorf("cas: packing threshold %d", max)
	}
	p := filepath.Join(dir, "blobs", packedDir)
	if err := os.MkdirAll(p, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(p, thresholdFile), []byte(strconv.Itoa(max)+"\n"), 0o600)
}

// packing returns the threshold recorded under bdir, or 0 when packing is off.
func packing(bdir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(bdir, packedDir, thresholdFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("cas: bad %s: %q", thresholdFile, b)
	}
	return n, nil
}

// splitStorage sends blobs of at most max bytes to small and the rest to
// large. Reads try small first.
type splitStorage struct {
	small, large blobserver.Storage
	max          int
}

func newSplit(bdir string, large blobserver.Storage, max int) (*splitStorage, error) {
	small, err := diskpacked.New(filepath.Join(bdir, packedDir))
	if err != nil {
		return nil, err
	}
	return &splitStorage{small: statNoCancel{small}, large: large, max: max}, nil
}

func (s *splitStorage) Close() error {
	if c, ok := s.small.(statNoCancel).Storage.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (s *splitStorage) ReceiveBlob(ctx context.Context, br blob.Ref, r io.Reader) (blob.SizedRef, error) {
	buf := make([]byte, s.max+1)
	n, err := io.ReadFull(r, buf)
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		return s.small.ReceiveBlob(ctx, br, bytes.NewReader(buf[:n]))
	case err != nil:
		return blob.SizedRef{}, err
	}
	return s.large.ReceiveBlob(ctx, br, io.MultiReader(bytes.NewReader(buf), r))
}

func (s *splitStorage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	rc, size, err := s.small.Fetch(ctx, br)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return rc, size, err
	}
	return s.large.Fetch(ctx, br)
}

func (s *splitStorage) SubFetch(ctx context.Context, br blob.Ref, offset, length int64) (io.ReadCloser, error) {
	if sf, ok := s.large.(blob.SubFetcher); ok {
		rc, err := sf.SubFetch(ctx, br, offset, length)
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return rc, err
		}
	}
	rc, _, err := s.small.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
		rc.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(rc, length), rc}, nil
}

func (s *splitStorage) StatBlobs(ctx context.Context, refs []blob.Ref, fn func(blob.SizedRef) error) error {
	found := map[blob.Ref]bool{}
	err := s.small.StatBlobs(ctx, refs, func(sb blob.SizedRef) error {
		found[sb.Ref] = true
		return fn(sb)
	})
	if err != nil {
		return err
	}
	var rest []blob.Ref
	for _, br := range refs {
		if !found[br] {
			rest = append(rest, br)
		}
	}
	if len(rest) == 0 {
		return nil
	}
	return s.large.StatBlobs(ctx, rest, fn)
}

func (s *splitStorage) EnumerateBlobs(ctx context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	return blobserver.MergedEnumerateStorage(ctx, dest, []blobserver.Storage{s.small, s.large}, after, limit)
}

func (s *splitStorage) RemoveBlobs(ctx context.Context, refs []blob.Ref) error {
	inSmall := map[blob.Ref]bool{}
	if err := s.small.StatBlobs(ctx, refs, func(sb blob.SizedRef) error {
		inSmall[sb.Ref] = true
		return nil
	}); err != nil {
		return err
	}
	var small, large []blob.Ref
	for _, br := range refs {
		if inSmall[br] {
			small = append(small, br)
		} else {
			large = append(large, br)
		}
	}
	if len(small) > 0 {
		if err := s.small.RemoveBlobs(ctx, small); err != nil {
			return err
		}
	}
	if len(large) > 0 {
		return s.large.RemoveBlobs(ctx, large)
	}
	return nil
}
