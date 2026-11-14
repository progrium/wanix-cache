package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
)

// Per-blob zstd (experimental). With it on, every blob is stored as one
// header byte and a payload: 0 and the blob's bytes, or 1 and a zstd frame
// of them when that is smaller. Refs stay the hashes of the uncompressed
// bytes, so dedupe is unchanged; Fetch decompresses. Sizes the blobserver
// reports (stat, enumerate, the quota) are stored bytes.
//
// It is turned on once, on an empty store, with EnableCompression and
// recorded in blobs/ZSTD (the encoder level); a store never mixes blobs
// with and without the header.

const zstdFile = "ZSTD"

const (
	hdrRaw  = 0
	hdrZstd = 1
)

// EnableCompression turns per-blob zstd on for the empty store under dir.
// level is a zstd encoder level: 1 fastest, 2 default, 3 better, 4 best.
func EnableCompression(dir string, level int) error {
	if level < 1 || level > 4 {
		return fmt.Errorf("cas: zstd level %d (want 1 to 4)", level)
	}
	bdir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(bdir, 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(bdir, zstdFile)); err == nil {
		return os.WriteFile(filepath.Join(bdir, zstdFile), []byte(strconv.Itoa(level)+"\n"), 0o600)
	}
	nonEmpty := errors.New("not empty")
	err := filepath.WalkDir(bdir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		n := d.Name()
		if !d.IsDir() && ((strings.HasSuffix(n, ".dat") && n != "GENERATION.dat") || strings.HasSuffix(n, ".blobs")) {
			return nonEmpty
		}
		return nil
	})
	if errors.Is(err, nonEmpty) {
		return fmt.Errorf("cas: %s already holds blobs; zstd can only be turned on for an empty store", bdir)
	}
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bdir, zstdFile), []byte(strconv.Itoa(level)+"\n"), 0o600)
}

func compression(bdir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(bdir, zstdFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || n < 1 || n > 4 {
		return 0, fmt.Errorf("cas: bad %s: %q", zstdFile, b)
	}
	return n, nil
}

type zstdStorage struct {
	blobserver.Storage
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func newZstd(inner blobserver.Storage, level int) (*zstdStorage, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, err
	}
	return &zstdStorage{Storage: inner, enc: enc, dec: dec}, nil
}

func (z *zstdStorage) ReceiveBlob(ctx context.Context, br blob.Ref, r io.Reader) (blob.SizedRef, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return blob.SizedRef{}, err
	}
	out := make([]byte, 1, len(raw)+1)
	out = z.enc.EncodeAll(raw, out)
	if len(out) < len(raw)+1 {
		out[0] = hdrZstd
	} else {
		out = append(out[:1], raw...)
		out[0] = hdrRaw
	}
	return z.Storage.ReceiveBlob(ctx, br, bytes.NewReader(out))
}

func (z *zstdStorage) Fetch(ctx context.Context, br blob.Ref) (io.ReadCloser, uint32, error) {
	rc, _, err := z.Storage.Fetch(ctx, br)
	if err != nil {
		return nil, 0, err
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, 0, err
	}
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("cas: %s: blob has no zstd header", br)
	}
	switch b[0] {
	case hdrRaw:
		b = b[1:]
	case hdrZstd:
		if b, err = z.dec.DecodeAll(b[1:], nil); err != nil {
			return nil, 0, fmt.Errorf("cas: %s: %w", br, err)
		}
	default:
		return nil, 0, fmt.Errorf("cas: %s: bad blob header %d", br, b[0])
	}
	return io.NopCloser(bytes.NewReader(b)), uint32(len(b)), nil
}

func (z *zstdStorage) Close() error {
	z.dec.Close()
	if c, ok := z.Storage.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
