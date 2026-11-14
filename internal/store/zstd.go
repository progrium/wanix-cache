package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Optional zstd, off by default. With it on, every stored file is one
// header byte and a payload: 0 and the file's bytes, or 1 and a zstd frame
// of them when that is smaller. Dedupe is unchanged: it is decided by the
// SHA-256 of the uncompressed bytes. A compressed file is decompressed as
// it is read; reading backwards restarts the decoder.
//
// It is turned on once, on an empty store, with EnableCompression and
// recorded in files/ZSTD (the encoder level); a store never mixes files
// with and without the header.

const (
	hdrRaw  = 0
	hdrZstd = 1
)

// EnableCompression turns zstd on for the empty store under dataDir.
// level is a zstd encoder level: 1 fastest, 2 default, 3 better, 4 best.
func EnableCompression(dataDir string, level int) error {
	if level < 1 || level > 4 {
		return fmt.Errorf("store: zstd level %d (want 1 to 4)", level)
	}
	dir := filepath.Join(dataDir, Dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	conf := filepath.Join(dir, zstdFile)
	if _, err := os.Stat(conf); err != nil {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.IsDir() {
				return fmt.Errorf("store: %s already holds files; zstd can only be turned on for an empty store", dir)
			}
		}
	}
	return os.WriteFile(conf, []byte(strconv.Itoa(level)+"\n"), 0o600)
}

func compression(dir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(dir, zstdFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || n < 1 || n > 4 {
		return 0, fmt.Errorf("store: bad %s: %q", zstdFile, b)
	}
	return n, nil
}

// compress writes r to w as a header byte and the smaller of the raw bytes
// and a zstd frame of them.
func (s *Store) compress(w io.Writer, r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	out := make([]byte, 1, len(raw)/2+64)
	out = s.enc.EncodeAll(raw, out)
	if len(out) < len(raw)+1 {
		out[0] = hdrZstd
	} else {
		out = append(out[:1], raw...)
		out[0] = hdrRaw
	}
	_, err = w.Write(out)
	return err
}

func (s *Store) decoder() (*zstd.Decoder, error) {
	if d, ok := s.dec.Get().(*zstd.Decoder); ok {
		return d, nil
	}
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
}

// rawFile is a stored file with the raw header: its bytes start at 1.
type rawFile struct{ f *os.File }

func (r rawFile) ReadAt(p []byte, off int64) (int, error) { return r.f.ReadAt(p, off+1) }
func (r rawFile) Close() error                            { return r.f.Close() }

// zstdReader decompresses a stored zstd frame as it is read. Sequential
// reads continue the stream; a read before the current position starts it
// again.
type zstdReader struct {
	s *Store
	f *os.File

	mu  sync.Mutex
	dec *zstd.Decoder
	br  *bufio.Reader
	pos int64
}

func (z *zstdReader) ReadAt(p []byte, off int64) (int, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.dec == nil || off < z.pos {
		if err := z.reset(); err != nil {
			return 0, err
		}
	}
	if off > z.pos {
		n, err := io.CopyN(io.Discard, z.br, off-z.pos)
		z.pos += n
		if err != nil {
			return 0, err
		}
	}
	n, err := io.ReadFull(z.br, p)
	z.pos += int64(n)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (z *zstdReader) reset() error {
	if z.dec == nil {
		d, err := z.s.decoder()
		if err != nil {
			return err
		}
		z.dec = d
	}
	if err := z.dec.Reset(io.NewSectionReader(z.f, 1, 1<<62)); err != nil {
		return err
	}
	if z.br == nil {
		z.br = bufio.NewReaderSize(z.dec, 64<<10)
	} else {
		z.br.Reset(z.dec)
	}
	z.pos = 0
	return nil
}

func (z *zstdReader) Close() error {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.dec != nil {
		z.dec.Reset(nil)
		z.s.dec.Put(z.dec)
		z.dec = nil
	}
	return z.f.Close()
}
