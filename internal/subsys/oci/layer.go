package oci

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/wanix-cache/internal/subsys"
)

// Whiteout names in layer tars (OCI image spec, "Whiteouts").
const (
	whiteoutPrefix = ".wh."
	opaqueMarker   = ".wh..wh..opq"
)

// ErrBlobDigest fails a layer download whose compressed bytes do not
// match the digest the manifest named.
var ErrBlobDigest = errors.New("oci: blob digest mismatch")

// unpackStats is what an unpack wrote.
type unpackStats struct {
	DiffID             string // sha256 of the uncompressed tar stream
	Files, Dirs, Links int
	Whiteouts, Skipped int
	Bytes              int64
}

// unpackLayer reads a compressed layer blob, checks its digest, and writes
// its entries into sink. Whiteout entries become Sink.Whiteout and
// Sink.Opaque. Entries whose parents are missing get implicit 0755
// directories, as container runtimes do.
func unpackLayer(ctx context.Context, blob io.Reader, blobDigest, mediaType string, sink subsys.Sink) (unpackStats, error) {
	var st unpackStats
	comp, err := compression(mediaType)
	if err != nil {
		return st, err
	}
	blobHash := sha256.New()
	cr := &ctxReader{ctx: ctx, r: io.TeeReader(blob, blobHash)}
	var raw io.Reader
	switch comp {
	case "gzip":
		gz, err := gzip.NewReader(cr)
		if err != nil {
			return st, fmt.Errorf("oci: layer %s: %w", blobDigest, err)
		}
		defer gz.Close()
		raw = gz
	case "zstd":
		zr, err := zstd.NewReader(cr, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return st, err
		}
		defer zr.Close()
		raw = zr
	default:
		raw = cr
	}
	diffHash := sha256.New()
	tr := tar.NewReader(io.TeeReader(raw, diffHash))

	u := &unpacker{sink: sink, dirs: map[string]bool{".": true}, seen: map[string]bool{}, st: &st}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return st, fmt.Errorf("oci: layer %s: %w", blobDigest, err)
		}
		if err := u.entry(hdr, tr); err != nil {
			return st, fmt.Errorf("oci: layer %s: %s: %w", blobDigest, hdr.Name, err)
		}
	}
	// The diff_id covers the whole uncompressed stream, tar padding and any
	// trailing bytes included, and the blob digest the whole download.
	if _, err := io.Copy(io.Discard, io.TeeReader(raw, diffHash)); err != nil {
		return st, fmt.Errorf("oci: layer %s: %w", blobDigest, err)
	}
	if _, err := io.Copy(io.Discard, cr); err != nil {
		return st, fmt.Errorf("oci: layer %s: %w", blobDigest, err)
	}
	if got := hexDigest(blobHash); got != blobDigest {
		return st, fmt.Errorf("%w: want %s, got %s", ErrBlobDigest, blobDigest, got)
	}
	st.DiffID = hexDigest(diffHash)
	return st, nil
}

func hexDigest(h hash.Hash) string { return "sha256:" + hex.EncodeToString(h.Sum(nil)) }

type unpacker struct {
	sink subsys.Sink
	dirs map[string]bool // directories created, unit-relative
	seen map[string]bool // non-directory names created
	st   *unpackStats
}

// cleanName turns a tar name into a unit-relative path, or "" for the root.
func cleanName(name string) (string, error) {
	p := path.Clean("/" + name)
	if strings.Contains(name, "\x00") {
		return "", errors.New("NUL in name")
	}
	return strings.TrimPrefix(p, "/"), nil
}

func (u *unpacker) mkdirAll(p string) error {
	if p == "" || p == "." || u.dirs[p] {
		return nil
	}
	if u.seen[p] {
		return fmt.Errorf("%s is not a directory", p)
	}
	if err := u.mkdirAll(path.Dir(p)); err != nil {
		return err
	}
	if err := u.sink.Dir(p, 0o755, time.Time{}); err != nil {
		return err
	}
	u.dirs[p] = true
	u.st.Dirs++
	return nil
}

func (u *unpacker) entry(hdr *tar.Header, r io.Reader) error {
	p, err := cleanName(hdr.Name)
	if err != nil {
		return err
	}
	if p == "" {
		return nil // the layer root itself
	}
	dir, base := path.Dir(p), path.Base(p)
	if dir == "" {
		dir = "."
	}
	if err := u.mkdirAll(dir); err != nil {
		return err
	}
	if base == opaqueMarker {
		u.st.Whiteouts++
		return u.sink.Opaque(dir)
	}
	if strings.HasPrefix(base, whiteoutPrefix) {
		target := path.Join(dir, strings.TrimPrefix(base, whiteoutPrefix))
		if u.seen[target] || u.dirs[target] {
			u.st.Skipped++
			return nil
		}
		u.seen[target] = true
		u.st.Whiteouts++
		return u.sink.Whiteout(target)
	}
	if u.dirs[p] {
		// Already created implicitly or by an earlier entry. The sink cannot
		// change a directory's mode, so the first one wins.
		if hdr.Typeflag != tar.TypeDir {
			u.st.Skipped++
		}
		return nil
	}
	if u.seen[p] {
		// A repeated name: in tar the last entry wins, but the sink cannot
		// replace an entry, so the first one is kept.
		u.st.Skipped++
		return nil
	}
	mode := fs.FileMode(hdr.Mode).Perm()
	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := u.sink.Dir(p, mode, hdr.ModTime); err != nil {
			return err
		}
		u.dirs[p] = true
		u.st.Dirs++
	case tar.TypeReg, tar.TypeRegA:
		w, err := u.sink.File(p, mode, hdr.Size, hdr.ModTime)
		if err != nil {
			return err
		}
		n, err := io.Copy(w, r)
		if cerr := w.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		u.seen[p] = true
		u.st.Files++
		u.st.Bytes += n
	case tar.TypeSymlink:
		if err := u.sink.Symlink(p, hdr.Linkname, hdr.ModTime); err != nil {
			return err
		}
		u.seen[p] = true
		u.st.Links++
	case tar.TypeLink:
		target, err := cleanName(hdr.Linkname)
		if err != nil {
			return err
		}
		if !u.seen[target] {
			u.st.Skipped++ // a link to something this layer does not have
			return nil
		}
		if err := u.sink.Hardlink(p, target); err != nil {
			return err
		}
		u.seen[p] = true
		u.st.Links++
	default:
		// Devices and FIFOs have no place in a read-only 9P tree.
		u.st.Skipped++
	}
	return nil
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
