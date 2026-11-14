package apk

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"
)

// An apk v2 package is two or three concatenated gzip streams:
//
//	[signature]  tar with .SIGN.RSA.<key>.rsa.pub (absent when unsigned)
//	control      tar with .PKGINFO and install scripts; its compressed
//	             bytes' SHA-1 is the APKINDEX C: checksum ("Q1" + base64)
//	data         tar with the package's files; its compressed bytes'
//	             SHA-256 is .PKGINFO's datahash
//
// The signature and control tars have their end-of-archive blocks stripped
// so the streams concatenate.

// Checksum formats the APKINDEX C: value for a control-stream SHA-1.
func Checksum(sum []byte) string { return "Q1" + base64.StdEncoding.EncodeToString(sum) }

// ParseChecksum returns the SHA-1 in an APKINDEX C: value.
func ParseChecksum(c string) ([]byte, error) {
	if !strings.HasPrefix(c, "Q1") {
		return nil, fmt.Errorf("apk: unsupported checksum %q", c)
	}
	b, err := base64.StdEncoding.DecodeString(c[2:])
	if err != nil || len(b) != sha1.Size {
		return nil, fmt.Errorf("apk: bad checksum %q", c)
	}
	return b, nil
}

// hashReader reads one byte stream for gzip, hashing what it consumes. It
// implements io.ByteReader, so gzip and flate read no further than the end
// of each member and member boundaries line up with hash boundaries.
type hashReader struct {
	r   *bufio.Reader
	hs  []hash.Hash
	buf []byte
}

func newHashReader(r io.Reader) *hashReader {
	return &hashReader{r: bufio.NewReaderSize(r, 64<<10), buf: make([]byte, 0, 4096)}
}

func (h *hashReader) flush() {
	if len(h.buf) > 0 {
		for _, x := range h.hs {
			x.Write(h.buf)
		}
		h.buf = h.buf[:0]
	}
}

// hash starts hashing into hs from the next byte read (nil stops).
func (h *hashReader) hash(hs ...hash.Hash) { h.flush(); h.hs = hs }

func (h *hashReader) ReadByte() (byte, error) {
	b, err := h.r.ReadByte()
	if err == nil && h.hs != nil {
		h.buf = append(h.buf, b)
		if len(h.buf) == cap(h.buf) {
			h.flush()
		}
	}
	return b, err
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 && h.hs != nil {
		h.flush()
		for _, x := range h.hs {
			x.Write(p[:n])
		}
	}
	return n, err
}

// Package is what parsing an .apk learned about it.
type Package struct {
	Checksum string            // "Q1..." over the control stream
	DataHash string            // hex SHA-256 over the data stream, as computed
	Info     map[string]string // .PKGINFO key = value (last value wins)
	Signed   bool
}

// EntryFunc receives each data tar entry. r reads a regular file's bytes.
type EntryFunc func(h *tar.Header, r io.Reader) error

// Parse reads an .apk from r, verifying the data stream against .PKGINFO's
// datahash. If entry is nil the data stream is hashed but not decompressed;
// otherwise every data tar entry is passed to entry in archive order.
// Install scripts in the control stream are never run or returned.
func Parse(r io.Reader, entry EntryFunc) (*Package, error) {
	hr := newHashReader(r)
	pkg := &Package{}
	zr := new(gzip.Reader)
	var control []byte
	// Signature (optional) and control streams.
	for i := 0; i < 2 && control == nil; i++ {
		s1 := sha1.New()
		hr.hash(s1)
		if err := zr.Reset(hr); err != nil {
			return nil, fmt.Errorf("apk: stream %d: %w", i+1, err)
		}
		zr.Multistream(false)
		tr := tar.NewReader(zr)
		isControl, isSig := false, false
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("apk: stream %d: %w", i+1, err)
			}
			switch {
			case strings.HasPrefix(h.Name, ".SIGN."):
				isSig = true
			case h.Name == ".PKGINFO":
				isControl = true
				b, err := io.ReadAll(io.LimitReader(tr, 1<<20))
				if err != nil {
					return nil, fmt.Errorf("apk: .PKGINFO: %w", err)
				}
				control = b
			}
		}
		if _, err := io.Copy(io.Discard, zr); err != nil {
			return nil, err
		}
		hr.hash()
		switch {
		case isControl:
			pkg.Checksum = Checksum(s1.Sum(nil))
		case isSig && i == 0:
			pkg.Signed = true
		default:
			return nil, fmt.Errorf("apk: stream %d is neither signature nor control", i+1)
		}
	}
	if control == nil {
		return nil, errors.New("apk: no .PKGINFO")
	}
	pkg.Info = parsePkgInfo(control)

	// Data stream: the rest of the file.
	s256 := sha256.New()
	hr.hash(s256)
	if entry == nil {
		if _, err := io.Copy(io.Discard, hr); err != nil {
			return nil, err
		}
	} else {
		if err := zr.Reset(hr); err != nil {
			return nil, fmt.Errorf("apk: data stream: %w", err)
		}
		zr.Multistream(false)
		tr := tar.NewReader(zr)
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("apk: data: %w", err)
			}
			if err := entry(h, tr); err != nil {
				return nil, err
			}
		}
		if _, err := io.Copy(io.Discard, zr); err != nil {
			return nil, err
		}
		// Anything after the data stream (there should be nothing) is
		// still part of what datahash covers.
		if _, err := io.Copy(io.Discard, hr); err != nil {
			return nil, err
		}
	}
	hr.hash()
	pkg.DataHash = hex.EncodeToString(s256.Sum(nil))
	if want := pkg.Info["datahash"]; want != "" && want != pkg.DataHash {
		return pkg, fmt.Errorf("apk: data stream sha256 %s, .PKGINFO datahash %s", pkg.DataHash, want)
	}
	return pkg, nil
}

func parsePkgInfo(b []byte) map[string]string {
	m := map[string]string{}
	for _, line := range bytes.Split(b, []byte("\n")) {
		s := strings.TrimSpace(string(line))
		if s == "" || s[0] == '#' {
			continue
		}
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			continue
		}
		m[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return m
}

// cleanName turns a tar entry name into a unit-relative path, or "" for
// the archive root. It rejects names that escape the root.
func cleanName(name string) (string, error) {
	p := path.Clean("/" + name)
	if strings.Contains(name, "..") {
		for _, part := range strings.Split(name, "/") {
			if part == ".." {
				return "", fmt.Errorf("apk: entry %q escapes the package root", name)
			}
		}
	}
	return strings.TrimPrefix(p, "/"), nil
}
