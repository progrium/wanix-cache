package apk

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// IndexEntry is one package record from an APKINDEX.
type IndexEntry struct {
	Checksum      string // C: "Q1..."
	Name          string // P:
	Version       string // V:
	Arch          string // A:
	Size          int64  // S: size of the .apk
	InstalledSize int64  // I: sum of the unpacked files
	BuildTime     int64  // t: unix seconds
	Origin        string // o:
}

// NV is the package's name-version, its file name without ".apk".
func (e IndexEntry) NV() string { return e.Name + "-" + e.Version }

// ParseIndex reads an APKINDEX.tar.gz (optionally preceded by a signature
// stream) and returns its records.
func ParseIndex(b []byte) ([]IndexEntry, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("apk: APKINDEX: %w", err)
	}
	tr := tar.NewReader(zr) // multistream: the signature tar runs into the index tar
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil, errors.New("apk: APKINDEX.tar.gz has no APKINDEX")
		}
		if err != nil {
			return nil, fmt.Errorf("apk: APKINDEX: %w", err)
		}
		if h.Name == "APKINDEX" {
			return parseIndexText(tr)
		}
	}
}

func parseIndexText(r io.Reader) ([]IndexEntry, error) {
	var out []IndexEntry
	var e IndexEntry
	flush := func() {
		if e.Checksum != "" && e.Name != "" && e.Version != "" {
			out = append(out, e)
		}
		e = IndexEntry{}
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			flush()
			continue
		}
		if len(line) < 2 || line[1] != ':' {
			continue
		}
		v := line[2:]
		switch line[0] {
		case 'C':
			e.Checksum = v
		case 'P':
			e.Name = v
		case 'V':
			e.Version = v
		case 'A':
			e.Arch = v
		case 'S':
			e.Size, _ = strconv.ParseInt(v, 10, 64)
		case 'I':
			e.InstalledSize, _ = strconv.ParseInt(v, 10, 64)
		case 't':
			e.BuildTime, _ = strconv.ParseInt(v, 10, 64)
		case 'o':
			e.Origin = v
		}
	}
	flush()
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("apk: APKINDEX: %w", err)
	}
	return out, nil
}

// validRepo reports whether repo is a "<branch>/<repo>/<arch>" path with
// plain segments, as used in mirror URLs (v3.20/main/x86_64).
func validRepo(repo string) bool {
	parts := strings.Split(repo, "/")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || strings.ContainsAny(p, "\\?#%") {
			return false
		}
	}
	return true
}
