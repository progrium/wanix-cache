// Package h9index generates, stores and serves the metadata index of a
// cache root, the /.h9index file of hyper9's mount-index proposal
// (https://github.com/progrium/hyper9/blob/main/design/mount-index.md).
//
// An index lists every name under a root with its qid, mode, length, mtime
// and owner, and none of the contents. h9 reads it once and answers walks,
// stats and directory reads from it, so only opens and reads reach the
// cache. The cache can produce an exact index cheaply: it already holds each
// root's manifest in SQLite, and its trees never change in place.
//
// The index describes a tree as the client's devmnt sees it, which depends
// on the transport h9 mounts the root over, so each root has one index per
// Variant: plain 9P2000 hides symlinks, 9P2000.L shows them. Each server
// serves its own variant at the same path.
package h9index

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"9fans.net/go/plan9"
)

// Name is the index file's name at the top of a root.
const Name = ".h9index"

// FormatVersion is the text format's version, the number on the first line.
const FormatVersion = 1

// MaxSize is the largest index h9 accepts; a larger one is not served.
const MaxSize = 256 << 20

// DMSYMLINK is 9P2000.u's mode bit for a symbolic link. Plan 9 has none; it
// marks symlinks in the dotl variant, as h9's 9P2000.L translator reports
// them.
const DMSYMLINK = plan9.DMSYMLINK

// Variant names the transport an index describes the tree for.
type Variant string

const (
	// NineP is plain 9P2000, what the cache's 9P2000 server serves. It has
	// no symlinks, so the server hides them and so does the index.
	NineP Variant = "9p"
	// DotL is 9P2000.L as h9 sees it through its translator: symlinks are
	// listed, with DMSYMLINK in the mode and the target's length.
	DotL Variant = "dotl"
)

// Variants lists every variant, in the order they are generated.
var Variants = []Variant{NineP, DotL}

// Entry is one line of an index: a path from the root ("/", "/bin/ls") and
// the stat(5) fields the server reports for it. Dir.Name is ignored.
type Entry struct {
	Path string
	plan9.Dir
}

// ValidName reports whether a name can be listed. A path ends at the first
// space, so a name with a space or a newline can't be, nor one that isn't
// UTF-8. Such a name is left out and its directory marked unindexed.
func ValidName(name string) bool {
	return name != "" && utf8.ValidString(name) && !strings.ContainsAny(name, " \n")
}

func qidType(t uint8) byte {
	switch {
	case t&plan9.QTDIR != 0:
		return 'd'
	case t&plan9.QTAPPEND != 0:
		return 'a'
	case t&plan9.QTEXCL != 0:
		return 'l'
	}
	return '-'
}

func (e Entry) appendLine(b []byte) []byte {
	b = append(b, e.Path...)
	b = fmt.Appendf(b, " %016x %d %c %#o %d %d %s %s\n",
		e.Qid.Path, e.Qid.Vers, qidType(e.Qid.Type), uint32(e.Mode), e.Length, e.Mtime, e.Uid, e.Gid)
	return b
}

// Encode writes the index of entries, sorted by path in byte order, with an
// "!unindexed DIR" line after each directory in unindexed. Every entry's
// path must be absolute and every name in it valid.
func Encode(entries []Entry, unindexed []string) []byte {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	un := map[string]bool{}
	for _, d := range unindexed {
		un[d] = true
	}
	b := fmt.Appendf(nil, "h9index %d\n", FormatVersion)
	for _, e := range entries {
		b = e.appendLine(b)
		if un[e.Path] {
			b = append(b, "!unindexed "...)
			b = append(b, e.Path...)
			b = append(b, '\n')
		}
	}
	return b
}

// Index is a parsed index.
type Index struct {
	Entries   []Entry
	Unindexed map[string]bool
}

// Lookup returns the entry for path p.
func (x *Index) Lookup(p string) (Entry, bool) {
	i := sort.Search(len(x.Entries), func(i int) bool { return x.Entries[i].Path >= p })
	if i < len(x.Entries) && x.Entries[i].Path == p {
		return x.Entries[i], true
	}
	return Entry{}, false
}

// Parse reads an index the way h9 does: a line that doesn't parse, an
// unknown version, paths out of order, or a path whose parent isn't an
// earlier directory fails the whole index.
func Parse(r io.Reader) (*Index, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	if !sc.Scan() {
		return nil, errors.New("h9index: empty")
	}
	if sc.Text() != fmt.Sprintf("h9index %d", FormatVersion) {
		return nil, fmt.Errorf("h9index: unknown header %q", sc.Text())
	}
	x := &Index{Unindexed: map[string]bool{}}
	dirs := map[string]bool{}
	n := 1
	for sc.Scan() {
		n++
		line := sc.Text()
		if d, ok := strings.CutPrefix(line, "!unindexed "); ok {
			if !dirs[d] {
				return nil, fmt.Errorf("h9index: line %d: %s is not an earlier directory", n, d)
			}
			x.Unindexed[d] = true
			continue
		}
		f := strings.Split(line, " ")
		if len(f) != 9 {
			return nil, fmt.Errorf("h9index: line %d: %d fields", n, len(f))
		}
		var e Entry
		var err error
		e.Path = f[0]
		if e.Qid.Path, err = strconv.ParseUint(f[1], 16, 64); err != nil {
			return nil, fmt.Errorf("h9index: line %d: qid.path: %w", n, err)
		}
		v, err := strconv.ParseUint(f[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("h9index: line %d: qid.vers: %w", n, err)
		}
		e.Qid.Vers = uint32(v)
		switch f[3] {
		case "d":
			e.Qid.Type = plan9.QTDIR
		case "a":
			e.Qid.Type = plan9.QTAPPEND
		case "l":
			e.Qid.Type = plan9.QTEXCL
		case "-":
		default:
			return nil, fmt.Errorf("h9index: line %d: qid.type %q", n, f[3])
		}
		m, err := strconv.ParseUint(f[4], 8, 32)
		if err != nil {
			return nil, fmt.Errorf("h9index: line %d: mode: %w", n, err)
		}
		e.Mode = plan9.Perm(m)
		if e.Length, err = strconv.ParseUint(f[5], 10, 64); err != nil {
			return nil, fmt.Errorf("h9index: line %d: length: %w", n, err)
		}
		mt, err := strconv.ParseUint(f[6], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("h9index: line %d: mtime: %w", n, err)
		}
		e.Mtime = uint32(mt)
		e.Uid, e.Gid = f[7], f[8]
		if len(x.Entries) == 0 {
			if e.Path != "/" {
				return nil, fmt.Errorf("h9index: line %d: first entry is %q, not /", n, e.Path)
			}
		} else {
			if prev := x.Entries[len(x.Entries)-1].Path; e.Path <= prev {
				return nil, fmt.Errorf("h9index: line %d: %s is out of order after %s", n, e.Path, prev)
			}
			i := strings.LastIndexByte(e.Path, '/')
			parent := e.Path[:i]
			if parent == "" {
				parent = "/"
			}
			if i < 0 || !dirs[parent] || !ValidName(e.Path[i+1:]) {
				return nil, fmt.Errorf("h9index: line %d: %s has no earlier parent directory", n, e.Path)
			}
		}
		if e.Mode&plan9.DMDIR != 0 {
			dirs[e.Path] = true
		}
		x.Entries = append(x.Entries, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(x.Entries) == 0 {
		return nil, errors.New("h9index: no entries")
	}
	return x, nil
}

// ParseBytes is Parse over a byte slice.
func ParseBytes(b []byte) (*Index, error) { return Parse(bytes.NewReader(b)) }
