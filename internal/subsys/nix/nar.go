package nix

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

// NAR is Nix's archive format: a deterministic serialization of a file,
// symlink or directory tree with only names, contents, symlink targets and
// an executable bit. Serializing the same tree always gives the same bytes,
// which is what lets the cache store unpacked trees and rebuild NARs that
// match the upstream NarHash.

const narMagic = "nix-archive-1"

// maxNARString bounds names, targets and tokens (not file contents).
const maxNARString = 1 << 16

// NARHandler receives a NAR's entries in archive order, parents first. p is
// slash separated and relative to the archive root; the root itself is "".
type NARHandler interface {
	Dir(p string) error
	// File receives a regular file. r yields exactly size bytes; whatever
	// the handler does not read is skipped.
	File(p string, executable bool, size int64, r io.Reader) error
	Symlink(p, target string) error
}

type narReader struct {
	r   io.Reader
	buf [8]byte
}

func (n *narReader) u64() (uint64, error) {
	if _, err := io.ReadFull(n.r, n.buf[:]); err != nil {
		return 0, unexpected(err)
	}
	return binary.LittleEndian.Uint64(n.buf[:]), nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (n *narReader) pad(size uint64) error {
	if p := (8 - size%8) % 8; p > 0 {
		if _, err := io.ReadFull(n.r, n.buf[:p]); err != nil {
			return unexpected(err)
		}
		for _, b := range n.buf[:p] {
			if b != 0 {
				return errors.New("nix: nar: non-zero padding")
			}
		}
	}
	return nil
}

func (n *narReader) str() (string, error) {
	size, err := n.u64()
	if err != nil {
		return "", err
	}
	if size > maxNARString {
		return "", fmt.Errorf("nix: nar: string of %d bytes", size)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(n.r, b); err != nil {
		return "", unexpected(err)
	}
	return string(b), n.pad(size)
}

func (n *narReader) expect(want string) error {
	got, err := n.str()
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("nix: nar: expected %q, got %q", want, got)
	}
	return nil
}

// ReadNAR parses a NAR from r and calls h for each entry. It reads exactly
// the archive's bytes, so a caller hashing r sees the whole NAR.
func ReadNAR(r io.Reader, h NARHandler) error {
	n := &narReader{r: r}
	if err := n.expect(narMagic); err != nil {
		return err
	}
	return n.node("", h)
}

func (n *narReader) node(p string, h NARHandler) error {
	if err := n.expect("("); err != nil {
		return err
	}
	if err := n.expect("type"); err != nil {
		return err
	}
	typ, err := n.str()
	if err != nil {
		return err
	}
	switch typ {
	case "regular":
		tok, err := n.str()
		if err != nil {
			return err
		}
		exec := false
		if tok == "executable" {
			exec = true
			if err := n.expect(""); err != nil {
				return err
			}
			if tok, err = n.str(); err != nil {
				return err
			}
		}
		if tok != "contents" {
			return fmt.Errorf("nix: nar: expected \"contents\", got %q", tok)
		}
		size, err := n.u64()
		if err != nil {
			return err
		}
		if size > 1<<62 {
			return fmt.Errorf("nix: nar: file of %d bytes", size)
		}
		lr := &io.LimitedReader{R: n.r, N: int64(size)}
		if err := h.File(p, exec, int64(size), lr); err != nil {
			return err
		}
		if _, err := io.Copy(io.Discard, lr); err != nil {
			return err
		}
		if lr.N != 0 {
			return io.ErrUnexpectedEOF
		}
		if err := n.pad(size); err != nil {
			return err
		}
		return n.expect(")")
	case "symlink":
		if err := n.expect("target"); err != nil {
			return err
		}
		target, err := n.str()
		if err != nil {
			return err
		}
		if err := h.Symlink(p, target); err != nil {
			return err
		}
		return n.expect(")")
	case "directory":
		if err := h.Dir(p); err != nil {
			return err
		}
		prev := ""
		for {
			tok, err := n.str()
			if err != nil {
				return err
			}
			if tok == ")" {
				return nil
			}
			if tok != "entry" {
				return fmt.Errorf("nix: nar: expected \"entry\", got %q", tok)
			}
			if err := n.expect("("); err != nil {
				return err
			}
			if err := n.expect("name"); err != nil {
				return err
			}
			name, err := n.str()
			if err != nil {
				return err
			}
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
				return fmt.Errorf("nix: nar: bad entry name %q", name)
			}
			if prev != "" && name <= prev {
				return fmt.Errorf("nix: nar: entries out of order: %q after %q", name, prev)
			}
			prev = name
			if err := n.expect("node"); err != nil {
				return err
			}
			if err := n.node(path.Join(p, name), h); err != nil {
				return err
			}
			if err := n.expect(")"); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("nix: nar: unknown node type %q", typ)
}

// Kind of a tree node.
type Kind uint8

const (
	KindDir Kind = iota + 1
	KindFile
	KindSymlink
)

// Node is one entry of a tree being serialized.
type Node struct {
	Kind       Kind
	Executable bool
	Size       int64
	Target     string
	// ID is the tree's own handle for the node.
	ID any
}

// Tree is what WriteNAR serializes.
type Tree interface {
	Root() (Node, error)
	// Children lists a directory's entries in any order.
	Children(dir Node) (names []string, nodes []Node, err error)
	// Open returns a regular file's bytes.
	Open(file Node) (io.ReadCloser, error)
}

type narWriter struct {
	w   io.Writer
	buf [8]byte
	n   int64
}

func (n *narWriter) write(b []byte) error {
	m, err := n.w.Write(b)
	n.n += int64(m)
	return err
}

func (n *narWriter) u64(v uint64) error {
	binary.LittleEndian.PutUint64(n.buf[:], v)
	return n.write(n.buf[:])
}

var zeros [8]byte

func (n *narWriter) pad(size uint64) error {
	if p := (8 - size%8) % 8; p > 0 {
		return n.write(zeros[:p])
	}
	return nil
}

func (n *narWriter) str(ss ...string) error {
	for _, s := range ss {
		if err := n.u64(uint64(len(s))); err != nil {
			return err
		}
		if err := n.write([]byte(s)); err != nil {
			return err
		}
		if err := n.pad(uint64(len(s))); err != nil {
			return err
		}
	}
	return nil
}

// WriteNAR serializes t to w and returns the number of bytes written.
func WriteNAR(w io.Writer, t Tree) (int64, error) {
	n := &narWriter{w: w}
	root, err := t.Root()
	if err != nil {
		return 0, err
	}
	if err := n.str(narMagic); err != nil {
		return n.n, err
	}
	err = n.node(t, root)
	return n.n, err
}

func (n *narWriter) node(t Tree, nd Node) error {
	switch nd.Kind {
	case KindFile:
		if err := n.str("(", "type", "regular"); err != nil {
			return err
		}
		if nd.Executable {
			if err := n.str("executable", ""); err != nil {
				return err
			}
		}
		if err := n.str("contents"); err != nil {
			return err
		}
		if err := n.u64(uint64(nd.Size)); err != nil {
			return err
		}
		if nd.Size > 0 {
			r, err := t.Open(nd)
			if err != nil {
				return err
			}
			c, err := io.Copy(writerFunc(n.write), io.LimitReader(r, nd.Size))
			r.Close()
			if err != nil {
				return err
			}
			if c != nd.Size {
				return fmt.Errorf("nix: nar: file is %d bytes, expected %d", c, nd.Size)
			}
		}
		if err := n.pad(uint64(nd.Size)); err != nil {
			return err
		}
		return n.str(")")
	case KindSymlink:
		return n.str("(", "type", "symlink", "target", nd.Target, ")")
	case KindDir:
		if err := n.str("(", "type", "directory"); err != nil {
			return err
		}
		names, nodes, err := t.Children(nd)
		if err != nil {
			return err
		}
		idx := make([]int, len(names))
		for i := range idx {
			idx[i] = i
		}
		// NAR orders entries by byte-wise name comparison.
		sort.Slice(idx, func(a, b int) bool { return names[idx[a]] < names[idx[b]] })
		for _, i := range idx {
			if err := n.str("entry", "(", "name", names[i], "node"); err != nil {
				return err
			}
			if err := n.node(t, nodes[i]); err != nil {
				return err
			}
			if err := n.str(")"); err != nil {
				return err
			}
		}
		return n.str(")")
	}
	return fmt.Errorf("nix: nar: unknown node kind %d", nd.Kind)
}

type writerFunc func([]byte) error

func (f writerFunc) Write(p []byte) (int, error) {
	if err := f(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// MemTree is an in-memory Tree, for tests and fixtures. Keys are slash
// separated paths relative to the root ("" is the root); a directory is a
// key with a nil MemFile.
type MemTree map[string]*MemFile

// MemFile is a MemTree entry. Nil means directory.
type MemFile struct {
	Data       []byte
	Executable bool
	Symlink    string // non-empty for a symlink
}

func (m MemTree) node(p string) (Node, error) {
	f, ok := m[p]
	if !ok {
		return Node{}, fmt.Errorf("nix: memtree: %q missing", p)
	}
	switch {
	case f == nil:
		return Node{Kind: KindDir, ID: p}, nil
	case f.Symlink != "":
		return Node{Kind: KindSymlink, Target: f.Symlink, ID: p}, nil
	}
	return Node{Kind: KindFile, Executable: f.Executable, Size: int64(len(f.Data)), ID: p}, nil
}

func (m MemTree) Root() (Node, error) { return m.node("") }

func (m MemTree) Children(dir Node) ([]string, []Node, error) {
	p := dir.ID.(string)
	var names []string
	var nodes []Node
	for k := range m {
		if k == "" || path.Dir("/"+k) != path.Clean("/"+p) {
			continue
		}
		nd, err := m.node(k)
		if err != nil {
			return nil, nil, err
		}
		names = append(names, path.Base(k))
		nodes = append(nodes, nd)
	}
	return names, nodes, nil
}

func (m MemTree) Open(file Node) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m[file.ID.(string)].Data)), nil
}

// NAR serializes the tree into memory.
func (m MemTree) NAR() []byte {
	var b bytes.Buffer
	if _, err := WriteNAR(&b, m); err != nil {
		panic(err)
	}
	return b.Bytes()
}
