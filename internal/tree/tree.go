// Package tree serves the metadata namespace with file bytes from the CAS,
// in the shape the 9P server wants.
package tree

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// FS adapts meta + cas to ninep.FS.
//
// Plain 9P2000 has no symlinks, so symlinks are stored in the metadata but
// not shown over 9P yet: they are skipped in listings and do not resolve in
// walks. How H9 wants them exposed is an open question.
type FS struct {
	Meta *meta.DB
	CAS  *cas.Store

	// Files, if set, opens file bytes in any unit state (the fill path's
	// spool or the CAS). Without it files are read from the CAS only.
	Files interface {
		Open(ctx context.Context, in meta.Inode) (cas.File, error)
	}
	// Faults, if set, is told about lookup misses so it can fault in the
	// unit that holds the name. It reports whether to retry the lookup.
	Faults interface {
		Miss(ctx context.Context, dir, name string) (bool, error)
	}
	// Index, if set, serves generated indexes: each indexed root directory
	// has a read-only /.h9index, the variant for the server's transport.
	// It shadows a real file of that name.
	Index *h9index.Store

	views viewSet // see view.go
	cache nsCache // see cache.go

	// Synthetic, if set, are read-only files in /.cache (SyntheticDir)
	// whose bytes are generated when they are looked up, such as
	// /.cache/usage. Call InitSynthetic before serving.
	Synthetic map[string]SyntheticFunc

	synthMu    sync.Mutex
	synthDir   uint64
	synthSnaps map[string]synthSnap
}

// lookup finds name in dir, faulting in a unit on a miss.
func (f *FS) lookup(ctx context.Context, dir uint64, name string) (meta.Inode, error) {
	if in, ok, err := f.viewLookup(ctx, dir, name); ok {
		return in, err
	}
	in, err := f.metaLookup(ctx, dir, name)
	if err == nil || f.Faults == nil || !errors.Is(err, meta.ErrNotExist) {
		return in, err
	}
	p, perr := f.Meta.PathOf(ctx, dir)
	if perr != nil {
		return in, err
	}
	retry, ferr := f.Faults.Miss(ctx, p, name)
	if ferr != nil {
		return in, ferr
	}
	if !retry {
		return in, err
	}
	return f.metaLookup(ctx, dir, name)
}

// open returns a file inode's bytes.
func (f *FS) open(ctx context.Context, in meta.Inode) (cas.File, error) {
	if f.Files != nil {
		return f.Files.Open(ctx, in)
	}
	if in.Content == "" {
		return emptyFile{}, nil
	}
	return f.CAS.Open(ctx, in.Content)
}

var _ ninep.FS = (*FS)(nil)

// node is how the 9P2000 server sees an inode. qid.vers is 0: a unit's
// tree never changes in place (refetching produces the same tree), and a
// version taken from the CAS ref would change when ingest sets the ref,
// which h9's index layer would see as a stale file.
func node(in meta.Inode) ninep.Node {
	n := ninep.Node{Ino: in.Ino, Dir: in.Kind == meta.KindDir, Mode: in.Mode, Size: in.Size, Mtime: in.Mtime}
	if n.Dir {
		n.Size = 0
	}
	return n
}

func visible(in meta.Inode) bool { return in.Kind == meta.KindDir || in.Kind == meta.KindFile }

func (f *FS) Root(ctx context.Context) (ninep.Node, error) {
	in, err := f.metaGet(ctx, meta.RootIno)
	return node(in), err
}

func (f *FS) Parent(ctx context.Context, dir ninep.Node) (ninep.Node, error) {
	in, err := f.get(ctx, dir.Ino)
	if err != nil {
		return ninep.Node{}, err
	}
	p, err := f.get(ctx, in.Parent)
	return node(p), err
}

func (f *FS) Lookup(ctx context.Context, dir ninep.Node, name string) (ninep.Node, error) {
	if name == h9index.Name {
		if x, ok, err := f.indexFile(ctx, dir.Ino, h9index.NineP); err != nil || ok {
			return indexNode(x), err
		}
	}
	if in, ok, err := f.synthLookup(ctx, dir.Ino, name); err != nil || ok {
		return node(in), err
	}
	in, err := f.lookup(ctx, dir.Ino, name)
	if err != nil {
		return ninep.Node{}, err
	}
	if !visible(in) {
		return ninep.Node{}, meta.ErrNotExist
	}
	return node(in), nil
}

func (f *FS) ReadDir(ctx context.Context, dir ninep.Node, after string, limit int) ([]ninep.Entry, string, bool, error) {
	if f.isSynthDir(dir.Ino) {
		des := f.synthEntries(ctx, after)
		if len(des) > 0 {
			after = des[len(des)-1].Name
		}
		return synthEntriesNineP(des), after, true, nil
	}
	var des []meta.Dirent
	var err error
	if subsys.IsVirtual(dir.Ino) {
		var done bool
		if des, done, err = f.viewPage(ctx, dir.Ino, after, limit); err == nil && done {
			limit = len(des) + 1 // report done below
		}
	} else {
		des, err = f.Meta.ReadDir(ctx, dir.Ino, after, limit)
	}
	if err != nil {
		return nil, after, false, err
	}
	done := len(des) < limit
	// An indexed directory lists its /.h9index in name order, so the
	// name cursor stays valid: it goes in the page whose range covers it.
	var x h9index.File
	var indexed bool
	if after < h9index.Name && (done || des[len(des)-1].Name >= h9index.Name) {
		if x, indexed, err = f.indexFile(ctx, dir.Ino, h9index.NineP); err != nil {
			return nil, after, false, err
		}
	}
	shadow := indexed
	out := make([]ninep.Entry, 0, len(des)+1)
	for _, de := range des {
		if indexed && de.Name >= h9index.Name {
			out = append(out, ninep.Entry{Name: h9index.Name, Node: indexNode(x)})
			indexed = false
		}
		after = de.Name
		if de.Name == h9index.Name && shadow {
			continue // shadowed by the generated index
		}
		if visible(de.Inode) {
			out = append(out, ninep.Entry{Name: de.Name, Node: node(de.Inode)})
		}
	}
	if indexed {
		out = append(out, ninep.Entry{Name: h9index.Name, Node: indexNode(x)})
	}
	return out, after, done, nil
}

// indexFile returns dir's generated index for variant v, if it has one.
func (f *FS) indexFile(ctx context.Context, dir uint64, v h9index.Variant) (h9index.File, bool, error) {
	if f.Index == nil {
		return h9index.File{}, false, nil
	}
	return f.Index.Get(ctx, dir, v)
}

func indexNode(x h9index.File) ninep.Node {
	return ninep.Node{Ino: x.Ino, Mode: 0o444, Size: x.Size, Mtime: x.Mtime, Version: x.Vers}
}

// openIndex returns the bytes of the index file numbered ino.
func (f *FS) openIndex(ctx context.Context, ino uint64) (cas.File, error) {
	dir, v, _ := h9index.IsFileIno(ino)
	if f.Index == nil {
		return nil, meta.ErrNotExist
	}
	b, err := f.Index.Read(ctx, dir, v)
	if err != nil {
		return nil, err
	}
	return bytesFile{bytes.NewReader(b)}, nil
}

type bytesFile struct{ *bytes.Reader }

func (bytesFile) Close() error { return nil }

func (f *FS) Open(ctx context.Context, n ninep.Node) (ninep.File, error) {
	if _, _, ok := h9index.IsFileIno(n.Ino); ok {
		return f.openIndex(ctx, n.Ino)
	}
	if isSynth(n.Ino) {
		return f.synthOpen(ctx, n.Ino)
	}
	in, err := f.metaGet(ctx, n.Ino)
	if err != nil {
		return nil, err
	}
	return f.open(ctx, in)
}

type emptyFile struct{}

func (emptyFile) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (emptyFile) Close() error                      { return nil }
