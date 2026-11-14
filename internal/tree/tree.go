// Package tree serves the metadata namespace with file bytes from the CAS,
// in the shape the 9P server wants.
package tree

import (
	"context"
	"hash/fnv"
	"io"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
)

// FS adapts meta + cas to ninep.FS.
//
// Plain 9P2000 has no symlinks, so symlinks are stored in the metadata but
// not shown over 9P yet: they are skipped in listings and do not resolve in
// walks. How H9 wants them exposed is an open question.
type FS struct {
	Meta *meta.DB
	CAS  *cas.Store
}

var _ ninep.FS = (*FS)(nil)

func node(in meta.Inode) ninep.Node {
	n := ninep.Node{Ino: in.Ino, Dir: in.Kind == meta.KindDir, Mode: in.Mode, Size: in.Size, Mtime: in.Mtime}
	if in.Kind == meta.KindFile && in.Content != "" {
		h := fnv.New32a()
		io.WriteString(h, in.Content)
		n.Version = h.Sum32()
	}
	if n.Dir {
		n.Size = 0
	}
	return n
}

func visible(in meta.Inode) bool { return in.Kind == meta.KindDir || in.Kind == meta.KindFile }

func (f *FS) Root(ctx context.Context) (ninep.Node, error) {
	in, err := f.Meta.Get(ctx, meta.RootIno)
	return node(in), err
}

func (f *FS) Parent(ctx context.Context, dir ninep.Node) (ninep.Node, error) {
	in, err := f.Meta.Get(ctx, dir.Ino)
	if err != nil {
		return ninep.Node{}, err
	}
	p, err := f.Meta.Get(ctx, in.Parent)
	return node(p), err
}

func (f *FS) Lookup(ctx context.Context, dir ninep.Node, name string) (ninep.Node, error) {
	in, err := f.Meta.Lookup(ctx, dir.Ino, name)
	if err != nil {
		return ninep.Node{}, err
	}
	if !visible(in) {
		return ninep.Node{}, meta.ErrNotExist
	}
	return node(in), nil
}

func (f *FS) ReadDir(ctx context.Context, dir ninep.Node, after string, limit int) ([]ninep.Entry, string, bool, error) {
	des, err := f.Meta.ReadDir(ctx, dir.Ino, after, limit)
	if err != nil {
		return nil, after, false, err
	}
	out := make([]ninep.Entry, 0, len(des))
	for _, de := range des {
		after = de.Name
		if visible(de.Inode) {
			out = append(out, ninep.Entry{Name: de.Name, Node: node(de.Inode)})
		}
	}
	return out, after, len(des) < limit, nil
}

func (f *FS) Open(ctx context.Context, n ninep.Node) (ninep.File, error) {
	in, err := f.Meta.Get(ctx, n.Ino)
	if err != nil {
		return nil, err
	}
	if in.Content == "" {
		return emptyFile{}, nil
	}
	return f.CAS.Open(ctx, in.Content)
}

type emptyFile struct{}

func (emptyFile) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (emptyFile) Close() error                      { return nil }
