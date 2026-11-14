package tree

import (
	"context"
	"fmt"
	"strings"

	"9fans.net/go/plan9"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
)

// IndexSources returns the h9index sources for fs: the tree as the 9P2000
// server shows it and as the 9P2000.L server does, through the same
// functions those servers answer stats and directory reads with, so an
// index can't disagree with the server it describes. user is the 9P2000
// server's User ("" for the default).
func (f *FS) IndexSources(user string) map[h9index.Variant]h9index.Source {
	return map[h9index.Variant]h9index.Source{
		h9index.NineP: nineSource{f, user},
		h9index.DotL:  dotlSource{f, user},
	}
}

// ResolveDir returns the directory at namespace path p, as the servers'
// walks find it. For h9index.Indexer.Resolve.
func (f *FS) ResolveDir(ctx context.Context, p string) (uint64, error) {
	cur, err := f.Meta.Get(ctx, meta.RootIno)
	if err != nil {
		return 0, err
	}
	for _, name := range strings.Split(p, "/") {
		if name == "" || name == "." {
			continue
		}
		if cur.Kind != meta.KindDir {
			return 0, meta.ErrNotDir
		}
		if cur, err = f.lookup(ctx, cur.Ino, name); err != nil {
			return 0, fmt.Errorf("%s: %w", p, err)
		}
	}
	if cur.Kind != meta.KindDir {
		return 0, fmt.Errorf("%s: %w", p, meta.ErrNotDir)
	}
	return cur.Ino, nil
}

// Indexer returns an h9index.Indexer over fs, storing into st.
func (f *FS) Indexer(st *h9index.Store, user string) *h9index.Indexer {
	return &h9index.Indexer{DB: f.Meta, Store: st, Sources: f.IndexSources(user), Resolve: f.ResolveDir}
}

type nineSource struct {
	fs   *FS
	user string
}

func (s nineSource) Stat(ctx context.Context, ino uint64) (h9index.Child, error) {
	in, err := s.fs.Meta.Get(ctx, ino)
	if err != nil {
		return h9index.Child{}, err
	}
	n := node(in)
	return h9index.Child{Ino: in.Ino, Dir: ninep.Stat("", n, s.user)}, nil
}

func (s nineSource) ReadDir(ctx context.Context, ino uint64, after string, limit int) ([]h9index.Child, string, bool, error) {
	ents, next, done, err := s.fs.ReadDir(ctx, ninep.Node{Ino: ino, Dir: true}, after, limit)
	if err != nil {
		return nil, after, false, err
	}
	out := make([]h9index.Child, 0, len(ents))
	for _, e := range ents {
		out = append(out, h9index.Child{Name: e.Name, Ino: e.Ino, Dir: ninep.Stat(e.Name, e.Node, s.user)})
	}
	return out, next, done, nil
}

// dotlSource is the tree as h9 sees a 9P2000.L root: through its
// translator to stat(5), with symlinks.
type dotlSource struct {
	fs   *FS
	user string
}

func (s dotlSource) Stat(ctx context.Context, ino uint64) (h9index.Child, error) {
	in, err := s.fs.Meta.Get(ctx, ino)
	if err != nil {
		return h9index.Child{}, err
	}
	return h9index.Child{Ino: in.Ino, Dir: dotlStat(in, s.user)}, nil
}

func (s dotlSource) ReadDir(ctx context.Context, ino uint64, after string, limit int) ([]h9index.Child, string, bool, error) {
	des, err := s.fs.Meta.ReadDir(ctx, ino, after, limit)
	if err != nil {
		return nil, after, false, err
	}
	out := make([]h9index.Child, 0, len(des))
	for _, de := range des {
		after = de.Name
		if !lvisible(de.Inode) {
			continue
		}
		out = append(out, h9index.Child{Name: de.Name, Ino: de.Ino, Dir: dotlStat(de.Inode, s.user)})
	}
	return out, after, len(des) < limit, nil
}

// dotlStat maps the 9P2000.L server's qid and attributes for in to
// stat(5), as h9's translator presents them to devmnt: the .L qid, the
// permission bits with DMDIR or DMSYMLINK, the size (0 for a directory, as
// on Plan 9, and the target's length for a symlink), the mtime in seconds,
// and the server's user as owner.
func dotlStat(in meta.Inode, user string) plan9.Dir {
	q, a := lqid(in), lattr(in)
	user = ninep.UserOrDefault(user)
	d := plan9.Dir{
		Qid:    plan9.Qid{Path: q.Path, Vers: q.Version},
		Mode:   plan9.Perm(a.Mode.Permissions()),
		Length: a.Size,
		Atime:  uint32(a.MTimeSeconds),
		Mtime:  uint32(a.MTimeSeconds),
		Uid:    user, Gid: user, Muid: user,
	}
	switch q.Type {
	case p9.TypeDir:
		d.Qid.Type, d.Mode, d.Length = plan9.QTDIR, d.Mode|plan9.DMDIR, 0
	case p9.TypeSymlink:
		d.Qid.Type, d.Mode = plan9.QTSYMLINK, d.Mode|plan9.DMSYMLINK
	}
	return d
}
