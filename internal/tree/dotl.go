package tree

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// DotL serves the namespace read-only over 9P2000.L, which is what Linux
// guests mount (v9fs, virtio-9p). Unlike plain 9P2000 it carries symlinks,
// hard link counts and full timestamps.
type DotL struct{ FS *FS }

var _ p9.Attacher = DotL{}

// Attach implements p9.Attacher.
func (d DotL) Attach() (p9.File, error) {
	in, err := d.FS.metaGet(context.Background(), meta.RootIno)
	if err != nil {
		return nil, errno(err)
	}
	return &lnode{fs: d.FS, in: in}, nil
}

// NewDotLServer returns a 9P2000.L server for fs.
func NewDotLServer(fs *FS) *p9.Server { return p9.NewServer(DotL{FS: fs}) }

type lnode struct {
	p9.DefaultWalkGetAttr
	templatefs.XattrUnimplemented
	templatefs.NotLockable
	templatefs.NoopRenamed

	fs  *FS
	in  meta.Inode
	idx *h9index.File // set for a root's generated /.h9index

	mu   sync.Mutex
	file cas.File // open regular file

	// readdir position cache: the entry after lastName is at lastOff
	lastOff  uint64
	lastName string
}

var _ p9.File = (*lnode)(nil)

func errno(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrNotExist):
		return linux.ENOENT
	case errors.Is(err, meta.ErrNotDir):
		return linux.ENOTDIR
	}
	return linux.EIO
}

// lqid is the 9P2000.L qid of an inode. Its version is 0, as over plain
// 9P2000: unit trees never change in place.
func lqid(in meta.Inode) p9.QID {
	q := p9.QID{Path: in.Ino, Type: p9.TypeRegular}
	switch in.Kind {
	case meta.KindDir:
		q.Type = p9.TypeDir
	case meta.KindSymlink:
		q.Type = p9.TypeSymlink
	}
	return q
}

func (n *lnode) qid() p9.QID {
	if n.idx != nil {
		return p9.QID{Path: n.idx.Ino, Version: n.idx.Vers, Type: p9.TypeRegular}
	}
	return lqid(n.in)
}

// lvisible reports whether the 9P2000.L server shows an inode.
func lvisible(in meta.Inode) bool { return in.Kind != meta.KindWhiteout }

// indexNode returns dir's generated /.h9index, if it has one.
func (n *lnode) indexNode(ctx context.Context, dir uint64) (*lnode, bool, error) {
	x, ok, err := n.fs.indexFile(ctx, dir, h9index.DotL)
	if err != nil || !ok {
		return nil, false, err
	}
	in := meta.Inode{Ino: x.Ino, Kind: meta.KindFile, Mode: 0o444, Size: x.Size, Mtime: x.Mtime}
	return &lnode{fs: n.fs, in: in, idx: &x}, true, nil
}

func lmode(in meta.Inode) p9.FileMode {
	m := p9.FileMode(in.Mode.Perm())
	switch in.Kind {
	case meta.KindDir:
		return m | p9.ModeDirectory
	case meta.KindSymlink:
		return m | p9.ModeSymlink
	}
	return m | p9.ModeRegular
}

func (n *lnode) Walk(names []string) ([]p9.QID, p9.File, error) {
	ctx := context.Background()
	cur := n.in
	var idx *lnode
	qids := make([]p9.QID, 0, len(names))
	for _, name := range names {
		if cur.Kind != meta.KindDir || idx != nil {
			return nil, nil, linux.ENOTDIR
		}
		var next meta.Inode
		var err error
		if name == h9index.Name {
			var ok bool
			if idx, ok, err = n.indexNode(ctx, cur.Ino); err != nil {
				return nil, nil, errno(err)
			} else if ok {
				qids = append(qids, idx.qid())
				continue
			}
		}
		if name == ".." {
			next, err = n.fs.get(ctx, cur.Parent)
		} else if sin, ok, serr := n.fs.synthLookup(ctx, cur.Ino, name); serr != nil || ok {
			next, err = sin, serr
		} else {
			next, err = n.fs.lookup(ctx, cur.Ino, name)
		}
		if err != nil {
			return nil, nil, errno(err)
		}
		if next.Kind == meta.KindWhiteout {
			return nil, nil, linux.ENOENT
		}
		cur = next
		qids = append(qids, lqid(cur))
	}
	if idx != nil {
		return qids, idx, nil
	}
	return qids, &lnode{fs: n.fs, in: cur}, nil
}

// lattr is the 9P2000.L attributes of an inode, except its link count.
func lattr(in meta.Inode) p9.Attr {
	a := p9.Attr{
		Mode:             lmode(in),
		NLink:            1,
		Size:             uint64(in.Size),
		BlockSize:        4096,
		Blocks:           (uint64(in.Size) + 511) / 512,
		ATimeSeconds:     uint64(in.Mtime.Unix()),
		ATimeNanoSeconds: uint64(in.Mtime.Nanosecond()),
		MTimeSeconds:     uint64(in.Mtime.Unix()),
		MTimeNanoSeconds: uint64(in.Mtime.Nanosecond()),
		CTimeSeconds:     uint64(in.Mtime.Unix()),
		CTimeNanoSeconds: uint64(in.Mtime.Nanosecond()),
	}
	if in.Kind == meta.KindDir {
		a.NLink, a.Size, a.Blocks = 2, 4096, 8
	}
	return a
}

func (n *lnode) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	in := n.in
	a := lattr(in)
	if in.Kind != meta.KindDir && n.idx == nil && !isSynth(in.Ino) && req.NLink {
		nl, err := n.fs.metaNlink(context.Background(), in.Ino)
		if err != nil {
			return p9.QID{}, p9.AttrMask{}, p9.Attr{}, linux.EIO
		}
		a.NLink = p9.NLink(nl)
	}
	valid := p9.AttrMask{Mode: true, NLink: true, UID: true, GID: true, Size: true,
		Blocks: true, ATime: true, MTime: true, CTime: true}
	return n.qid(), valid, a, nil
}

func (n *lnode) StatFS() (p9.FSStat, error) {
	s, err := n.fs.Meta.Stats(context.Background())
	if err != nil {
		return p9.FSStat{}, linux.EIO
	}
	used, _ := n.fs.CAS.Usage()
	return p9.FSStat{
		Type:       0x01021997, // V9FS_MAGIC
		BlockSize:  4096,
		Blocks:     uint64(used) / 4096,
		Files:      uint64(s.Inodes),
		NameLength: 255,
	}, nil
}

func (n *lnode) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	if mode.Mode() != p9.ReadOnly {
		return p9.QID{}, 0, linux.EROFS
	}
	switch {
	case n.idx != nil:
		f, err := n.fs.openIndex(context.Background(), n.idx.Ino)
		if err != nil {
			return p9.QID{}, 0, errno(err)
		}
		n.mu.Lock()
		n.file = f
		n.mu.Unlock()
	case isSynth(n.in.Ino):
		f, err := n.fs.synthOpen(context.Background(), n.in.Ino)
		if err != nil {
			return p9.QID{}, 0, errno(err)
		}
		n.mu.Lock()
		n.file = f
		n.mu.Unlock()
	case n.in.Kind == meta.KindFile:
		// Re-read: the walk may predate an ingest or a refault. (The cache
		// is dropped on every namespace commit; fill.Manager re-reads
		// uncached before it opens.)
		in, err := n.fs.metaGet(context.Background(), n.in.Ino)
		if err != nil {
			return p9.QID{}, 0, errno(err)
		}
		n.in = in
		f, err := n.fs.open(context.Background(), in)
		if err != nil {
			return p9.QID{}, 0, errno(err)
		}
		n.mu.Lock()
		n.file = f
		n.mu.Unlock()
	case n.in.Kind == meta.KindSymlink:
		return p9.QID{}, 0, linux.ELOOP
	}
	return n.qid(), 0, nil
}

func (n *lnode) ReadAt(p []byte, off int64) (int, error) {
	n.mu.Lock()
	f := n.file
	n.mu.Unlock()
	if n.in.Kind == meta.KindDir {
		return 0, linux.EISDIR
	}
	if f == nil {
		return 0, linux.EBADF
	}
	nr, err := f.ReadAt(p, off)
	if errors.Is(err, io.EOF) {
		err = nil
	} else if err != nil {
		err = linux.EIO
	}
	return nr, err
}

// indexOffset is the readdir offset of a root's /.h9index, which is listed
// after every real entry. Real entries' offsets are their positions, so
// they stay the same whether or not the directory has an index, even if
// one is generated in the middle of a listing.
const indexOffset = 1 << 62

func (n *lnode) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	if n.in.Kind != meta.KindDir {
		return nil, linux.ENOTDIR
	}
	if offset >= indexOffset {
		return nil, nil
	}
	ctx := context.Background()
	if n.fs.isSynthDir(n.in.Ino) {
		return n.synthReaddir(ctx, offset, count)
	}
	idx, indexed, err := n.indexNode(ctx, n.in.Ino)
	if err != nil {
		return nil, linux.EIO
	}
	out, done, used, err := n.readdir(ctx, offset, count, indexed)
	if err != nil || !done || !indexed {
		return out, err
	}
	if used+direntFixed+len(h9index.Name) > int(count) {
		return out, nil // the next call, from the last offset, lists it
	}
	q := idx.qid()
	return append(out, p9.Dirent{QID: q, Type: q.Type, Name: h9index.Name, Offset: indexOffset}), nil
}

// direntFixed is an encoded dirent's size without its name: a 13-byte
// qid, an 8-byte offset, a type byte and a 2-byte name length.
const direntFixed = 13 + 8 + 1 + 2

// readdir lists real entries from position offset. It reports whether it
// reached the end of the listing and how many reply bytes it used.
func (n *lnode) readdir(ctx context.Context, offset uint64, count uint32, hideIndex bool) (p9.Dirents, bool, int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// count is the reply size in bytes.
	limit := int(count)/direntFixed + 1
	if limit > 1024 {
		limit = 1024
	}
	var des []meta.Dirent
	var err error
	switch {
	case subsys.IsVirtual(n.in.Ino):
		des, err = n.fs.viewReadDir(ctx, n.in.Ino)
		if err == nil {
			des = des[min(offset, uint64(len(des))):]
			des = des[:min(limit, len(des))]
		}
	case offset == 0:
		des, err = n.fs.Meta.ReadDir(ctx, n.in.Ino, "", limit)
	case offset == n.lastOff && n.lastName != "":
		des, err = n.fs.Meta.ReadDir(ctx, n.in.Ino, n.lastName, limit)
	default:
		des, err = n.fs.Meta.ReadDirAt(ctx, n.in.Ino, offset, limit)
	}
	if err != nil {
		return nil, false, 0, linux.EIO
	}
	// Offsets are positions in the full listing, hidden entries included,
	// so a cursor stays valid whatever is skipped.
	out := make(p9.Dirents, 0, len(des))
	size, used := 0, 0
	for i, de := range des {
		if lvisible(de.Inode) && !(hideIndex && de.Name == h9index.Name) {
			if size+direntFixed+len(de.Name) > int(count) {
				break
			}
			size += direntFixed + len(de.Name)
			q := lqid(de.Inode)
			out = append(out, p9.Dirent{QID: q, Type: q.Type, Name: de.Name, Offset: offset + uint64(i) + 1})
		}
		used = i + 1
	}
	if used > 0 {
		n.lastOff, n.lastName = offset+uint64(used), des[used-1].Name
	}
	done := used == len(des) && len(des) < limit
	if len(out) == 0 && used == limit {
		// A whole page of hidden entries: an empty reply would end the
		// listing, so read on.
		n.mu.Unlock()
		defer n.mu.Lock()
		return n.readdir(ctx, offset+uint64(used), count, hideIndex)
	}
	return out, done, size, nil
}

func (n *lnode) Readlink() (string, error) {
	if n.in.Kind != meta.KindSymlink {
		return "", linux.EINVAL
	}
	return n.in.Target, nil
}

func (n *lnode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.file != nil {
		n.file.Close()
		n.file = nil
	}
	return nil
}

// Everything below changes the tree, and the tree is read-only to clients.

func (*lnode) WriteAt([]byte, int64) (int, error)       { return 0, linux.EROFS }
func (*lnode) FSync() error                             { return linux.EROFS }
func (*lnode) SetAttr(p9.SetAttrMask, p9.SetAttr) error { return linux.EROFS }
func (*lnode) Remove() error                            { return linux.EROFS }
func (*lnode) Rename(p9.File, string) error             { return linux.EROFS }
func (*lnode) RenameAt(string, p9.File, string) error   { return linux.EROFS }
func (*lnode) UnlinkAt(string, uint32) error            { return linux.EROFS }
func (*lnode) Link(p9.File, string) error               { return linux.EROFS }
func (*lnode) Mkdir(string, p9.FileMode, p9.UID, p9.GID) (p9.QID, error) {
	return p9.QID{}, linux.EROFS
}
func (*lnode) Symlink(string, string, p9.UID, p9.GID) (p9.QID, error) {
	return p9.QID{}, linux.EROFS
}
func (*lnode) Mknod(string, p9.FileMode, uint32, uint32, p9.UID, p9.GID) (p9.QID, error) {
	return p9.QID{}, linux.EROFS
}
func (*lnode) Create(string, p9.OpenFlags, p9.FileMode, p9.UID, p9.GID) (p9.File, p9.QID, uint32, error) {
	return nil, p9.QID{}, 0, linux.EROFS
}
