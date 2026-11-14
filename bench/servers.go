package bench

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/tree"
)

// The disk-backed servers below are the production servers with one
// change: an open regular file reads from its plain-disk copy instead of
// the CAS. Walks, stats and listings still go to the same metadata DB.

// DiskFS is tree.FS (the plain 9P2000 adapter) reading bytes from disk.
type DiskFS struct {
	*tree.FS
	Paths map[uint64]string
}

var _ ninep.FS = DiskFS{}

// Open implements ninep.FS.
func (d DiskFS) Open(ctx context.Context, n ninep.Node) (ninep.File, error) {
	if p, ok := d.Paths[n.Ino]; ok {
		return os.Open(p)
	}
	return d.FS.Open(ctx, n)
}

// OpenFunc opens the bytes of a regular file by inode. ok is false when
// the inode is not one it serves; the wrapped server then handles it.
type OpenFunc func(ino uint64) (h Handle, ok bool, err error)

// DotLWith is tree.DotL (the 9P2000.L server) with regular-file bytes from
// Open instead of the CAS.
type DotLWith struct {
	DotL tree.DotL
	Open OpenFunc
}

var _ p9.Attacher = DotLWith{}

// DiskOpen serves files from their plain-disk paths.
func DiskOpen(paths map[uint64]string) OpenFunc {
	return func(ino uint64) (Handle, bool, error) {
		p, ok := paths[ino]
		if !ok {
			return nil, false, nil
		}
		f, err := os.Open(p)
		return f, true, err
	}
}

// Attach implements p9.Attacher.
func (d DotLWith) Attach() (p9.File, error) {
	f, err := d.DotL.Attach()
	if err != nil {
		return nil, err
	}
	return &dnode{File: f, open: d.Open, qid: p9.QID{Path: meta.RootIno, Type: p9.TypeDir}}, nil
}

type dnode struct {
	p9.File
	open OpenFunc
	qid  p9.QID

	mu sync.Mutex
	f  Handle
}

// WalkGetAttr reports ENOSYS so the server falls back to Walk on the
// wrapper (the inner file would hand back an unwrapped node).
func (n *dnode) WalkGetAttr([]string) ([]p9.QID, p9.File, p9.AttrMask, p9.Attr, error) {
	return nil, nil, p9.AttrMask{}, p9.Attr{}, linux.ENOSYS
}

func (n *dnode) Walk(names []string) ([]p9.QID, p9.File, error) {
	qids, f, err := n.File.Walk(names)
	if err != nil {
		return nil, nil, err
	}
	q := n.qid
	if len(qids) > 0 {
		q = qids[len(qids)-1]
	}
	return qids, &dnode{File: f, open: n.open, qid: q}, nil
}

func (n *dnode) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	if n.qid.Type != p9.TypeRegular {
		return n.File.Open(mode)
	}
	if mode.Mode() != p9.ReadOnly {
		return p9.QID{}, 0, linux.EROFS
	}
	f, ok, err := n.open(n.qid.Path)
	if !ok {
		return n.File.Open(mode)
	}
	if err != nil {
		return p9.QID{}, 0, linux.EIO
	}
	n.mu.Lock()
	n.f = f
	n.mu.Unlock()
	return n.qid, 0, nil
}

func (n *dnode) ReadAt(p []byte, off int64) (int, error) {
	n.mu.Lock()
	f := n.f
	n.mu.Unlock()
	if f == nil {
		return n.File.ReadAt(p, off)
	}
	nr, err := f.ReadAt(p, off)
	if errors.Is(err, io.EOF) {
		err = nil
	} else if err != nil {
		err = linux.EIO
	}
	return nr, err
}

func (n *dnode) Close() error {
	n.mu.Lock()
	if n.f != nil {
		n.f.Close()
		n.f = nil
	}
	n.mu.Unlock()
	return n.File.Close()
}

// Servers are the 9P servers of one fixture, on loopback TCP.
type Servers struct {
	DotLCAS, DotLDisk, NineCAS, NineDisk string // listen addresses
	DotLCached                           string // 9P2000.L over Cached, if set
	Cached                               *CachedCAS
	lns                                  []net.Listener
}

// Prototype cache settings for the +cache readers.
var (
	CacheBytes     int64 = 256 << 20
	CacheReadahead int64 = 1 << 20
)

// StartServers starts the servers for f.
func StartServers(f *Fixture) (*Servers, error) {
	fs := &tree.FS{Meta: f.DB, CAS: f.CAS}
	s := &Servers{}
	listen := func(serve func(net.Listener)) (string, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return "", err
		}
		s.lns = append(s.lns, ln)
		go serve(ln)
		return ln.Addr().String(), nil
	}
	var err error
	if s.DotLCAS, err = listen(func(ln net.Listener) { tree.NewDotLServer(fs).Serve(ln) }); err != nil {
		return nil, err
	}
	if s.DotLDisk, err = listen(func(ln net.Listener) {
		p9.NewServer(DotLWith{DotL: tree.DotL{FS: fs}, Open: DiskOpen(f.Paths)}).Serve(ln)
	}); err != nil {
		s.Close()
		return nil, err
	}
	if s.NineCAS, err = listen(func(ln net.Listener) { (&ninep.Server{FS: fs}).Serve(ln) }); err != nil {
		s.Close()
		return nil, err
	}
	if s.NineDisk, err = listen(func(ln net.Listener) {
		(&ninep.Server{FS: DiskFS{FS: fs, Paths: f.Paths}}).Serve(ln)
	}); err != nil {
		s.Close()
		return nil, err
	}
	if s.Cached, err = OpenCachedCAS(f.Work, CacheBytes, CacheReadahead); err != nil {
		s.Close()
		return nil, err
	}
	open := func(ino uint64) (Handle, bool, error) {
		ref, ok := f.Refs[ino]
		if !ok || ref == "" {
			return nil, false, nil
		}
		h, err := s.Cached.Open(context.Background(), ref)
		return h, true, err
	}
	if s.DotLCached, err = listen(func(ln net.Listener) {
		p9.NewServer(DotLWith{DotL: tree.DotL{FS: fs}, Open: open}).Serve(ln)
	}); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Close stops listening. Open connections are left to their clients.
func (s *Servers) Close() {
	for _, ln := range s.lns {
		ln.Close()
	}
}
