// Package ninep is a small read-only 9P2000 file server.
//
// It replaces the prototype's go9p tree, whose walks listed every child of
// a directory (and, through the volume, scanned every entry in the index)
// at each step. Here a walk step is one FS.Lookup, directory reads page
// through FS.ReadDir with a cursor, and qids are inode numbers, so a qid
// stays the same across renames and identifies hard links as one file.
//
// Clients are read-only by design: create, write, remove and wstat fail.
package ninep

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"9fans.net/go/plan9"
)

// Node is a file or directory as the server sees it.
type Node struct {
	Ino     uint64
	Dir     bool
	Mode    fs.FileMode // permission bits
	Size    int64
	Mtime   time.Time
	Version uint32 // qid.vers; 0 for the cache's trees, which never change in place
}

// Entry is a named Node in a directory listing.
type Entry struct {
	Name string
	Node
}

// File is an open file's contents.
type File interface {
	io.ReaderAt
	io.Closer
}

// FS is the namespace the server exports.
type FS interface {
	Root(ctx context.Context) (Node, error)
	// Parent returns the parent of a directory; the root is its own parent.
	Parent(ctx context.Context, dir Node) (Node, error)
	Lookup(ctx context.Context, dir Node, name string) (Node, error)
	// ReadDir returns up to limit entries after the cursor ("" = start),
	// the cursor to continue from, and whether the listing is finished.
	ReadDir(ctx context.Context, dir Node, after string, limit int) (entries []Entry, next string, done bool, err error)
	Open(ctx context.Context, n Node) (File, error)
}

// Server serves an FS over 9P2000.
type Server struct {
	FS FS
	// MaxMsize caps the negotiated message size. Default 1 MiB + header.
	MaxMsize uint32
	// User is reported as the owner of every file. Default DefaultUser.
	User string
	// Logf, if set, receives connection-level errors.
	Logf func(format string, args ...any)
}

const dirPage = 256

// DefaultUser owns every file when Server.User is empty.
const DefaultUser = "cache"

// UserOrDefault returns user, or DefaultUser if it is empty.
func UserOrDefault(user string) string {
	if user == "" {
		return DefaultUser
	}
	return user
}

func (s *Server) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// ListenAndServe serves 9P on a TCP address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until it fails.
func (s *Server) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			if err := s.ServeConn(c); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logf("9p: %v: %v", c.RemoteAddr(), err)
			}
		}()
	}
}

type fid struct {
	mu   sync.Mutex
	root uint64 // inode of the attach root; ".." stops there
	node Node
	name string
	open bool
	file File

	// directory read state
	dirOff   uint64
	dirAfter string
	dirDone  bool
	dirPend  [][]byte
}

type conn struct {
	s     *Server
	rwc   io.ReadWriteCloser
	ctx   context.Context
	msize uint32

	wmu sync.Mutex

	mu       sync.Mutex
	fids     map[uint32]*fid
	inflight map[uint16]chan struct{}
	wg       sync.WaitGroup
}

// ServeConn serves one client connection until it closes.
func (s *Server) ServeConn(rwc io.ReadWriteCloser) error {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{s: s, rwc: rwc, ctx: ctx, msize: 8192 + plan9.IOHDRSZ,
		fids: map[uint32]*fid{}, inflight: map[uint16]chan struct{}{}}
	defer func() {
		cancel()
		c.wg.Wait()
		c.clunkAll()
		rwc.Close()
	}()
	for {
		tx, err := plan9.ReadFcall(rwc)
		if err != nil {
			return err
		}
		if tx.Type == plan9.Tversion {
			c.wg.Wait()
			c.reply(c.version(tx))
			continue
		}
		if tx.Type == plan9.Tflush {
			c.mu.Lock()
			done := c.inflight[tx.Oldtag]
			c.mu.Unlock()
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				if done != nil {
					<-done
				}
				c.reply(&plan9.Fcall{Type: plan9.Rflush, Tag: tx.Tag})
			}()
			continue
		}
		done := make(chan struct{})
		c.mu.Lock()
		c.inflight[tx.Tag] = done
		c.mu.Unlock()
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			rx := c.handle(tx)
			rx.Tag = tx.Tag
			c.reply(rx)
			c.mu.Lock()
			delete(c.inflight, tx.Tag)
			c.mu.Unlock()
			close(done)
		}()
	}
}

func (c *conn) reply(rx *plan9.Fcall) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := plan9.WriteFcall(c.rwc, rx); err != nil {
		c.s.logf("9p: write: %v", err)
	}
}

func (c *conn) clunkAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, f := range c.fids {
		if f.file != nil {
			f.file.Close()
		}
		delete(c.fids, id)
	}
}

func rerror(err error) *plan9.Fcall {
	msg := err.Error()
	switch {
	case errors.Is(err, fs.ErrNotExist):
		msg = "file does not exist"
	case errors.Is(err, fs.ErrPermission):
		msg = "permission denied"
	}
	return &plan9.Fcall{Type: plan9.Rerror, Ename: msg}
}

var (
	errReadOnly = errors.New("read-only file system")
	errNoFid    = errors.New("unknown fid")
	errFidInUse = errors.New("fid already in use")
	errNotDir   = errors.New("not a directory")
	errOpen     = errors.New("fid is open")
	errNotOpen  = errors.New("fid not open")
	errOffset   = errors.New("bad directory read offset")
)

func (c *conn) version(tx *plan9.Fcall) *plan9.Fcall {
	c.clunkAll()
	max := c.s.MaxMsize
	if max == 0 {
		max = 1<<20 + plan9.IOHDRSZ
	}
	m := tx.Msize
	if m > max {
		m = max
	}
	c.msize = m
	v := "unknown"
	if strings.HasPrefix(tx.Version, "9P2000") {
		v = "9P2000"
	}
	return &plan9.Fcall{Type: plan9.Rversion, Tag: tx.Tag, Msize: m, Version: v}
}

func (c *conn) getFid(id uint32) (*fid, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.fids[id]
	if f == nil {
		return nil, errNoFid
	}
	return f, nil
}

func (c *conn) addFid(id uint32, f *fid) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.fids[id]; ok {
		return errFidInUse
	}
	c.fids[id] = f
	return nil
}

// Qid is the qid the server reports for n.
func Qid(n Node) plan9.Qid {
	q := plan9.Qid{Path: n.Ino, Vers: n.Version, Type: plan9.QTFILE}
	if n.Dir {
		q.Type = plan9.QTDIR
	}
	return q
}

func (c *conn) handle(tx *plan9.Fcall) *plan9.Fcall {
	switch tx.Type {
	case plan9.Tauth:
		return rerror(errors.New("authentication not required"))
	case plan9.Tattach:
		root, name, err := c.attachRoot(tx.Aname)
		if err != nil {
			return rerror(err)
		}
		if err := c.addFid(tx.Fid, &fid{root: root.Ino, node: root, name: name}); err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rattach, Qid: Qid(root)}
	case plan9.Twalk:
		return c.walk(tx)
	case plan9.Topen:
		return c.open(tx)
	case plan9.Tread:
		return c.read(tx)
	case plan9.Tstat:
		f, err := c.getFid(tx.Fid)
		if err != nil {
			return rerror(err)
		}
		f.mu.Lock()
		b, err := c.stat(f.name, f.node)
		f.mu.Unlock()
		if err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rstat, Stat: b}
	case plan9.Tclunk, plan9.Tremove:
		c.mu.Lock()
		f := c.fids[tx.Fid]
		delete(c.fids, tx.Fid)
		c.mu.Unlock()
		if f == nil {
			return rerror(errNoFid)
		}
		f.mu.Lock()
		if f.file != nil {
			f.file.Close()
			f.file = nil
		}
		f.mu.Unlock()
		if tx.Type == plan9.Tremove {
			return rerror(errReadOnly)
		}
		return &plan9.Fcall{Type: plan9.Rclunk}
	case plan9.Tcreate, plan9.Twrite, plan9.Twstat:
		return rerror(errReadOnly)
	}
	return rerror(errors.New("unsupported message"))
}

// attachRoot resolves an attach name to the directory it selects. The
// attach name is a slash-separated path into the namespace ("local/alpine",
// "/oci/x/rootfs"); empty selects the whole namespace.
func (c *conn) attachRoot(aname string) (Node, string, error) {
	cur, err := c.s.FS.Root(c.ctx)
	if err != nil {
		return Node{}, "", err
	}
	name := "/"
	for _, w := range strings.Split(aname, "/") {
		switch w {
		case "", ".":
			continue
		case "..":
			return Node{}, "", errors.New("attach name may not contain ..")
		}
		if !cur.Dir {
			return Node{}, "", errNotDir
		}
		if cur, err = c.s.FS.Lookup(c.ctx, cur, w); err != nil {
			return Node{}, "", err
		}
		name = w
	}
	if !cur.Dir {
		return Node{}, "", errNotDir
	}
	return cur, name, nil
}

func (c *conn) walk(tx *plan9.Fcall) *plan9.Fcall {
	f, err := c.getFid(tx.Fid)
	if err != nil {
		return rerror(err)
	}
	if len(tx.Wname) > plan9.MAXWELEM {
		return rerror(errors.New("too many names in walk"))
	}
	f.mu.Lock()
	if f.open {
		f.mu.Unlock()
		return rerror(errOpen)
	}
	cur, name, root := f.node, f.name, f.root
	f.mu.Unlock()

	qids := make([]plan9.Qid, 0, len(tx.Wname))
	for _, w := range tx.Wname {
		if !cur.Dir {
			err = errNotDir
			break
		}
		var next Node
		switch w {
		case ".":
			next = cur
		case "..":
			if cur.Ino == root {
				next = cur
			} else {
				next, err = c.s.FS.Parent(c.ctx, cur)
			}
		default:
			next, err = c.s.FS.Lookup(c.ctx, cur, w)
		}
		if err != nil {
			break
		}
		cur, name = next, w
		qids = append(qids, Qid(cur))
	}
	if len(qids) < len(tx.Wname) {
		if len(qids) == 0 {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rwalk, Wqid: qids}
	}
	nf := &fid{root: root, node: cur, name: name}
	if tx.Newfid == tx.Fid {
		c.mu.Lock()
		c.fids[tx.Fid] = nf
		c.mu.Unlock()
	} else if err := c.addFid(tx.Newfid, nf); err != nil {
		return rerror(err)
	}
	return &plan9.Fcall{Type: plan9.Rwalk, Wqid: qids}
}

func (c *conn) open(tx *plan9.Fcall) *plan9.Fcall {
	f, err := c.getFid(tx.Fid)
	if err != nil {
		return rerror(err)
	}
	if m := tx.Mode & 3; (m != plan9.OREAD && m != plan9.OEXEC) || tx.Mode&(plan9.OTRUNC|plan9.ORCLOSE) != 0 {
		return rerror(errReadOnly)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.open {
		return rerror(errOpen)
	}
	if !f.node.Dir {
		file, err := c.s.FS.Open(c.ctx, f.node)
		if err != nil {
			return rerror(err)
		}
		f.file = file
	}
	f.open = true
	return &plan9.Fcall{Type: plan9.Ropen, Qid: Qid(f.node), Iounit: c.msize - plan9.IOHDRSZ}
}

func (c *conn) read(tx *plan9.Fcall) *plan9.Fcall {
	f, err := c.getFid(tx.Fid)
	if err != nil {
		return rerror(err)
	}
	count := tx.Count
	if max := c.msize - plan9.IOHDRSZ; count > max {
		count = max
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.open {
		return rerror(errNotOpen)
	}
	if f.node.Dir {
		data, err := c.readDir(f, tx.Offset, count)
		if err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rread, Data: data}
	}
	buf := make([]byte, count)
	n, err := f.file.ReadAt(buf, int64(tx.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return rerror(err)
	}
	return &plan9.Fcall{Type: plan9.Rread, Data: buf[:n]}
}

func (c *conn) readDir(f *fid, off uint64, count uint32) ([]byte, error) {
	if off == 0 {
		f.dirOff, f.dirAfter, f.dirDone, f.dirPend = 0, "", false, nil
	} else if off != f.dirOff {
		return nil, errOffset
	}
	var out []byte
	for {
		if len(f.dirPend) > 0 {
			b := f.dirPend[0]
			if len(out)+len(b) > int(count) {
				if len(out) == 0 {
					return nil, errors.New("read count too small for directory entry")
				}
				break
			}
			out = append(out, b...)
			f.dirPend = f.dirPend[1:]
			continue
		}
		if f.dirDone {
			break
		}
		ents, next, done, err := c.s.FS.ReadDir(c.ctx, f.node, f.dirAfter, dirPage)
		if err != nil {
			return nil, err
		}
		f.dirAfter, f.dirDone = next, done
		for _, e := range ents {
			b, err := c.stat(e.Name, e.Node)
			if err != nil {
				return nil, err
			}
			f.dirPend = append(f.dirPend, b)
		}
	}
	f.dirOff += uint64(len(out))
	return out, nil
}

func (c *conn) stat(name string, n Node) ([]byte, error) {
	d := Stat(name, n, c.s.User)
	return d.Bytes()
}

// Stat is the directory entry the server reports for n: the reply to Tstat
// and each record of a directory read. h9index builds its lines from it so
// an index can't disagree with the server.
func Stat(name string, n Node, user string) plan9.Dir {
	user = UserOrDefault(user)
	mode := plan9.Perm(n.Mode.Perm())
	if n.Dir {
		mode |= plan9.DMDIR
	}
	mt := uint32(n.Mtime.Unix())
	return plan9.Dir{Qid: Qid(n), Mode: mode, Atime: mt, Mtime: mt, Length: uint64(n.Size),
		Name: name, Uid: user, Gid: user, Muid: user}
}

// Logger adapts the standard logger for Server.Logf.
func Logger() func(string, ...any) { return log.Printf }
