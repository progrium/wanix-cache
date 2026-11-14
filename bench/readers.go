package bench

import (
	"context"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"9fans.net/go/plan9"
	"9fans.net/go/plan9/client"
	"github.com/hugelgupf/p9/p9"
)

// Msize is the 9P message size both clients ask for: 128 KiB, the Linux
// v9fs default for TCP. Reads larger than the payload are split.
const Msize = 128 << 10

// Handle is an open file.
type Handle interface {
	io.ReaderAt
	io.Closer
}

// Reader opens fixture files one way.
type Reader interface {
	Name() string
	Open(f *File) (Handle, error)
	Close() error
}

// Reader names, in report order.
const (
	DirectDisk = "direct-disk"
	DirectCAS  = "direct-cas"
	DotLDisk   = "9p2000.L-disk"
	DotLCAS    = "9p2000.L-cas"
	NineDisk   = "9p2000-disk"
	NineCAS    = "9p2000-cas"

	// The prototype CAS read path (CachedCAS).
	DirectCached = "direct-cas+cache"
	DotLCached   = "9p2000.L-cas+cache"
)

// ReaderNames lists every reader, disk before CAS in each pair, then the
// prototype.
var ReaderNames = []string{DirectDisk, DirectCAS, DotLDisk, DotLCAS, NineDisk, NineCAS, DirectCached, DotLCached}

// NewReader returns the named reader for fixture fx served by s.
func NewReader(name string, fx *Fixture, s *Servers) (Reader, error) {
	switch name {
	case DirectDisk:
		return diskReader{}, nil
	case DirectCAS:
		return casReader{fx}, nil
	case DotLDisk:
		return dialDotL(name, s.DotLDisk, fx.Prefix())
	case DotLCAS:
		return dialDotL(name, s.DotLCAS, fx.Prefix())
	case NineDisk:
		return dial9P(name, s.NineDisk, fx.Prefix())
	case NineCAS:
		return dial9P(name, s.NineCAS, fx.Prefix())
	case DirectCached:
		return cachedReader{s.Cached}, nil
	case DotLCached:
		return dialDotL(name, s.DotLCached, fx.Prefix())
	}
	return nil, os.ErrNotExist
}

type diskReader struct{}

func (diskReader) Name() string                 { return DirectDisk }
func (diskReader) Open(f *File) (Handle, error) { return os.Open(f.Disk) }
func (diskReader) Close() error                 { return nil }

type casReader struct{ fx *Fixture }

func (casReader) Name() string { return DirectCAS }
func (r casReader) Open(f *File) (Handle, error) {
	if f.Ref == "" {
		return empty{}, nil
	}
	return r.fx.CAS.Open(context.Background(), f.Ref)
}
func (casReader) Close() error { return nil }

type cachedReader struct{ c *CachedCAS }

func (cachedReader) Name() string { return DirectCached }
func (r cachedReader) Open(f *File) (Handle, error) {
	if f.Ref == "" {
		return empty{}, nil
	}
	return r.c.Open(context.Background(), f.Ref)
}
func (cachedReader) Close() error { return nil }

type empty struct{}

func (empty) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (empty) Close() error                      { return nil }

// dotlReader is a hugelgupf/p9 9P2000.L client. Each Open walks from the
// attach root, as a client with no dentry cache would.
type dotlReader struct {
	name   string
	cl     *p9.Client
	root   p9.File
	prefix []string
}

func dialDotL(name, addr, prefix string) (*dotlReader, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cl, err := p9.NewClient(c, p9.WithMessageSize(Msize))
	if err != nil {
		c.Close()
		return nil, err
	}
	root, err := cl.Attach("")
	if err != nil {
		cl.Close()
		return nil, err
	}
	return &dotlReader{name: name, cl: cl, root: root, prefix: split(prefix)}, nil
}

func split(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func (r *dotlReader) Name() string { return r.name }

func (r *dotlReader) Open(f *File) (Handle, error) {
	names := append(append([]string(nil), r.prefix...), split(f.Rel)...)
	cur := r.root
	for first := true; first || len(names) > 0; first = false {
		n := len(names)
		if n > plan9.MAXWELEM {
			n = plan9.MAXWELEM
		}
		_, next, err := cur.Walk(names[:n])
		if cur != r.root {
			cur.Close()
		}
		if err != nil {
			return nil, err
		}
		cur, names = next, names[n:]
	}
	if _, _, err := cur.Open(p9.ReadOnly); err != nil {
		cur.Close()
		return nil, err
	}
	return cur, nil
}

func (r *dotlReader) Close() error { return r.cl.Close() }

// nineReader is a 9fans.net plain 9P2000 client. It keeps a pool of
// connections and gives each open file one to itself until it is closed:
// the 9fans client frees a fid number before its Tclunk is sent, and the
// server handles requests concurrently, so a second goroutine sharing the
// connection can reuse the number in a Twalk that overtakes the clunk
// ("fid already in use"). One connection per in-flight open avoids that.
type nineReader struct {
	name, addr, prefix string
	pool               chan *nineConn
	mu                 sync.Mutex
	all                []*nineConn
}

type nineConn struct {
	conn *client.Conn
	fsys *client.Fsys
}

func dial9P(name, addr, prefix string) (*nineReader, error) {
	r := &nineReader{name: name, addr: addr, prefix: prefix, pool: make(chan *nineConn, 64)}
	c, err := r.dial()
	if err != nil {
		return nil, err
	}
	r.pool <- c
	return r, nil
}

func (r *nineReader) dial() (*nineConn, error) {
	c, err := net.Dial("tcp", r.addr)
	if err != nil {
		return nil, err
	}
	conn, err := client.NewConn(c) // asks for msize 131072
	if err != nil {
		c.Close()
		return nil, err
	}
	fsys, err := conn.Attach(nil, "bench", "")
	if err != nil {
		conn.Close()
		return nil, err
	}
	nc := &nineConn{conn, fsys}
	r.mu.Lock()
	r.all = append(r.all, nc)
	r.mu.Unlock()
	return nc, nil
}

func (r *nineReader) Name() string { return r.name }

func (r *nineReader) Open(f *File) (Handle, error) {
	var nc *nineConn
	select {
	case nc = <-r.pool:
	default:
		var err error
		if nc, err = r.dial(); err != nil {
			return nil, err
		}
	}
	fid, err := nc.fsys.Open(r.prefix+"/"+f.Rel, plan9.OREAD)
	if err != nil {
		r.put(nc)
		return nil, err
	}
	return &nineHandle{Fid: fid, r: r, nc: nc}, nil
}

func (r *nineReader) put(nc *nineConn) {
	select {
	case r.pool <- nc:
	default:
	}
}

type nineHandle struct {
	*client.Fid
	r  *nineReader
	nc *nineConn
}

func (h *nineHandle) Close() error {
	err := h.Fid.Close()
	h.r.put(h.nc)
	return err
}

func (r *nineReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.all {
		c.conn.Close()
	}
	return nil
}
