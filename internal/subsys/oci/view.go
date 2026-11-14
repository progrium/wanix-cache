package oci

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// The namespace under /oci/ besides layers/ is virtual:
//
//	/oci/<registry>/            nsdir
//	/oci/<registry>/<a>/<b>/    nsdir (repository path components)
//	/oci/<registry>/<repo>@<v>/ the image's merged rootfs (mdir), and
//	                            every directory below it (mdir)
//
// Files and symlinks below an image are the real inodes of the layer that
// wins the lookup.

type vnode interface{ inode() meta.Inode }

func vino(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return subsys.VirtualBit | (h.Sum64() & subsys.VirtualMask)
}

const maxRepoDepth = 16

// nsdir is a registry or repository path prefix.
type nsdir struct {
	ino, parent uint64
	path        string // relative to /oci/, e.g. "docker.io/library"
}

func (d *nsdir) inode() meta.Inode {
	return meta.Inode{Ino: d.ino, Kind: meta.KindDir, Mode: 0o555, Parent: d.parent}
}

// ViewLookup implements subsys.Viewer.
func (o *OCI) ViewLookup(ctx context.Context, dir uint64, name string) (meta.Inode, bool, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return meta.Inode{}, false, nil
	}
	if !subsys.IsVirtual(dir) {
		// The subsystem root: registries are virtual, layers/ is real.
		if name == LayersDir || !IsRegistry(name) {
			return meta.Inode{}, false, nil
		}
		o.mu.Lock()
		o.rootIno = dir
		o.mu.Unlock()
		return o.nsChild(dir, name).inode(), true, nil
	}
	n, err := o.node(ctx, dir)
	if err != nil {
		return meta.Inode{}, true, err
	}
	switch n := n.(type) {
	case *nsdir:
		p := n.path + "/" + name
		if strings.Contains(name, "@") {
			r, err := ParseRef(p)
			if err != nil {
				return meta.Inode{}, true, fs.ErrNotExist
			}
			v, err := o.viewFor(ctx, r, p, n.ino)
			if err != nil {
				return meta.Inode{}, true, err
			}
			return v.root.inode(), true, nil
		}
		if !compRE.MatchString(name) || strings.Count(p, "/") > maxRepoDepth {
			return meta.Inode{}, true, fs.ErrNotExist
		}
		return o.nsChild(n.ino, p).inode(), true, nil
	case *mdir:
		in, err := n.lookup(ctx, name)
		return in, true, err
	}
	return meta.Inode{}, true, fs.ErrNotExist
}

func (o *OCI) nsChild(parent uint64, p string) *nsdir {
	ino := vino("ns", p)
	o.mu.Lock()
	defer o.mu.Unlock()
	if n, ok := o.nodes[ino].(*nsdir); ok {
		return n
	}
	n := &nsdir{ino: ino, parent: parent, path: p}
	o.nodes[ino] = n
	o.paths[ino] = p
	return n
}

// ViewGet implements subsys.Viewer.
func (o *OCI) ViewGet(ctx context.Context, ino uint64) (meta.Inode, error) {
	n, err := o.node(ctx, ino)
	if err != nil {
		return meta.Inode{}, err
	}
	return n.inode(), nil
}

// ViewReadDir implements subsys.Viewer.
func (o *OCI) ViewReadDir(ctx context.Context, ino uint64) ([]meta.Dirent, error) {
	n, err := o.node(ctx, ino)
	if err != nil {
		return nil, err
	}
	switch n := n.(type) {
	case *nsdir:
		return o.nsList(ctx, n)
	case *mdir:
		return n.readDir(ctx)
	}
	return nil, fs.ErrNotExist
}

// node returns a live virtual directory, walking to it again by path if
// its view was rebuilt.
func (o *OCI) node(ctx context.Context, ino uint64) (vnode, error) {
	o.mu.Lock()
	n, ok := o.nodes[ino]
	p, known := o.paths[ino]
	root := o.rootIno
	o.mu.Unlock()
	if ok {
		if m, isM := n.(*mdir); !isM || !m.v.broken.Load() {
			return n, nil
		}
	}
	if !known || root == 0 {
		return nil, fs.ErrNotExist
	}
	dir := root
	for _, name := range strings.Split(p, "/") {
		in, ok, err := o.ViewLookup(ctx, dir, name)
		if err != nil {
			return nil, err
		}
		if !ok || in.Kind != meta.KindDir {
			return nil, fs.ErrNotExist
		}
		dir = in.Ino
	}
	o.mu.Lock()
	n, ok = o.nodes[ino]
	o.mu.Unlock()
	if !ok {
		return nil, fs.ErrNotExist // the tag now names another image
	}
	return n, nil
}

// nsList lists the images and repository prefixes known under an nsdir.
func (o *OCI) nsList(ctx context.Context, n *nsdir) ([]meta.Dirent, error) {
	rows, err := o.st.db.QueryContext(ctx, `
		SELECT registry || '/' || repo || '@' || tag, image FROM oci_tag WHERE platform = ?
		UNION SELECT registry || '/' || repo || '@' || digest, digest FROM oci_image`, o.opts.Platform.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var out []meta.Dirent
	prefix := n.path + "/"
	for rows.Next() {
		var full, digest string
		if err := rows.Scan(&full, &digest); err != nil {
			return nil, err
		}
		rest, ok := strings.CutPrefix(full, prefix)
		if !ok {
			continue
		}
		name, _, sub := strings.Cut(rest, "/")
		if sub && strings.Contains(name, "@") {
			sub = false // a tag can't contain "/", so this is the image
			name = rest
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		p := prefix + name
		var ino uint64
		if sub {
			ino = vino("ns", p)
		} else {
			ino = vino("v", p, digest, "")
		}
		o.mu.Lock()
		o.paths[ino] = p
		o.mu.Unlock()
		out = append(out, meta.Dirent{Name: name, Inode: meta.Inode{Ino: ino, Kind: meta.KindDir, Mode: 0o555, Parent: n.ino}})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// view is one image reference's merged rootfs.
type view struct {
	o        *OCI
	ref      Ref
	path     string // ref path under /oci/
	digest   string
	resolved time.Time
	layers   []*vlayer // top layer first
	root     *mdir

	last   atomic.Int64
	broken atomic.Bool

	mu       sync.Mutex
	active   bool
	releases []func() // per layer, while active
}

// vlayer is one layer of a view.
type vlayer struct {
	key subsys.Key
	idx int

	mu       sync.Mutex
	unit     meta.Unit
	complete bool // the layer's listing is final
}

func (o *OCI) viewFor(ctx context.Context, r Ref, p string, parent uint64) (*view, error) {
	o.mu.Lock()
	v := o.views[p]
	o.mu.Unlock()
	stale := v != nil && r.Tag != "" && o.opts.TagTTL >= 0 && time.Since(v.resolved) > o.opts.TagTTL && !v.isActive()
	if v != nil && !v.broken.Load() && !stale {
		v.use(ctx)
		return v, nil
	}
	im, err := o.ResolveImage(ctx, r)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	o.mu.Lock()
	if cur := o.views[p]; cur != nil && cur != v && cur.digest == im.Digest && !cur.broken.Load() {
		o.mu.Unlock()
		cur.use(ctx)
		return cur, nil
	}
	nv := &view{o: o, ref: r, path: p, digest: im.Digest, resolved: time.Now()}
	for i := len(im.Layers) - 1; i >= 0; i-- {
		nv.layers = append(nv.layers, &vlayer{key: subsys.Key(im.Layers[i].DiffID), idx: len(nv.layers)})
	}
	nv.root = &mdir{v: nv, ino: vino("v", p, im.Digest, ""), parent: parent, kids: map[string]*mdir{}}
	o.views[p] = nv
	o.nodes[nv.root.ino] = nv.root
	o.paths[nv.root.ino] = p
	o.mu.Unlock()
	if v != nil {
		v.release()
	}
	nv.use(ctx)
	if o.opts.Indexer != nil {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			nv.index()
		}()
	}
	return nv, nil
}

// index asks for the rootfs's /.h9index once every layer's listing is
// final. The view's directories are virtual, so the indexer can't tell on
// its own whether the layers under it are ready.
func (v *view) index() {
	ctx := v.o.ctx
	for _, l := range v.layers {
		if _, err := v.start(ctx, l); err != nil {
			return
		}
		if err := v.waitComplete(ctx, l); err != nil {
			return
		}
	}
	if err := v.o.opts.Indexer.IndexRoot(context.WithValue(ctx, quietKey{}, true), "/"+Name+"/"+v.path); err != nil && ctx.Err() == nil {
		v.o.opts.Logf("oci: %s: index: %v", v.ref, err)
	}
}

func (v *view) lastUse() time.Time { return time.Unix(0, v.last.Load()) }

func (v *view) isActive() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.active
}

// quietKey marks a context whose walks are not uses: index generation
// must not keep an image pinned or refresh its LRU position.
type quietKey struct{}

// use marks the view used and, if it was idle, pins its layers and starts
// every layer's fill, top layer first.
func (v *view) use(ctx context.Context) {
	if ctx.Value(quietKey{}) != nil {
		return
	}
	v.last.Store(time.Now().UnixNano())
	v.o.touch(v.digest)
	v.mu.Lock()
	if v.active {
		v.mu.Unlock()
		return
	}
	v.active = true
	v.releases = make([]func(), len(v.layers))
	v.mu.Unlock()
	// One goroutine starts the layers in order, top first. Each start
	// returns once its fill has begun, and fills take the registry's
	// download slots in the order they ask, so the upper layers, which
	// lookups wait on first, download first.
	v.o.wg.Add(1)
	go func() {
		defer v.o.wg.Done()
		for _, l := range v.layers {
			if _, err := v.start(v.o.ctx, l); err != nil && v.o.ctx.Err() == nil {
				v.o.opts.Logf("oci: %s: layer %s: %v", v.ref, l.key, err)
			}
		}
	}()
}

func (v *view) release() {
	v.mu.Lock()
	rs := v.releases
	v.releases, v.active = nil, false
	v.mu.Unlock()
	for _, r := range rs {
		if r != nil {
			r()
		}
	}
}

func (v *view) pin(l *vlayer, unitID int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.active && v.releases[l.idx] == nil {
		v.releases[l.idx] = v.o.units.Acquire(unitID)
	}
}

// listingDone reports whether a unit's manifest is final: it finished a
// fill (spooled, resident), was evicted with its manifest kept, or is
// being refetched into that kept manifest.
func listingDone(u meta.Unit) bool {
	switch u.State {
	case meta.UnitSpooled, meta.UnitResident, meta.UnitEvicted:
		return true
	case meta.UnitFetching:
		return u.LogicalBytes > 0 // a refault; first fills record 0 until spooled
	}
	return false
}

// start makes sure the layer's fill has begun (so its root exists) and
// returns its unit.
func (v *view) start(ctx context.Context, l *vlayer) (meta.Unit, error) {
	l.mu.Lock()
	u := l.unit
	l.mu.Unlock()
	if u.Root == 0 {
		var err error
		u, err = v.o.db.Unit(ctx, Name, string(l.key))
		switch {
		case err == nil && u.Root != 0 && (u.State == meta.UnitSpooled || u.State == meta.UnitResident):
		case err == nil && u.State == meta.UnitEvicted:
			// The kept manifest answers lookups; opens fault the layer back
			// in. Prefetch it too, since a booting image will read it.
			go v.o.units.Ensure(v.o.ctx, Name, l.key, false)
		default:
			if u, err = v.o.units.Ensure(ctx, Name, l.key, false); err != nil {
				return u, err
			}
		}
		l.mu.Lock()
		l.unit = u
		if listingDone(u) {
			l.complete = true
		}
		l.mu.Unlock()
	}
	v.pin(l, u.ID)
	return u, nil
}

// waitComplete blocks until the layer's listing is final.
func (v *view) waitComplete(ctx context.Context, l *vlayer) error {
	l.mu.Lock()
	done := l.complete
	l.mu.Unlock()
	if done {
		return nil
	}
	u, err := v.o.units.Ensure(ctx, Name, l.key, true)
	if err != nil {
		if ctx.Err() == nil {
			// The fill failed; a first fill's partial tree is gone, so the
			// view must be rebuilt on the next access.
			v.broken.Store(true)
		}
		return fmt.Errorf("oci: %s: layer %s: %w", v.ref, l.key, err)
	}
	l.mu.Lock()
	moved := l.unit.Root != 0 && l.unit.Root != u.Root
	l.unit, l.complete = u, true
	l.mu.Unlock()
	if moved {
		// An earlier fill failed and a new one built a new tree: entries
		// this view found in the old one are gone.
		v.broken.Store(true)
		return fmt.Errorf("oci: %s: layer %s was refetched; retry", v.ref, l.key)
	}
	return nil
}

func (l *vlayer) isComplete() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.complete
}

// dent is a directory's entry in one layer.
type dent struct {
	l  int // index into view.layers, top first
	in meta.Inode
}

// mdir is a merged directory: the same path in every layer that has it as
// a directory, top first, down to the first layer where it is opaque,
// whited out or not a directory. The list is extended lazily, so a lookup
// that the top layers answer never waits on the lower ones.
type mdir struct {
	v      *view
	ino    uint64
	parent uint64
	inner  string // path inside the rootfs, "" for the root
	up     *mdir  // nil for the root
	name   string

	mu     sync.Mutex
	ents   []dent
	cursor int // next index of up's entries to examine
	done   bool
	kids   map[string]*mdir
	list   []meta.Dirent // once every layer is complete
}

func (m *mdir) inode() meta.Inode {
	in := meta.Inode{Kind: meta.KindDir, Mode: 0o555}
	m.mu.Lock()
	if len(m.ents) > 0 {
		in = m.ents[0].in
	}
	m.mu.Unlock()
	in.Ino, in.Parent, in.Unit, in.Flags, in.Size = m.ino, m.parent, 0, 0, 0
	return in
}

// entry returns the k'th layer entry, extending the list as needed.
func (m *mdir) entry(ctx context.Context, k int) (dent, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.ents) <= k {
		if m.done {
			return dent{}, false, nil
		}
		if err := m.extend(ctx); err != nil {
			return dent{}, false, err
		}
	}
	return m.ents[k], true, nil
}

// extend adds the next layer entry or sets done. m.mu is held.
func (m *mdir) extend(ctx context.Context) error {
	v := m.v
	if m.up == nil {
		i := len(m.ents)
		if i >= len(v.layers) {
			m.done = true
			return nil
		}
		u, err := v.start(ctx, v.layers[i])
		if err != nil {
			return err
		}
		in, err := v.o.db.Get(ctx, u.Root)
		if errors.Is(err, meta.ErrNotExist) {
			// The layer's first fill failed and took its tree with it.
			v.broken.Store(true)
			return fmt.Errorf("oci: %s: layer %s: fill failed", v.ref, v.layers[i].key)
		}
		if err != nil {
			return err
		}
		m.ents = append(m.ents, dent{l: i, in: in})
		return nil
	}
	if n := len(m.ents); n > 0 {
		// Going below a layer's directory needs that layer's final word on
		// whether the directory is opaque.
		last := m.ents[n-1]
		if err := v.waitComplete(ctx, v.layers[last.l]); err != nil {
			return err
		}
		in, err := v.o.db.Get(ctx, last.in.Ino)
		if err != nil {
			return err
		}
		if in.Flags&meta.FlagOpaque != 0 {
			m.done = true
			return nil
		}
	}
	for {
		pe, ok, err := m.up.entry(ctx, m.cursor)
		if err != nil {
			return err
		}
		if !ok {
			m.done = true
			return nil
		}
		m.cursor++
		in, found, err := v.lookupIn(ctx, pe, m.name)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if in.Kind == meta.KindDir {
			m.ents = append(m.ents, dent{l: pe.l, in: in})
		} else {
			m.done = true // a whiteout or a non-directory hides the rest
		}
		return nil
	}
}

// lookupIn looks name up in one layer's directory. A miss in a layer that
// is still being fetched waits for its listing to be final.
func (v *view) lookupIn(ctx context.Context, d dent, name string) (meta.Inode, bool, error) {
	l := v.layers[d.l]
	in, err := v.o.db.Lookup(ctx, d.in.Ino, name)
	if errors.Is(err, meta.ErrNotExist) && !l.isComplete() {
		if err := v.waitComplete(ctx, l); err != nil {
			return in, false, err
		}
		in, err = v.o.db.Lookup(ctx, d.in.Ino, name)
	}
	if errors.Is(err, meta.ErrNotExist) {
		return in, false, nil
	}
	return in, err == nil, err
}

func (m *mdir) lookup(ctx context.Context, name string) (meta.Inode, error) {
	m.v.use(ctx)
	m.mu.Lock()
	if k := m.kids[name]; k != nil {
		m.mu.Unlock()
		return k.inode(), nil
	}
	m.mu.Unlock()
	for k := 0; ; k++ {
		pe, ok, err := m.entry(ctx, k)
		if err != nil {
			return meta.Inode{}, err
		}
		if !ok {
			return meta.Inode{}, fs.ErrNotExist
		}
		in, found, err := m.v.lookupIn(ctx, pe, name)
		if err != nil {
			return meta.Inode{}, err
		}
		if !found {
			continue
		}
		switch in.Kind {
		case meta.KindWhiteout:
			return meta.Inode{}, fs.ErrNotExist
		case meta.KindDir:
			return m.child(name, dent{l: pe.l, in: in}, k+1).inode(), nil
		}
		return in, nil
	}
}

func (m *mdir) child(name string, first dent, cursor int) *mdir {
	inner := path.Join(m.inner, name)
	o := m.v.o
	m.mu.Lock()
	k := m.kids[name]
	if k == nil {
		k = &mdir{v: m.v, ino: vino("v", m.v.path, m.v.digest, inner), parent: m.ino, inner: inner, up: m, name: name,
			ents: []dent{first}, cursor: cursor, kids: map[string]*mdir{}}
		m.kids[name] = k
	}
	m.mu.Unlock()
	o.mu.Lock()
	o.nodes[k.ino] = k
	o.paths[k.ino] = m.v.path + "/" + inner
	o.mu.Unlock()
	return k
}

// readDir merges the directory's listing across its layers. It waits for
// each of those layers' listings to be final.
func (m *mdir) readDir(ctx context.Context) ([]meta.Dirent, error) {
	m.v.use(ctx)
	m.mu.Lock()
	if m.list != nil {
		l := m.list
		m.mu.Unlock()
		return l, nil
	}
	m.mu.Unlock()
	var ents []dent
	for k := 0; ; k++ {
		e, ok, err := m.entry(ctx, k)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		ents = append(ents, e)
	}
	v, o := m.v, m.v.o
	decided := map[string]bool{}
	var out []meta.Dirent
	for _, e := range ents {
		if err := v.waitComplete(ctx, v.layers[e.l]); err != nil {
			return nil, err
		}
		var hidden []string
		for after := ""; ; {
			des, err := o.db.ReadDir(ctx, e.in.Ino, after, 1024)
			if err != nil {
				return nil, err
			}
			for _, de := range des {
				if decided[de.Name] {
					continue
				}
				// A whiteout hides lower layers, not its own layer.
				hidden = append(hidden, de.Name)
				if de.Kind == meta.KindWhiteout {
					continue
				}
				if de.Kind == meta.KindDir {
					inner := path.Join(m.inner, de.Name)
					ino := vino("v", v.path, v.digest, inner)
					de.Inode.Ino, de.Inode.Parent, de.Inode.Unit, de.Inode.Flags = ino, m.ino, 0, 0
					o.mu.Lock()
					o.paths[ino] = v.path + "/" + inner
					o.mu.Unlock()
				}
				out = append(out, de)
			}
			if len(des) < 1024 {
				break
			}
			after = des[len(des)-1].Name
		}
		for _, n := range hidden {
			decided[n] = true
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if out == nil {
		out = []meta.Dirent{}
	}
	m.mu.Lock()
	m.list = out
	m.mu.Unlock()
	return out, nil
}
