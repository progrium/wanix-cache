package nix

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"sort"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Closure views. /nix/closure/<hash>-<name>/ is a metadata-only root for H9:
//
//	/nix/closure/                      vdir (lists the closures seen so far)
//	/nix/closure/<path>/               vdir, the root
//	/nix/closure/<path>/nix/           vdir
//	/nix/closure/<path>/nix/store/     vdir, one entry per closure member
//	/nix/closure/<path>/nix/store/<m>  the real root inode of unit <m>
//
// The member set comes from narinfo References alone, so no Nix is needed.
// Everything below a member is that member's real tree: opens fault it in
// through the fill path, and usage counts against the member. While a view
// is in use its members are pinned against eviction, and each member's fill
// is started, so booting a closure pulls it all in parallel.

const closureDir = "closure"

type vkind uint8

const (
	vClosures vkind = iota + 1 // /nix/closure
	vRoot                      // /nix/closure/<path>
	vNix                       // .../nix
	vStore                     // .../nix/store
)

type vdir struct {
	ino, parent uint64
	kind        vkind
	view        *cview // nil for vClosures
}

func (d *vdir) inode() meta.Inode {
	return meta.Inode{Ino: d.ino, Kind: meta.KindDir, Mode: 0o555, Mtime: storeMtime, Parent: d.parent}
}

// cview is one closure.
type cview struct {
	s       *Subsystem
	root    StorePath
	members []StorePath
	dirs    [3]*vdir // vRoot, vNix, vStore

	mu       sync.Mutex
	units    map[StorePath]meta.Unit
	releases []func()
	idle     *time.Timer
	indexed  bool
}

type viewState struct {
	mu       sync.Mutex
	nixRoot  uint64
	closures *vdir
	nodes    map[uint64]*vdir
	views    map[StorePath]*cview
}

func (v *viewState) init() {
	v.nodes = map[uint64]*vdir{}
	v.views = map[StorePath]*cview{}
}

func vino(parts ...string) uint64 {
	h := fnv.New64a()
	h.Write([]byte("nix"))
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return subsys.VirtualBit | (h.Sum64() & subsys.VirtualMask)
}

// ViewLookup implements subsys.Viewer.
func (s *Subsystem) ViewLookup(ctx context.Context, dir uint64, name string) (meta.Inode, bool, error) {
	if !subsys.IsVirtual(dir) {
		if name != closureDir {
			return meta.Inode{}, false, nil
		}
		s.views.mu.Lock()
		defer s.views.mu.Unlock()
		if s.views.closures == nil {
			s.views.nixRoot = dir
			s.views.closures = &vdir{ino: vino(closureDir), parent: dir, kind: vClosures}
			s.views.nodes[s.views.closures.ino] = s.views.closures
		}
		return s.views.closures.inode(), true, nil
	}
	d, err := s.vnode(dir)
	if err != nil {
		return meta.Inode{}, true, err
	}
	switch d.kind {
	case vClosures:
		sp, err := ParseStorePath(name)
		if err != nil {
			return meta.Inode{}, true, fs.ErrNotExist
		}
		v, err := s.view(ctx, sp)
		if err != nil {
			return meta.Inode{}, true, err
		}
		v.use()
		return v.dirs[0].inode(), true, nil
	case vRoot, vNix:
		d.view.use()
		if want := map[vkind]string{vRoot: "nix", vNix: "store"}[d.kind]; name == want {
			return d.view.dirs[d.kind-vRoot+1].inode(), true, nil
		}
		return meta.Inode{}, true, fs.ErrNotExist
	case vStore:
		d.view.use()
		sp, err := ParseStorePath(name)
		if err != nil || !d.view.has(sp) {
			return meta.Inode{}, true, fs.ErrNotExist
		}
		in, err := d.view.memberRoot(ctx, sp)
		return in, true, err
	}
	return meta.Inode{}, true, fs.ErrNotExist
}

// ViewGet implements subsys.Viewer.
func (s *Subsystem) ViewGet(ctx context.Context, ino uint64) (meta.Inode, error) {
	d, err := s.vnode(ino)
	if err != nil {
		return meta.Inode{}, err
	}
	return d.inode(), nil
}

// ViewReadDir implements subsys.Viewer.
func (s *Subsystem) ViewReadDir(ctx context.Context, ino uint64) ([]meta.Dirent, error) {
	d, err := s.vnode(ino)
	if err != nil {
		return nil, err
	}
	switch d.kind {
	case vClosures:
		s.views.mu.Lock()
		var out []meta.Dirent
		for sp, v := range s.views.views {
			out = append(out, meta.Dirent{Name: string(sp), Inode: v.dirs[0].inode()})
		}
		s.views.mu.Unlock()
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out, nil
	case vRoot:
		d.view.use()
		return []meta.Dirent{{Name: "nix", Inode: d.view.dirs[1].inode()}}, nil
	case vNix:
		d.view.use()
		return []meta.Dirent{{Name: "store", Inode: d.view.dirs[2].inode()}}, nil
	case vStore:
		d.view.use()
		out := make([]meta.Dirent, 0, len(d.view.members))
		for _, m := range d.view.members {
			in, err := d.view.memberRoot(ctx, m)
			if err != nil {
				return nil, err
			}
			out = append(out, meta.Dirent{Name: string(m), Inode: in})
		}
		return out, nil
	}
	return nil, fs.ErrNotExist
}

func (s *Subsystem) vnode(ino uint64) (*vdir, error) {
	s.views.mu.Lock()
	defer s.views.mu.Unlock()
	d, ok := s.views.nodes[ino]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return d, nil
}

// view returns the closure view for sp, building it from narinfos.
func (s *Subsystem) view(ctx context.Context, sp StorePath) (*cview, error) {
	s.views.mu.Lock()
	v := s.views.views[sp]
	s.views.mu.Unlock()
	if v != nil {
		return v, nil
	}
	if s.manager() == nil {
		return nil, fmt.Errorf("nix: closure views need a fill manager (Attach)")
	}
	members, err := s.Closure(ctx, sp)
	if err != nil {
		return nil, err
	}
	s.views.mu.Lock()
	defer s.views.mu.Unlock()
	if v := s.views.views[sp]; v != nil {
		return v, nil
	}
	v = &cview{s: s, root: sp, members: members, units: map[StorePath]meta.Unit{}}
	parent := s.views.closures.ino
	for i, k := range []vkind{vRoot, vNix, vStore} {
		d := &vdir{ino: vino(closureDir, string(sp), fmt.Sprint(k)), parent: parent, kind: k, view: v}
		v.dirs[i] = d
		s.views.nodes[d.ino] = d
		parent = d.ino
	}
	s.views.views[sp] = v
	return v, nil
}

func (v *cview) has(sp StorePath) bool {
	i := sort.Search(len(v.members), func(i int) bool { return v.members[i] >= sp })
	return i < len(v.members) && v.members[i] == sp
}

// use pins the view's members while it is in use and starts their fills.
// The pins drop after Options.ViewIdle without a lookup.
func (v *cview) use() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.idle != nil {
		v.idle.Reset(v.s.opts.ViewIdle)
		return
	}
	v.idle = time.AfterFunc(v.s.opts.ViewIdle, v.release)
	go v.activate()
}

func (v *cview) release() {
	v.mu.Lock()
	rs := v.releases
	v.releases, v.idle = nil, nil
	v.units = map[StorePath]meta.Unit{} // re-pinned on the next use
	v.mu.Unlock()
	for _, r := range rs {
		r()
	}
}

// activate starts every member's fill, pins it, and indexes the root once
// every member has its manifest.
func (v *cview) activate() {
	ctx := context.Background()
	m := v.s.manager()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	var failed bool
	var fmu sync.Mutex
	for _, sp := range v.members {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			if _, err := v.memberRoot(ctx, sp); err != nil {
				fmu.Lock()
				failed = true
				fmu.Unlock()
				v.s.logf("nix: closure %s: %s: %v", v.root, sp, err)
				return
			}
			// Wait for the manifest, so the index sees the whole tree.
			if _, err := m.Ensure(ctx, Name, subsys.Key(sp), true); err != nil {
				fmu.Lock()
				failed = true
				fmu.Unlock()
				v.s.logf("nix: closure %s: %s: %v", v.root, sp, err)
			}
		}()
	}
	wg.Wait()
	v.mu.Lock()
	index := !failed && !v.indexed && v.s.opts.Indexer != nil
	v.indexed = v.indexed || index
	v.mu.Unlock()
	if index {
		if err := v.s.opts.Indexer.IndexRoot(ctx, "/"+Name+"/"+closureDir+"/"+string(v.root)); err != nil {
			v.s.logf("nix: closure %s: index: %v", v.root, err)
			v.mu.Lock()
			v.indexed = false
			v.mu.Unlock()
		}
	}
}

// memberRoot returns a member's real root inode, starting its fill if it
// has none yet, and pins the member while the view is active.
func (v *cview) memberRoot(ctx context.Context, sp StorePath) (meta.Inode, error) {
	s, m := v.s, v.s.manager()
	v.mu.Lock()
	u, ok := v.units[sp]
	v.mu.Unlock()
	if !ok {
		var err error
		u, err = s.db.Unit(ctx, Name, string(sp))
		switch {
		case err == nil && u.Root != 0 && u.State != meta.UnitAbsent && u.State != meta.UnitFetching:
			// A kept manifest (spooled, resident or evicted) answers
			// lookups; opens fault evicted content back in.
		default:
			// A store path whose NAR root is a file or symlink replaces the
			// fill's placeholder root directory with its first entry. Such
			// paths are small, so wait for small fills to finish rather
			// than hand out the placeholder.
			wait := true
			if ni, err := s.NarInfo(ctx, sp); err == nil && ni.NarSize > smallNAR {
				wait = false
			}
			if u, err = m.Ensure(ctx, Name, subsys.Key(sp), wait); err != nil {
				return meta.Inode{}, err
			}
		}
		v.mu.Lock()
		if _, dup := v.units[sp]; !dup {
			v.units[sp] = u
			if v.idle != nil {
				v.releases = append(v.releases, m.Acquire(u.ID))
			}
		}
		v.mu.Unlock()
	}
	in, err := s.db.Get(ctx, u.Root)
	if err == nil && in.Kind == meta.KindDir && in.Parent != 0 {
		return in, nil
	}
	if err != nil && !errors.Is(err, meta.ErrNotExist) {
		return meta.Inode{}, err
	}
	if err == nil && in.Kind != meta.KindDir {
		return in, nil
	}
	// The root moved (a file root replaced the placeholder, or a failed
	// first fill was retried): wait for the fill and look again.
	if u, err = m.Ensure(ctx, Name, subsys.Key(sp), true); err != nil {
		return meta.Inode{}, err
	}
	v.mu.Lock()
	v.units[sp] = u
	v.mu.Unlock()
	return s.db.Get(ctx, u.Root)
}

// smallNAR is the NarSize under which a closure member's fill is awaited
// before its root is handed out.
const smallNAR = 64 << 10

func (s *Subsystem) logf(format string, args ...any) {
	if s.opts.Logf != nil {
		s.opts.Logf(format, args...)
	}
}
