package tree

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Views: subsystems that implement subsys.Viewer serve virtual directories
// (an OCI image's merged rootfs, a Nix closure) under their root. Lookups,
// listings and parents of virtual inodes go to the viewer; files inside
// views are real inodes and are served as usual.

type viewSet struct {
	mu    sync.RWMutex
	roots map[uint64]subsys.Viewer // real /<name>/ dir -> viewer
	virt  map[uint64]subsys.Viewer // virtual dir -> its viewer
}

// AddViewer serves v's views under /<name>/, creating that directory if
// it does not exist yet.
func (f *FS) AddViewer(ctx context.Context, name string, v subsys.Viewer) error {
	tx, err := f.Meta.Begin(ctx)
	if err != nil {
		return err
	}
	ino, err := tx.MkdirAll("/" + name)
	if err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	f.views.mu.Lock()
	defer f.views.mu.Unlock()
	if f.views.roots == nil {
		f.views.roots = map[uint64]subsys.Viewer{}
		f.views.virt = map[uint64]subsys.Viewer{}
	}
	f.views.roots[ino] = v
	return nil
}

func (f *FS) viewerOf(ino uint64) (subsys.Viewer, bool) {
	f.views.mu.RLock()
	defer f.views.mu.RUnlock()
	if subsys.IsVirtual(ino) {
		v, ok := f.views.virt[ino]
		return v, ok
	}
	v, ok := f.views.roots[ino]
	return v, ok
}

// viewLookup answers a lookup in a virtual directory, or a view name in a
// viewer's root. ok is false when the real namespace should answer.
func (f *FS) viewLookup(ctx context.Context, dir uint64, name string) (meta.Inode, bool, error) {
	v, ok := f.viewerOf(dir)
	if !ok {
		if subsys.IsVirtual(dir) {
			return meta.Inode{}, true, meta.ErrNotExist
		}
		return meta.Inode{}, false, nil
	}
	in, ok, err := v.ViewLookup(ctx, dir, name)
	if subsys.IsVirtual(dir) {
		ok = true // virtual directories have no real entries
	}
	if !ok {
		return in, false, nil
	}
	if err == nil && subsys.IsVirtual(in.Ino) {
		f.views.mu.Lock()
		f.views.virt[in.Ino] = v
		f.views.mu.Unlock()
	}
	return in, true, err
}

// get returns an inode, real or virtual.
func (f *FS) get(ctx context.Context, ino uint64) (meta.Inode, error) {
	if !subsys.IsVirtual(ino) {
		return f.metaGet(ctx, ino)
	}
	v, ok := f.viewerOf(ino)
	if !ok {
		return meta.Inode{}, meta.ErrNotExist
	}
	return v.ViewGet(ctx, ino)
}

// viewReadDir lists a virtual directory, registering its subdirectories.
func (f *FS) viewReadDir(ctx context.Context, ino uint64) ([]meta.Dirent, error) {
	v, ok := f.viewerOf(ino)
	if !ok {
		return nil, meta.ErrNotExist
	}
	des, err := v.ViewReadDir(ctx, ino)
	if err != nil {
		return nil, err
	}
	if !sort.SliceIsSorted(des, func(i, j int) bool { return des[i].Name < des[j].Name }) {
		return nil, fmt.Errorf("tree: view listing of %#x is not sorted", ino)
	}
	f.views.mu.Lock()
	for _, de := range des {
		if subsys.IsVirtual(de.Ino) {
			f.views.virt[de.Ino] = v
		}
	}
	f.views.mu.Unlock()
	return des, nil
}

// viewPage returns up to limit entries of a virtual directory after the
// name after, and whether the listing is finished.
func (f *FS) viewPage(ctx context.Context, ino uint64, after string, limit int) ([]meta.Dirent, bool, error) {
	des, err := f.viewReadDir(ctx, ino)
	if err != nil {
		return nil, false, err
	}
	i := sort.Search(len(des), func(i int) bool { return des[i].Name > after })
	des = des[i:]
	if len(des) > limit {
		return des[:limit], false, nil
	}
	return des, true, nil
}

// entries returns a directory's whole listing in name order, real or
// virtual, whiteouts included for real directories. For walkers of the
// tree such as index generation.
func (f *FS) entries(ctx context.Context, ino uint64) ([]meta.Dirent, error) {
	if subsys.IsVirtual(ino) {
		return f.viewReadDir(ctx, ino)
	}
	var out []meta.Dirent
	for after := ""; ; {
		des, err := f.Meta.ReadDir(ctx, ino, after, 1024)
		if err != nil {
			return nil, err
		}
		out = append(out, des...)
		if len(des) < 1024 {
			return out, nil
		}
		after = des[len(des)-1].Name
	}
}
