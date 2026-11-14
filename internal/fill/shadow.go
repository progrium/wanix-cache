package fill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
)

// The debug savings mode keeps every resident unit unpacked on plain disk
// as well as in the CAS: the shadow tree, baseline B2 of the savings test
// plan. A unit's tree is at <shadow>/<subsystem>/<unit id> (a directory, or
// a single file or symlink for a non-directory root). Right after ingest it
// is hard-linked from the unit's spool files, so building it costs no
// copy; SyncShadow builds missing ones from the CAS instead.

// ShadowPath is where a unit's shadow tree goes.
func ShadowPath(dir string, u meta.Unit) string {
	return filepath.Join(dir, u.Subsystem, strconv.FormatInt(u.ID, 10))
}

// RemoveShadow drops a unit's shadow tree, for eviction.
func RemoveShadow(dir string, u meta.Unit) error {
	if dir == "" {
		return nil
	}
	return os.RemoveAll(ShadowPath(dir, u))
}

// shadowUnit writes u's shadow tree, linking spool files where it can.
func (m *LocalManager) shadowUnit(ctx context.Context, u meta.Unit) error {
	m.shadowMu.Lock()
	defer m.shadowMu.Unlock()
	return writeShadow(ctx, m.db, m.store, m.spool, m.opts.Shadow, u)
}

// SyncShadow runs SyncShadow on the manager's shadow tree, serialized with
// the trees ingest writes. It does nothing unless Options.Shadow is set.
func (m *LocalManager) SyncShadow(ctx context.Context) (wrote, removed int, err error) {
	if m.opts.Shadow == "" {
		return 0, 0, nil
	}
	m.shadowMu.Lock()
	defer m.shadowMu.Unlock()
	return SyncShadow(ctx, m.db, m.store, m.opts.Shadow)
}

// ShadowDir returns Options.Shadow.
func (m *LocalManager) ShadowDir() string { return m.opts.Shadow }

// SyncShadow makes the shadow tree under dir match the resident units:
// trees of units that are no longer resident are removed and missing ones
// are written from the CAS. It returns how many it wrote and removed.
func SyncShadow(ctx context.Context, db *meta.DB, store *cas.Store, dir string) (wrote, removed int, err error) {
	us, err := db.UnitsInState(ctx, meta.UnitResident)
	if err != nil {
		return 0, 0, err
	}
	want := map[string]meta.Unit{}
	for _, u := range us {
		want[ShadowPath(dir, u)] = u
	}
	subs, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, 0, err
	}
	have := map[string]bool{}
	for _, sd := range subs {
		if !sd.IsDir() {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(dir, sd.Name()))
		if err != nil {
			return wrote, removed, err
		}
		for _, e := range ents {
			p := filepath.Join(dir, sd.Name(), e.Name())
			if _, ok := want[p]; ok {
				have[p] = true
				continue
			}
			if err := os.RemoveAll(p); err != nil {
				return wrote, removed, err
			}
			removed++
		}
	}
	for p, u := range want {
		if have[p] {
			continue
		}
		if err := writeShadow(ctx, db, store, nil, dir, u); err != nil {
			return wrote, removed, fmt.Errorf("shadow %s/%s: %w", u.Subsystem, u.Key, err)
		}
		wrote++
	}
	return wrote, removed, nil
}

// A tree is written under <path>.partial and renamed into place, so an
// interrupted write is never taken for a complete one.
const partialSuffix = ".partial"

// writeShadow writes u's tree. With a spool it hard-links the unit's spool
// files (falling back to a copy across filesystems); without one, or for a
// file the spool no longer has, it copies the bytes from the CAS.
func writeShadow(ctx context.Context, db *meta.DB, store *cas.Store, spool *DiskSpool, dir string, u meta.Unit) error {
	dst := ShadowPath(dir, u)
	tmp := dst + partialSuffix
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	root, err := db.Get(ctx, u.Root)
	if err != nil {
		return err
	}
	w := shadowWriter{ctx: ctx, db: db, store: store, spool: spool, unit: u.ID, linked: map[uint64]string{}}
	if err := w.write(root, tmp); err != nil {
		os.RemoveAll(tmp)
		return err
	}
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

type shadowWriter struct {
	ctx    context.Context
	db     *meta.DB
	store  *cas.Store
	spool  *DiskSpool
	unit   int64
	linked map[uint64]string // file inode -> first path written, for hard links
}

func (w *shadowWriter) write(in meta.Inode, p string) error {
	switch in.Kind {
	case meta.KindDir:
		if err := os.Mkdir(p, 0o755); err != nil {
			return err
		}
		after := ""
		for {
			ents, err := w.db.ReadDir(w.ctx, in.Ino, after, 512)
			if err != nil {
				return err
			}
			for _, e := range ents {
				if e.Unit != w.unit {
					continue // another unit mounted below this one
				}
				if err := w.write(e.Inode, filepath.Join(p, e.Name)); err != nil {
					return err
				}
			}
			if len(ents) < 512 {
				return nil
			}
			after = ents[len(ents)-1].Name
		}
	case meta.KindSymlink:
		return os.Symlink(in.Target, p)
	case meta.KindFile:
		if first, ok := w.linked[in.Ino]; ok {
			return os.Link(first, p)
		}
		if err := w.file(in, p); err != nil {
			return err
		}
		w.linked[in.Ino] = p
		return nil
	}
	return nil // whiteouts have no plain-disk form
}

func (w *shadowWriter) file(in meta.Inode, p string) error {
	if w.spool != nil {
		if sp, ok := w.spool.path(in.Ino); ok {
			if err := os.Link(sp, p); err == nil {
				return nil
			}
		}
	}
	out, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if in.Size > 0 {
		var src io.ReaderAt
		if w.spool != nil {
			if f, err := w.spool.Open(in.Ino); err == nil {
				defer f.Close()
				src = f
			}
		}
		if src == nil {
			if in.Content == "" {
				out.Close()
				return fmt.Errorf("inode %d: no bytes in the spool or the CAS", in.Ino)
			}
			f, err := w.store.Open(w.ctx, in.Content)
			if err != nil {
				out.Close()
				return err
			}
			defer f.Close()
			src = f
		}
		if _, err := io.Copy(out, io.NewSectionReader(src, 0, in.Size)); err != nil {
			out.Close()
			return err
		}
	}
	return out.Close()
}
