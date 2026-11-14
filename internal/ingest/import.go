// Package ingest loads trees into the cache as units.
//
// For phase 1 the only source is a local directory (subsystem "local"),
// which is enough to exercise the metadata, the CAS and the 9P server on
// real trees before the fetch path and subsystems exist. A local import
// moves the unit absent -> fetching -> spooled -> resident in one go.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
)

// Result summarizes an import.
type Result struct {
	Unit         meta.Unit
	Files        int
	Dirs         int
	Symlinks     int
	Hardlinks    int
	LogicalBytes int64
	NewContents  int // files whose bytes were not already in the CAS
}

// ImportDir imports the tree at src as unit (subsystem, key), mounted at
// /<subsystem>/<key>. Files whose whole-file digest is already known are
// not chunked again.
func ImportDir(ctx context.Context, db *meta.DB, store *cas.Store, subsystem, key, src string) (Result, error) {
	var res Result
	tx, err := db.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	id, err := tx.CreateUnit(subsystem, key)
	if err != nil {
		return res, err
	}
	if err := tx.SetUnitState(id, meta.UnitAbsent, meta.UnitFetching); err != nil {
		return res, err
	}
	parent, err := tx.MkdirAll(path.Join("/", subsystem))
	if err != nil {
		return res, err
	}
	rootInfo, err := os.Stat(src)
	if err != nil {
		return res, err
	}
	if !rootInfo.IsDir() {
		return res, fmt.Errorf("ingest: %s is not a directory", src)
	}
	root, err := tx.Mkdir(parent, key, rootInfo.Mode().Perm(), rootInfo.ModTime(), id)
	if err != nil {
		return res, err
	}

	type devino struct{ dev, ino uint64 }
	dirs := map[string]uint64{".": root}
	links := map[devino]uint64{}

	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil || rel == "." {
			return err
		}
		rel = filepath.ToSlash(rel)
		dir, ok := dirs[path.Dir(rel)]
		if !ok {
			return fmt.Errorf("ingest: parent of %s not seen", rel)
		}
		name := path.Base(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			ino, err := tx.Mkdir(dir, name, info.Mode().Perm(), info.ModTime(), id)
			if err != nil {
				return err
			}
			dirs[rel] = ino
			res.Dirs++
		case d.Type()&fs.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			if _, err := tx.Symlink(dir, name, target, info.ModTime(), id); err != nil {
				return err
			}
			res.Symlinks++
		case d.Type().IsRegular():
			var di devino
			st, _ := info.Sys().(*syscall.Stat_t)
			if st != nil && st.Nlink > 1 {
				di = devino{uint64(st.Dev), uint64(st.Ino)}
				if ino, ok := links[di]; ok {
					res.Hardlinks++
					return tx.Link(dir, name, ino)
				}
			}
			ref, isNew, err := putFile(ctx, tx, store, p, info.Size())
			if err != nil {
				return fmt.Errorf("ingest: %s: %w", rel, err)
			}
			ino, err := tx.File(dir, name, info.Mode().Perm(), info.Size(), info.ModTime(), ref, id)
			if err != nil {
				return err
			}
			if st != nil && st.Nlink > 1 {
				links[di] = ino
			}
			res.Files++
			res.LogicalBytes += info.Size()
			if isNew {
				res.NewContents++
			}
		default:
			// Devices, sockets and fifos are not cached.
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	if err := tx.SetUnitManifest(id, root, res.LogicalBytes); err != nil {
		return res, err
	}
	if err := tx.SetUnitState(id, meta.UnitFetching, meta.UnitSpooled); err != nil {
		return res, err
	}
	if err := tx.SetUnitState(id, meta.UnitSpooled, meta.UnitResident); err != nil {
		return res, err
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}
	res.Unit, err = db.Unit(ctx, subsystem, key)
	return res, err
}

// putFile stores a file's bytes, reusing a known content object when the
// whole-file digest matches. Empty files have no content ref.
func putFile(ctx context.Context, tx *meta.Tx, store *cas.Store, p string, size int64) (ref string, isNew bool, err error) {
	if size == 0 {
		return "", false, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", false, err
	}
	digest := "sha256-" + hex.EncodeToString(h.Sum(nil))
	ref, ok, err := tx.ContentRef(digest)
	if err != nil {
		return "", false, err
	}
	if !ok {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return "", false, err
		}
		if ref, err = store.Put(ctx, f); err != nil {
			return "", false, err
		}
		isNew = true
	}
	return ref, isNew, tx.RefContent(digest, ref, size)
}
