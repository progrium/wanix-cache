// Package ingest loads trees into the cache as units.
//
// For phase 1 the only source is a local directory (subsystem "local"),
// which is enough to exercise the metadata, the CAS and the 9P server on
// real trees before the fetch path and subsystems exist. A local import
// moves the unit absent -> fetching -> spooled -> resident in one go, and
// RefaultDir brings an evicted local unit back from the same directory.
//
// File bytes are put into the CAS through a cas.Session before the
// metadata transaction opens, and the session is closed after it commits.
// That keeps the GC's sweep from deleting blobs the import deduped against,
// and lets a full CAS evict (which needs the DB) before a put fails.
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
	rootInfo, err := os.Stat(src)
	if err != nil {
		return res, err
	}
	if !rootInfo.IsDir() {
		return res, fmt.Errorf("ingest: %s is not a directory", src)
	}
	if _, err := db.Unit(ctx, subsystem, key); err == nil {
		return res, fmt.Errorf("meta: unit %s/%s: %w", subsystem, key, meta.ErrExist)
	}

	// Pass 1: walk and put file bytes, outside any transaction.
	sess := store.Session()
	defer sess.Close()
	known := map[string]string{} // digest -> ref put by this session
	type entry struct {
		rel    string
		info   fs.FileInfo
		kind   meta.Kind
		target string // symlink target
		linkTo string // hard link: rel of the first name
		f      putResult
	}
	type devino struct{ dev, ino uint64 }
	var entries []entry
	links := map[devino]string{}
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil || rel == "." {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		e := entry{rel: rel, info: info}
		switch {
		case d.IsDir():
			e.kind = meta.KindDir
		case d.Type()&fs.ModeSymlink != 0:
			e.kind = meta.KindSymlink
			if e.target, err = os.Readlink(p); err != nil {
				return err
			}
		case d.Type().IsRegular():
			e.kind = meta.KindFile
			st, _ := info.Sys().(*syscall.Stat_t)
			if st != nil && st.Nlink > 1 {
				di := devino{uint64(st.Dev), uint64(st.Ino)}
				if first, ok := links[di]; ok {
					e.linkTo = first
					break
				}
				links[di] = rel
			}
			if e.f, err = putFile(ctx, db, sess, known, p, info.Size()); err != nil {
				return fmt.Errorf("ingest: %s: %w", rel, err)
			}
		default:
			return nil // devices, sockets and fifos are not cached
		}
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		return res, err
	}

	// Pass 2: the manifest and content refs, in one transaction.
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
	root, err := tx.Mkdir(parent, key, rootInfo.Mode().Perm(), rootInfo.ModTime(), id)
	if err != nil {
		return res, err
	}
	dirs := map[string]uint64{".": root}
	files := map[string]uint64{}
	for _, e := range entries {
		dir, ok := dirs[path.Dir(e.rel)]
		if !ok {
			return res, fmt.Errorf("ingest: parent of %s not seen", e.rel)
		}
		name := path.Base(e.rel)
		switch {
		case e.kind == meta.KindDir:
			ino, err := tx.Mkdir(dir, name, e.info.Mode().Perm(), e.info.ModTime(), id)
			if err != nil {
				return res, err
			}
			dirs[e.rel] = ino
			res.Dirs++
		case e.kind == meta.KindSymlink:
			if _, err := tx.Symlink(dir, name, e.target, e.info.ModTime(), id); err != nil {
				return res, err
			}
			res.Symlinks++
		case e.linkTo != "":
			if err := tx.Link(dir, name, files[e.linkTo]); err != nil {
				return res, err
			}
			res.Hardlinks++
		default:
			ino, err := tx.File(dir, name, e.info.Mode().Perm(), e.info.Size(), e.info.ModTime(), e.f.ref, id)
			if err != nil {
				return res, err
			}
			if e.f.ref != "" {
				if err := tx.RefContent(e.f.digest, e.f.ref, e.info.Size()); err != nil {
					return res, err
				}
			}
			files[e.rel] = ino
			res.Files++
			res.LogicalBytes += e.info.Size()
			if e.f.isNew {
				res.NewContents++
			}
		}
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

// RefaultDir brings an evicted unit back from src, which must still hold
// the same tree: the unit's manifest is reused, so every file is put again
// and must come out with the bytes it had. It stands in for a subsystem
// fetcher until the fill path lands.
func RefaultDir(ctx context.Context, db *meta.DB, store *cas.Store, subsystem, key, src string) (Result, error) {
	var res Result
	u, err := db.Unit(ctx, subsystem, key)
	if err != nil {
		return res, err
	}
	if u.State != meta.UnitEvicted {
		return res, fmt.Errorf("ingest: %s/%s is %v, not evicted", subsystem, key, u.State)
	}
	// Collect the unit's file inodes and their paths.
	type file struct {
		ino  uint64
		rel  string
		size int64
		f    putResult
	}
	var files []file
	seen := map[uint64]bool{}
	var walk func(dir uint64, rel string) error
	walk = func(dir uint64, rel string) error {
		after := ""
		for {
			ents, err := db.ReadDir(ctx, dir, after, 1000)
			if err != nil {
				return err
			}
			if len(ents) == 0 {
				return nil
			}
			for _, de := range ents {
				p := path.Join(rel, de.Name)
				switch de.Kind {
				case meta.KindDir:
					if err := walk(de.Ino, p); err != nil {
						return err
					}
				case meta.KindFile:
					if !seen[de.Ino] {
						seen[de.Ino] = true
						files = append(files, file{ino: de.Ino, rel: p, size: de.Size})
					}
				}
			}
			after = ents[len(ents)-1].Name
		}
	}
	if err := walk(u.Root, ""); err != nil {
		return res, err
	}

	sess := store.Session()
	defer sess.Close()
	known := map[string]string{} // digest -> ref put by this session
	for i := range files {
		f := &files[i]
		p := filepath.Join(src, filepath.FromSlash(f.rel))
		info, err := os.Stat(p)
		if err != nil {
			return res, err
		}
		if info.Size() != f.size {
			return res, fmt.Errorf("ingest: %s changed size since import (%d, was %d)", f.rel, info.Size(), f.size)
		}
		if f.f, err = putFile(ctx, db, sess, known, p, f.size); err != nil {
			return res, fmt.Errorf("ingest: %s: %w", f.rel, err)
		}
		if f.f.isNew {
			res.NewContents++
		}
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()
	if err := tx.SetUnitState(u.ID, meta.UnitEvicted, meta.UnitFetching); err != nil {
		return res, err
	}
	for _, f := range files {
		if err := tx.SetContent(f.ino, f.f.ref); err != nil {
			return res, err
		}
		if f.f.ref != "" {
			if err := tx.RefContent(f.f.digest, f.f.ref, f.size); err != nil {
				return res, err
			}
		}
		res.Files++
		res.LogicalBytes += f.size
	}
	if err := tx.SetUnitState(u.ID, meta.UnitFetching, meta.UnitSpooled); err != nil {
		return res, err
	}
	if err := tx.SetUnitState(u.ID, meta.UnitSpooled, meta.UnitResident); err != nil {
		return res, err
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}
	res.Unit, err = db.Unit(ctx, subsystem, key)
	return res, err
}

type putResult struct {
	digest, ref string
	isNew       bool
}

// putFile stores a file's bytes, reusing a known content object when the
// whole-file digest matches and its blobs are still all there. Empty files
// have no content ref.
func putFile(ctx context.Context, db *meta.DB, sess *cas.Session, known map[string]string, p string, size int64) (putResult, error) {
	var r putResult
	if size == 0 {
		return r, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return r, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return r, err
	}
	r.digest = "sha256-" + hex.EncodeToString(h.Sum(nil))
	if ref, ok := known[r.digest]; ok {
		r.ref = ref // put or held earlier in this session
		return r, nil
	}
	ref, ok, err := db.ContentRef(ctx, r.digest)
	if err != nil {
		return r, err
	}
	if ok {
		// Hold its blobs until our commit; if a sweep already took some,
		// put the bytes again.
		if _, err := sess.Hold(ctx, ref); err == nil {
			r.ref = ref
			known[r.digest] = ref
			return r, nil
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return r, err
	}
	if r.ref, err = sess.Put(ctx, f); err != nil {
		return r, err
	}
	r.isNew = !ok
	known[r.digest] = r.ref
	return r, nil
}
