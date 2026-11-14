// Package ingest loads trees into the cache as units.
//
// The source is a local directory (subsystem "local"), which exercises the
// metadata, the file store and the 9P servers on real trees without a
// fetcher. A local import moves the unit absent -> fetching -> spooled ->
// resident in one go, and RefaultDir brings an evicted local unit back from
// the same directory.
//
// The manifest is written first (unit fetching), then every file is copied
// into the store at files/<unit>/<ino>, or hard-linked to a stored file
// with the same whole-file digest, and then one transaction records the
// digests and makes the unit resident. If the import fails in between, the
// unit goes back to absent (evicted for a refault) and its files are
// removed.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
)

// Result summarizes an import.
type Result struct {
	Unit         meta.Unit
	Files        int
	Dirs         int
	Symlinks     int
	Hardlinks    int
	LogicalBytes int64
	NewContents  int // files whose bytes were not already in the store
}

// ImportDir imports the tree at src as unit (subsystem, key), mounted at
// /<subsystem>/<key>. Files whose whole-file digest is already stored are
// hard-linked to the stored copy instead of copied.
func ImportDir(ctx context.Context, db *meta.DB, st *store.Store, subsystem, key, src string) (Result, error) {
	var res Result
	rootInfo, err := os.Stat(src)
	if err != nil {
		return res, err
	}
	if !rootInfo.IsDir() {
		return res, fmt.Errorf("ingest: %s is not a directory", src)
	}
	old, err := db.Unit(ctx, subsystem, key)
	if err == nil && old.State != meta.UnitAbsent {
		return res, fmt.Errorf("meta: unit %s/%s: %w", subsystem, key, meta.ErrExist)
	}

	// Pass 1: walk and hash, outside any transaction.
	type entry struct {
		rel    string
		info   fs.FileInfo
		kind   meta.Kind
		target string // symlink target
		linkTo string // hard link: rel of the first name
		digest string
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
			if info.Size() > 0 {
				if e.digest, err = hashFile(p); err != nil {
					return fmt.Errorf("ingest: %s: %w", rel, err)
				}
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

	// Pass 2: the manifest, with no content yet.
	tx, err := db.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()
	id := old.ID
	if id == 0 {
		if id, err = tx.CreateUnit(subsystem, key); err != nil {
			return res, err
		}
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
	var place []placeFile
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
			ino, err := tx.File(dir, name, e.info.Mode().Perm(), e.info.Size(), e.info.ModTime(), "", id)
			if err != nil {
				return res, err
			}
			files[e.rel] = ino
			res.Files++
			res.LogicalBytes += e.info.Size()
			if e.digest != "" {
				place = append(place, placeFile{ino, filepath.Join(src, filepath.FromSlash(e.rel)), e.digest, e.info.Size()})
			}
		}
	}
	if err := tx.SetUnitManifest(id, root, res.LogicalBytes); err != nil {
		return res, err
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}

	// Pass 3: the bytes, then the digests in one transaction.
	if res.NewContents, err = finish(ctx, db, st, id, place); err != nil {
		abandon(db, st, id, meta.UnitAbsent)
		return res, err
	}
	res.Unit, err = db.Unit(ctx, subsystem, key)
	return res, err
}

// RefaultDir brings an evicted unit back from src, which must still hold
// the same tree: the unit's manifest is reused, so every file is stored
// again and must come out with the bytes it had.
func RefaultDir(ctx context.Context, db *meta.DB, st *store.Store, subsystem, key, src string) (Result, error) {
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

	var place []placeFile
	for _, f := range files {
		p := filepath.Join(src, filepath.FromSlash(f.rel))
		info, err := os.Stat(p)
		if err != nil {
			return res, err
		}
		if info.Size() != f.size {
			return res, fmt.Errorf("ingest: %s changed size since import (%d, was %d)", f.rel, info.Size(), f.size)
		}
		res.Files++
		res.LogicalBytes += f.size
		if f.size == 0 {
			continue
		}
		digest, err := hashFile(p)
		if err != nil {
			return res, fmt.Errorf("ingest: %s: %w", f.rel, err)
		}
		place = append(place, placeFile{f.ino, p, digest, f.size})
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return res, err
	}
	if err := tx.SetUnitState(u.ID, meta.UnitEvicted, meta.UnitFetching); err != nil {
		tx.Rollback()
		return res, err
	}
	if err := tx.Commit(); err != nil {
		return res, err
	}
	if res.NewContents, err = finish(ctx, db, st, u.ID, place); err != nil {
		abandon(db, st, u.ID, meta.UnitEvicted)
		return res, err
	}
	res.Unit, err = db.Unit(ctx, subsystem, key)
	return res, err
}

// placeFile is a file inode to store from a source path.
type placeFile struct {
	ino    uint64
	src    string
	digest string
	size   int64
}

// finish stores the files of a fetching unit, then records their digests
// and makes the unit resident. It returns how many contents were new to
// the store.
func finish(ctx context.Context, db *meta.DB, st *store.Store, unit int64, files []placeFile) (newContents int, err error) {
	first := map[string]uint64{} // digest -> the inode stored first in this unit
	for _, f := range files {
		if ino, ok := first[f.digest]; ok {
			if err := st.Link(unit, ino, unit, f.ino); err == nil {
				continue
			} else if !errors.Is(err, syscall.EMLINK) {
				return newContents, err
			}
		} else if su, sino, ok, err := db.LinkSource(ctx, f.digest); err != nil {
			return newContents, err
		} else if ok {
			err := st.Link(su, sino, unit, f.ino)
			if err == nil {
				first[f.digest] = f.ino
				continue
			}
			if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.EMLINK) {
				return newContents, err
			}
		}
		if err := copyFile(st, unit, f); err != nil {
			return newContents, fmt.Errorf("ingest: %s: %w", f.src, err)
		}
		if _, ok := first[f.digest]; !ok {
			first[f.digest] = f.ino
			newContents++
		}
	}
	if err := st.SyncUnit(unit); err != nil {
		return newContents, err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return newContents, err
	}
	defer tx.Rollback()
	for _, f := range files {
		if err := tx.RefContent(f.digest, f.size); err != nil {
			return newContents, err
		}
		if err := tx.SetContent(f.ino, f.digest); err != nil {
			return newContents, err
		}
	}
	if err := tx.SetUnitState(unit, meta.UnitFetching, meta.UnitSpooled); err != nil {
		return newContents, err
	}
	if err := tx.SetUnitState(unit, meta.UnitSpooled, meta.UnitResident); err != nil {
		return newContents, err
	}
	return newContents, tx.Commit()
}

// copyFile copies a source file into the store and checks that it still
// has the digest it was hashed with.
func copyFile(st *store.Store, unit int64, f placeFile) error {
	in, err := os.Open(f.src)
	if err != nil {
		return err
	}
	defer in.Close()
	h := sha256.New()
	if err := st.Write(unit, f.ino, io.TeeReader(io.LimitReader(in, f.size+1), h)); err != nil {
		return err
	}
	if got := "sha256-" + hex.EncodeToString(h.Sum(nil)); got != f.digest {
		st.RemoveFile(unit, f.ino)
		return errors.New("changed while importing")
	}
	return nil
}

// abandon undoes a failed import or refault: the unit's files go, and the
// unit returns to state to (absent, with its manifest dropped, or evicted).
func abandon(db *meta.DB, st *store.Store, unit int64, to meta.UnitState) {
	ctx := context.Background()
	st.RemoveUnit(unit)
	tx, err := db.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	if to == meta.UnitAbsent {
		if tx.DeleteUnitTree(unit) != nil || tx.SetUnitManifest(unit, 0, 0) != nil {
			return
		}
	}
	if tx.SetUnitState(unit, meta.UnitFetching, to) == nil {
		tx.Commit()
	}
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256-" + hex.EncodeToString(h.Sum(nil)), nil
}
