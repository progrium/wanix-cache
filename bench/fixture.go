// Package bench measures reads of cached file bytes from the Perkeep CAS
// against reads of the same bytes from plain disk files (what the spool, or
// a hot-unit-stays-on-disk policy, would serve).
//
// Every comparison uses the same SQLite metadata. Only the source of file
// bytes changes, so the numbers isolate the content layer. Six readers are
// compared: direct (no 9P) from disk and from the CAS, the 9P2000.L server
// with each backend, and the plain 9P2000 server with each backend.
//
// The harness is cmd/readbench. Results and the recommendation are in
// RESULTS.md.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
)

// File is one regular file of a fixture.
type File struct {
	Rel  string // path inside the unit, slash-separated
	Ino  uint64
	Ref  string // CAS ref; empty for an empty file
	Size int64
	Disk string // the plain-disk copy
}

// Fixture is a tree imported into its own cache (metadata DB plus CAS),
// with the source tree kept as the plain-disk copy.
type Fixture struct {
	Name string // also the unit key, served at /local/<Name>
	Src  string // plain-disk copy
	Work string // holds meta.db and blobs/

	DB  *meta.DB
	CAS *cas.Store

	Files []File            // regular files, in directory order; hard links appear once per name
	Paths map[uint64]string // inode -> plain-disk path, for the disk-backed servers
	Refs  map[uint64]string // inode -> CAS ref

	Imported   bool          // false when an earlier import was reused
	ImportTime time.Duration // zero when reused
}

// Prefix is the path of the fixture's root in the served namespace.
func (f *Fixture) Prefix() string { return "/local/" + f.Name }

// Load opens (or creates) the cache in work and imports src as unit
// local/name unless that unit already exists.
func Load(ctx context.Context, name, src, work string) (*Fixture, error) {
	src, err := filepath.Abs(src)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(work, 0o700); err != nil {
		return nil, err
	}
	db, err := meta.Open(filepath.Join(work, "meta.db"))
	if err != nil {
		return nil, err
	}
	store, err := cas.Open(work, 0)
	if err != nil {
		db.Close()
		return nil, err
	}
	fx := &Fixture{Name: name, Src: src, Work: work, DB: db, CAS: store, Paths: map[uint64]string{}, Refs: map[uint64]string{}}
	if _, err := db.Unit(ctx, "local", name); errors.Is(err, meta.ErrNotExist) {
		t0 := time.Now()
		if _, err := ingest.ImportDir(ctx, db, store, "local", name, src); err != nil {
			db.Close()
			return nil, fmt.Errorf("import %s: %w", name, err)
		}
		fx.Imported, fx.ImportTime = true, time.Since(t0)
	} else if err != nil {
		db.Close()
		return nil, err
	}
	if err := fx.collect(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return fx, nil
}

// Close closes the metadata DB.
func (f *Fixture) Close() error { return f.DB.Close() }

func (f *Fixture) collect(ctx context.Context) error {
	local, err := f.DB.Lookup(ctx, meta.RootIno, "local")
	if err != nil {
		return err
	}
	root, err := f.DB.Lookup(ctx, local.Ino, f.Name)
	if err != nil {
		return err
	}
	var walk func(dir uint64, rel string) error
	walk = func(dir uint64, rel string) error {
		after := ""
		for {
			des, err := f.DB.ReadDir(ctx, dir, after, 1000)
			if err != nil {
				return err
			}
			for _, de := range des {
				after = de.Name
				r := path.Join(rel, de.Name)
				switch de.Kind {
				case meta.KindDir:
					if err := walk(de.Ino, r); err != nil {
						return err
					}
				case meta.KindFile:
					disk := filepath.Join(f.Src, filepath.FromSlash(r))
					f.Files = append(f.Files, File{Rel: r, Ino: de.Ino, Ref: de.Content, Size: de.Size, Disk: disk})
					if _, ok := f.Paths[de.Ino]; !ok {
						f.Paths[de.Ino] = disk
						f.Refs[de.Ino] = de.Content
					}
				}
			}
			if len(des) < 1000 {
				return nil
			}
		}
	}
	return walk(root.Ino, "")
}

// Usage is the on-disk footprint of both copies, by allocated blocks.
type Usage struct {
	Files        int
	LogicalBytes int64
	PlainBytes   int64 // st_blocks*512 over the source tree, hard links once
	PlainFiles   int
	CASBytes     int64 // st_blocks*512 over blobs/
	CASFiles     int   // blob files
}

// DiskUsage measures both copies.
func (f *Fixture) DiskUsage() (Usage, error) {
	u := Usage{Files: len(f.Files)}
	for _, fl := range f.Files {
		u.LogicalBytes += fl.Size
	}
	var err error
	if u.PlainBytes, u.PlainFiles, err = allocated(f.Src); err != nil {
		return u, err
	}
	u.CASBytes, u.CASFiles, err = allocated(filepath.Join(f.Work, "blobs"))
	return u, err
}

func allocated(root string) (bytes int64, files int, err error) {
	type devino struct{ dev, ino uint64 }
	seen := map[devino]bool{}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil
		}
		k := devino{uint64(st.Dev), st.Ino}
		if seen[k] {
			return nil
		}
		seen[k] = true
		bytes += st.Blocks * 512
		if info.Mode().IsRegular() {
			files++
		}
		return nil
	})
	return bytes, files, err
}
