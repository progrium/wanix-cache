// Package store keeps the bytes of resident units as plain files.
//
// Every file inode of a resident unit is one file on disk,
// DATA/files/<unit id>/<inode number>, so a read is one open and pread of
// a plain file. Identical files are stored once: ingest hard-links a new
// file to an existing one whose whole-file SHA-256 matches (the metadata
// DB finds it), across units and subsystems. The kernel's link count is the
// refcount: evicting a unit removes its directory, and a file's blocks are
// freed when its last link goes.
//
// Optional zstd (EnableCompression, off by default) stores every file as a
// header byte and either its bytes or a zstd frame of them, whichever is
// smaller. It saves space at a read cost, so it is only for caches that
// ask for it.
package store

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

// ErrFull is returned when a write does not fit the cache's cap.
var ErrFull = syscall.ENOSPC

// File is an open file's bytes.
type File interface {
	io.ReaderAt
	io.Closer
}

// Dir is the store's directory under the cache directory.
const Dir = "files"

const zstdFile = "ZSTD"

// Store is the plain-file store of one cache directory.
type Store struct {
	dir   string // DATA/files
	level int    // zstd level; 0 = plain files

	enc *zstd.Encoder
	dec sync.Pool // *zstd.Decoder

	// mu serializes creating links and removing them, so the nlink a
	// removal sees tells it exactly whether it frees the file's bytes.
	mu   sync.Mutex
	used atomic.Int64 // stored bytes, each inode once
}

// Open opens or creates the store under dataDir/files and counts the bytes
// already there.
func Open(dataDir string) (*Store, error) {
	dir := filepath.Join(dataDir, Dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	level, err := compression(dir)
	if err != nil {
		return nil, err
	}
	if level > 0 {
		s.level = level
		if s.enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)), zstd.WithEncoderConcurrency(1)); err != nil {
			return nil, err
		}
	}
	du, err := s.DiskUsage(nil)
	if err != nil {
		return nil, err
	}
	s.used.Store(du.Bytes)
	return s, nil
}

// Close releases the store. It is safe to call more than once.
func (s *Store) Close() error { return nil }

// Dir returns the store's directory.
func (s *Store) Dir() string { return s.dir }

// Compression returns the zstd level, or 0 for plain files.
func (s *Store) Compression() int { return s.level }

// Used returns the stored bytes: each file on disk once, however many
// links it has (compressed bytes with zstd on).
func (s *Store) Used() int64 { return s.used.Load() }

func (s *Store) unitDir(unit int64) string {
	return filepath.Join(s.dir, strconv.FormatInt(unit, 10))
}

// Path returns where a file inode's bytes live.
func (s *Store) Path(unit int64, ino uint64) string {
	return filepath.Join(s.unitDir(unit), strconv.FormatUint(ino, 10))
}

// Open opens a file inode's bytes for reading.
func (s *Store) Open(unit int64, ino uint64) (File, error) {
	f, err := os.Open(s.Path(unit, ino))
	if err != nil || s.level == 0 {
		return f, err
	}
	var hdr [1]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("store: %s: no header: %w", f.Name(), err)
	}
	switch hdr[0] {
	case hdrRaw:
		return rawFile{f}, nil
	case hdrZstd:
		return &zstdReader{s: s, f: f}, nil
	}
	f.Close()
	return nil, fmt.Errorf("store: %s: bad header %d", f.Name(), hdr[0])
}

func (s *Store) mkUnit(unit int64) error {
	return os.MkdirAll(s.unitDir(unit), 0o700)
}

// clear removes whatever is at dst before a new file takes its place (left
// by an ingest that was retried or a crash), keeping Used exact.
func (s *Store) clear(dst string) error {
	info, err := os.Lstat(dst)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := os.Remove(dst); err != nil {
		return err
	}
	if nlink(info) == 1 {
		s.used.Add(-info.Size())
	}
	return nil
}

// Link makes file ino of unit a hard link to file fromIno of fromUnit,
// which holds identical bytes. It fails with an error matching
// os.ErrNotExist if the source is gone (its unit was evicted since the
// caller looked it up), and with syscall.EMLINK if the source has as many
// links as the filesystem allows; the caller then stores its own copy.
func (s *Store) Link(fromUnit int64, fromIno uint64, unit int64, ino uint64) error {
	src, dst := s.Path(fromUnit, fromIno), s.Path(unit, ino)
	if src == dst {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, err := os.Lstat(src); err != nil {
		return err
	} else if b, err := os.Lstat(dst); err == nil && os.SameFile(a, b) {
		return nil
	}
	if err := s.mkUnit(unit); err != nil {
		return err
	}
	if err := s.clear(dst); err != nil {
		return err
	}
	return os.Link(src, dst)
}

// Add stores a finished spool file as file ino of unit. Plain files are
// hard-linked from the spool, so nothing is copied and the spool copy
// stays readable until the spool removes it; with zstd the file is
// compressed into place.
func (s *Store) Add(unit int64, ino uint64, src string) error {
	if s.level > 0 {
		f, err := os.Open(src)
		if err != nil {
			return err
		}
		defer f.Close()
		return s.Write(unit, ino, f)
	}
	if err := s.mkUnit(unit); err != nil {
		return err
	}
	dst := s.Path(unit, ino)
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if b, err := os.Lstat(dst); err == nil && os.SameFile(a, b) {
		return nil
	}
	if err := s.clear(dst); err != nil {
		return err
	}
	if err := os.Link(src, dst); err != nil {
		return err
	}
	s.used.Add(a.Size())
	return nil
}

// Write stores the bytes of r as file ino of unit, as a new file.
func (s *Store) Write(unit int64, ino uint64, r io.Reader) error {
	if err := s.mkUnit(unit); err != nil {
		return err
	}
	dst := s.Path(unit, ino)
	tmp, err := os.CreateTemp(s.unitDir(unit), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if s.level > 0 {
		err = s.compress(tmp, r)
	} else {
		_, err = io.Copy(tmp, r)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	info, err := os.Stat(tmp.Name())
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.clear(dst); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}
	s.used.Add(info.Size())
	return nil
}

// SyncUnit makes a unit's directory entries durable.
func (s *Store) SyncUnit(unit int64) error {
	for _, d := range []string{s.unitDir(unit), s.dir} {
		f, err := os.Open(d)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// RemoveUnit deletes a unit's files and returns the bytes that freed: the
// files that had no other link.
func (s *Store) RemoveUnit(unit int64) (freed int64, err error) {
	d := s.unitDir(unit)
	ents, err := os.ReadDir(d)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	for _, e := range ents {
		p := filepath.Join(d, e.Name())
		s.mu.Lock()
		info, err := os.Lstat(p)
		if err == nil {
			if err = os.Remove(p); err == nil && nlink(info) == 1 {
				s.used.Add(-info.Size())
				freed += info.Size()
			}
		}
		s.mu.Unlock()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return freed, err
		}
	}
	if err := os.Remove(d); err != nil && !errors.Is(err, os.ErrNotExist) {
		return freed, err
	}
	return freed, nil
}

// RemoveFile deletes one file and returns the bytes that freed.
func (s *Store) RemoveFile(unit int64, ino uint64) (freed int64, err error) {
	p := s.Path(unit, ino)
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	if err := os.Remove(p); err != nil {
		return 0, err
	}
	if nlink(info) == 1 {
		s.used.Add(-info.Size())
		return info.Size(), nil
	}
	return 0, nil
}

// Units lists the units that have a directory in the store.
func (s *Store) Units() ([]int64, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []int64
	for _, e := range ents {
		id, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil || !e.IsDir() {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// Entry is one file in the store.
type Entry struct {
	Unit       int64
	Ino        uint64
	Size       int64 // stored bytes
	Allocated  int64 // blocks the filesystem allocated, in bytes
	Nlink      uint64
	Dev, Inode uint64 // the file's identity on disk
}

// UnitFiles lists one unit's files.
func (s *Store) UnitFiles(unit int64) ([]Entry, error) {
	d := s.unitDir(unit)
	ents, err := os.ReadDir(d)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(ents))
	for _, e := range ents {
		ino, err := strconv.ParseUint(e.Name(), 10, 64)
		if err != nil {
			continue // a temporary file
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		en := Entry{Unit: unit, Ino: ino, Size: info.Size(), Allocated: Allocated(info), Nlink: nlink(info)}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			en.Dev, en.Inode = uint64(st.Dev), uint64(st.Ino)
		}
		out = append(out, en)
	}
	return out, nil
}

// DiskTotals is what the store occupies.
type DiskTotals struct {
	Files        int64 // distinct files on disk (inodes)
	Links        int64 // names in unit directories
	Bytes        int64 // stored bytes, each file once
	Allocated    int64 // files and directories, each file once
	Dirs         int64
	DirAllocated int64
}

// DiskUsage walks the store, counting each file once however many links
// it has, and calls fn (if not nil) for every name.
func (s *Store) DiskUsage(fn func(Entry)) (DiskTotals, error) {
	var t DiskTotals
	type key struct{ dev, ino uint64 }
	seen := map[key]bool{}
	err := filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		info, err := d.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		a := Allocated(info)
		if d.IsDir() {
			t.Dirs++
			t.DirAllocated += a
			t.Allocated += a
			return nil
		}
		if !info.Mode().IsRegular() || d.Name() == zstdFile || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		t.Links++
		en := Entry{Size: info.Size(), Allocated: a, Nlink: nlink(info)}
		en.Unit, _ = strconv.ParseInt(filepath.Base(filepath.Dir(p)), 10, 64)
		en.Ino, _ = strconv.ParseUint(d.Name(), 10, 64)
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			en.Dev, en.Inode = uint64(st.Dev), uint64(st.Ino)
			k := key{en.Dev, en.Inode}
			if !seen[k] {
				seen[k] = true
				t.Files++
				t.Bytes += info.Size()
				t.Allocated += a
			}
		} else {
			t.Files++
			t.Bytes += info.Size()
			t.Allocated += a
		}
		if fn != nil {
			fn(en)
		}
		return nil
	})
	return t, err
}

// Allocated returns the bytes the filesystem allocated for a file
// (st_blocks × 512), or its size where that is not known.
func Allocated(info fs.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512
	}
	return info.Size()
}

func nlink(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink)
	}
	return 1
}
