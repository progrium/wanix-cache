package fill

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/progrium/wanix-cache/internal/cas"
)

// DiskSpool is the plain-disk Spool: dir/<unit>/<ino>.
type DiskSpool struct {
	dir   string
	bytes atomic.Int64

	mu    sync.Mutex
	files map[uint64]*sfile
}

type sfile struct {
	unit int64
	path string

	mu      sync.Mutex
	cond    *sync.Cond
	w       *os.File // nil once finished
	written int64
	done    bool
	err     error
}

var _ Spool = (*DiskSpool)(nil)

// OpenDiskSpool opens the spool under dir, counting the bytes already
// there. Leftover files are known to the spool only after Recover.
func OpenDiskSpool(dir string) (*DiskSpool, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &DiskSpool{dir: dir, files: map[uint64]*sfile{}}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if info, err := d.Info(); err == nil {
			s.bytes.Add(info.Size())
		}
		return nil
	})
	return s, err
}

func (s *DiskSpool) unitDir(unit int64) string {
	return filepath.Join(s.dir, strconv.FormatInt(unit, 10))
}

// Create implements Spool.
func (s *DiskSpool) Create(unit int64, ino uint64) (SpoolFile, error) {
	d := s.unitDir(unit)
	if err := os.MkdirAll(d, 0o700); err != nil {
		return nil, err
	}
	p := filepath.Join(d, strconv.FormatUint(ino, 10))
	w, err := os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	f := &sfile{unit: unit, path: p, w: w}
	f.cond = sync.NewCond(&f.mu)
	s.mu.Lock()
	if old := s.files[ino]; old != nil {
		old.Fail(errors.New("fill: spool file replaced"))
	}
	s.files[ino] = f
	s.mu.Unlock()
	return &spoolWriter{s: s, f: f}, nil
}

type spoolWriter struct {
	s *DiskSpool
	f *sfile
}

func (w *spoolWriter) Write(p []byte) (int, error) {
	f := w.f
	f.mu.Lock()
	out := f.w
	f.mu.Unlock()
	if out == nil {
		return 0, errors.New("fill: write to finished spool file")
	}
	n, err := out.Write(p)
	if n > 0 {
		w.s.bytes.Add(int64(n))
		f.mu.Lock()
		f.written += int64(n)
		f.cond.Broadcast()
		f.mu.Unlock()
	}
	return n, err
}

func (w *spoolWriter) Finish() error {
	f := w.f
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	if f.w != nil {
		err = f.w.Close()
		f.w = nil
	}
	f.done = true
	f.cond.Broadcast()
	return err
}

func (w *spoolWriter) Fail(err error) { w.f.Fail(err) }

func (f *sfile) Fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.w != nil {
		f.w.Close()
		f.w = nil
	}
	if f.err == nil {
		f.err = err
	}
	f.cond.Broadcast()
}

// Open implements Spool. The returned file holds its own descriptor, so it
// keeps working after Remove.
func (s *DiskSpool) Open(ino uint64) (cas.File, error) {
	s.mu.Lock()
	f := s.files[ino]
	s.mu.Unlock()
	if f == nil {
		return nil, os.ErrNotExist
	}
	r, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	return &spoolReader{f: f, r: r}, nil
}

type spoolReader struct {
	f *sfile
	r *os.File
}

// ReadAt blocks until [off, off+len(p)) is written, the file is finished,
// or the fill fails.
func (r *spoolReader) ReadAt(p []byte, off int64) (int, error) {
	f := r.f
	want := off + int64(len(p))
	f.mu.Lock()
	for f.written < want && !f.done && f.err == nil {
		f.cond.Wait()
	}
	written, done, ferr := f.written, f.done, f.err
	f.mu.Unlock()
	if ferr != nil && written < want {
		return 0, ferr
	}
	if off >= written {
		if done {
			return 0, io.EOF
		}
		return 0, ferr
	}
	if want > written {
		p = p[:written-off]
	}
	n, err := r.r.ReadAt(p, off)
	if err == nil && n < len(p) {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && done && off+int64(n) == written && want > written {
		err = io.EOF
	}
	return n, err
}

func (r *spoolReader) Close() error { return r.r.Close() }

// Sync implements Spool: fsync every file of the unit and its directory.
func (s *DiskSpool) Sync(unit int64) error {
	d := s.unitDir(unit)
	ents, err := os.ReadDir(d)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	for _, e := range ents {
		f, err := os.Open(filepath.Join(d, e.Name()))
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	df, err := os.Open(d)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

// Remove implements Spool.
func (s *DiskSpool) Remove(unit int64) error {
	s.mu.Lock()
	for ino, f := range s.files {
		if f.unit == unit {
			delete(s.files, ino)
		}
	}
	s.mu.Unlock()
	d := s.unitDir(unit)
	var size int64
	filepath.WalkDir(d, func(p string, de os.DirEntry, err error) error {
		if err == nil && !de.IsDir() {
			if info, err := de.Info(); err == nil {
				size += info.Size()
			}
		}
		return nil
	})
	if err := os.RemoveAll(d); err != nil {
		return err
	}
	s.bytes.Add(-size)
	return nil
}

// Units implements Spool.
func (s *DiskSpool) Units() ([]int64, error) {
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

// Recover makes a unit's leftover spool files readable again as finished
// files, after a restart.
func (s *DiskSpool) Recover(unit int64) error {
	d := s.unitDir(unit)
	ents, err := os.ReadDir(d)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range ents {
		ino, err := strconv.ParseUint(e.Name(), 10, 64)
		if err != nil {
			return fmt.Errorf("fill: unexpected spool file %s", filepath.Join(d, e.Name()))
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		f := &sfile{unit: unit, path: filepath.Join(d, e.Name()), written: info.Size(), done: true}
		f.cond = sync.NewCond(&f.mu)
		s.files[ino] = f
	}
	return nil
}

// Bytes implements Spool.
func (s *DiskSpool) Bytes() int64 { return s.bytes.Load() }
