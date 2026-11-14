package bench

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"
)

// DropCaches syncs and drops the kernel page, dentry and inode caches, so
// the next read of every file (source tree, blobs and meta.db alike) comes
// from the block device. It needs root.
func DropCaches() error {
	syscall.Sync()
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0); err != nil {
		return fmt.Errorf("drop caches (cold runs need root): %w", err)
	}
	return nil
}

// BufSize is the read size the workloads use, one Msize payload or less.
const BufSize = 128 << 10

// Result is one timed run.
type Result struct {
	Files   int
	Bytes   int64
	Elapsed time.Duration
}

// ReadAll opens every file, reads it start to end in BufSize reads, and
// closes it, with conc workers.
func ReadAll(r Reader, files []File, conc int) (Result, error) {
	return run(r, files, conc, -1)
}

// Head opens every file, reads at most n bytes from offset 0, and closes
// it: the pattern of exec, ld.so and file(1), and of most opens in a
// package manager or interpreter start-up.
func Head(r Reader, files []File, n int64, conc int) (Result, error) {
	return run(r, files, conc, n)
}

func run(r Reader, files []File, conc int, limit int64) (Result, error) {
	if conc < 1 {
		conc = 1
	}
	var (
		mu    sync.Mutex
		res   Result
		first error
		next  int
		wg    sync.WaitGroup
	)
	t0 := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, BufSize)
			for {
				mu.Lock()
				i := next
				next++
				stop := first != nil
				mu.Unlock()
				if stop || i >= len(files) {
					return
				}
				n, err := readFile(r, &files[i], buf, limit)
				mu.Lock()
				if err != nil && first == nil {
					first = fmt.Errorf("%s: %s: %w", r.Name(), files[i].Rel, err)
				}
				res.Files++
				res.Bytes += n
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	res.Elapsed = time.Since(t0)
	return res, first
}

// readFile reads f from offset 0 up to limit bytes (all of it if limit < 0).
func readFile(r Reader, f *File, buf []byte, limit int64) (int64, error) {
	h, err := r.Open(f)
	if err != nil {
		return 0, err
	}
	defer h.Close()
	end := f.Size
	if limit >= 0 && limit < end {
		end = limit
	}
	var off int64
	for off < end {
		want := min(int64(len(buf)), end-off)
		n, err := h.ReadAt(buf[:want], off)
		off += int64(n)
		if n == 0 {
			if err == nil || errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return off, err
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return off, err
		}
	}
	return off, nil
}

// Latencies holds per-operation times.
type Latencies []time.Duration

// Quantile returns the q quantile (0..1).
func (l Latencies) Quantile(q float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	s := slices.Clone(l)
	slices.Sort(s)
	i := int(q * float64(len(s)-1))
	return s[i]
}

// Mean returns the mean.
func (l Latencies) Mean() time.Duration {
	if len(l) == 0 {
		return 0
	}
	var t time.Duration
	for _, d := range l {
		t += d
	}
	return t / time.Duration(len(l))
}

// Random opens each of files once and then issues ops reads of blk bytes
// at random blk-aligned offsets spread across all of them, one at a time.
// Opens are not timed. The offsets come from seed, so every reader does
// the same reads.
func Random(r Reader, files []File, ops int, blk int64, seed uint64) (Latencies, error) {
	hs := make([]Handle, len(files))
	defer func() {
		for _, h := range hs {
			if h != nil {
				h.Close()
			}
		}
	}()
	for i := range files {
		h, err := r.Open(&files[i])
		if err != nil {
			return nil, err
		}
		hs[i] = h
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	buf := make([]byte, blk)
	lat := make(Latencies, 0, ops)
	for range ops {
		i := rng.IntN(len(files))
		blocks := files[i].Size / blk
		if blocks == 0 {
			blocks = 1
		}
		off := rng.Int64N(blocks) * blk
		want := min(blk, files[i].Size-off)
		t0 := time.Now()
		n, err := hs[i].ReadAt(buf[:want], off)
		lat = append(lat, time.Since(t0))
		if int64(n) != want && (err == nil || errors.Is(err, io.EOF)) {
			return lat, fmt.Errorf("%s: %s: short read %d of %d at %d", r.Name(), files[i].Rel, n, want, off)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return lat, err
		}
	}
	return lat, nil
}
