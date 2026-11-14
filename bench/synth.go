package bench

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
)

// SizeClass is a group of same-size synthetic files.
type SizeClass struct {
	Label string
	Size  int64
	Count int
}

// SizeClasses is the 1 KiB to 100 MiB sweep. Each class has at least 64
// MiB (or 1000 files) in total so small-file numbers average over many
// opens, and at least 3 files.
func SizeClasses() []SizeClass {
	sizes := []struct {
		l string
		n int64
	}{
		{"1K", 1 << 10}, {"4K", 4 << 10}, {"16K", 16 << 10}, {"64K", 64 << 10},
		{"256K", 256 << 10}, {"1M", 1 << 20}, {"4M", 4 << 20}, {"16M", 16 << 20},
		{"100M", 100 << 20},
	}
	var out []SizeClass
	for _, s := range sizes {
		c := int((64 << 20) / s.n)
		c = max(3, min(c, 1000))
		out = append(out, SizeClass{s.l, s.n, c})
	}
	return out
}

// GenSynthetic writes each class to dir/<label>/f<i> as incompressible
// pseudo-random bytes (from seed), unless dir.done exists.
func GenSynthetic(dir string, classes []SizeClass, seed uint64) error {
	done := filepath.Clean(dir) + ".done"
	if _, err := os.Stat(done); err == nil {
		return nil
	}
	rng := rand.NewChaCha8([32]byte{byte(seed), byte(seed >> 8)})
	buf := make([]byte, 1<<20)
	for _, c := range classes {
		d := filepath.Join(dir, c.Label)
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		for i := 0; i < c.Count; i++ {
			f, err := os.Create(filepath.Join(d, fmt.Sprintf("f%04d", i)))
			if err != nil {
				return err
			}
			w := bufio.NewWriterSize(f, 1<<20)
			for left := c.Size; left > 0; {
				n := min(left, int64(len(buf)))
				rng.Read(buf[:n])
				w.Write(buf[:n])
				left -= n
			}
			if err := w.Flush(); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return os.WriteFile(done, nil, 0o644)
}
