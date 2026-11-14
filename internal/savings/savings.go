// Package savings reports what the cache holds and what deduplication
// saves: `wanix-cache stats` and /.cache/stats.
//
// It reads the metadata DB (which content digest each resident file has)
// and walks the file store (the blocks each stored file occupies, counting
// a hard-linked file once). Everything is per subsystem as well as in
// total.
//
// Sizes are allocated bytes (st_blocks × 512) unless a field says logical.
package savings

import (
	"context"
	"errors"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
)

// Options says where the cache lives.
type Options struct {
	// Dir is the cache directory (meta.db and files/).
	Dir string
	// Top bounds the largest-exclusive-units and most-shared lists.
	// Default 20.
	Top int
}

// Report is everything `wanix-cache stats` and /.cache/stats show.
type Report struct {
	At         time.Time  `json:"at"`
	Filesystem Filesystem `json:"filesystem"`
	// Compression is the store's zstd level, 0 for plain files.
	Compression int `json:"compression"`

	Total       Totals      `json:"total"`
	Subsystems  []Subsystem `json:"subsystems"`
	SizeClasses []SizeClass `json:"size_classes"`

	TopExclusiveUnits []UnitRow    `json:"top_exclusive_units"`
	TopShared         []ContentRow `json:"top_shared_contents"`
}

// Filesystem is the filesystem the cache directory is on.
type Filesystem struct {
	Type      string `json:"type"`
	BlockSize int64  `json:"block_size"`
}

// Totals is the whole cache.
type Totals struct {
	Units   int   `json:"resident_units"`
	Files   int64 `json:"files"`   // file inodes in resident units
	Logical int64 `json:"logical"` // their bytes

	Contents     int64 `json:"contents"`      // distinct content digests
	ContentBytes int64 `json:"content_bytes"` // their logical bytes

	StoreFiles     int64 `json:"store_files"`     // distinct files on disk
	StoreLinks     int64 `json:"store_links"`     // names in unit directories
	StoreBytes     int64 `json:"store_bytes"`     // stored bytes, each file once
	StoreAllocated int64 `json:"store_allocated"` // files and directories
	StoreDirs      int64 `json:"store_dirs"`
	StoreDirBytes  int64 `json:"store_dir_allocated"`
	// Copies are stored files beyond one per distinct content: identical
	// files that were not hard-linked (`wanix-cache audit -fix` links them).
	Copies     int64 `json:"copies"`
	CopyBytes  int64 `json:"copy_bytes"`
	DB         int64 `json:"db"`    // meta.db and its WAL
	CacheTotal int64 `json:"total"` // StoreAllocated + DB

	// NoDedupe estimates the same files with no hard links: every file
	// inode rounded up to the block size, plus the store's directories.
	NoDedupe int64 `json:"no_dedupe_estimate"`

	Ratios Ratios `json:"ratios"`
}

// Ratios are fractions in [0, 1].
type Ratios struct {
	// WholeFileDedupe is 1 − ContentBytes ÷ Logical.
	WholeFileDedupe float64 `json:"whole_file_dedupe"`
	// Compression is 1 − StoreBytes ÷ ContentBytes (0 without zstd).
	Compression float64 `json:"compression"`
	// DirShare is StoreDirBytes ÷ CacheTotal.
	DirShare float64 `json:"dir_share"`
	// SavingsNoDedupe is 1 − StoreAllocated ÷ NoDedupe.
	SavingsNoDedupe float64 `json:"savings_vs_no_dedupe"`
}

// Subsystem is one subsystem's share. A content is exclusive to a
// subsystem when only that subsystem's resident files have it, and shared
// when several do. Attributed splits each shared file evenly between the
// subsystems that have it, so the subsystems' Attributed sum to the stored
// files' allocated bytes (the DB and directories are not split).
type Subsystem struct {
	Name    string `json:"name"`
	Units   int    `json:"resident_units"`
	Files   int64  `json:"files"`
	Logical int64  `json:"logical"`

	ContentBytes int64 `json:"content_bytes"` // distinct content it has, logical

	ExclusiveBytes     int64            `json:"exclusive_bytes"` // logical
	ExclusiveAllocated int64            `json:"exclusive_allocated"`
	SharedBytes        int64            `json:"shared_bytes"` // logical
	SharedAllocated    int64            `json:"shared_allocated"`
	SharedWith         map[string]int64 `json:"shared_with,omitempty"` // logical bytes shared with each other subsystem
	Attributed         int64            `json:"attributed"`

	WholeFileDedupe float64 `json:"whole_file_dedupe"`
}

// SizeClass groups distinct contents by size. Slack is the stored files'
// allocated bytes minus their stored bytes: what one file per content
// costs in partly used blocks.
type SizeClass struct {
	Label      string  `json:"label"`
	Max        int64   `json:"max"` // exclusive upper bound; 0 for the last class
	Contents   int64   `json:"contents"`
	Bytes      int64   `json:"bytes"` // logical
	Allocated  int64   `json:"allocated"`
	Slack      int64   `json:"slack"`
	SlackShare float64 `json:"slack_share"` // of all slack
}

// UnitRow is a unit and the bytes only it has.
type UnitRow struct {
	Subsystem string `json:"subsystem"`
	Key       string `json:"key"`
	Logical   int64  `json:"logical"`
	Exclusive int64  `json:"exclusive_allocated"`
}

// ContentRow is a content shared by many files.
type ContentRow struct {
	Digest     string   `json:"digest"`
	Size       int64    `json:"size"`
	Files      int64    `json:"files"`
	Units      int      `json:"units"`
	Subsystems []string `json:"subsystems"`
	Path       string   `json:"path"` // one of its names
}

var classes = []struct {
	label string
	max   int64
}{{"< 4 KiB", 4 << 10}, {"4-16 KiB", 16 << 10}, {"16-64 KiB", 64 << 10}, {">= 64 KiB", 0}}

func classOf(size int64) int {
	for i, c := range classes {
		if c.max == 0 || size < c.max {
			return i
		}
	}
	return len(classes) - 1
}

type content struct {
	size  int64
	subs  uint64 // bitmask of subsystem indexes
	unit  int64  // first unit that has it
	multi bool   // in more than one unit
	files int64
	units map[int64]bool
	ino   uint64 // a file inode with it, in unit
	alloc int64  // allocated bytes of its stored file
}

type fileKey struct {
	unit int64
	ino  uint64
}

// Measure builds a report. It reads the DB and the store while the server
// runs; numbers taken mid-fill are a snapshot, so the savings plan
// measures after the cache settles.
func Measure(ctx context.Context, db *meta.DB, st *store.Store, opts Options) (*Report, error) {
	if opts.Top <= 0 {
		opts.Top = 20
	}
	r := &Report{At: time.Now().UTC(), Filesystem: filesystem(opts.Dir), Compression: st.Compression()}
	bs := r.Filesystem.BlockSize

	// Resident units and their files.
	units, err := db.UnitsInState(ctx, meta.UnitResident)
	if err != nil {
		return nil, err
	}
	subIdx := map[string]int{}
	var subs []*Subsystem
	unitByID := map[int64]meta.Unit{}
	for _, u := range units {
		unitByID[u.ID] = u
		i, ok := subIdx[u.Subsystem]
		if !ok {
			if len(subs) == 64 {
				return nil, errors.New("savings: more than 64 subsystems")
			}
			i = len(subs)
			subIdx[u.Subsystem] = i
			subs = append(subs, &Subsystem{Name: u.Subsystem, SharedWith: map[string]int64{}})
		}
		subs[i].Units++
	}
	r.Total.Units = len(units)

	contents := map[string]*content{}
	rows, err := db.SQL().QueryContext(ctx, `SELECT i.ino, i.size, i.content, i.unit FROM inode i
		JOIN unit u ON u.id = i.unit WHERE i.kind = ? AND u.state = ?`, meta.KindFile, meta.UnitResident)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var (
			ino    uint64
			size   int64
			digest string
			unitID int64
		)
		if err := rows.Scan(&ino, &size, &digest, &unitID); err != nil {
			rows.Close()
			return nil, err
		}
		u, ok := unitByID[unitID]
		if !ok {
			continue
		}
		si := subIdx[u.Subsystem]
		s := subs[si]
		s.Files++
		s.Logical += size
		r.Total.Files++
		r.Total.Logical += size
		r.Total.NoDedupe += roundUp(size, bs)
		if digest == "" {
			continue // empty file
		}
		c := contents[digest]
		if c == nil {
			c = &content{size: size, unit: unitID, units: map[int64]bool{}, ino: ino}
			contents[digest] = c
		}
		c.subs |= 1 << si
		c.files++
		c.units[unitID] = true
		if unitID != c.unit {
			c.multi = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// The store on disk.
	alloc := map[fileKey]int64{}
	du, err := st.DiskUsage(func(e store.Entry) { alloc[fileKey{e.Unit, e.Ino}] = e.Allocated })
	if err != nil {
		return nil, err
	}
	t := &r.Total
	t.StoreFiles, t.StoreLinks, t.StoreBytes = du.Files, du.Links, du.Bytes
	t.StoreAllocated, t.StoreDirs, t.StoreDirBytes = du.Allocated, du.Dirs, du.DirAllocated
	t.NoDedupe += du.DirAllocated
	t.DB = dbSize(opts.Dir)
	t.CacheTotal = t.StoreAllocated + t.DB

	// Attribute contents to subsystems, units and size classes.
	digests := make([]string, 0, len(contents))
	for d := range contents {
		digests = append(digests, d)
	}
	sort.Strings(digests)
	sc := make([]SizeClass, len(classes))
	for i, c := range classes {
		sc[i].Label, sc[i].Max = c.label, c.max
	}
	unitExcl := map[int64]int64{}
	var contentAlloc int64
	for _, d := range digests {
		c := contents[d]
		c.alloc = alloc[fileKey{c.unit, c.ino}]
		contentAlloc += c.alloc
		t.Contents++
		t.ContentBytes += c.size
		k := classOf(c.size)
		sc[k].Contents++
		sc[k].Bytes += c.size
		sc[k].Allocated += c.alloc
		stored := c.size
		if r.Compression > 0 {
			stored = c.alloc // the stored size is not tracked per file
		}
		sc[k].Slack += max(c.alloc-stored, 0)
		n := bits.OnesCount64(c.subs)
		forEach(c.subs, func(i int) {
			s := subs[i]
			s.ContentBytes += c.size
			s.Attributed += c.alloc / int64(n)
			if n == 1 {
				s.ExclusiveBytes += c.size
				s.ExclusiveAllocated += c.alloc
				return
			}
			s.SharedBytes += c.size
			s.SharedAllocated += c.alloc
			forEach(c.subs, func(j int) {
				if j != i {
					s.SharedWith[subs[j].Name] += c.size
				}
			})
		})
		if !c.multi {
			unitExcl[c.unit] += c.alloc
		}
	}
	if t.StoreFiles > t.Contents {
		t.Copies = t.StoreFiles - t.Contents
		t.CopyBytes = max(t.StoreAllocated-t.StoreDirBytes-contentAlloc, 0)
	}
	var slack int64
	for _, c := range sc {
		slack += c.Slack
	}
	for i := range sc {
		sc[i].SlackShare = ratio(sc[i].Slack, slack)
	}
	r.SizeClasses = sc

	t.Ratios = Ratios{
		WholeFileDedupe: 1 - ratio(t.ContentBytes, t.Logical),
		DirShare:        ratio(t.StoreDirBytes, t.CacheTotal),
		SavingsNoDedupe: savings(t.StoreAllocated, t.NoDedupe),
	}
	if r.Compression > 0 {
		t.Ratios.Compression = 1 - ratio(t.StoreBytes, t.ContentBytes)
	}
	for _, s := range subs {
		s.WholeFileDedupe = 1 - ratio(s.ContentBytes, s.Logical)
		if len(s.SharedWith) == 0 {
			s.SharedWith = nil
		}
		r.Subsystems = append(r.Subsystems, *s)
	}
	sort.Slice(r.Subsystems, func(i, j int) bool { return r.Subsystems[i].Name < r.Subsystems[j].Name })

	// The lists that explain surprises.
	for id, n := range unitExcl {
		u := unitByID[id]
		r.TopExclusiveUnits = append(r.TopExclusiveUnits, UnitRow{u.Subsystem, u.Key, u.LogicalBytes, n})
	}
	sort.Slice(r.TopExclusiveUnits, func(i, j int) bool {
		a, b := r.TopExclusiveUnits[i], r.TopExclusiveUnits[j]
		if a.Exclusive != b.Exclusive {
			return a.Exclusive > b.Exclusive
		}
		return a.Subsystem+a.Key < b.Subsystem+b.Key
	})
	if len(r.TopExclusiveUnits) > opts.Top {
		r.TopExclusiveUnits = r.TopExclusiveUnits[:opts.Top]
	}
	var shared []string
	for _, d := range digests {
		if contents[d].files > 1 {
			shared = append(shared, d)
		}
	}
	sort.SliceStable(shared, func(i, j int) bool {
		a, b := contents[shared[i]], contents[shared[j]]
		if a.files*a.size != b.files*b.size {
			return a.files*a.size > b.files*b.size // bytes saved
		}
		return a.files > b.files
	})
	if len(shared) > opts.Top {
		shared = shared[:opts.Top]
	}
	for _, d := range shared {
		c := contents[d]
		row := ContentRow{Digest: d, Size: c.size, Files: c.files, Units: len(c.units), Path: pathOf(ctx, db, c.ino)}
		forEach(c.subs, func(i int) { row.Subsystems = append(row.Subsystems, subs[i].Name) })
		sort.Strings(row.Subsystems)
		r.TopShared = append(r.TopShared, row)
	}
	return r, nil
}

func forEach(mask uint64, fn func(int)) {
	for mask != 0 {
		i := bits.TrailingZeros64(mask)
		fn(i)
		mask &^= 1 << i
	}
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func savings(have, baseline int64) float64 {
	if baseline == 0 {
		return 0
	}
	return 1 - float64(have)/float64(baseline)
}

func roundUp(n, bs int64) int64 {
	if bs <= 0 {
		bs = 4096
	}
	return (n + bs - 1) / bs * bs
}

func dbSize(dir string) int64 {
	var n int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Stat(filepath.Join(dir, "meta.db"+suffix)); err == nil {
			n += store.Allocated(info)
		}
	}
	return n
}

func pathOf(ctx context.Context, db *meta.DB, ino uint64) string {
	var parent uint64
	var name string
	if err := db.SQL().QueryRowContext(ctx, `SELECT parent, name FROM dirent WHERE ino = ? ORDER BY parent, name LIMIT 1`, ino).Scan(&parent, &name); err != nil {
		return ""
	}
	dir, err := db.PathOf(ctx, parent)
	if err != nil {
		return name
	}
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}
