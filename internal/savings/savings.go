// Package savings measures what the CAS saves over plain disk: the debug
// savings mode of phase 6, and the metrics of the savings test plan.
//
// It reads the metadata DB (which content each resident file has), the
// CAS (which schema and data blobs each content object is made of, and the
// blocks each blob file occupies) and, when the server runs with a shadow
// tree, the shadow tree (every resident unit unpacked on plain disk with no
// dedupe: baseline B2). Everything is per subsystem as well as in total.
//
// Sizes are allocated bytes (st_blocks × 512) unless a field says logical.
package savings

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
)

// Options says where the cache lives.
type Options struct {
	// Dir is the cache directory (meta.db and blobs/).
	Dir string
	// Shadow is the shadow tree directory, or "" when the debug shadow is
	// off; B2 is then left out.
	Shadow string
	// Top bounds the largest-exclusive-units and most-shared lists.
	// Default 20.
	Top int
}

// Report is everything `wanix-cache stats` and /.cache/stats show.
type Report struct {
	At         time.Time  `json:"at"`
	Filesystem Filesystem `json:"filesystem"`

	Total       Totals      `json:"total"`
	Subsystems  []Subsystem `json:"subsystems"`
	SizeClasses []SizeClass `json:"size_classes"`

	TopExclusiveUnits []UnitRow    `json:"top_exclusive_units"`
	TopShared         []ContentRow `json:"top_shared_contents"`

	// Broken counts content objects with blobs missing from the store.
	Broken int `json:"broken_contents,omitempty"`
}

// Filesystem is the filesystem the cache directory is on.
type Filesystem struct {
	Type      string `json:"type"`
	BlockSize int64  `json:"block_size"`
}

// Plain is a plain-disk tree's size.
type Plain struct {
	Files     int64 `json:"files"`     // distinct inodes
	Logical   int64 `json:"logical"`   // file bytes
	Allocated int64 `json:"allocated"` // files, directories and symlinks
	Units     int   `json:"units"`     // unit trees present
}

// Totals is the whole cache.
type Totals struct {
	Units   int   `json:"resident_units"`
	Files   int64 `json:"files"`   // file inodes in resident units
	Logical int64 `json:"logical"` // their bytes

	Contents     int64 `json:"contents"`      // distinct content objects
	ContentBytes int64 `json:"content_bytes"` // their logical bytes

	DataBlobs   int64 `json:"data_blobs"`   // unique chunks reached from live content
	DataBytes   int64 `json:"data_bytes"`   // logical
	SchemaBlobs int64 `json:"schema_blobs"` // file and bytes schemas
	SchemaBytes int64 `json:"schema_bytes"` // logical

	Blobs          int64 `json:"blobs"`           // every blob file on disk
	BlobBytes      int64 `json:"blob_bytes"`      // logical
	BlobAllocated  int64 `json:"blob_allocated"`  // allocated
	StoreAllocated int64 `json:"store_allocated"` // blob files and the store's directories
	// StoreDirs are localdisk's hash-prefix directories (sha224/xx/yy),
	// one block each: overhead on top of BlobOverhead.
	StoreDirs         int64 `json:"store_dirs"`
	StoreDirAllocated int64 `json:"store_dir_allocated"`
	// Unreferenced blobs are on disk but reached from no resident unit's
	// content: evicted units' blobs waiting for a sweep.
	UnreferencedBlobs int64 `json:"unreferenced_blobs"`
	UnreferencedBytes int64 `json:"unreferenced_bytes"`
	DB                int64 `json:"db"` // meta.db and its WAL

	// CASTotal is the cache's real footprint: StoreAllocated + DB.
	CASTotal int64 `json:"cas_total"`
	// BlobOverhead is what one file per blob costs: BlobAllocated −
	// BlobBytes.
	BlobOverhead int64 `json:"blob_overhead"`

	// B2 is the shadow tree: every unit unpacked, no dedupe. Nil when the
	// shadow is off.
	B2 *Plain `json:"b2,omitempty"`
	// B3Estimate approximates B2 with identical files hard-linked: each
	// distinct content once, rounded up to the block size, plus B2's
	// directories and symlinks. Running rdfind or nix-store --optimise over
	// a copy of the shadow gives the measured B3.
	B3Estimate int64 `json:"b3_estimate"`

	Ratios Ratios `json:"ratios"`
}

// Ratios are fractions in [0, 1] (negative when the CAS is bigger).
type Ratios struct {
	// WholeFileDedupe is 1 − ContentBytes ÷ Logical.
	WholeFileDedupe float64 `json:"whole_file_dedupe"`
	// ChunkDedupe is 1 − DataBytes ÷ ContentBytes: what chunking adds.
	ChunkDedupe float64 `json:"chunk_dedupe"`
	// SchemaShare is SchemaBytes ÷ CASTotal.
	SchemaShare float64 `json:"schema_share"`
	// BlobOverheadShare is BlobOverhead ÷ CASTotal.
	BlobOverheadShare float64 `json:"blob_overhead_share"`
	// StoreDirShare is StoreDirAllocated ÷ CASTotal.
	StoreDirShare float64 `json:"store_dir_share"`
	// SavingsB2 is 1 − CASTotal ÷ B2.Allocated; 0 without a shadow.
	SavingsB2 float64 `json:"savings_vs_b2"`
	// SavingsB3Estimate is 1 − CASTotal ÷ B3Estimate.
	SavingsB3Estimate float64 `json:"savings_vs_b3_estimate"`
	// SavingsLogical is 1 − CASTotal ÷ Logical, for when there is no
	// shadow. It ignores the plain filesystem's own block overhead.
	SavingsLogical float64 `json:"savings_vs_logical"`
}

// Subsystem is one subsystem's share. A blob is exclusive to a subsystem
// when only that subsystem's resident files reach it, and shared when
// several do. Attributed splits each shared blob evenly between the
// subsystems that reach it, so the subsystems' Attributed sum to the blob
// files' allocated bytes (the DB and store directories are not split).
type Subsystem struct {
	Name    string `json:"name"`
	Units   int    `json:"resident_units"`
	Files   int64  `json:"files"`
	Logical int64  `json:"logical"`

	ContentBytes int64 `json:"content_bytes"` // distinct content it reaches, logical
	DataBytes    int64 `json:"data_bytes"`    // unique chunks it reaches, logical

	ExclusiveBytes     int64            `json:"exclusive_bytes"` // logical
	ExclusiveAllocated int64            `json:"exclusive_allocated"`
	SharedBytes        int64            `json:"shared_bytes"` // logical
	SharedAllocated    int64            `json:"shared_allocated"`
	SharedWith         map[string]int64 `json:"shared_with,omitempty"` // logical bytes shared with each other subsystem
	Attributed         int64            `json:"attributed"`

	B2 *Plain `json:"b2,omitempty"`

	WholeFileDedupe float64 `json:"whole_file_dedupe"`
	ChunkDedupe     float64 `json:"chunk_dedupe"`
	SavingsB2       float64 `json:"savings_vs_b2"` // 1 − Attributed ÷ B2.Allocated
}

// SizeClass groups distinct content objects by size. Each blob counts in
// the class of the first content (in ref order) that uses it. Slack is the
// blob files' allocated bytes minus their logical bytes: the per-blob file
// overhead that packing small blobs together would remove.
type SizeClass struct {
	Label       string  `json:"label"`
	Max         int64   `json:"max"` // exclusive upper bound; 0 for the last class
	Contents    int64   `json:"contents"`
	Bytes       int64   `json:"bytes"` // logical
	Blobs       int64   `json:"blobs"`
	SchemaBytes int64   `json:"schema_bytes"`
	Allocated   int64   `json:"allocated"`
	Slack       int64   `json:"slack"`
	SlackShare  float64 `json:"slack_share"` // of all slack
}

// UnitRow is a unit and the blob bytes only it reaches.
type UnitRow struct {
	Subsystem string `json:"subsystem"`
	Key       string `json:"key"`
	Logical   int64  `json:"logical"`
	Exclusive int64  `json:"exclusive_allocated"`
}

// ContentRow is a content object shared by many files.
type ContentRow struct {
	Ref        string   `json:"ref"`
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
	size   int64
	subs   uint64 // bitmask of subsystem indexes
	unit   int64  // first unit that reaches it
	multi  bool   // reached by more than one unit
	files  int64
	units  map[int64]bool
	anyIno uint64
}

type blobInfo struct {
	size   int64
	schema bool
	subs   uint64
	unit   int64
	multi  bool
	class  int
}

// Measure builds a report. It reads the DB and the store while the server
// runs; numbers taken mid-fill are a snapshot, so the test plan measures
// after the cache settles.
func Measure(ctx context.Context, db *meta.DB, store *cas.Store, opts Options) (*Report, error) {
	if opts.Top <= 0 {
		opts.Top = 20
	}
	r := &Report{At: time.Now().UTC(), Filesystem: filesystem(opts.Dir)}
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
			ref    string
			unitID int64
		)
		if err := rows.Scan(&ino, &size, &ref, &unitID); err != nil {
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
		if ref == "" {
			continue // empty file
		}
		c := contents[ref]
		if c == nil {
			c = &content{size: size, unit: unitID, units: map[int64]bool{}, anyIno: ino}
			contents[ref] = c
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

	// Blobs of each content object.
	refs := make([]string, 0, len(contents))
	for ref := range contents {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	blobs := map[string]*blobInfo{}
	for _, ref := range refs {
		c := contents[ref]
		r.Total.Contents++
		r.Total.ContentBytes += c.size
		forEach(c.subs, func(i int) { subs[i].ContentBytes += c.size })
		cl := classOf(c.size)
		schemas, data, err := store.Parts(ctx, ref)
		if err != nil {
			if errors.Is(err, cas.ErrMissing) || errors.Is(err, os.ErrNotExist) {
				r.Broken++
				continue
			}
			return nil, fmt.Errorf("savings: content %s: %w", ref, err)
		}
		add := func(b cas.Blob, schema bool) {
			bi := blobs[b.Ref]
			if bi == nil {
				bi = &blobInfo{size: b.Size, schema: schema, unit: c.unit, multi: c.multi, class: cl}
				blobs[b.Ref] = bi
			} else if bi.unit != c.unit || c.multi {
				bi.multi = true
			}
			bi.subs |= c.subs
		}
		for _, b := range schemas {
			add(b, true)
		}
		for _, b := range data {
			add(b, false)
		}
	}

	// Blob files on disk.
	onDisk := map[string]cas.BlobFile{}
	du, err := store.DiskUsage(func(f cas.BlobFile) { onDisk[f.Ref] = f })
	if err != nil {
		return nil, err
	}
	r.Total.StoreAllocated, r.Total.StoreDirs, r.Total.StoreDirAllocated = du.Allocated, du.Dirs, du.DirAllocated
	for ref, f := range onDisk {
		r.Total.Blobs++
		r.Total.BlobBytes += f.Size
		r.Total.BlobAllocated += f.Allocated
		if blobs[ref] == nil {
			r.Total.UnreferencedBlobs++
			r.Total.UnreferencedBytes += f.Size
		}
	}

	// Attribute blobs to subsystems, units and size classes.
	sc := make([]SizeClass, len(classes))
	for i, c := range classes {
		sc[i].Label, sc[i].Max = c.label, c.max
	}
	for _, c := range contents {
		k := classOf(c.size)
		sc[k].Contents++
		sc[k].Bytes += c.size
	}
	unitExcl := map[int64]int64{}
	for ref, b := range blobs {
		alloc := b.size
		if f, ok := onDisk[ref]; ok {
			alloc = f.Allocated
		}
		if b.schema {
			r.Total.SchemaBlobs++
			r.Total.SchemaBytes += b.size
			sc[b.class].SchemaBytes += b.size
		} else {
			r.Total.DataBlobs++
			r.Total.DataBytes += b.size
			forEach(b.subs, func(i int) { subs[i].DataBytes += b.size })
		}
		sc[b.class].Blobs++
		sc[b.class].Allocated += alloc
		sc[b.class].Slack += alloc - b.size
		n := bits.OnesCount64(b.subs)
		forEach(b.subs, func(i int) {
			s := subs[i]
			s.Attributed += alloc / int64(n)
			if n == 1 {
				s.ExclusiveBytes += b.size
				s.ExclusiveAllocated += alloc
				return
			}
			s.SharedBytes += b.size
			s.SharedAllocated += alloc
			forEach(b.subs, func(j int) {
				if j != i {
					s.SharedWith[subs[j].Name] += b.size
				}
			})
		})
		if !b.multi {
			unitExcl[b.unit] += alloc
		}
	}
	var slack int64
	for _, c := range sc {
		slack += c.Slack
	}
	for i := range sc {
		sc[i].SlackShare = ratio(sc[i].Slack, slack)
	}
	r.SizeClasses = sc

	r.Total.DB = dbSize(opts.Dir)
	r.Total.CASTotal = r.Total.StoreAllocated + r.Total.DB
	r.Total.BlobOverhead = r.Total.BlobAllocated - r.Total.BlobBytes

	// The shadow tree (B2) and the hard-link estimate (B3).
	var b2Other int64
	if opts.Shadow != "" {
		r.Total.B2 = &Plain{}
		for _, s := range subs {
			p, other, err := plainSize(filepath.Join(opts.Shadow, s.Name), unitByID)
			if err != nil {
				return nil, err
			}
			s.B2 = p
			r.Total.B2.Files += p.Files
			r.Total.B2.Logical += p.Logical
			r.Total.B2.Allocated += p.Allocated
			r.Total.B2.Units += p.Units
			b2Other += other
		}
	}
	r.Total.B3Estimate = b2Other
	for _, c := range contents {
		r.Total.B3Estimate += roundUp(c.size, bs)
	}

	t := &r.Total
	t.Ratios = Ratios{
		WholeFileDedupe:   1 - ratio(t.ContentBytes, t.Logical),
		ChunkDedupe:       1 - ratio(t.DataBytes, t.ContentBytes),
		SchemaShare:       ratio(t.SchemaBytes, t.CASTotal),
		BlobOverheadShare: ratio(t.BlobOverhead, t.CASTotal),
		StoreDirShare:     ratio(t.StoreDirAllocated, t.CASTotal),
		SavingsB3Estimate: savings(t.CASTotal, t.B3Estimate),
		SavingsLogical:    savings(t.CASTotal, t.Logical),
	}
	if t.B2 != nil {
		t.Ratios.SavingsB2 = savings(t.CASTotal, t.B2.Allocated)
	}
	for _, s := range subs {
		s.WholeFileDedupe = 1 - ratio(s.ContentBytes, s.Logical)
		s.ChunkDedupe = 1 - ratio(s.DataBytes, s.ContentBytes)
		if s.B2 != nil {
			s.SavingsB2 = savings(s.Attributed, s.B2.Allocated)
		}
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
	shared := make([]string, 0)
	for _, ref := range refs {
		if contents[ref].files > 1 {
			shared = append(shared, ref)
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
	for _, ref := range shared {
		c := contents[ref]
		row := ContentRow{Ref: ref, Size: c.size, Files: c.files, Units: len(c.units), Path: pathOf(ctx, db, c.anyIno)}
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

func savings(cas, baseline int64) float64 {
	if baseline == 0 {
		return 0
	}
	return 1 - float64(cas)/float64(baseline)
}

func roundUp(n, bs int64) int64 {
	if bs <= 0 {
		bs = 4096
	}
	return (n + bs - 1) / bs * bs
}

// plainSize measures one subsystem's shadow trees. Hard-linked inodes count
// once. other is the allocated bytes of everything but regular files.
func plainSize(dir string, units map[int64]meta.Unit) (p *Plain, other int64, err error) {
	p = &Plain{}
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return p, 0, nil
	} else if err != nil {
		return nil, 0, err
	}
	type key struct{ dev, ino uint64 }
	seen := map[key]bool{}
	for _, e := range ents {
		id, err := strconv.ParseInt(e.Name(), 10, 64)
		if _, ok := units[id]; err != nil || !ok {
			continue // a partial tree, or a unit no longer resident
		}
		p.Units++
		err = filepath.WalkDir(filepath.Join(dir, e.Name()), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				k := key{uint64(st.Dev), uint64(st.Ino)}
				if seen[k] {
					return nil
				}
				seen[k] = true
			}
			a := cas.Allocated(info)
			p.Allocated += a
			if info.Mode().IsRegular() {
				p.Files++
				p.Logical += info.Size()
			} else {
				other += a
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	return p, other, nil
}

func dbSize(dir string) int64 {
	var n int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Stat(filepath.Join(dir, "meta.db"+suffix)); err == nil {
			n += cas.Allocated(info)
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
