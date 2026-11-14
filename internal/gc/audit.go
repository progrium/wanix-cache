package gc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/progrium/wanix-cache/internal/meta"
)

// AuditOptions controls an audit.
type AuditOptions struct {
	// Fix repairs what the audit finds: refcounts are rewritten, files no
	// resident unit uses are deleted, identical files stored separately are
	// hard-linked, and units whose files are missing or wrong are evicted
	// so their next read refetches them. Without Fix the audit only reports.
	Fix bool
	// Deep re-hashes every stored file and compares it with its digest,
	// instead of checking only that it is there with the right size.
	Deep bool
}

// AuditReport is what an audit found (and, with Fix, repaired).
type AuditReport struct {
	Contents  int   // content rows checked
	Files     int   // file inodes of resident units checked
	DiskFiles int64 // distinct files in the store
	DiskBytes int64 // their stored bytes

	ContentRefsFixed int      // content.refs that did not match the inodes
	ContentsDropped  int      // content rows no resident inode uses
	MissingContent   []string // digests of resident inodes with no content row
	MissingFiles     int      // resident file inodes with no file in the store
	BadFiles         int      // stored files with the wrong size (or digest, Deep)
	OrphanUnits      int      // store directories of units that are not resident
	OrphanFiles      int      // files in a resident unit's directory no inode uses
	OrphanBytes      int64    // bytes the orphans free
	OrphansRemoved   int      // with Fix
	Duplicates       int      // files stored separately from an identical one
	DuplicateBytes   int64    //
	Relinked         int      // with Fix
	BrokenUnits      []meta.Unit
	BrokenEvicted    int // with Fix
}

// Clean reports whether the audit found nothing to repair.
func (r AuditReport) Clean() bool {
	return r.ContentRefsFixed == 0 && r.ContentsDropped == 0 && len(r.MissingContent) == 0 &&
		r.MissingFiles == 0 && r.BadFiles == 0 && r.OrphanUnits == 0 && r.OrphanFiles == 0 &&
		r.Duplicates == 0 && len(r.BrokenUnits) == 0
}

func (r AuditReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "checked %d content objects, %d resident files, %d files in the store (%d bytes)\n", r.Contents, r.Files, r.DiskFiles, r.DiskBytes)
	line := func(n int, what string) {
		if n > 0 {
			fmt.Fprintf(&b, "  %d %s\n", n, what)
		}
	}
	line(r.ContentRefsFixed, "content refcounts wrong")
	line(r.ContentsDropped, "content objects unused by any resident unit")
	line(len(r.MissingContent), "content digests with no content row")
	line(r.MissingFiles, "files missing from the store")
	line(r.BadFiles, "stored files with the wrong bytes")
	if r.OrphanUnits+r.OrphanFiles > 0 {
		fmt.Fprintf(&b, "  %d leftover unit directories and %d leftover files (%d bytes), %d removed\n", r.OrphanUnits, r.OrphanFiles, r.OrphanBytes, r.OrphansRemoved)
	}
	if r.Duplicates > 0 {
		fmt.Fprintf(&b, "  %d identical files stored separately (%d bytes), %d hard-linked\n", r.Duplicates, r.DuplicateBytes, r.Relinked)
	}
	if len(r.BrokenUnits) > 0 {
		fmt.Fprintf(&b, "  %d units with missing or wrong bytes, %d evicted for refetch:", len(r.BrokenUnits), r.BrokenEvicted)
		for _, u := range r.BrokenUnits {
			fmt.Fprintf(&b, " %s/%s", u.Subsystem, u.Key)
		}
		b.WriteString("\n")
	}
	if r.Clean() {
		b.WriteString("  clean\n")
	}
	return b.String()
}

type residentFile struct {
	unit   int64
	ino    uint64
	size   int64
	digest string
}

// Audit is a full check of the metadata against the store. It recomputes
// content refcounts from the inodes of resident units, checks that every
// resident file is in the store with the right size (and digest, with
// Deep), and finds leftover files and identical files that are not
// hard-linked. Without opts.Fix it changes nothing. Units being fetched or
// ingested are left alone.
func (c *Collector) Audit(ctx context.Context, opts AuditOptions) (AuditReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var rep AuditReport

	// 1. content.refs against resident inodes.
	err := c.write(ctx, func(q querier) error {
		want := map[string]int64{}
		sizes := map[string]int64{}
		rows, err := q.QueryContext(ctx, `SELECT i.content, count(*), max(i.size) FROM inode i JOIN unit u ON u.id = i.unit
			WHERE u.state = ? AND i.kind = ? AND i.content != '' GROUP BY i.content`, meta.UnitResident, meta.KindFile)
		if err != nil {
			return err
		}
		for rows.Next() {
			var d string
			var n, size int64
			if err := rows.Scan(&d, &n, &size); err != nil {
				rows.Close()
				return err
			}
			want[d], sizes[d] = n, size
		}
		rows.Close()
		inflight := map[string]bool{}
		rows, err = q.QueryContext(ctx, `SELECT DISTINCT i.content FROM inode i JOIN unit u ON u.id = i.unit
			WHERE u.state IN (?, ?) AND i.kind = ? AND i.content != ''`, meta.UnitFetching, meta.UnitSpooled, meta.KindFile)
		if err != nil {
			return err
		}
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				rows.Close()
				return err
			}
			inflight[d] = true
		}
		rows.Close()
		have := map[string]int64{}
		rows, err = q.QueryContext(ctx, `SELECT digest, refs FROM content`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var d string
			var n int64
			if err := rows.Scan(&d, &n); err != nil {
				rows.Close()
				return err
			}
			have[d] = n
		}
		rows.Close()
		rep.Contents = len(have)
		for d, n := range have {
			if inflight[d] || n == want[d] {
				continue
			}
			if want[d] == 0 {
				rep.ContentsDropped++
				if opts.Fix {
					if _, err := q.ExecContext(ctx, `DELETE FROM content WHERE digest = ?`, d); err != nil {
						return err
					}
				}
				continue
			}
			rep.ContentRefsFixed++
			if opts.Fix {
				if _, err := q.ExecContext(ctx, `UPDATE content SET refs = ? WHERE digest = ?`, want[d], d); err != nil {
					return err
				}
			}
		}
		for d, n := range want {
			if _, ok := have[d]; ok {
				continue
			}
			rep.MissingContent = append(rep.MissingContent, d)
			if opts.Fix {
				if _, err := q.ExecContext(ctx, `INSERT INTO content (digest, ref, size, refs) VALUES (?, ?, ?, ?)`, d, d, sizes[d], n); err != nil {
					return err
				}
			}
		}
		sort.Strings(rep.MissingContent)
		return nil
	})
	if err != nil {
		return rep, err
	}

	// 2. The resident files the metadata expects, by unit.
	units := map[int64]meta.Unit{}
	busy := map[int64]bool{}
	rows, err := c.sql.QueryContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes FROM unit WHERE state IN (?, ?, ?)`,
		meta.UnitResident, meta.UnitFetching, meta.UnitSpooled)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var u meta.Unit
		if err := rows.Scan(&u.ID, &u.Subsystem, &u.Key, &u.State, &u.Root, &u.LogicalBytes); err != nil {
			rows.Close()
			return rep, err
		}
		if u.State == meta.UnitResident {
			units[u.ID] = u
		} else {
			busy[u.ID] = true
		}
	}
	rows.Close()
	expect := map[int64]map[uint64]residentFile{}
	rows, err = c.sql.QueryContext(ctx, `SELECT i.unit, i.ino, i.size, i.content FROM inode i JOIN unit u ON u.id = i.unit
		WHERE u.state = ? AND i.kind = ? AND i.content != ''`, meta.UnitResident, meta.KindFile)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var f residentFile
		if err := rows.Scan(&f.unit, &f.ino, &f.size, &f.digest); err != nil {
			rows.Close()
			return rep, err
		}
		if expect[f.unit] == nil {
			expect[f.unit] = map[uint64]residentFile{}
		}
		expect[f.unit][f.ino] = f
		rep.Files++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return rep, err
	}

	// 3. The store against them.
	broken := map[int64]bool{}
	type ident struct{ dev, ino uint64 }
	type copyOf struct {
		f     residentFile
		id    ident
		bytes int64
	}
	byDigest := map[string][]copyOf{}
	du, err := c.store.DiskUsage(nil)
	if err != nil {
		return rep, err
	}
	rep.DiskFiles, rep.DiskBytes = du.Files, du.Bytes
	dirs, err := c.store.Units()
	if err != nil {
		return rep, err
	}
	onDisk := map[int64]bool{}
	for _, id := range dirs {
		onDisk[id] = true
		if busy[id] {
			continue
		}
		ents, err := c.store.UnitFiles(id)
		if err != nil {
			return rep, err
		}
		want := expect[id]
		if _, ok := units[id]; !ok {
			rep.OrphanUnits++
			for _, e := range ents {
				if e.Nlink == 1 {
					rep.OrphanBytes += e.Size
				}
			}
			if opts.Fix {
				if _, err := c.store.RemoveUnit(id); err != nil {
					return rep, err
				}
				rep.OrphansRemoved++
			}
			continue
		}
		seen := map[uint64]bool{}
		for _, e := range ents {
			f, ok := want[e.Ino]
			if !ok {
				rep.OrphanFiles++
				if e.Nlink == 1 {
					rep.OrphanBytes += e.Size
				}
				if opts.Fix {
					if _, err := c.store.RemoveFile(id, e.Ino); err != nil {
						return rep, err
					}
					rep.OrphansRemoved++
				}
				continue
			}
			seen[e.Ino] = true
			bad := c.store.Compression() == 0 && e.Size != f.size
			if !bad && opts.Deep {
				d, err := c.hashStored(id, e.Ino)
				bad = err != nil || d != f.digest
			}
			if bad {
				rep.BadFiles++
				broken[id] = true
				continue
			}
			byDigest[f.digest] = append(byDigest[f.digest], copyOf{f, ident{e.Dev, e.Inode}, e.Size})
		}
		for ino := range want {
			if !seen[ino] {
				rep.MissingFiles++
				broken[id] = true
			}
		}
	}
	for id, want := range expect {
		if !onDisk[id] {
			rep.MissingFiles += len(want)
			broken[id] = true
		}
	}

	// 4. Identical files that are separate copies.
	for _, cs := range byDigest {
		if len(cs) < 2 {
			continue
		}
		first := cs[0]
		done := map[ident]bool{first.id: true}
		for _, cp := range cs[1:] {
			if cp.id == first.id {
				continue
			}
			if !done[cp.id] {
				rep.Duplicates++
				rep.DuplicateBytes += cp.bytes
				done[cp.id] = true
			}
			if opts.Fix && !broken[cp.f.unit] && !broken[first.f.unit] {
				if err := c.store.Link(first.f.unit, first.f.ino, cp.f.unit, cp.f.ino); err != nil {
					c.cfg.Logf("gc: audit: relink %d/%d: %v", cp.f.unit, cp.f.ino, err)
					continue
				}
				rep.Relinked++
			}
		}
	}

	// 5. Units with missing or wrong bytes refetch.
	for id := range broken {
		rep.BrokenUnits = append(rep.BrokenUnits, units[id])
	}
	sort.Slice(rep.BrokenUnits, func(i, j int) bool { return rep.BrokenUnits[i].ID < rep.BrokenUnits[j].ID })
	if opts.Fix {
		for _, u := range rep.BrokenUnits {
			// Hooks still run and pins still hold: a pinned broken unit is
			// reported and left for the next audit.
			if _, err := c.evict(ctx, u); err != nil {
				c.cfg.Logf("gc: audit: evict broken %s/%s: %v", u.Subsystem, u.Key, err)
				continue
			}
			rep.BrokenEvicted++
		}
	}
	return rep, nil
}

func (c *Collector) hashStored(unit int64, ino uint64) (string, error) {
	f, err := c.store.Open(unit, ino)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, 1<<62)); err != nil {
		return "", err
	}
	return "sha256-" + hex.EncodeToString(h.Sum(nil)), nil
}
