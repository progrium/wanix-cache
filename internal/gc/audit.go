package gc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
)

// AuditOptions controls an audit.
type AuditOptions struct {
	// Fix repairs what the audit finds: refcounts are rewritten, orphan
	// blobs deleted, and units whose blobs are missing evicted so their next
	// read refetches them. Without Fix the audit only reports.
	Fix bool
	// Deep re-walks every content object's schema and checks its recorded
	// blob list, instead of trusting gc_content_blob.
	Deep bool
}

// AuditReport is what an audit found (and, with Fix, repaired).
type AuditReport struct {
	Contents, Blobs, DiskBlobs int   // rows checked and blobs on disk
	DiskBytes                  int64 // bytes on disk

	Backfilled         int      // content rows tracked for the first time
	ContentRefsFixed   int      // content.refs that did not match the inodes
	ContentsDropped    int      // content rows no resident inode uses
	BlobListsFixed     int      // content rows whose blob list was wrong (Deep)
	BlobRefsFixed      int      // gc_blob.refs that did not match
	Orphans            int      // blobs on disk no content uses
	OrphanBytes        int64    //
	OrphansRemoved     int      // with Fix
	MissingBlobs       []string // blobs some content uses that are not on disk
	MissingContent     []string // content refs of resident inodes with no content row
	BrokenUnits        []meta.Unit
	BrokenUnitsEvicted int // with Fix
}

// Clean reports whether the audit found nothing to repair.
func (r AuditReport) Clean() bool {
	return r.ContentRefsFixed == 0 && r.ContentsDropped == 0 && r.BlobListsFixed == 0 &&
		r.BlobRefsFixed == 0 && r.Orphans == 0 && len(r.MissingBlobs) == 0 &&
		len(r.MissingContent) == 0 && len(r.BrokenUnits) == 0
}

func (r AuditReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "checked %d content objects, %d blob rows, %d blobs on disk (%d bytes)\n", r.Contents, r.Blobs, r.DiskBlobs, r.DiskBytes)
	line := func(n int, what string) {
		if n > 0 {
			fmt.Fprintf(&b, "  %d %s\n", n, what)
		}
	}
	line(r.Backfilled, "content objects tracked for the first time")
	line(r.ContentRefsFixed, "content refcounts wrong")
	line(r.ContentsDropped, "content objects unused by any resident unit")
	line(r.BlobListsFixed, "content blob lists wrong")
	line(r.BlobRefsFixed, "blob refcounts wrong")
	if r.Orphans > 0 {
		fmt.Fprintf(&b, "  %d orphan blobs (%d bytes), %d removed\n", r.Orphans, r.OrphanBytes, r.OrphansRemoved)
	}
	line(len(r.MissingBlobs), "blobs missing from disk")
	line(len(r.MissingContent), "content refs with no content row")
	if len(r.BrokenUnits) > 0 {
		fmt.Fprintf(&b, "  %d units with missing bytes, %d evicted for refetch:", len(r.BrokenUnits), r.BrokenUnitsEvicted)
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

// Audit is a full mark-and-compare pass. It recomputes content refcounts
// from the inodes of resident units, blob refcounts from the content blob
// lists, and compares the blob rows with the blobs on disk. Without
// opts.Fix it changes nothing except tracking new content (which every GC
// pass does). Content used by a fill in progress is left alone.
func (c *Collector) Audit(ctx context.Context, opts AuditOptions) (AuditReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var rep AuditReport

	before, err := c.untracked(ctx)
	if err != nil {
		return rep, err
	}
	missing, err := c.backfill(ctx)
	if err != nil {
		return rep, err
	}
	rep.Backfilled = before - len(missing)
	brokenContent := map[string]bool{}
	for _, r := range missing {
		brokenContent[r] = true
	}

	// The blobs on disk.
	disk := map[string]int64{}
	if err := c.store.Enumerate(ctx, func(b cas.Blob) error {
		disk[b.Ref] = b.Size
		rep.DiskBytes += b.Size
		return nil
	}); err != nil {
		return rep, err
	}
	rep.DiskBlobs = len(disk)

	if opts.Deep {
		if err := c.auditBlobLists(ctx, opts, &rep, brokenContent); err != nil {
			return rep, err
		}
	}

	err = c.write(ctx, func(q querier) error {
		// 1. content.refs against resident inodes.
		want := map[string]int64{}
		rows, err := q.QueryContext(ctx, `SELECT i.content, count(*) FROM inode i JOIN unit u ON u.id = i.unit
			WHERE u.state = ? AND i.kind = ? AND i.content != '' GROUP BY i.content`, meta.UnitResident, meta.KindFile)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r string
			var n int64
			if err := rows.Scan(&r, &n); err != nil {
				rows.Close()
				return err
			}
			want[r] = n
		}
		rows.Close()
		inflight := map[string]bool{}
		rows, err = q.QueryContext(ctx, `SELECT DISTINCT i.content FROM inode i JOIN unit u ON u.id = i.unit
			WHERE u.state IN (?, ?) AND i.kind = ? AND i.content != ''`, meta.UnitFetching, meta.UnitSpooled, meta.KindFile)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r string
			if err := rows.Scan(&r); err != nil {
				rows.Close()
				return err
			}
			inflight[r] = true
		}
		rows.Close()
		have := map[string]int64{}
		rows, err = q.QueryContext(ctx, `SELECT ref, refs FROM content`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r string
			var n int64
			if err := rows.Scan(&r, &n); err != nil {
				rows.Close()
				return err
			}
			have[r] = n
		}
		rows.Close()
		rep.Contents = len(have)
		now := time.Now().UnixNano()
		for r, n := range have {
			if inflight[r] || n == want[r] {
				continue
			}
			if want[r] == 0 {
				rep.ContentsDropped++
				if opts.Fix {
					if err := dropContent(ctx, q, r, now); err != nil {
						return err
					}
				}
				continue
			}
			rep.ContentRefsFixed++
			if opts.Fix {
				if _, err := q.ExecContext(ctx, `UPDATE content SET refs = ? WHERE ref = ?`, want[r], r); err != nil {
					return err
				}
			}
		}
		for r := range want {
			if _, ok := have[r]; !ok {
				rep.MissingContent = append(rep.MissingContent, r)
				brokenContent[r] = true
			}
		}

		// 2. gc_blob.refs against content blob lists.
		wantB := map[string]int64{}
		rows, err = q.QueryContext(ctx, `SELECT blob, count(*) FROM gc_content_blob GROUP BY blob`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r string
			var n int64
			if err := rows.Scan(&r, &n); err != nil {
				rows.Close()
				return err
			}
			wantB[r] = n
		}
		rows.Close()
		haveB := map[string]int64{}
		rows, err = q.QueryContext(ctx, `SELECT ref, refs FROM gc_blob`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r string
			var n int64
			if err := rows.Scan(&r, &n); err != nil {
				rows.Close()
				return err
			}
			haveB[r] = n
		}
		rows.Close()
		rep.Blobs = len(haveB)
		for r, n := range haveB {
			w := wantB[r]
			if n == w || (n < 0 && w == 0) {
				continue
			}
			rep.BlobRefsFixed++
			if opts.Fix {
				if _, err := q.ExecContext(ctx, `UPDATE gc_blob SET refs = ?, doomed_at = CASE WHEN ? = 0 THEN ? ELSE 0 END WHERE ref = ?`, w, w, now, r); err != nil {
					return err
				}
			}
		}
		for r, w := range wantB {
			if _, ok := haveB[r]; ok {
				continue
			}
			rep.BlobRefsFixed++
			if size, ok := disk[r]; ok && opts.Fix {
				if _, err := q.ExecContext(ctx, `INSERT INTO gc_blob (ref, size, refs) VALUES (?, ?, ?)`, r, size, w); err != nil {
					return err
				}
			}
		}

		// 3. Blobs in use but not on disk.
		for r, w := range wantB {
			if _, ok := disk[r]; !ok && w > 0 {
				rep.MissingBlobs = append(rep.MissingBlobs, r)
			}
		}
		sort.Strings(rep.MissingBlobs)
		for _, b := range rep.MissingBlobs {
			rows, err := q.QueryContext(ctx, `SELECT content FROM gc_content_blob WHERE blob = ?`, b)
			if err != nil {
				return err
			}
			for rows.Next() {
				var r string
				if err := rows.Scan(&r); err != nil {
					rows.Close()
					return err
				}
				brokenContent[r] = true
			}
			rows.Close()
		}
		return nil
	})
	if err != nil {
		return rep, err
	}

	// 4. Units that read broken content.
	if err := c.brokenUnits(ctx, brokenContent, &rep); err != nil {
		return rep, err
	}
	if opts.Fix {
		for _, u := range rep.BrokenUnits {
			// Hooks still run and pins still hold: a pinned broken unit is
			// reported and left for the next audit.
			if err := c.evict(ctx, u); err != nil {
				c.cfg.Logf("gc: audit: evict broken %s/%s: %v", u.Subsystem, u.Key, err)
				continue
			}
			rep.BrokenUnitsEvicted++
		}
	}

	// 5. Orphans: on disk, used by no content. A blob written by an ingest
	// that has not committed yet is held by its session and skipped.
	var orphans []string
	live, err := c.liveBlobs(ctx, keys(disk))
	if err != nil {
		return rep, err
	}
	for r, size := range disk {
		if !live[r] {
			orphans = append(orphans, r)
			rep.OrphanBytes += size
		}
	}
	sort.Strings(orphans)
	rep.Orphans = len(orphans)
	if opts.Fix && len(orphans) > 0 {
		removed, _, err := c.removeUnreferenced(ctx, orphans)
		if err != nil {
			return rep, err
		}
		rep.OrphansRemoved = len(removed)
	}
	// Rows for doomed blobs that are already gone from disk.
	if opts.Fix {
		if err := c.write(ctx, func(q querier) error {
			for r := range haveRowsNotOnDisk(ctx, c, disk) {
				if _, err := q.ExecContext(ctx, `DELETE FROM gc_blob WHERE ref = ? AND refs <= 0`, r); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

func haveRowsNotOnDisk(ctx context.Context, c *Collector, disk map[string]int64) map[string]bool {
	out := map[string]bool{}
	rows, err := c.sql.QueryContext(ctx, `SELECT ref FROM gc_blob WHERE refs <= 0`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var r string
		if rows.Scan(&r) == nil {
			if _, ok := disk[r]; !ok {
				out[r] = true
			}
		}
	}
	return out
}

func (c *Collector) untracked(ctx context.Context) (int, error) {
	var n int
	err := c.sql.QueryRowContext(ctx, `SELECT count(*) FROM content c
		WHERE NOT EXISTS (SELECT 1 FROM gc_content_blob cb WHERE cb.content = c.ref)`).Scan(&n)
	return n, err
}

// auditBlobLists re-walks every tracked content object and compares its
// blobs with gc_content_blob, fixing the list (refcounts are recomputed
// from the lists afterwards).
func (c *Collector) auditBlobLists(ctx context.Context, opts AuditOptions, rep *AuditReport, broken map[string]bool) error {
	rows, err := c.sql.QueryContext(ctx, `SELECT content, blob FROM gc_content_blob ORDER BY content`)
	if err != nil {
		return err
	}
	lists := map[string][]string{}
	for rows.Next() {
		var cr, b string
		if err := rows.Scan(&cr, &b); err != nil {
			rows.Close()
			return err
		}
		lists[cr] = append(lists[cr], b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	fixes := map[string][]cas.Blob{}
	for cr, have := range lists {
		bs, err := c.store.Blobs(ctx, cr)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.cfg.Logf("gc: audit: content %s: %v", cr, err)
			broken[cr] = true
			continue
		}
		if !sameRefs(have, bs) {
			rep.BlobListsFixed++
			fixes[cr] = bs
		}
	}
	if !opts.Fix || len(fixes) == 0 {
		return nil
	}
	return c.write(ctx, func(q querier) error {
		for cr, bs := range fixes {
			if _, err := q.ExecContext(ctx, `DELETE FROM gc_content_blob WHERE content = ?`, cr); err != nil {
				return err
			}
			for _, b := range bs {
				if _, err := q.ExecContext(ctx, `INSERT INTO gc_content_blob (content, blob) VALUES (?, ?)`, cr, b.Ref); err != nil {
					return err
				}
				if _, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO gc_blob (ref, size, refs) VALUES (?, ?, 0)`, b.Ref, b.Size); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func sameRefs(have []string, bs []cas.Blob) bool {
	if len(have) != len(bs) {
		return false
	}
	sort.Strings(have)
	for i, b := range bs {
		if have[i] != b.Ref {
			return false
		}
	}
	return true
}

func (c *Collector) brokenUnits(ctx context.Context, broken map[string]bool, rep *AuditReport) error {
	if len(broken) == 0 {
		return nil
	}
	seen := map[int64]bool{}
	for r := range broken {
		rows, err := c.sql.QueryContext(ctx, `SELECT DISTINCT u.id FROM inode i JOIN unit u ON u.id = i.unit
			WHERE i.content = ? AND i.kind = ? AND u.state = ?`, r, meta.KindFile, meta.UnitResident)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			u, err := c.unit(ctx, id)
			if err != nil {
				return err
			}
			rep.BrokenUnits = append(rep.BrokenUnits, u)
		}
	}
	sort.Slice(rep.BrokenUnits, func(i, j int) bool { return rep.BrokenUnits[i].ID < rep.BrokenUnits[j].ID })
	return nil
}

func keys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
