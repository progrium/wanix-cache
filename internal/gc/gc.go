// Package gc evicts units and frees the CAS blobs nobody references.
//
// Refcounts are kept at two levels, both global with no subsystem column, so
// a blob shared by an apk and an OCI layer is freed only when the last
// reference anywhere is gone:
//
//	content.refs   file inodes of resident units that use a content object
//	               (maintained by ingest through meta.Tx.RefContent)
//	gc_blob.refs   content objects that use a blob (maintained here)
//
// Evicting a unit moves it resident -> evicted, drops its inodes' content
// refs, drops a content object whose refs reach zero, and dooms the blobs
// that object leaves at zero. The unit's manifest (inodes, dirents, sizes)
// stays with each file's content cleared, so a later read refaults the unit
// through the fill path into the same inodes. A sweep deletes doomed
// blobs through the blobserver; Perkeep has no GC of its own.
//
// Ingest never talks to this package. It records content with
// RefContent as before and puts blobs through a cas.Session that it closes
// after committing. The GC finds content rows it has not seen yet (they
// have no gc_content_blob rows), walks their schemas and counts their
// blobs ("backfill") before every reclaim, sweep and audit. A blob that was
// doomed and is now used by new content is revived by that backfill, and
// the session's hold keeps a sweep from deleting it in between.
//
// Victims are chosen least recently used first among resident units that
// are not pinned. A unit whose eviction would free no bytes (everything it
// holds is shared) is passed over while some unit would free bytes, then
// taken in LRU order, since evicting it can make its sharers' blobs
// exclusive.
package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

var (
	// ErrPinned is returned when evicting a pinned unit.
	ErrPinned = errors.New("gc: unit is pinned")
	// ErrNotResident is returned when evicting a unit that is not resident.
	ErrNotResident = errors.New("gc: unit is not resident")
)

// Config tunes a Collector. The zero value works: no cap (so no watermark
// eviction), CAS bytes as usage, and no hooks.
type Config struct {
	// Max is the cache cap in bytes. 0 uses the CAS store's quota.
	Max int64
	// High and Low are the watermarks, as fractions of Max: when usage goes
	// over High, units are evicted until it is under Low. Defaults 0.95 and
	// 0.85.
	High, Low float64
	// Usage returns the accounted bytes (fill.Quota's CAS + spool +
	// reserved). Defaults to the CAS store's unique blob bytes.
	Usage func() int64
	// Pinned reports in-memory pins held elsewhere, normally
	// fill.Manager.Pinned (open handles, fills, views). Pins taken with
	// Collector.Acquire and durable pins are always honored too.
	Pinned func(unitID int64) bool
	// Subsystem looks up a unit's subsystem so its subsys.Evicter hook, if
	// it has one, runs before the unit is evicted. Normally
	// fill.Manager.Subsystem.
	Subsystem func(name string) (subsys.Subsystem, bool)
	// Notify is called after each eviction, outside any lock.
	Notify func(fill.UnitEvent)
	// Interval is how often Run checks the watermarks. Default 30s.
	Interval time.Duration
	// Logf logs evictions and repairs. Default: discard.
	Logf func(format string, args ...any)
}

// Collector is the eviction policy and blob GC. It implements
// fill.Reclaimer. It is safe for concurrent use; evictions, sweeps and
// audits run one at a time.
type Collector struct {
	db    *meta.DB
	sql   *sql.DB
	store *cas.Store
	cfg   Config
	pins  fill.Pins
	kick  chan struct{}

	mu sync.Mutex // serializes everything that changes refcounts or deletes blobs
}

var _ fill.Reclaimer = (*Collector)(nil)

// New applies the GC's migrations and returns a collector. It installs
// itself as the store's reclaimer, so a CAS write that would exceed the
// store's quota evicts before it fails.
func New(ctx context.Context, db *meta.DB, store *cas.Store, cfg Config) (*Collector, error) {
	if err := db.Migrate(ctx, Owner, Migrations); err != nil {
		return nil, err
	}
	if cfg.High == 0 {
		cfg.High = 0.95
	}
	if cfg.Low == 0 {
		cfg.Low = 0.85
	}
	if cfg.Low > cfg.High {
		return nil, fmt.Errorf("gc: low watermark %.2f above high %.2f", cfg.Low, cfg.High)
	}
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	c := &Collector{db: db, sql: db.SQL(), store: store, cfg: cfg, kick: make(chan struct{}, 1)}
	store.SetReclaimer(c.Reclaim)
	return c, nil
}

// Attach wires the collector into a fill manager: it becomes the manager's
// reclaimer, uses the manager's pins and subsystems where the config left
// them unset, and checks the watermarks whenever a unit becomes resident.
func (c *Collector) Attach(m fill.Manager) {
	if c.cfg.Pinned == nil {
		c.cfg.Pinned = m.Pinned
	}
	if c.cfg.Subsystem == nil {
		c.cfg.Subsystem = m.Subsystem
	}
	m.SetReclaimer(c)
	m.Watch(func(ev fill.UnitEvent) {
		if ev.To == meta.UnitResident || ev.To == meta.UnitSpooled {
			c.Kick()
		}
	})
}

// Max returns the cap the watermarks apply to; 0 means none.
func (c *Collector) Max() int64 {
	if c.cfg.Max > 0 {
		return c.cfg.Max
	}
	_, max := c.store.Usage()
	return max
}

func (c *Collector) usage() int64 {
	if c.cfg.Usage != nil {
		return c.cfg.Usage()
	}
	used, _ := c.store.Usage()
	return used
}

// Acquire pins a unit in memory until release is called.
func (c *Collector) Acquire(unitID int64) (release func()) { return c.pins.Acquire(unitID) }

// Kick asks Run to check the watermarks now.
func (c *Collector) Kick() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Run checks the watermarks every Interval and on Kick until ctx is done.
func (c *Collector) Run(ctx context.Context) error {
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := c.Collect(ctx); err != nil && ctx.Err() == nil {
			c.cfg.Logf("gc: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		case <-c.kick:
		}
	}
}

// Collect sweeps doomed blobs and, if usage is over the high watermark,
// evicts down to the low one. It returns the bytes freed.
func (c *Collector) Collect(ctx context.Context) (int64, error) {
	sw, err := c.Sweep(ctx)
	if err != nil {
		return 0, err
	}
	max := c.Max()
	if max <= 0 {
		return sw.Bytes, nil
	}
	used := c.usage()
	if float64(used) <= c.cfg.High*float64(max) {
		return sw.Bytes, nil
	}
	need := used - int64(c.cfg.Low*float64(max))
	freed, err := c.Reclaim(ctx, need)
	return sw.Bytes + freed, err
}

// Reclaim evicts unpinned units, least recently used first, and sweeps
// their blobs until at least need bytes are freed or nothing evictable
// remains. It returns the bytes actually freed, which can be less than need
// without an error; the caller decides whether that is a failure.
func (c *Collector) Reclaim(ctx context.Context, need int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.backfill(ctx); err != nil {
		return 0, err
	}
	sw, err := c.sweep(ctx)
	if err != nil {
		return 0, err
	}
	freed := sw.Bytes
	skip := map[int64]bool{}     // vetoed by a hook or pinned mid-way
	deferred := map[int64]bool{} // held back by a subsystem's EvictionOrder
	exclude := func(id int64) bool { return skip[id] || deferred[id] }
	for freed < need {
		if err := ctx.Err(); err != nil {
			return freed, err
		}
		raw, err := c.candidates(ctx, 64, exclude)
		if err != nil {
			return freed, err
		}
		if len(raw) == 0 {
			break
		}
		window := c.order(ctx, raw, deferred)
		if len(window) == 0 {
			continue // all deferred; the next window looks further
		}
		before := freed
		progress := false
		for _, u := range window {
			ex, err := c.exclusive(ctx, u.ID)
			if err != nil {
				return freed, err
			}
			if ex == 0 {
				continue
			}
			n, ok, err := c.evictAndSweep(ctx, u, skip)
			if err != nil {
				return freed, err
			}
			freed += n
			progress = progress || ok
			if freed >= need {
				break
			}
		}
		if !progress {
			// Everything in the window is shared. Evict the oldest so its
			// sharers' blobs can become exclusive.
			n, ok, err := c.evictAndSweep(ctx, window[0], skip)
			if err != nil {
				return freed, err
			}
			freed += n
			progress = ok
		}
		if freed > before || progress {
			// Something left; units held back for it may be next.
			clear(deferred)
		}
	}
	return freed, nil
}

// EvictionOrderer is optionally implemented by subsystems whose units must
// leave in a particular order, such as Nix evicting referrers before the
// store paths they reference. It is given the subsystem's evictable units
// in LRU order and returns the ones that may go now, in the order to try;
// the rest stay this round. It matches subsys.EvictionOrderer.
type EvictionOrderer interface {
	EvictionOrder(ctx context.Context, units []meta.Unit) ([]meta.Unit, error)
}

// order applies subsystems' EvictionOrder to an LRU window. Units a
// subsystem holds back are added to deferred.
func (c *Collector) order(ctx context.Context, window []meta.Unit, deferred map[int64]bool) []meta.Unit {
	if c.cfg.Subsystem == nil {
		return window
	}
	groups := map[string][]meta.Unit{}
	for _, u := range window {
		groups[u.Subsystem] = append(groups[u.Subsystem], u)
	}
	ordered := map[string][]meta.Unit{}
	for name, us := range groups {
		s, ok := c.cfg.Subsystem(name)
		if !ok {
			continue
		}
		o, ok := s.(EvictionOrderer)
		if !ok {
			continue
		}
		allowed, err := o.EvictionOrder(ctx, us)
		if err != nil {
			c.cfg.Logf("gc: %s eviction order: %v", name, err)
			allowed = nil
		}
		in := map[int64]bool{}
		for _, u := range us {
			in[u.ID] = true
		}
		var keep []meta.Unit
		kept := map[int64]bool{}
		for _, u := range allowed {
			if in[u.ID] && !kept[u.ID] { // only units it was offered
				keep = append(keep, u)
				kept[u.ID] = true
			}
		}
		for _, u := range us {
			if !kept[u.ID] {
				deferred[u.ID] = true
			}
		}
		ordered[name] = keep
	}
	// Keep the LRU slots, filling each ordered subsystem's slots with its
	// own order.
	out := make([]meta.Unit, 0, len(window))
	next := map[string]int{}
	for _, u := range window {
		us, ok := ordered[u.Subsystem]
		if !ok {
			out = append(out, u)
			continue
		}
		if i := next[u.Subsystem]; i < len(us) {
			out = append(out, us[i])
			next[u.Subsystem] = i + 1
		}
	}
	return out
}

func (c *Collector) evictAndSweep(ctx context.Context, u meta.Unit, skip map[int64]bool) (freed int64, evicted bool, err error) {
	if err := c.evict(ctx, u); err != nil {
		if errors.Is(err, ErrPinned) || errors.Is(err, ErrNotResident) || errors.Is(err, errVetoed) {
			c.cfg.Logf("gc: skip %s/%s: %v", u.Subsystem, u.Key, err)
			skip[u.ID] = true
			return 0, false, nil
		}
		return 0, false, err
	}
	sw, err := c.sweep(ctx)
	return sw.Bytes, true, err
}

// Evict evicts one unit now, whatever the watermarks say, and sweeps. It
// fails with ErrPinned or ErrNotResident, or with the subsystem hook's error.
func (c *Collector) Evict(ctx context.Context, unitID int64) (freed int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.backfill(ctx); err != nil {
		return 0, err
	}
	u, err := c.unit(ctx, unitID)
	if err != nil {
		return 0, err
	}
	if err := c.evict(ctx, u); err != nil {
		return 0, err
	}
	sw, err := c.sweep(ctx)
	return sw.Bytes, err
}

func (c *Collector) unit(ctx context.Context, id int64) (meta.Unit, error) {
	var u meta.Unit
	var last int64
	err := c.sql.QueryRowContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit WHERE id = ?`, id).
		Scan(&u.ID, &u.Subsystem, &u.Key, &u.State, &u.Root, &u.LogicalBytes, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return u, fmt.Errorf("gc: unit %d: %w", id, meta.ErrNotExist)
	}
	if last != 0 {
		u.LastAccess = time.Unix(0, last)
	}
	return u, err
}

func (c *Collector) pinned(ctx context.Context, q querier, id int64) (bool, error) {
	if c.pins.Pinned(id) || (c.cfg.Pinned != nil && c.cfg.Pinned(id)) {
		return true, nil
	}
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM gc_pin WHERE unit = ?`, id).Scan(&n)
	return n > 0, err
}

// candidates returns up to n resident, unpinned units in LRU order.
func (c *Collector) candidates(ctx context.Context, n int, exclude func(int64) bool) ([]meta.Unit, error) {
	var out []meta.Unit
	var lastAccess, lastID int64 = -1 << 63, 0
	for len(out) < n {
		rows, err := c.sql.QueryContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit
			WHERE state = ? AND (last_access, id) > (?, ?)
			AND id NOT IN (SELECT unit FROM gc_pin)
			ORDER BY last_access, id LIMIT ?`, meta.UnitResident, lastAccess, lastID, n)
		if err != nil {
			return nil, err
		}
		got := 0
		for rows.Next() {
			var u meta.Unit
			var last int64
			if err := rows.Scan(&u.ID, &u.Subsystem, &u.Key, &u.State, &u.Root, &u.LogicalBytes, &last); err != nil {
				rows.Close()
				return nil, err
			}
			got++
			lastAccess, lastID = last, u.ID
			if last != 0 {
				u.LastAccess = time.Unix(0, last)
			}
			if exclude(u.ID) || c.pins.Pinned(u.ID) || (c.cfg.Pinned != nil && c.cfg.Pinned(u.ID)) {
				continue
			}
			if len(out) < n {
				out = append(out, u)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if got < n {
			break
		}
	}
	return out, nil
}

// Exclusive returns the bytes evicting unit id would free now: the sizes of
// blobs used only by content that only this unit uses.
func (c *Collector) Exclusive(ctx context.Context, id int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.backfill(ctx); err != nil {
		return 0, err
	}
	return c.exclusive(ctx, id)
}

func (c *Collector) exclusive(ctx context.Context, id int64) (int64, error) {
	var n int64
	err := c.sql.QueryRowContext(ctx, `
		WITH uc AS (SELECT content AS ref, count(*) AS n FROM inode
		            WHERE unit = ? AND kind = ? AND content != '' GROUP BY content),
		     dc AS (SELECT uc.ref FROM uc JOIN content c ON c.ref = uc.ref WHERE c.refs <= uc.n),
		     bc AS (SELECT cb.blob, count(*) AS n FROM gc_content_blob cb JOIN dc ON dc.ref = cb.content GROUP BY cb.blob)
		SELECT coalesce(sum(b.size), 0) FROM bc JOIN gc_blob b ON b.ref = bc.blob WHERE b.refs <= bc.n`,
		id, meta.KindFile).Scan(&n)
	return n, err
}

var errVetoed = errors.New("gc: eviction vetoed by subsystem")

// evict moves one unit resident -> evicted and drops its references. Its
// blobs are only doomed; the caller sweeps.
func (c *Collector) evict(ctx context.Context, u meta.Unit) error {
	if u.State != meta.UnitResident {
		return fmt.Errorf("%w: %s/%s is %v", ErrNotResident, u.Subsystem, u.Key, u.State)
	}
	if p, err := c.pinned(ctx, c.sql, u.ID); err != nil {
		return err
	} else if p {
		return fmt.Errorf("%w: %s/%s", ErrPinned, u.Subsystem, u.Key)
	}
	if c.cfg.Subsystem != nil {
		if s, ok := c.cfg.Subsystem(u.Subsystem); ok {
			if e, ok := s.(subsys.Evicter); ok {
				if err := e.OnEvict(ctx, subsys.Key(u.Key)); err != nil {
					return fmt.Errorf("%w: %s/%s: %v", errVetoed, u.Subsystem, u.Key, err)
				}
			}
		}
	}
	err := c.write(ctx, func(q querier) error {
		res, err := q.ExecContext(ctx, `UPDATE unit SET state = ? WHERE id = ? AND state = ?`, meta.UnitEvicted, u.ID, meta.UnitResident)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: %s/%s", ErrNotResident, u.Subsystem, u.Key)
		}
		refs, err := unitContents(ctx, q, u.ID)
		if err != nil {
			return err
		}
		now := time.Now().UnixNano()
		for ref, n := range refs {
			if err := unrefContent(ctx, q, ref, n, now); err != nil {
				return err
			}
		}
		// The manifest stays; only the content refs go. An empty content
		// on a non-empty file tells fill.Manager.Open to refault the unit,
		// which reuses these inodes.
		if _, err := q.ExecContext(ctx, `UPDATE inode SET content = '' WHERE unit = ? AND kind = ? AND content != ''`, u.ID, meta.KindFile); err != nil {
			return err
		}
		// Last look, inside the transaction: a pin taken since the check
		// above wins. (fill.Manager.Open must still re-read the unit state
		// after it pins, and treat a missing blob as a refault.)
		if p, err := c.pinned(ctx, q, u.ID); err != nil {
			return err
		} else if p {
			return fmt.Errorf("%w: %s/%s", ErrPinned, u.Subsystem, u.Key)
		}
		return nil
	})
	c.db.Changed() // the content refs above, written outside a meta.Tx
	if err != nil {
		return err
	}
	c.cfg.Logf("gc: evicted %s/%s (%d logical bytes)", u.Subsystem, u.Key, u.LogicalBytes)
	if c.cfg.Notify != nil {
		ev := u
		ev.State = meta.UnitEvicted
		c.cfg.Notify(fill.UnitEvent{Unit: ev, From: meta.UnitResident, To: meta.UnitEvicted})
	}
	return nil
}

// unitContents counts a unit's file inodes per content ref.
func unitContents(ctx context.Context, q querier, unit int64) (map[string]int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT content, count(*) FROM inode WHERE unit = ? AND kind = ? AND content != '' GROUP BY content`, unit, meta.KindFile)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var ref string
		var n int64
		if err := rows.Scan(&ref, &n); err != nil {
			return nil, err
		}
		out[ref] = n
	}
	return out, rows.Err()
}

// unrefContent drops n references to content ref, and the content object
// itself when none remain.
func unrefContent(ctx context.Context, q querier, ref string, n int64, now int64) error {
	var refs int64
	err := q.QueryRowContext(ctx, `UPDATE content SET refs = max(refs - ?, 0) WHERE ref = ? RETURNING refs`, n, ref).Scan(&refs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // already gone (a repaired audit, or never recorded)
	}
	if err != nil {
		return err
	}
	if refs > 0 {
		return nil
	}
	return dropContent(ctx, q, ref, now)
}

// dropContent deletes a content object and dooms blobs it leaves unused.
func dropContent(ctx context.Context, q querier, ref string, now int64) error {
	if _, err := q.ExecContext(ctx, `UPDATE gc_blob SET refs = refs - 1,
		doomed_at = CASE WHEN refs - 1 <= 0 THEN ? ELSE doomed_at END
		WHERE ref IN (SELECT blob FROM gc_content_blob WHERE content = ?)`, now, ref); err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM gc_content_blob WHERE content = ?`, ref); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `DELETE FROM content WHERE ref = ?`, ref)
	return err
}

// SweepResult reports what a sweep deleted.
type SweepResult struct {
	Blobs int
	Bytes int64
	Held  int // doomed blobs skipped because an ingest holds them
}

// Sweep deletes doomed blobs from the CAS.
func (c *Collector) Sweep(ctx context.Context) (SweepResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.backfill(ctx); err != nil {
		return SweepResult{}, err
	}
	return c.sweep(ctx)
}

func (c *Collector) sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult
	after := ""
	for {
		var doomed []string
		rows, err := c.sql.QueryContext(ctx, `SELECT ref FROM gc_blob WHERE refs <= 0 AND ref > ? ORDER BY ref LIMIT 500`, after)
		if err != nil {
			return res, err
		}
		for rows.Next() {
			var r string
			if err := rows.Scan(&r); err != nil {
				rows.Close()
				return res, err
			}
			doomed = append(doomed, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return res, err
		}
		if len(doomed) == 0 {
			return res, nil
		}
		after = doomed[len(doomed)-1]
		removed, freed, err := c.removeUnreferenced(ctx, doomed)
		if err != nil {
			return res, err
		}
		res.Blobs += len(removed)
		res.Bytes += freed
		res.Held += len(doomed) - len(removed)
	}
}

// removeUnreferenced deletes those of refs that no content uses, as
// confirmed from the DB after they are locked against new holds, and then
// their gc_blob rows.
func (c *Collector) removeUnreferenced(ctx context.Context, refs []string) ([]string, int64, error) {
	removed, freed, err := c.store.Remove(ctx, refs, func(cand []string) ([]string, error) {
		// New content may have committed since the candidates were read;
		// count it first so a revived blob is seen as referenced.
		if _, err := c.backfill(ctx); err != nil {
			return nil, err
		}
		live, err := c.liveBlobs(ctx, cand)
		if err != nil {
			return nil, err
		}
		out := cand[:0:0]
		for _, r := range cand {
			if !live[r] {
				out = append(out, r)
			}
		}
		return out, nil
	})
	if err != nil || len(removed) == 0 {
		return removed, freed, err
	}
	err = c.write(ctx, func(q querier) error {
		for _, r := range removed {
			if _, err := q.ExecContext(ctx, `DELETE FROM gc_blob WHERE ref = ? AND refs <= 0`, r); err != nil {
				return err
			}
		}
		return nil
	})
	return removed, freed, err
}

// liveBlobs returns which of refs have a gc_blob row with refs > 0.
func (c *Collector) liveBlobs(ctx context.Context, refs []string) (map[string]bool, error) {
	live := map[string]bool{}
	for len(refs) > 0 {
		n := min(len(refs), 500)
		batch := refs[:n]
		refs = refs[n:]
		args := make([]any, len(batch))
		for i, r := range batch {
			args[i] = r
		}
		rows, err := c.sql.QueryContext(ctx, `SELECT ref FROM gc_blob WHERE refs > 0 AND ref IN (?`+strings.Repeat(",?", len(batch)-1)+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r string
			if err := rows.Scan(&r); err != nil {
				rows.Close()
				return nil, err
			}
			live[r] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return live, nil
}

// backfill tracks content rows the GC has not seen: it walks each one's
// schema in the CAS and counts its blobs. It returns the refs of content
// whose blobs are missing; those stay untracked and the audit reports them.
func (c *Collector) backfill(ctx context.Context) (missing []string, err error) {
	after := ""
	for {
		rows, err := c.sql.QueryContext(ctx, `SELECT c.ref FROM content c
			WHERE c.ref > ? AND NOT EXISTS (SELECT 1 FROM gc_content_blob cb WHERE cb.content = c.ref)
			ORDER BY c.ref LIMIT 200`, after)
		if err != nil {
			return missing, err
		}
		var refs []string
		for rows.Next() {
			var r string
			if err := rows.Scan(&r); err != nil {
				rows.Close()
				return missing, err
			}
			refs = append(refs, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return missing, err
		}
		if len(refs) == 0 {
			return missing, nil
		}
		after = refs[len(refs)-1]
		walked := map[string][]cas.Blob{}
		for _, r := range refs {
			bs, err := c.store.Blobs(ctx, r)
			if err != nil {
				c.cfg.Logf("gc: content %s: %v", r, err)
				missing = append(missing, r)
				continue
			}
			walked[r] = bs
		}
		err = c.write(ctx, func(q querier) error {
			for r, bs := range walked {
				if err := trackContent(ctx, q, r, bs); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return missing, err
		}
	}
}

// trackContent records that content ref is made of blobs, once.
func trackContent(ctx context.Context, q querier, ref string, blobs []cas.Blob) error {
	var exists int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM content WHERE ref = ?`, ref).Scan(&exists); err != nil || exists == 0 {
		return err
	}
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM gc_content_blob WHERE content = ?`, ref).Scan(&exists); err != nil || exists > 0 {
		return err
	}
	for _, b := range blobs {
		if _, err := q.ExecContext(ctx, `INSERT INTO gc_content_blob (content, blob) VALUES (?, ?)`, ref, b.Ref); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO gc_blob (ref, size, refs) VALUES (?, ?, 1)
			ON CONFLICT (ref) DO UPDATE SET refs = max(refs, 0) + 1, doomed_at = 0`, b.Ref, b.Size); err != nil {
			return err
		}
	}
	return nil
}

// Pin is a durable pin.
type Pin struct {
	Unit   meta.Unit
	Reason string
}

// Pin pins a unit durably for reason until Unpin with the same reason.
func (c *Collector) Pin(ctx context.Context, unitID int64, reason string) error {
	return c.write(ctx, func(q querier) error {
		_, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO gc_pin (unit, reason) VALUES (?, ?)`, unitID, reason)
		return err
	})
}

// Unpin removes one durable pin.
func (c *Collector) Unpin(ctx context.Context, unitID int64, reason string) error {
	return c.write(ctx, func(q querier) error {
		_, err := q.ExecContext(ctx, `DELETE FROM gc_pin WHERE unit = ? AND reason = ?`, unitID, reason)
		return err
	})
}

// Pins lists durable pins.
func (c *Collector) Pins(ctx context.Context) ([]Pin, error) {
	rows, err := c.sql.QueryContext(ctx, `SELECT u.id, u.subsystem, u.key, u.state, u.root, u.logical_bytes, p.reason
		FROM gc_pin p JOIN unit u ON u.id = p.unit ORDER BY u.subsystem, u.key, p.reason`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pin
	for rows.Next() {
		var p Pin
		if err := rows.Scan(&p.Unit.ID, &p.Unit.Subsystem, &p.Unit.Key, &p.Unit.State, &p.Unit.Root, &p.Unit.LogicalBytes, &p.Reason); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// querier is what *sql.DB, *sql.Conn and *sql.Tx have in common.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// write runs fn in a BEGIN IMMEDIATE transaction, so its reads and writes
// see one snapshot and cannot fail late on a write-lock upgrade.
func (c *Collector) write(ctx context.Context, fn func(q querier) error) (err error) {
	conn, err := c.sql.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if err = fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}
