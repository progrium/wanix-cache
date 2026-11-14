// Package usage records how often packages, content objects and paths are
// used, in tables separate from the unit table, so history survives
// eviction and upgrades and never feeds the LRU on its own.
//
// Three levels, each with daily buckets and running totals:
//
//	package  (subsystem, name, version)  opens, fetches, refetches, bytes
//	object   content digest              opens, bytes; one row per distinct
//	                                     file content, across every unit
//	                                     and subsystem that contains it
//	path     (subsystem, package, path)  opens, bytes; package is the
//	                                     unit's Info.Name, or its key
//
// A refetch is a fault on a unit that was evicted before, which measures
// eviction regret directly. Units a subsystem reports as derived (built from
// other cached units, like an apk's unpacked layer) do not count as fetches.
//
// Hard links are one inode, so opens through any of a file's names count
// at one of them; the never-opened report treats every name of an opened
// inode as opened.
//
// The hot path is one increment in a sharded in-memory map keyed by inode,
// on each open, plus a bytes-read sum at close. Flush, every 30 s or so,
// maps touched inodes to units, contents and paths and writes everything in
// one transaction, so reads never touch SQL.
//
// A guest's page cache can serve repeat opens without asking the server, so
// raw open counts undercount; days used is the robust signal.
package usage

import (
	"context"
	"database/sql"
	"errors"
	"hash/maphash"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Owner is the migration owner of the usage tables.
const Owner = "usage"

// Retention of daily rows; totals are kept forever.
const (
	PackageDays = 90
	ObjectDays  = 30
	PathDays    = 30
)

// Derived is optionally implemented by subsystems whose units can be built
// from other cached units rather than fetched from upstream. Fills of such
// units are not counted as package fetches.
type Derived interface {
	DerivedUnit(key subsys.Key) bool
}

// Subsystems looks up a registered subsystem; fill.Manager has this method.
type Subsystems interface {
	Subsystem(name string) (subsys.Subsystem, bool)
}

var migrations = []meta.Migration{
	{Version: 1, SQL: `
CREATE TABLE usage_unit (
	unit      INTEGER PRIMARY KEY,
	subsystem TEXT NOT NULL,
	key       TEXT NOT NULL,
	name      TEXT NOT NULL DEFAULT '',
	version   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE usage_pkg (
	subsystem TEXT NOT NULL, name TEXT NOT NULL, version TEXT NOT NULL,
	first_seen INTEGER NOT NULL, last_used INTEGER NOT NULL, days_used INTEGER NOT NULL DEFAULT 0,
	opens INTEGER NOT NULL DEFAULT 0, fetches INTEGER NOT NULL DEFAULT 0,
	refetches INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (subsystem, name, version)
) WITHOUT ROWID;
CREATE TABLE usage_pkg_day (
	subsystem TEXT NOT NULL, name TEXT NOT NULL, version TEXT NOT NULL, day INTEGER NOT NULL,
	opens INTEGER NOT NULL DEFAULT 0, fetches INTEGER NOT NULL DEFAULT 0,
	refetches INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (subsystem, name, version, day)
) WITHOUT ROWID;
CREATE INDEX usage_pkg_day_day ON usage_pkg_day (day);
CREATE TABLE usage_obj (
	ref TEXT PRIMARY KEY, size INTEGER NOT NULL DEFAULT 0,
	first_seen INTEGER NOT NULL, last_used INTEGER NOT NULL, days_used INTEGER NOT NULL DEFAULT 0,
	opens INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE TABLE usage_obj_day (
	ref TEXT NOT NULL, day INTEGER NOT NULL,
	opens INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (ref, day)
) WITHOUT ROWID;
CREATE INDEX usage_obj_day_day ON usage_obj_day (day);
CREATE TABLE usage_path (
	subsystem TEXT NOT NULL, pkg TEXT NOT NULL, path TEXT NOT NULL,
	ref TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
	first_seen INTEGER NOT NULL, last_used INTEGER NOT NULL, days_used INTEGER NOT NULL DEFAULT 0,
	opens INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (subsystem, pkg, path)
) WITHOUT ROWID;
CREATE INDEX usage_path_ref ON usage_path (ref);
CREATE TABLE usage_path_day (
	subsystem TEXT NOT NULL, pkg TEXT NOT NULL, path TEXT NOT NULL, day INTEGER NOT NULL,
	opens INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (subsystem, pkg, path, day)
) WITHOUT ROWID;
CREATE INDEX usage_path_day_day ON usage_path_day (day);
`},
}

// Migrate applies the usage schema. New does this; reports opened on a
// database without a Tracker call it too.
func Migrate(ctx context.Context, db *meta.DB) error { return db.Migrate(ctx, Owner, migrations) }

const nshards = 16

type counter struct {
	unit    int64
	content string
	size    int64
	opens   int64
	bytes   int64
}

type shard struct {
	mu sync.Mutex
	m  map[uint64]*counter
}

type unitCounter struct{ fetches, refetches int64 }

// unitInfo is what flushes need to know about a unit, cached per unit.
type unitInfo struct {
	sub, key, name, version string
	pkg                     string // name, or key when there is none
	rootPath                string
	derived                 bool
}

// pendingObj is an object open whose content ref was not known yet (the
// unit was still in the spool); it is retried on later flushes.
type pendingObj struct {
	ino   uint64
	opens int64
	bytes int64
	day   int64
	tries int
}

// Tracker counts usage in memory and flushes it to the database.
type Tracker struct {
	db   *meta.DB
	sql  *sql.DB
	subs Subsystems
	Logf func(string, ...any)
	now  func() time.Time

	seed   maphash.Seed
	shards [nshards]shard

	mu    sync.Mutex
	units map[int64]*unitCounter

	flushMu   sync.Mutex // one flush at a time; guards the fields below
	info      map[int64]*unitInfo
	paths     map[uint64]string // ino -> absolute path
	pending   []pendingObj
	lastPrune time.Time
}

// New returns a Tracker on db, applying the usage schema. subs resolves
// unit subsystems for package names; it may be nil.
func New(ctx context.Context, db *meta.DB, subs Subsystems) (*Tracker, error) {
	if err := Migrate(ctx, db); err != nil {
		return nil, err
	}
	t := &Tracker{db: db, sql: db.SQL(), subs: subs, Logf: log.Printf, now: time.Now,
		seed: maphash.MakeSeed(), units: map[int64]*unitCounter{},
		info: map[int64]*unitInfo{}, paths: map[uint64]string{}}
	for i := range t.shards {
		t.shards[i].m = map[uint64]*counter{}
	}
	return t, nil
}

// Backfill records package names for units that have none in usage_unit
// yet (units cached before usage tracking ran), so offline reports can
// name their packages. Run it after the subsystems are registered.
func (t *Tracker) Backfill(ctx context.Context) error {
	rows, err := t.sql.QueryContext(ctx, `SELECT id FROM unit WHERE id NOT IN (SELECT unit FROM usage_unit)`)
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
	if err := rows.Err(); err != nil {
		return err
	}
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	for _, id := range ids {
		ui, err := t.unitInfo(ctx, id)
		if err != nil {
			continue
		}
		if _, err := t.sql.ExecContext(ctx, `INSERT OR IGNORE INTO usage_unit (unit, subsystem, key, name, version) VALUES (?, ?, ?, ?, ?)`,
			id, ui.sub, ui.key, ui.name, ui.version); err != nil {
			return err
		}
	}
	return nil
}

// Attach subscribes the tracker to a manager's open and unit events.
func (t *Tracker) Attach(m fill.Manager) {
	m.WatchOpens(t.Open)
	m.Watch(t.Unit)
}

// Open records a file open or close. It is the hot path: a map increment.
func (t *Tracker) Open(ev fill.OpenEvent) {
	in := ev.Inode
	if in.Kind != meta.KindFile || ev.Internal {
		return
	}
	sh := &t.shards[maphash.Comparable(t.seed, in.Ino)%nshards]
	sh.mu.Lock()
	c := sh.m[in.Ino]
	if c == nil {
		c = &counter{unit: in.Unit}
		sh.m[in.Ino] = c
	}
	if in.Content != "" {
		c.content = in.Content
	}
	c.size = in.Size
	if ev.Closed {
		c.bytes += ev.BytesRead
	} else {
		c.opens++
	}
	sh.mu.Unlock()
}

// Unit records fetches and refetches from unit state changes.
func (t *Tracker) Unit(ev fill.UnitEvent) {
	if ev.To != meta.UnitFetching {
		if ev.To == meta.UnitSpooled {
			// Make sure the unit gets a usage_unit row at the next flush,
			// even if nothing opens it.
			t.mu.Lock()
			if t.units[ev.Unit.ID] == nil {
				t.units[ev.Unit.ID] = &unitCounter{}
			}
			t.mu.Unlock()
		}
		return
	}
	t.mu.Lock()
	c := t.units[ev.Unit.ID]
	if c == nil {
		c = &unitCounter{}
		t.units[ev.Unit.ID] = c
	}
	if ev.From == meta.UnitEvicted {
		c.refetches++
	} else {
		c.fetches++
	}
	t.mu.Unlock()
}

// Run flushes every interval until ctx is done, then flushes once more.
func (t *Tracker) Run(ctx context.Context, every time.Duration) {
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := t.Flush(context.Background()); err != nil {
				t.Logf("usage: flush: %v", err)
			}
			return
		case <-tk.C:
			if err := t.Flush(ctx); err != nil && ctx.Err() == nil {
				t.Logf("usage: flush: %v", err)
			}
		}
	}
}

func day(t time.Time) int64 { return t.Unix() / 86400 }

func (t *Tracker) unitInfo(ctx context.Context, id int64) (*unitInfo, error) {
	if ui := t.info[id]; ui != nil {
		return ui, nil
	}
	u, err := t.db.UnitByID(ctx, id)
	if err != nil {
		return nil, err
	}
	ui := &unitInfo{sub: u.Subsystem, key: u.Key, pkg: u.Key}
	if t.subs != nil {
		if s, ok := t.subs.Subsystem(u.Subsystem); ok {
			if info, err := s.Describe(ctx, subsys.Key(u.Key)); err == nil {
				ui.name, ui.version = info.Name, info.Version
				if info.Name != "" {
					ui.pkg = info.Name
				}
			}
			if d, ok := s.(Derived); ok {
				ui.derived = d.DerivedUnit(subsys.Key(u.Key))
			}
		}
	}
	if u.Root != 0 {
		ui.rootPath, _ = t.db.PathOf(ctx, u.Root)
	}
	t.info[id] = ui
	return ui, nil
}

// relPath returns ino's path inside its unit.
func (t *Tracker) relPath(ctx context.Context, ui *unitInfo, ino uint64) (string, error) {
	p, ok := t.paths[ino]
	if !ok {
		var err error
		if p, err = t.db.PathOf(ctx, ino); err != nil {
			return "", err
		}
		if len(t.paths) > 100_000 {
			clear(t.paths)
		}
		t.paths[ino] = p
	}
	if ui.rootPath == "" || !strings.HasPrefix(p, ui.rootPath+"/") {
		return strings.TrimPrefix(p, "/"), nil
	}
	return p[len(ui.rootPath)+1:], nil
}

type pkgKey struct{ sub, name, version string }
type pkgAcc struct{ opens, fetches, refetches, bytes int64 }
type pathKey struct{ sub, pkg, path string }
type pathAcc struct {
	ref         string
	size        int64
	opens, byts int64
}
type objAcc struct {
	size        int64
	opens, byts int64
}

// Flush writes the counts gathered since the last flush.
func (t *Tracker) Flush(ctx context.Context) error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	opens := map[uint64]*counter{}
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for ino, c := range sh.m {
			opens[ino] = c
		}
		sh.m = map[uint64]*counter{}
		sh.mu.Unlock()
	}
	t.mu.Lock()
	units := t.units
	t.units = map[int64]*unitCounter{}
	t.mu.Unlock()
	pending := t.pending
	t.pending = nil
	if len(opens) == 0 && len(units) == 0 && len(pending) == 0 {
		return t.prune(ctx)
	}

	now := t.now()
	today := day(now)
	pkgs := map[pkgKey]*pkgAcc{}
	paths := map[pathKey]*pathAcc{}
	objs := map[string]*objAcc{}
	seenUnits := map[int64]*unitInfo{}

	pkgOf := func(ui *unitInfo) *pkgAcc {
		if ui.name == "" {
			return nil
		}
		k := pkgKey{ui.sub, ui.name, ui.version}
		a := pkgs[k]
		if a == nil {
			a = &pkgAcc{}
			pkgs[k] = a
		}
		return a
	}
	addObj := func(ref string, size, n, b int64) {
		o := objs[ref]
		if o == nil {
			o = &objAcc{size: size}
			objs[ref] = o
		}
		o.opens += n
		o.byts += b
	}

	for id, c := range units {
		ui, err := t.unitInfo(ctx, id)
		if err != nil {
			continue
		}
		seenUnits[id] = ui
		if ui.derived || c.fetches+c.refetches == 0 {
			continue
		}
		if a := pkgOf(ui); a != nil {
			a.fetches += c.fetches
			a.refetches += c.refetches
		}
	}
	for ino, c := range opens {
		ui, err := t.unitInfo(ctx, c.unit)
		if err != nil {
			continue // unit gone
		}
		seenUnits[c.unit] = ui
		if a := pkgOf(ui); a != nil {
			a.opens += c.opens
			a.bytes += c.bytes
		}
		if rel, err := t.relPath(ctx, ui, ino); err == nil {
			k := pathKey{ui.sub, ui.pkg, rel}
			p := paths[k]
			if p == nil {
				p = &pathAcc{}
				paths[k] = p
			}
			p.opens += c.opens
			p.byts += c.bytes
			p.size = c.size
			if c.content != "" {
				p.ref = c.content
			}
		}
		switch {
		case c.content != "":
			addObj(c.content, c.size, c.opens, c.bytes)
		case c.size > 0:
			pending = append(pending, pendingObj{ino: ino, opens: c.opens, bytes: c.bytes, day: today})
		}
	}
	// Objects opened while their unit was in the spool: look up the
	// content ref now that ingest may have set it.
	var retry []pendingObj
	for _, po := range pending {
		in, err := t.db.Get(ctx, po.ino)
		if err != nil {
			continue
		}
		if in.Content == "" {
			if po.tries++; po.tries < 20 {
				retry = append(retry, po)
			}
			continue
		}
		addObj(in.Content, in.Size, po.opens, po.bytes)
		if ui := seenUnits[in.Unit]; ui != nil {
			if rel, err := t.relPath(ctx, ui, po.ino); err == nil {
				if p := paths[pathKey{ui.sub, ui.pkg, rel}]; p != nil && p.ref == "" {
					p.ref = in.Content
				}
			}
		}
	}

	tx, err := t.sql.BeginTx(ctx, nil)
	if err != nil {
		t.pending = append(t.pending, pending...)
		return err
	}
	defer tx.Rollback()
	ts := now.UnixNano()
	for id, ui := range seenUnits {
		if _, err := tx.ExecContext(ctx, `INSERT INTO usage_unit (unit, subsystem, key, name, version) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (unit) DO UPDATE SET subsystem = excluded.subsystem, key = excluded.key, name = excluded.name, version = excluded.version`,
			id, ui.sub, ui.key, ui.name, ui.version); err != nil {
			return err
		}
	}
	for k, a := range pkgs {
		if err := upsert(ctx, tx, "usage_pkg", []string{"subsystem", "name", "version"}, []any{k.sub, k.name, k.version},
			today, ts, []string{"opens", "fetches", "refetches", "bytes"}, []int64{a.opens, a.fetches, a.refetches, a.bytes}, nil); err != nil {
			return err
		}
	}
	for ref, o := range objs {
		if err := upsert(ctx, tx, "usage_obj", []string{"ref"}, []any{ref},
			today, ts, []string{"opens", "bytes"}, []int64{o.opens, o.byts}, map[string]any{"size": o.size}); err != nil {
			return err
		}
	}
	for k, p := range paths {
		extra := map[string]any{"size": p.size}
		if p.ref != "" {
			extra["ref"] = p.ref
		}
		if err := upsert(ctx, tx, "usage_path", []string{"subsystem", "pkg", "path"}, []any{k.sub, k.pkg, k.path},
			today, ts, []string{"opens", "bytes"}, []int64{p.opens, p.byts}, extra); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.pending = append(t.pending, retry...)
	return t.prune(ctx)
}

// upsert adds counts to table's daily row and its totals row, bumping
// days_used when the daily row is new. extra columns are set on the
// totals row only.
func upsert(ctx context.Context, tx *sql.Tx, table string, keys []string, kv []any, today, ts int64, cols []string, vals []int64, extra map[string]any) error {
	where := strings.Join(keys, " = ? AND ") + " = ?"
	ph := strings.Repeat("?, ", len(keys))
	res, err := tx.ExecContext(ctx, `INSERT INTO `+table+`_day (`+strings.Join(keys, ", ")+`, day) VALUES (`+ph+`?) ON CONFLICT DO NOTHING`,
		append(append([]any{}, kv...), today)...)
	if err != nil {
		return err
	}
	newDay, _ := res.RowsAffected()
	var set []string
	args := []any{}
	for i, c := range cols {
		set = append(set, c+" = "+c+" + ?")
		args = append(args, vals[i])
	}
	if _, err := tx.ExecContext(ctx, `UPDATE `+table+`_day SET `+strings.Join(set, ", ")+` WHERE `+where+` AND day = ?`,
		append(append(args, kv...), today)...); err != nil {
		return err
	}

	icols := append(append([]string{}, keys...), "first_seen", "last_used", "days_used")
	iargs := append(append([]any{}, kv...), ts, ts, newDay)
	upd := []string{"last_used = max(last_used, excluded.last_used)", "days_used = days_used + excluded.days_used"}
	for i, c := range cols {
		icols = append(icols, c)
		iargs = append(iargs, vals[i])
		upd = append(upd, c+" = "+c+" + excluded."+c)
	}
	for c, v := range extra {
		icols = append(icols, c)
		iargs = append(iargs, v)
		upd = append(upd, c+" = excluded."+c)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (`+strings.Join(icols, ", ")+`) VALUES (`+
		strings.TrimSuffix(strings.Repeat("?, ", len(icols)), ", ")+`) ON CONFLICT (`+strings.Join(keys, ", ")+`) DO UPDATE SET `+
		strings.Join(upd, ", "), iargs...)
	return err
}

// prune drops daily rows past their retention, at most hourly.
func (t *Tracker) prune(ctx context.Context) error {
	now := t.now()
	if now.Sub(t.lastPrune) < time.Hour {
		return nil
	}
	t.lastPrune = now
	today := day(now)
	for table, keep := range map[string]int64{"usage_pkg_day": PackageDays, "usage_obj_day": ObjectDays, "usage_path_day": PathDays} {
		if _, err := t.sql.ExecContext(ctx, `DELETE FROM `+table+` WHERE day < ?`, today-keep); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}
