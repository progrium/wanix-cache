package usage

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
)

// Query selects what a report covers.
type Query struct {
	Subsystem string // "" for all
	Days      int    // window for the recent columns; default 30
	Limit     int    // rows per section; default 20
	// Untouched lists never-opened files per unit, up to Limit per unit.
	Untouched bool
}

func (q *Query) defaults() {
	if q.Days <= 0 {
		q.Days = 30
	}
	if q.Limit <= 0 {
		q.Limit = 20
	}
}

// PackageStat is one package version's usage.
type PackageStat struct {
	Subsystem, Name, Version string
	FirstSeen, LastUsed      time.Time
	DaysUsed                 int64 // all time
	RecentDays               int64 // days used in the window
	Opens, Fetches           int64
	Refetches, Bytes         int64
	RecentOpens              int64
}

// ObjectStat is one content object's usage across every unit holding it.
type ObjectStat struct {
	Ref          string
	Size         int64
	Opens, Bytes int64
	DaysUsed     int64
	LastUsed     time.Time
	Paths        int64  // distinct (subsystem, package, path) seen opening it
	ExamplePath  string // one of them, "subsystem:package/path"
	RecentOpens  int64
	FirstSeen    time.Time
}

// PathStat is one path's usage within a package (or unit).
type PathStat struct {
	Subsystem, Package, Path string
	Size                     int64
	Opens, Bytes, DaysUsed   int64
	LastUsed                 time.Time
}

// UnitUntouched summarizes the never-opened files of one cached unit.
type UnitUntouched struct {
	Subsystem, Key, Package string
	State                   meta.UnitState
	Files, Untouched        int64
	Bytes, UntouchedBytes   int64
	List                    []FileSize // up to Query.Limit, largest first
}

// FileSize is a path in a unit and its size.
type FileSize struct {
	Path string
	Size int64
}

// Report is everything `wanix-cache usage` and /.cache/usage show.
type Report struct {
	Generated time.Time
	Query     Query
	Packages  []PackageStat
	Objects   []ObjectStat
	Paths     []PathStat
	Untouched []UnitUntouched
}

func ts(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Build reads a report from the database. It applies the usage schema if
// needed, so it works on a cache that has never been served.
func Build(ctx context.Context, db *meta.DB, q Query) (*Report, error) {
	q.defaults()
	if err := Migrate(ctx, db); err != nil {
		return nil, err
	}
	s := db.SQL()
	now := time.Now()
	since := day(now) - int64(q.Days) + 1
	r := &Report{Generated: now, Query: q}

	rows, err := s.QueryContext(ctx, `SELECT p.subsystem, p.name, p.version, p.first_seen, p.last_used, p.days_used,
			p.opens, p.fetches, p.refetches, p.bytes,
			coalesce((SELECT count(*) FROM usage_pkg_day d WHERE d.subsystem = p.subsystem AND d.name = p.name AND d.version = p.version AND d.day >= ?), 0),
			coalesce((SELECT sum(opens) FROM usage_pkg_day d WHERE d.subsystem = p.subsystem AND d.name = p.name AND d.version = p.version AND d.day >= ?), 0)
		FROM usage_pkg p WHERE ? = '' OR p.subsystem = ?
		ORDER BY p.days_used DESC, p.opens DESC, p.name LIMIT ?`, since, since, q.Subsystem, q.Subsystem, q.Limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p PackageStat
		var first, last int64
		if err := rows.Scan(&p.Subsystem, &p.Name, &p.Version, &first, &last, &p.DaysUsed,
			&p.Opens, &p.Fetches, &p.Refetches, &p.Bytes, &p.RecentDays, &p.RecentOpens); err != nil {
			rows.Close()
			return nil, err
		}
		p.FirstSeen, p.LastUsed = ts(first), ts(last)
		r.Packages = append(r.Packages, p)
	}
	rows.Close()

	rows, err = s.QueryContext(ctx, `SELECT o.ref, o.size, o.opens, o.bytes, o.days_used, o.first_seen, o.last_used,
			(SELECT count(*) FROM usage_path p WHERE p.ref = o.ref),
			coalesce((SELECT p.subsystem || ':' || p.pkg || '/' || p.path FROM usage_path p WHERE p.ref = o.ref ORDER BY p.opens DESC LIMIT 1), ''),
			coalesce((SELECT sum(opens) FROM usage_obj_day d WHERE d.ref = o.ref AND d.day >= ?), 0)
		FROM usage_obj o
		WHERE ? = '' OR EXISTS (SELECT 1 FROM usage_path p WHERE p.ref = o.ref AND p.subsystem = ?)
		ORDER BY o.opens DESC, o.ref LIMIT ?`, since, q.Subsystem, q.Subsystem, q.Limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var o ObjectStat
		var first, last int64
		if err := rows.Scan(&o.Ref, &o.Size, &o.Opens, &o.Bytes, &o.DaysUsed, &first, &last, &o.Paths, &o.ExamplePath, &o.RecentOpens); err != nil {
			rows.Close()
			return nil, err
		}
		o.FirstSeen, o.LastUsed = ts(first), ts(last)
		r.Objects = append(r.Objects, o)
	}
	rows.Close()

	rows, err = s.QueryContext(ctx, `SELECT subsystem, pkg, path, size, opens, bytes, days_used, last_used FROM usage_path
		WHERE ? = '' OR subsystem = ? ORDER BY opens DESC, subsystem, pkg, path LIMIT ?`, q.Subsystem, q.Subsystem, q.Limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p PathStat
		var last int64
		if err := rows.Scan(&p.Subsystem, &p.Package, &p.Path, &p.Size, &p.Opens, &p.Bytes, &p.DaysUsed, &last); err != nil {
			rows.Close()
			return nil, err
		}
		p.LastUsed = ts(last)
		r.Paths = append(r.Paths, p)
	}
	rows.Close()

	if q.Untouched {
		if r.Untouched, err = untouched(ctx, db, q); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// untouched walks every unit whose files are cached (spooled or resident)
// and lists the files no open was ever recorded for.
func untouched(ctx context.Context, db *meta.DB, q Query) ([]UnitUntouched, error) {
	s := db.SQL()
	units, err := db.Units(ctx, q.Subsystem)
	if err != nil {
		return nil, err
	}
	opened, err := s.PrepareContext(ctx, `SELECT 1 FROM usage_path WHERE subsystem = ? AND pkg = ? AND path = ?`)
	if err != nil {
		return nil, err
	}
	defer opened.Close()
	var out []UnitUntouched
	for _, u := range units {
		if u.Root == 0 || (u.State != meta.UnitResident && u.State != meta.UnitSpooled) {
			continue
		}
		uu := UnitUntouched{Subsystem: u.Subsystem, Key: u.Key, Package: u.Key, State: u.State}
		var name, version string
		err := s.QueryRowContext(ctx, `SELECT name, version FROM usage_unit WHERE unit = ?`, u.ID).Scan(&name, &version)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if name != "" {
			uu.Package = name
		}
		type file struct {
			path string
			in   meta.Inode
		}
		var files []file
		openedIno := map[uint64]bool{}
		err = walk(ctx, db, u.Root, "", func(p string, in meta.Inode) error {
			if in.Unit != u.ID {
				return nil
			}
			files = append(files, file{p, in})
			var one int
			switch err := opened.QueryRowContext(ctx, u.Subsystem, uu.Package, p).Scan(&one); err {
			case nil:
				openedIno[in.Ino] = true
			case sql.ErrNoRows:
			default:
				return err
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		var list []FileSize
		for _, f := range files {
			uu.Files++
			uu.Bytes += f.in.Size
			if !openedIno[f.in.Ino] { // any name of a hard-linked inode counts
				uu.Untouched++
				uu.UntouchedBytes += f.in.Size
				list = append(list, FileSize{f.path, f.in.Size})
			}
		}
		if uu.Files == 0 {
			continue
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Size != list[j].Size {
				return list[i].Size > list[j].Size
			}
			return list[i].Path < list[j].Path
		})
		if len(list) > q.Limit {
			list = list[:q.Limit]
		}
		uu.List = list
		if name != "" && version != "" {
			uu.Package = name + "-" + version
		}
		out = append(out, uu)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].UntouchedBytes > out[j].UntouchedBytes })
	return out, nil
}

// walk calls fn for every regular file under dir, with its path relative
// to the walk's root. Hard links are visited once per name.
func walk(ctx context.Context, db *meta.DB, dir uint64, prefix string, fn func(string, meta.Inode) error) error {
	after := ""
	for {
		des, err := db.ReadDir(ctx, dir, after, 512)
		if err != nil {
			return err
		}
		for _, de := range des {
			p := path.Join(prefix, de.Name)
			switch de.Kind {
			case meta.KindDir:
				if err := walk(ctx, db, de.Ino, p, fn); err != nil {
					return err
				}
			case meta.KindFile:
				if err := fn(p, de.Inode); err != nil {
					return err
				}
			}
			after = de.Name
		}
		if len(des) < 512 {
			return nil
		}
	}
}

func size(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func date(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02")
}

func shortRef(r string) string {
	if i := strings.IndexByte(r, '-'); i >= 0 && len(r) > i+13 {
		return r[:i+13]
	}
	return r
}

// WriteText writes the report as plain text tables.
func (r *Report) WriteText(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "usage report, %s UTC; recent = last %d days\n", r.Generated.UTC().Format("2006-01-02 15:04"), r.Query.Days)
	fmt.Fprintln(tw, "open counts undercount when a guest's page cache serves repeat opens; days used is the robust signal")

	fmt.Fprintf(tw, "\nPACKAGES (top %d by days used)\n", r.Query.Limit)
	fmt.Fprintln(tw, "SUBSYSTEM\tPACKAGE\tVERSION\tDAYS\tRECENT DAYS\tOPENS\tRECENT OPENS\tFETCHES\tREFETCHES\tREAD\tFIRST SEEN\tLAST USED")
	for _, p := range r.Packages {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n", p.Subsystem, p.Name, p.Version, p.DaysUsed, p.RecentDays,
			p.Opens, p.RecentOpens, p.Fetches, p.Refetches, size(p.Bytes), date(p.FirstSeen), date(p.LastUsed))
	}
	if len(r.Packages) == 0 {
		fmt.Fprintln(tw, "(none yet)")
	}

	fmt.Fprintf(tw, "\nOBJECTS (top %d by opens; one row per distinct file content)\n", r.Query.Limit)
	fmt.Fprintln(tw, "REF\tSIZE\tOPENS\tRECENT OPENS\tDAYS\tPATHS\tEXAMPLE\tLAST USED")
	for _, o := range r.Objects {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s\n", shortRef(o.Ref), size(o.Size), o.Opens, o.RecentOpens, o.DaysUsed, o.Paths, o.ExamplePath, date(o.LastUsed))
	}
	if len(r.Objects) == 0 {
		fmt.Fprintln(tw, "(none yet)")
	}

	fmt.Fprintf(tw, "\nPATHS (top %d by opens)\n", r.Query.Limit)
	fmt.Fprintln(tw, "SUBSYSTEM\tPACKAGE\tPATH\tSIZE\tOPENS\tDAYS\tREAD\tLAST USED")
	for _, p := range r.Paths {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n", p.Subsystem, p.Package, p.Path, size(p.Size), p.Opens, p.DaysUsed, size(p.Bytes), date(p.LastUsed))
	}
	if len(r.Paths) == 0 {
		fmt.Fprintln(tw, "(none yet)")
	}

	if r.Query.Untouched {
		var files, untouched, bytes, ubytes int64
		for _, u := range r.Untouched {
			files += u.Files
			untouched += u.Untouched
			bytes += u.Bytes
			ubytes += u.UntouchedBytes
		}
		fmt.Fprintf(tw, "\nNEVER OPENED (files in cached units with no recorded open)\n")
		if files > 0 {
			fmt.Fprintf(tw, "%d of %d files, %s of %s (%.0f%% of bytes)\n", untouched, files, size(ubytes), size(bytes), 100*float64(ubytes)/float64(max(bytes, 1)))
		}
		fmt.Fprintln(tw, "SUBSYSTEM\tPACKAGE\tSTATE\tFILES\tNEVER OPENED\tBYTES\tNEVER READ\tSHARE")
		for _, u := range r.Untouched {
			fmt.Fprintf(tw, "%s\t%s\t%v\t%d\t%d\t%s\t%s\t%.0f%%\n", u.Subsystem, u.Package, u.State, u.Files, u.Untouched,
				size(u.Bytes), size(u.UntouchedBytes), 100*float64(u.UntouchedBytes)/float64(max(u.Bytes, 1)))
		}
		for _, u := range r.Untouched {
			if len(u.List) == 0 {
				continue
			}
			fmt.Fprintf(tw, "\n%s %s (%s):\n", u.Subsystem, u.Package, u.Key)
			for _, f := range u.List {
				fmt.Fprintf(tw, "  %s\t%s\n", f.Path, size(f.Size))
			}
		}
	}
	return tw.Flush()
}
