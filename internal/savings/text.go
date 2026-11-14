package savings

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

// WriteText renders the report for people: /.cache/stats and
// `wanix-cache stats`.
func (r *Report) WriteText(w io.Writer) error {
	t := r.Total
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	p := func(f string, a ...any) { fmt.Fprintf(tw, f, a...) }

	p("wanix-cache stats  %s  (%s, %d B blocks)\n\n", r.At.Format("2006-01-02 15:04:05Z"), r.Filesystem.Type, r.Filesystem.BlockSize)
	p("resident units\t%d\n", t.Units)
	p("files\t%d\t%s logical\n", t.Files, Size(t.Logical))
	p("distinct contents\t%d\t%s\twhole-file dedupe %s\n", t.Contents, Size(t.ContentBytes), Pct(t.Ratios.WholeFileDedupe))
	if r.Compression > 0 {
		p("stored files\t%d\t%s stored, %s allocated\tzstd level %d saves %s\n", t.StoreFiles, Size(t.StoreBytes), Size(t.StoreAllocated-t.StoreDirBytes), r.Compression, Pct(t.Ratios.Compression))
	} else {
		p("stored files\t%d\t%s, %s allocated\t%d names (hard links)\n", t.StoreFiles, Size(t.StoreBytes), Size(t.StoreAllocated-t.StoreDirBytes), t.StoreLinks)
	}
	p("store directories\t%d\t%s allocated\t%s of total\n", t.StoreDirs, Size(t.StoreDirBytes), Pct(t.Ratios.DirShare))
	if t.Copies > 0 {
		p("unlinked copies\t%d\t%s\tidentical files stored twice (audit -fix links them)\n", t.Copies, Size(t.CopyBytes))
	}
	p("metadata DB\t\t%s\n", Size(t.DB))
	p("total\t\t%s\tstore %s + DB\n", Size(t.CacheTotal), Size(t.StoreAllocated))
	p("\n")
	p("without dedupe (est.)\t\t%s\tdedupe saves %s\n", Size(t.NoDedupe), Pct(t.Ratios.SavingsNoDedupe))
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintln(w, "\nper subsystem (attributed splits each shared file evenly; the DB is not split)")
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "SUBSYSTEM\tUNITS\tFILES\tLOGICAL\tWHOLE-FILE\tEXCLUSIVE\tSHARED\tATTRIBUTED\t")
	for _, s := range r.Subsystems {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t\n", s.Name, s.Units, s.Files, Size(s.Logical),
			Pct(s.WholeFileDedupe), Size(s.ExclusiveAllocated), Size(s.SharedAllocated), Size(s.Attributed))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, s := range r.Subsystems {
		if len(s.SharedWith) == 0 {
			continue
		}
		names := make([]string, 0, len(s.SharedWith))
		for n := range s.SharedWith {
			names = append(names, n)
		}
		sort.Strings(names)
		parts := make([]string, len(names))
		for i, n := range names {
			parts[i] = fmt.Sprintf("%s %s", n, Size(s.SharedWith[n]))
		}
		fmt.Fprintf(w, "  %s shares with: %s\n", s.Name, strings.Join(parts, ", "))
	}

	fmt.Fprintln(w, "\nby content size (slack = stored files' allocated minus stored bytes)")
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "SIZE\tCONTENTS\tBYTES\tALLOCATED\tSLACK\tOF SLACK\t")
	for _, c := range r.SizeClasses {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t\n", c.Label, c.Contents, Size(c.Bytes),
			Size(c.Allocated), Size(c.Slack), Pct(c.SlackShare))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(r.TopExclusiveUnits) > 0 {
		fmt.Fprintln(w, "\nlargest exclusive units (stored bytes no other unit has)")
		tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		for _, u := range r.TopExclusiveUnits {
			fmt.Fprintf(tw, "  %s\t%s\t%s exclusive\t%s logical\n", u.Subsystem, u.Key, Size(u.Exclusive), Size(u.Logical))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if len(r.TopShared) > 0 {
		fmt.Fprintln(w, "\nmost-shared contents (by bytes the sharing saves)")
		tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		for _, c := range r.TopShared {
			fmt.Fprintf(tw, "  %s\t× %d files\t%d units\t%s\t%s\n", Size(c.Size), c.Files, c.Units, strings.Join(c.Subsystems, ","), c.Path)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Size formats bytes.
func Size(n int64) string {
	neg := ""
	if n < 0 {
		neg, n = "-", -n
	}
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%s%.2f GiB", neg, float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%s%.1f MiB", neg, float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%s%.1f KiB", neg, float64(n)/(1<<10))
	}
	return fmt.Sprintf("%s%d B", neg, n)
}

// Pct formats a fraction as a percentage.
func Pct(f float64) string { return fmt.Sprintf("%.1f%%", 100*f) }
