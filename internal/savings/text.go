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

	p("wanix-cache savings  %s  (%s, %d B blocks)\n\n", r.At.Format("2006-01-02 15:04:05Z"), r.Filesystem.Type, r.Filesystem.BlockSize)
	p("resident units\t%d\n", t.Units)
	p("files\t%d\t%s logical\n", t.Files, Size(t.Logical))
	p("distinct contents\t%d\t%s\twhole-file dedupe %s\n", t.Contents, Size(t.ContentBytes), Pct(t.Ratios.WholeFileDedupe))
	p("unique data chunks\t%d\t%s\tchunk dedupe %s (on top of whole-file)\n", t.DataBlobs, Size(t.DataBytes), Pct(t.Ratios.ChunkDedupe))
	p("schema blobs\t%d\t%s\t%s of CAS total\n", t.SchemaBlobs, Size(t.SchemaBytes), Pct(t.Ratios.SchemaShare))
	p("blob files\t%d\t%s logical, %s allocated\tper-blob overhead %s (%s of CAS total)\n",
		t.Blobs, Size(t.BlobBytes), Size(t.BlobAllocated), Size(t.BlobOverhead), Pct(t.Ratios.BlobOverheadShare))
	p("store directories\t%d\t%s allocated\t%s of CAS total\n", t.StoreDirs, Size(t.StoreDirAllocated), Pct(t.Ratios.StoreDirShare))
	if t.UnreferencedBlobs > 0 {
		p("unreferenced blobs\t%d\t%s\tawaiting a sweep\n", t.UnreferencedBlobs, Size(t.UnreferencedBytes))
	}
	p("metadata DB\t\t%s\n", Size(t.DB))
	p("CAS total\t\t%s\tstore %s + DB\n", Size(t.CASTotal), Size(t.StoreAllocated))
	p("\n")
	if t.B2 != nil {
		p("B2 plain unpacked\t\t%s\t%d files in %d unit trees  savings %s\n", Size(t.B2.Allocated), t.B2.Files, t.B2.Units, Pct(t.Ratios.SavingsB2))
		if t.B2.Units != t.Units {
			p("\t\t\tshadow covers %d of %d resident units\n", t.B2.Units, t.Units)
		}
	} else {
		p("B2 plain unpacked\t\t-\tserve with -debug-shadow to measure\n")
	}
	p("B3 hard-linked (est.)\t\t%s\tsavings %s\n", Size(t.B3Estimate), Pct(t.Ratios.SavingsB3Estimate))
	p("logical bytes\t\t%s\tsavings %s (ignores plain-disk block overhead)\n", Size(t.Logical), Pct(t.Ratios.SavingsLogical))
	if r.Broken > 0 {
		p("\nWARNING: %d content objects have blobs missing from the store (run wanix-cache audit)\n", r.Broken)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintln(w, "\nper subsystem (attributed splits each shared blob evenly; the DB is not split)")
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "SUBSYSTEM\tUNITS\tFILES\tLOGICAL\tWHOLE-FILE\tCHUNK\tEXCLUSIVE\tSHARED\tATTRIBUTED\tB2\tSAVINGS vs B2\t")
	for _, s := range r.Subsystems {
		b2, sv := "-", "-"
		if s.B2 != nil {
			b2, sv = Size(s.B2.Allocated), Pct(s.SavingsB2)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", s.Name, s.Units, s.Files, Size(s.Logical),
			Pct(s.WholeFileDedupe), Pct(s.ChunkDedupe), Size(s.ExclusiveAllocated), Size(s.SharedAllocated),
			Size(s.Attributed), b2, sv)
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

	fmt.Fprintln(w, "\nby content size (slack = blob files' allocated minus logical bytes)")
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "SIZE\tCONTENTS\tBYTES\tBLOBS\tSCHEMA\tALLOCATED\tSLACK\tOF SLACK\t")
	for _, c := range r.SizeClasses {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t\n", c.Label, c.Contents, Size(c.Bytes), c.Blobs,
			Size(c.SchemaBytes), Size(c.Allocated), Size(c.Slack), Pct(c.SlackShare))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(r.TopExclusiveUnits) > 0 {
		fmt.Fprintln(w, "\nlargest exclusive units (blob bytes no other unit reaches)")
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
