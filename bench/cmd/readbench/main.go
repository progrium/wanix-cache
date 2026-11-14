// Command readbench runs the phase 2 read benchmark: plain-disk reads
// against CAS reads, direct and through both 9P servers, cold and warm.
//
//	go run ./bench/cmd/readbench -work /var/tmp/rb \
//	    -tree alpine-mini=./alpine-mini -tree nix=./nix-store \
//	    -json results.json > results.md
//
// Each fixture is imported into its own cache under -work (reused on later
// runs). Cold runs drop the kernel caches first and need root. Times are
// the median of -reps runs; readers are interleaved within each rep.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"time"

	"github.com/progrium/wanix-cache/bench"
)

type treeFlag []string

func (t *treeFlag) String() string     { return strings.Join(*t, ",") }
func (t *treeFlag) Set(v string) error { *t = append(*t, v); return nil }

// Record is one measured cell.
type Record struct {
	Fixture  string  `json:"fixture"`
	Workload string  `json:"workload"` // readall, head4k, random4k
	Class    string  `json:"class,omitempty"`
	Reader   string  `json:"reader"`
	Cache    string  `json:"cache"` // cold, warm
	Conc     int     `json:"conc"`
	Files    int     `json:"files"`
	Bytes    int64   `json:"bytes"`
	Seconds  float64 `json:"seconds"` // median
	MiBps    float64 `json:"mib_per_s,omitempty"`
	PerFile  float64 `json:"us_per_file,omitempty"`
	P50us    float64 `json:"p50_us,omitempty"`
	P99us    float64 `json:"p99_us,omitempty"`
	MeanUs   float64 `json:"mean_us,omitempty"`
	Ops      int     `json:"ops,omitempty"`
}

type usageRec struct {
	Fixture string `json:"fixture"`
	bench.Usage
	ImportSeconds float64 `json:"import_seconds,omitempty"`
}

var (
	reps    = flag.Int("reps", 3, "repetitions per cell (median reported)")
	conc    = flag.Int("conc", 8, "workers for the concurrent read-all runs")
	ops     = flag.Int("random-ops", 2000, "random 4 KiB reads per warm run (cold runs use a quarter)")
	readers = flag.String("readers", strings.Join(bench.ReaderNames, ","), "readers to run")
	noCold  = flag.Bool("no-cold", false, "skip cold runs (no root needed)")
	verbose = flag.Bool("v", false, "log every run to stderr")
	cpuprof = flag.String("cpuprofile", "", "write a CPU profile (client and servers share the process)")
)

func main() {
	log.SetFlags(0)
	work := flag.String("work", "./readbench-work", "work directory (caches and synthetic files)")
	synth := flag.Bool("synthetic", true, "run the 1 KiB to 100 MiB size sweep")
	jsonOut := flag.String("json", "", "also write records as JSON here")
	var trees treeFlag
	flag.Var(&trees, "tree", "name=dir: a real tree to benchmark (repeatable)")
	flag.Parse()

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	ctx := context.Background()
	var recs []Record
	var usage []usageRec
	names := strings.Split(*readers, ",")

	type fixture struct{ name, src string }
	var fxs []fixture
	if *synth {
		dir := filepath.Join(*work, "synthetic-src")
		log.Printf("generating synthetic files in %s", dir)
		if err := bench.GenSynthetic(dir, bench.SizeClasses(), 1); err != nil {
			log.Fatal(err)
		}
		fxs = append(fxs, fixture{"synthetic", dir})
	}
	for _, t := range trees {
		name, dir, ok := strings.Cut(t, "=")
		if !ok {
			log.Fatalf("-tree %q: want name=dir", t)
		}
		fxs = append(fxs, fixture{name, dir})
	}

	for _, f := range fxs {
		log.Printf("fixture %s: loading %s", f.name, f.src)
		fx, err := bench.Load(ctx, f.name, f.src, filepath.Join(*work, "cache-"+f.name))
		if err != nil {
			log.Fatal(err)
		}
		u, err := fx.DiskUsage()
		if err != nil {
			log.Fatal(err)
		}
		usage = append(usage, usageRec{f.name, u, fx.ImportTime.Seconds()})
		srv, err := bench.StartServers(fx)
		if err != nil {
			log.Fatal(err)
		}
		curSrv = srv
		var rs []bench.Reader
		for _, n := range names {
			r, err := bench.NewReader(n, fx, srv)
			if err != nil {
				log.Fatalf("reader %s: %v", n, err)
			}
			rs = append(rs, r)
		}
		if f.name == "synthetic" {
			recs = append(recs, sweep(fx, rs)...)
		} else {
			recs = append(recs, treeRuns(fx, rs)...)
		}
		for _, r := range rs {
			r.Close()
		}
		srv.Close()
		fx.Close()
	}

	if *jsonOut != "" {
		b, _ := json.MarshalIndent(map[string]any{"records": recs, "usage": usage, "env": env()}, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			log.Fatal(err)
		}
	}
	report(os.Stdout, recs, usage)
}

func env() map[string]any {
	return map[string]any{"go": runtime.Version(), "cpus": runtime.NumCPU(),
		"msize": bench.Msize, "buf": bench.BufSize, "reps": *reps, "time": time.Now().UTC().Format(time.RFC3339)}
}

func caches() []string {
	if *noCold {
		return []string{"warm"}
	}
	return []string{"cold", "warm"}
}

var curSrv *bench.Servers

func prepare(cache string, r bench.Reader, files []bench.File) {
	if cache == "cold" {
		if curSrv != nil && curSrv.Cached != nil {
			curSrv.Cached.Reset()
		}
		if err := bench.DropCaches(); err != nil {
			log.Fatal(err)
		}
		return
	}
	// warm: one untimed pass first
	if _, err := bench.ReadAll(r, files, 4); err != nil {
		log.Fatal(err)
	}
}

// timed runs fn reps times per reader, interleaving readers, and returns
// the median result for each reader.
func timed(rs []bench.Reader, cache string, files []bench.File, fn func(bench.Reader) (bench.Result, error)) []bench.Result {
	all := make([][]bench.Result, len(rs))
	for rep := 0; rep < *reps; rep++ {
		for i, r := range rs {
			prepare(cache, r, files)
			res, err := fn(r)
			if err != nil {
				log.Fatal(err)
			}
			if *verbose {
				log.Printf("  %s %s rep %d: %v", r.Name(), cache, rep, res.Elapsed)
			}
			all[i] = append(all[i], res)
		}
	}
	out := make([]bench.Result, len(rs))
	for i, a := range all {
		slices.SortFunc(a, func(x, y bench.Result) int { return int(x.Elapsed - y.Elapsed) })
		out[i] = a[len(a)/2]
	}
	return out
}

func rec(fx, workload, class, reader, cache string, c int, r bench.Result) Record {
	s := r.Elapsed.Seconds()
	out := Record{Fixture: fx, Workload: workload, Class: class, Reader: reader, Cache: cache, Conc: c,
		Files: r.Files, Bytes: r.Bytes, Seconds: s}
	if s > 0 {
		out.MiBps = float64(r.Bytes) / (1 << 20) / s
	}
	if r.Files > 0 {
		out.PerFile = s * 1e6 / float64(r.Files)
	}
	return out
}

func sweep(fx *bench.Fixture, rs []bench.Reader) []Record {
	var out []Record
	for _, c := range bench.SizeClasses() {
		var files []bench.File
		for _, f := range fx.Files {
			if strings.HasPrefix(f.Rel, c.Label+"/") {
				files = append(files, f)
			}
		}
		log.Printf("synthetic %s: %d files", c.Label, len(files))
		for _, cache := range caches() {
			res := timed(rs, cache, files, func(r bench.Reader) (bench.Result, error) { return bench.ReadAll(r, files, 1) })
			for i, r := range rs {
				out = append(out, rec("synthetic", "readall", c.Label, r.Name(), cache, 1, res[i]))
			}
		}
		if c.Size < 1<<20 {
			continue
		}
		for _, cache := range caches() {
			n := *ops
			if cache == "cold" {
				n /= 4
			}
			for i, r := range rs {
				var lat bench.Latencies
				for rep := 0; rep < *reps; rep++ {
					prepare(cache, r, files)
					l, err := bench.Random(r, files, n, 4096, uint64(rep+1))
					if err != nil {
						log.Fatal(err)
					}
					lat = append(lat, l...)
				}
				_ = i
				out = append(out, Record{Fixture: "synthetic", Workload: "random4k", Class: c.Label, Reader: r.Name(),
					Cache: cache, Conc: 1, Files: len(files), Ops: len(lat),
					P50us: us(lat.Quantile(0.5)), P99us: us(lat.Quantile(0.99)), MeanUs: us(lat.Mean())})
			}
		}
	}
	return out
}

func us(d time.Duration) float64 { return float64(d) / 1e3 }

func treeRuns(fx *bench.Fixture, rs []bench.Reader) []Record {
	var out []Record
	log.Printf("tree %s: %d files", fx.Name, len(fx.Files))
	for _, cache := range caches() {
		for _, c := range []int{1, *conc} {
			res := timed(rs, cache, fx.Files, func(r bench.Reader) (bench.Result, error) { return bench.ReadAll(r, fx.Files, c) })
			for i, r := range rs {
				out = append(out, rec(fx.Name, "readall", "", r.Name(), cache, c, res[i]))
			}
		}
		res := timed(rs, cache, fx.Files, func(r bench.Reader) (bench.Result, error) { return bench.Head(r, fx.Files, 4096, 1) })
		for i, r := range rs {
			out = append(out, rec(fx.Name, "head4k", "", r.Name(), cache, 1, res[i]))
		}
	}
	return out
}

// report prints markdown tables: one row per case, disk and CAS side by
// side for each access path, with the CAS/disk ratio.
func report(w io.Writer, recs []Record, usage []usageRec) {
	fmt.Fprintf(w, "# readbench results\n\n%s, %d CPUs, msize %d KiB, read size %d KiB, median of %d\n\n",
		runtime.Version(), runtime.NumCPU(), bench.Msize>>10, bench.BufSize>>10, *reps)

	fmt.Fprint(w, "## Disk footprint\n\n")
	fmt.Fprintln(w, "| fixture | files | logical | plain disk | CAS | CAS/plain | blob files | import |")
	fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---:|")
	for _, u := range usage {
		imp := "reused"
		if u.ImportSeconds > 0 {
			imp = fmt.Sprintf("%.1f s", u.ImportSeconds)
		}
		fmt.Fprintf(w, "| %s | %d | %s | %s | %s | %.2fx | %d | %s |\n", u.Fixture, u.Files, mib(u.LogicalBytes),
			mib(u.PlainBytes), mib(u.CASBytes), float64(u.CASBytes)/float64(u.PlainBytes), u.CASFiles, imp)
	}
	fmt.Fprintln(w)

	pairs := [][2]string{{bench.DirectDisk, bench.DirectCAS}, {bench.DotLDisk, bench.DotLCAS}, {bench.NineDisk, bench.NineCAS},
		{bench.DirectDisk, bench.DirectCached}, {bench.DotLDisk, bench.DotLCached}}
	find := func(match func(Record) bool, reader string) *Record {
		for i := range recs {
			if recs[i].Reader == reader && match(recs[i]) {
				return &recs[i]
			}
		}
		return nil
	}
	header := func(title, unit string) {
		fmt.Fprintf(w, "## %s\n\n%s\n\n| case | direct disk | direct CAS | x | 9P2000.L disk | 9P2000.L CAS | x | 9P2000 disk | 9P2000 CAS | x | direct disk | direct CAS+cache | x | 9P2000.L disk | 9P2000.L CAS+cache | x |\n", title, unit)
		fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	}
	row := func(label string, match func(Record) bool, val func(Record) float64, f string) {
		if !slices.ContainsFunc(recs, match) {
			return
		}
		fmt.Fprintf(w, "| %s |", label)
		for _, p := range pairs {
			d, c := find(match, p[0]), find(match, p[1])
			if d == nil || c == nil {
				fmt.Fprint(w, " | | |")
				continue
			}
			fmt.Fprintf(w, " "+f+" | "+f+" | %.2f |", val(*d), val(*c), val(*c)/val(*d))
		}
		fmt.Fprintln(w)
	}
	perFile := func(r Record) float64 { return r.PerFile }
	ms := func(r Record) float64 { return r.Seconds * 1e3 }

	var classes []string
	for _, r := range recs {
		if r.Fixture == "synthetic" && !slices.Contains(classes, r.Class) {
			classes = append(classes, r.Class)
		}
	}
	if len(classes) > 0 {
		header("Size sweep: open, read whole file, close (one at a time)", "µs per file. x = CAS ÷ disk (above 1 means the CAS is slower).")
		for _, cache := range caches() {
			for _, cl := range classes {
				row(cache+" "+cl, func(r Record) bool {
					return r.Fixture == "synthetic" && r.Workload == "readall" && r.Class == cl && r.Cache == cache
				}, perFile, "%.0f")
			}
		}
		fmt.Fprintln(w)
		header("Size sweep: random 4 KiB reads on open files", "p50 µs per read (p99 in the JSON).")
		for _, cache := range caches() {
			for _, cl := range classes {
				row(cache+" "+cl, func(r Record) bool {
					return r.Fixture == "synthetic" && r.Workload == "random4k" && r.Class == cl && r.Cache == cache
				}, func(r Record) float64 { return r.P50us }, "%.1f")
			}
		}
		fmt.Fprintln(w)
		header("Size sweep: random 4 KiB reads, p99", "p99 µs per read.")
		for _, cache := range caches() {
			for _, cl := range classes {
				row(cache+" "+cl, func(r Record) bool {
					return r.Fixture == "synthetic" && r.Workload == "random4k" && r.Class == cl && r.Cache == cache
				}, func(r Record) float64 { return r.P99us }, "%.0f")
			}
		}
		fmt.Fprintln(w)
	}
	var trees []string
	for _, r := range recs {
		if r.Fixture != "synthetic" && !slices.Contains(trees, r.Fixture) {
			trees = append(trees, r.Fixture)
		}
	}
	if len(trees) > 0 {
		header("Real trees: read every file", "Milliseconds for the whole tree. x = CAS ÷ disk.")
		for _, t := range trees {
			for _, cache := range caches() {
				for _, c := range []int{1, *conc} {
					row(fmt.Sprintf("%s %s ×%d", t, cache, c), func(r Record) bool {
						return r.Fixture == t && r.Workload == "readall" && r.Cache == cache && r.Conc == c
					}, ms, "%.0f")
				}
			}
		}
		fmt.Fprintln(w)
		header("Real trees: open every file and read its first 4 KiB", "Milliseconds for the whole tree.")
		for _, t := range trees {
			for _, cache := range caches() {
				row(t+" "+cache, func(r Record) bool {
					return r.Fixture == t && r.Workload == "head4k" && r.Cache == cache
				}, ms, "%.0f")
			}
		}
		fmt.Fprintln(w)
	}
}

func mib(b int64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }
