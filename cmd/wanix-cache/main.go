// Command wanix-cache is the H9 read-through cache.
//
//	wanix-cache serve  [-data DIR] [-9p ADDR] [-9p2000 ADDR] [-max-gib N] [-zstd L]
//	wanix-cache import [-data DIR] [-max-gib N] -key NAME SRC
//	wanix-cache index  [-data DIR] [-all] [ROOT...]
//	wanix-cache stats  [-data DIR] [-json] [-n N]
//	wanix-cache units  [-data DIR] [-subsystem NAME]
//	wanix-cache usage  [-data DIR] [-subsystem NAME] [-untouched] [-json]
//	wanix-cache gc     [-data DIR] [-max-gib N] [-high F] [-low F]
//	wanix-cache evict  [-data DIR] [-subsystem NAME] -key KEY
//	wanix-cache refault [-data DIR] -key KEY SRC
//	wanix-cache audit  [-data DIR] [-fix] [-deep]
//	wanix-cache pin    [-data DIR] [-subsystem NAME] [-reason R] [-unpin] [-key KEY]
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/gc"
	"github.com/progrium/wanix-cache/internal/h9index"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/savings"
	"github.com/progrium/wanix-cache/internal/store"
	"github.com/progrium/wanix-cache/internal/subsys"
	"github.com/progrium/wanix-cache/internal/tree"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: wanix-cache <command> [flags]

commands:
  serve    serve the cache read-only over 9P2000.L (and optionally plain 9P2000)
  import   import a local directory as a unit under /local/<key>
  index    generate the /.h9index of roots (such as /local/alpine)
  stats    what the cache stores and what dedupe saves, per subsystem
  units    list units
  usage    package, object and path usage; -untouched lists never-opened files
  gc       evict down to the low watermark when over the high one
  evict    evict one unit now (its manifest stays; a read refaults it)
  refault  bring an evicted local unit back from its source directory
  audit    check refcounts and stored files against the metadata (-fix repairs)
  pin      pin a unit against eviction (-unpin removes; no -key lists pins)`)
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fl := flag.NewFlagSet(cmd, flag.ExitOnError)
	data := fl.String("data", "./cache", "cache directory")
	var err error
	switch cmd {
	case "serve":
		addrL := fl.String("9p", "127.0.0.1:5640", "9P2000.L TCP listen address (Linux guests); empty to disable")
		addr := fl.String("9p2000", "", "plain 9P2000 TCP listen address (no symlinks); empty to disable")
		gib := fl.Int64("max-gib", 10, "cap on stored + spool bytes, GiB; 0 = unlimited")
		workers := fl.Int("ingest-workers", 4, "spool files hashed and stored concurrently")
		zst := fl.Int("zstd", 0, "optional, off by default (0): compress every stored file with zstd at this level (1 fastest to 4 best), trading read speed for space; only on a new, empty cache, and stays on")
		addSubsystemFlags(fl)
		fl.Parse(args)
		if *zst > 0 {
			if err := store.EnableCompression(*data, *zst); err != nil {
				log.Fatal(err)
			}
		}
		err = serve(*data, *addrL, *addr, *gib<<30, *workers)
	case "import":
		key := fl.String("key", "", "unit key (defaults to the source directory's name)")
		gib := fl.Int64("max-gib", 0, "cap on stored bytes, GiB: least recently used units are evicted first to fit; 0 = unlimited")
		fl.Parse(args)
		if fl.NArg() != 1 {
			log.Fatal("import: need exactly one source directory")
		}
		err = importDir(*data, *key, fl.Arg(0), *gib<<30)
	case "index":
		all := fl.Bool("all", false, "regenerate every root that already has an index")
		fl.Parse(args)
		if !*all && fl.NArg() == 0 {
			log.Fatal("index: need a root path (such as /local/alpine) or -all")
		}
		err = indexRoots(*data, *all, fl.Args())
	case "stats":
		asJSON := fl.Bool("json", false, "print JSON")
		top := fl.Int("n", 20, "rows in the largest-exclusive and most-shared lists")
		fl.Parse(args)
		err = stats(*data, *asJSON, *top)
	case "units":
		sub := fl.String("subsystem", "", "only this subsystem")
		fl.Parse(args)
		err = units(*data, *sub)
	case "usage":
		err = usageCommand(fl, data, args)
	case "gc", "evict", "refault", "audit", "pin":
		err = gcCommand(cmd, fl, data, args)
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func open(dir string) (*meta.DB, *store.Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return nil, nil, err
	}
	st, err := store.Open(dir)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	// Directories left by earlier versions: the CAS blob store, its debug
	// shadow tree. A cache made with the CAS keeps its manifests and
	// refaults its units into the file store as they are used.
	for _, old := range []string{"blobs", "shadow"} {
		if _, err := os.Stat(filepath.Join(dir, old)); err == nil {
			log.Printf("note: %s is left from the old CAS store and is no longer used; remove it to free the space", filepath.Join(dir, old))
		}
	}
	return db, st, nil
}

func serve(dir, addrL, addr string, max int64, workers int) error {
	if addrL == "" && addr == "" {
		return fmt.Errorf("serve: no listen address")
	}
	db, st, err := open(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	defer st.Close()
	spool, err := fill.OpenDiskSpool(filepath.Join(dir, "spool"))
	if err != nil {
		return err
	}
	quota := &fill.LocalQuota{Max: max, Store: st.Used, Spool: spool.Bytes}
	mgr, err := fill.NewManager(db, st, spool, quota, fill.Options{Workers: workers})
	if err != nil {
		return err
	}
	defer mgr.Close()
	idx, err := h9index.Open(context.Background(), db)
	if err != nil {
		return err
	}
	fs := &tree.FS{Meta: db, Store: st, Files: mgr, Faults: mgr, Index: idx,
		Synthetic: map[string]tree.SyntheticFunc{
			"usage": func(ctx context.Context) ([]byte, error) { return usageReport(ctx, db) },
			"stats": func(ctx context.Context) ([]byte, error) { return statsReport(ctx, dir, db, st) },
		}}
	if err := fs.InitSynthetic(context.Background()); err != nil {
		return err
	}
	rootIndexer = fs.Indexer(idx, "")
	if err := registerSubsystems(context.Background(), mgr, dir); err != nil {
		return err
	}
	for name, v := range viewers {
		if err := fs.AddViewer(context.Background(), name, v); err != nil {
			return err
		}
	}
	if err := startGC(db, st, mgr, quota, max); err != nil {
		return err
	}
	if err := startUsage(context.Background(), db, mgr); err != nil {
		return err
	}
	errc := make(chan error, 2)
	if addrL != "" {
		ln, err := net.Listen("tcp", addrL)
		if err != nil {
			return err
		}
		log.Printf("serving %s read-only over 9P2000.L on %s", dir, ln.Addr())
		go func() { errc <- tree.NewDotLServer(fs).Serve(ln) }()
	}
	if addr != "" {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		log.Printf("serving %s read-only over 9P2000 on %s", dir, ln.Addr())
		srv := &ninep.Server{FS: fs, Logf: log.Printf}
		go func() { errc <- srv.Serve(ln) }()
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		mgr.Close() // spooled units are ingested on the next start
		flushUsage()
		db.Close()
		os.Exit(0)
	}()
	return <-errc
}

func importDir(dir, key, src string, quota int64) error {
	if key == "" {
		abs, err := filepath.Abs(src)
		if err != nil {
			return err
		}
		key = filepath.Base(abs)
	}
	db, st, err := open(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	c, err := gc.New(ctx, db, st, gc.Config{Max: quota, High: 1, Low: 1, Logf: log.Printf})
	if err != nil {
		return err
	}
	before := st.Used()
	start := time.Now()
	res, err := ingest.ImportDir(ctx, db, st, "local", key, src)
	if err != nil {
		return err
	}
	after := st.Used()
	el := time.Since(start)
	fmt.Printf("imported /local/%s: %d files, %d dirs, %d symlinks, %d hard links\n",
		key, res.Files, res.Dirs, res.Symlinks, res.Hardlinks)
	fmt.Printf("logical %s, new stored bytes %s (%d new contents), %v (%.1f MiB/s)\n",
		size(res.LogicalBytes), size(after-before), res.NewContents, el.Round(time.Millisecond),
		float64(res.LogicalBytes)/(1<<20)/el.Seconds())
	// With a cap, evict least recently used units (not this one) until the
	// store fits.
	if quota > 0 {
		release := c.Acquire(res.Unit.ID)
		defer release()
		if freed, err := c.Collect(ctx); err != nil {
			return err
		} else if freed > 0 {
			fmt.Printf("evicted to fit -max-gib: freed %s\n", size(freed))
		}
	}
	ix, err := indexer(db, st)
	if err != nil {
		return err
	}
	r, err := ix.Index(context.Background(), "/local/"+key)
	if err != nil {
		return err
	}
	printIndex(r)
	return nil
}

// rootIndexer is the h9index hook while serving. It is set before
// registerSubsystems runs, so a subsystem's register function can hand it
// to the subsystem (subsys.RootIndexer).
var rootIndexer subsys.RootIndexer

// indexer returns an h9index generator over the metadata, for commands
// that run without the server.
func indexer(db *meta.DB, st *store.Store) (*h9index.Indexer, error) {
	idx, err := h9index.Open(context.Background(), db)
	if err != nil {
		return nil, err
	}
	fs := &tree.FS{Meta: db, Store: st, Index: idx}
	return fs.Indexer(idx, ""), nil
}

func printIndex(r h9index.Result) {
	for _, v := range h9index.Variants {
		st, ok := r.Stats[v]
		if !ok {
			continue
		}
		fmt.Printf("indexed %s/%s (%s): %d entries, %d dirs, %d unindexed, %s\n",
			r.Root, h9index.Name, v, st.Entries, st.Dirs, st.Unindexed, size(int64(st.Bytes)))
	}
}

// indexRoots generates /.h9index for the given roots, or with all for
// every root that already has one.
func indexRoots(dir string, all bool, roots []string) error {
	db, st, err := open(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	ix, err := indexer(db, st)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if all {
		rs, err := ix.IndexAll(ctx)
		for _, r := range rs {
			printIndex(r)
		}
		if err != nil {
			return err
		}
	}
	for _, root := range roots {
		r, err := ix.Index(ctx, root)
		if err != nil {
			return err
		}
		printIndex(r)
	}
	return nil
}

func stats(dir string, asJSON bool, top int) error {
	db, st, err := open(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	r, err := savings.Measure(context.Background(), db, st, savings.Options{Dir: dir, Top: top})
	if err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	return r.WriteText(os.Stdout)
}

// statsReport renders /.cache/stats.
func statsReport(ctx context.Context, dir string, db *meta.DB, st *store.Store) ([]byte, error) {
	r, err := savings.Measure(ctx, db, st, savings.Options{Dir: dir})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = r.WriteText(&buf)
	return buf.Bytes(), err
}

func units(dir, sub string) error {
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	us, err := db.Units(context.Background(), sub)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "SUBSYSTEM\tKEY\tSTATE\tLOGICAL\tROOT")
	for _, u := range us {
		fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%d\n", u.Subsystem, u.Key, u.State, size(u.LogicalBytes), u.Root)
	}
	return w.Flush()
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
