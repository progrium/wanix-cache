// Command wanix-cache is the H9 read-through cache.
//
//	wanix-cache serve  [-data DIR] [-9p ADDR]
//	wanix-cache import [-data DIR] [-max-gib N] -key NAME SRC
//	wanix-cache stats  [-data DIR]
//	wanix-cache units  [-data DIR] [-subsystem NAME]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/ninep"
	"github.com/progrium/wanix-cache/internal/tree"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: wanix-cache <command> [flags]

commands:
  serve    serve the cache read-only over 9P2000
  import   import a local directory as a unit under /local/<key>
  stats    print store totals
  units    list units`)
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
		addr := fl.String("9p", "127.0.0.1:5640", "9P2000 TCP listen address")
		fl.Parse(args)
		err = serve(*data, *addr)
	case "import":
		key := fl.String("key", "", "unit key (defaults to the source directory's name)")
		gib := fl.Int64("max-gib", 0, "quota on unique CAS bytes, GiB; 0 = unlimited")
		fl.Parse(args)
		if fl.NArg() != 1 {
			log.Fatal("import: need exactly one source directory")
		}
		err = importDir(*data, *key, fl.Arg(0), *gib<<30)
	case "stats":
		fl.Parse(args)
		err = stats(*data)
	case "units":
		sub := fl.String("subsystem", "", "only this subsystem")
		fl.Parse(args)
		err = units(*data, *sub)
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func open(dir string, quota int64) (*meta.DB, *cas.Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return nil, nil, err
	}
	store, err := cas.Open(dir, quota)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, store, nil
}

func serve(dir, addr string) error {
	db, store, err := open(dir, 0)
	if err != nil {
		return err
	}
	defer db.Close()
	srv := &ninep.Server{FS: &tree.FS{Meta: db, CAS: store}, Logf: log.Printf}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		db.Close()
		os.Exit(0)
	}()
	log.Printf("serving %s read-only over 9P2000 on %s", dir, addr)
	return srv.ListenAndServe(addr)
}

func importDir(dir, key, src string, quota int64) error {
	if key == "" {
		abs, err := filepath.Abs(src)
		if err != nil {
			return err
		}
		key = filepath.Base(abs)
	}
	db, store, err := open(dir, quota)
	if err != nil {
		return err
	}
	defer db.Close()
	before, _ := store.Usage()
	start := time.Now()
	res, err := ingest.ImportDir(context.Background(), db, store, "local", key, src)
	if err != nil {
		return err
	}
	after, _ := store.Usage()
	el := time.Since(start)
	fmt.Printf("imported /local/%s: %d files, %d dirs, %d symlinks, %d hard links\n",
		key, res.Files, res.Dirs, res.Symlinks, res.Hardlinks)
	fmt.Printf("logical %s, new CAS bytes %s (%d new content objects), %v (%.1f MiB/s)\n",
		size(res.LogicalBytes), size(after-before), res.NewContents, el.Round(time.Millisecond),
		float64(res.LogicalBytes)/(1<<20)/el.Seconds())
	return nil
}

func stats(dir string) error {
	db, store, err := open(dir, 0)
	if err != nil {
		return err
	}
	defer db.Close()
	s, err := db.Stats(context.Background())
	if err != nil {
		return err
	}
	used, _ := store.Usage()
	fmt.Printf("units            %d\n", s.Units)
	fmt.Printf("inodes           %d\n", s.Inodes)
	fmt.Printf("content objects  %d\n", s.Contents)
	fmt.Printf("logical bytes    %s (all files)\n", size(s.FileBytes))
	fmt.Printf("distinct bytes   %s (after whole-file dedup)\n", size(s.ContentBytes))
	fmt.Printf("CAS blob bytes   %s (after chunk dedup)\n", size(used))
	if s.FileBytes > 0 {
		fmt.Printf("savings          %.1f%% (blob bytes vs logical; ignores filesystem overhead)\n",
			100*(1-float64(used)/float64(s.FileBytes)))
	}
	return nil
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
