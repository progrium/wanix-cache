package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/gc"
	"github.com/progrium/wanix-cache/internal/ingest"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
)

// startGC attaches the eviction policy to a serving manager and runs the
// watermark loop in the background.
func startGC(db *meta.DB, st *store.Store, mgr fill.Manager, quota *fill.LocalQuota, max int64) error {
	c, err := gc.New(context.Background(), db, st, gc.Config{
		Max: max,
		Usage: func() int64 {
			u := quota.Usage()
			return u.Store + u.Spool + u.Reserved
		},
		Logf: log.Printf,
	})
	if err != nil {
		return err
	}
	c.Attach(mgr)
	go c.Run(context.Background())
	return nil
}

func gcCommand(cmd string, fl *flag.FlagSet, data *string, args []string) error {
	ctx := context.Background()
	sub := fl.String("subsystem", "local", "unit subsystem")
	key := fl.String("key", "", "unit key")
	var (
		gib       *int64
		high, low *float64
		fix, deep *bool
		reason    *string
		unpin     *bool
	)
	switch cmd {
	case "gc":
		gib = fl.Int64("max-gib", 0, "cap for the watermarks, GiB; 0 = do nothing")
		high = fl.Float64("high", 0.95, "evict when usage is over this fraction of the cap")
		low = fl.Float64("low", 0.85, "evict down to this fraction of the cap")
	case "audit":
		fix = fl.Bool("fix", false, "repair what the audit finds")
		deep = fl.Bool("deep", false, "re-hash every stored file and compare it with its digest")
	case "pin":
		reason = fl.String("reason", "manual", "pin reason; each reason is a separate pin")
		unpin = fl.Bool("unpin", false, "remove the pin instead")
	}
	fl.Parse(args)

	var max int64
	if gib != nil {
		max = *gib << 30
	}
	db, st, err := open(*data)
	if err != nil {
		return err
	}
	defer db.Close()
	cfg := gc.Config{Max: max, Logf: log.Printf}
	if high != nil {
		cfg.High, cfg.Low = *high, *low
	}
	c, err := gc.New(ctx, db, st, cfg)
	if err != nil {
		return err
	}
	unit := func() (meta.Unit, error) {
		if *key == "" {
			return meta.Unit{}, fmt.Errorf("%s: -key is required", cmd)
		}
		return db.Unit(ctx, *sub, *key)
	}

	switch cmd {
	case "gc":
		before := st.Used()
		freed, err := c.Collect(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("freed %s; stored %s -> %s\n", size(freed), size(before), size(st.Used()))
	case "evict":
		u, err := unit()
		if err != nil {
			return err
		}
		freed, err := c.Evict(ctx, u.ID)
		if err != nil {
			return err
		}
		fmt.Printf("evicted %s/%s, freed %s\n", u.Subsystem, u.Key, size(freed))
	case "refault":
		if fl.NArg() != 1 {
			return fmt.Errorf("refault: need the source directory")
		}
		res, err := ingest.RefaultDir(ctx, db, st, *sub, *key, fl.Arg(0))
		if err != nil {
			return err
		}
		fmt.Printf("refaulted %s/%s: %d files, %s, %d new contents\n",
			res.Unit.Subsystem, res.Unit.Key, res.Files, size(res.LogicalBytes), res.NewContents)
	case "audit":
		rep, err := c.Audit(ctx, gc.AuditOptions{Fix: *fix, Deep: *deep})
		if err != nil {
			return err
		}
		fmt.Print(rep)
		if !rep.Clean() && !*fix {
			os.Exit(1)
		}
	case "pin":
		if *key == "" {
			pins, err := c.Pins(ctx)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "SUBSYSTEM\tKEY\tSTATE\tREASON")
			for _, p := range pins {
				fmt.Fprintf(w, "%s\t%s\t%v\t%s\n", p.Unit.Subsystem, p.Unit.Key, p.Unit.State, p.Reason)
			}
			return w.Flush()
		}
		u, err := unit()
		if err != nil {
			return err
		}
		if *unpin {
			return c.Unpin(ctx, u.ID, *reason)
		}
		return c.Pin(ctx, u.ID, *reason)
	}
	return nil
}
