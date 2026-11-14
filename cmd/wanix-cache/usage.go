package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	usagestats "github.com/progrium/wanix-cache/internal/usage"
)

// usageTracker is the running server's usage tracker, set by startUsage.
var usageTracker *usagestats.Tracker

// startUsage counts opens and fetches in memory and flushes them every 30 s.
func startUsage(ctx context.Context, db *meta.DB, m *fill.LocalManager) error {
	t, err := usagestats.New(ctx, db, m)
	if err != nil {
		return err
	}
	t.Attach(m)
	usageTracker = t
	go t.Run(ctx, 30*time.Second)
	return nil
}

// flushUsage writes pending counts, for shutdown.
func flushUsage() {
	if usageTracker != nil {
		if err := usageTracker.Flush(context.Background()); err != nil {
			log.Printf("usage: flush: %v", err)
		}
	}
}

// usageReport renders the report served as /.cache/usage.
func usageReport(ctx context.Context, db *meta.DB) ([]byte, error) {
	if usageTracker != nil {
		usageTracker.Flush(ctx)
	}
	r, err := usagestats.Build(ctx, db, usagestats.Query{Untouched: true, Limit: 50})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = r.WriteText(&buf)
	return buf.Bytes(), err
}

func usageCommand(fl *flag.FlagSet, data *string, args []string) error {
	sub := fl.String("subsystem", "", "only this subsystem (apk, nix, oci, local)")
	days := fl.Int("days", 30, "window for the recent columns")
	limit := fl.Int("n", 20, "rows per section (and never-opened files listed per unit)")
	untouched := fl.Bool("untouched", false, "also report files in cached units that were never opened")
	asJSON := fl.Bool("json", false, "print JSON")
	fl.Parse(args)
	db, err := meta.Open(filepath.Join(*data, "meta.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	r, err := usagestats.Build(context.Background(), db, usagestats.Query{Subsystem: *sub, Days: *days, Limit: *limit, Untouched: *untouched})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	return r.WriteText(os.Stdout)
}
