package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/subsys/nix"
)

var nixFlags struct {
	substituters string
	cacheAddr    string
	evictStore   string
	evict        bool
}

func addNixFlags(fl *flag.FlagSet) {
	fl.StringVar(&nixFlags.substituters, "nix-substituters", nix.DefaultSubstituter, "Nix binary caches to read through, space separated, in order")
	fl.StringVar(&nixFlags.cacheAddr, "nix-cache", "", "serve a Nix binary cache (Compression: none) on this HTTP address; empty to disable")
	fl.BoolVar(&nixFlags.evict, "nix-evict-delete", false, "run nix-store --delete on a store path before evicting it")
	fl.StringVar(&nixFlags.evictStore, "nix-evict-store", "", "with -nix-evict-delete, the --store to delete from (default: nix-store's own)")
}

func registerNix(ctx context.Context, m *fill.LocalManager) error {
	opts := nix.Options{Substituters: strings.Fields(nixFlags.substituters), Indexer: rootIndexer, Logf: log.Printf}
	if nixFlags.evict {
		opts.OnEvict = nix.NixStoreDelete("", nixFlags.evictStore)
	}
	n, err := nix.New(m.DB(), opts)
	if err != nil {
		return err
	}
	if err := m.Register(ctx, n); err != nil {
		return err
	}
	if err := n.Attach(ctx, m); err != nil {
		return err
	}
	if nixFlags.cacheAddr != "" {
		ln, err := net.Listen("tcp", nixFlags.cacheAddr)
		if err != nil {
			return err
		}
		log.Printf("serving a Nix binary cache on http://%s", ln.Addr())
		go func() {
			if err := http.Serve(ln, &nix.BinaryCache{Nix: n}); err != nil {
				log.Printf("nix binary cache: %v", err)
			}
		}()
	}
	return nil
}
