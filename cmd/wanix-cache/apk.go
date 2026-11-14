package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/subsys/apk"
)

var apkFlags struct {
	upstream string
	addr     string
	ttl      time.Duration
	unpack   bool
}

func addAPKFlags(fl *flag.FlagSet) {
	fl.StringVar(&apkFlags.upstream, "apk-upstream", apk.DefaultUpstream, "Alpine mirror to read through")
	fl.StringVar(&apkFlags.addr, "apk-mirror", "", "serve an apk HTTP mirror on this address (guest repositories: http://ADDR/v3.20/main); empty to disable")
	fl.DurationVar(&apkFlags.ttl, "apk-index-ttl", time.Minute, "serve a cached APKINDEX this long before revalidating it upstream")
	fl.BoolVar(&apkFlags.unpack, "apk-unpack", true, "after serving an .apk, unpack it in the background into /apk/<arch>/<name>-<ver>/")
}

func registerAPK(ctx context.Context, m *fill.LocalManager) error {
	a := apk.New(m.DB(), apk.Config{Upstream: apkFlags.upstream, IndexTTL: apkFlags.ttl, Unpack: apkFlags.unpack, Indexer: rootIndexer})
	if err := m.Register(ctx, a); err != nil {
		return err
	}
	a.Attach(m)
	if apkFlags.addr != "" {
		ln, err := net.Listen("tcp", apkFlags.addr)
		if err != nil {
			return err
		}
		log.Printf("serving an apk mirror of %s on http://%s", apkFlags.upstream, ln.Addr())
		go func() {
			if err := http.Serve(ln, a); err != nil {
				log.Printf("apk mirror: %v", err)
			}
		}()
	}
	return nil
}
