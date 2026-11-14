package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/subsys"
	"github.com/progrium/wanix-cache/internal/subsys/oci"
)

var ociFlags struct {
	platform    string
	perRegistry int
	tagTTL      time.Duration
}

// viewers are subsystems that serve views; serve adds them to the tree.
var viewers = map[string]subsys.Viewer{}

func addOCIFlags(fl *flag.FlagSet) {
	fl.StringVar(&ociFlags.platform, "oci-platform", "linux/amd64", "platform picked from multi-platform OCI images (os/arch[/variant])")
	fl.IntVar(&ociFlags.perRegistry, "oci-downloads", 4, "concurrent layer downloads per registry")
	fl.DurationVar(&ociFlags.tagTTL, "oci-tag-ttl", 24*time.Hour, "how long a resolved OCI tag is trusted before asking the registry again; negative = forever")
}

// registerOCI serves layers at /oci/layers/<diff_id>/ and merged image
// roots at /oci/<registry>/<repo>@<tag|digest>/.
func registerOCI(ctx context.Context, m *fill.LocalManager) error {
	plat, err := oci.ParsePlatform(ociFlags.platform)
	if err != nil {
		return err
	}
	o := oci.New(m.DB(), m, oci.Options{
		Client:   &oci.Client{PerRegistry: ociFlags.perRegistry, UserAgent: "wanix-cache"},
		Platform: plat,
		TagTTL:   ociFlags.tagTTL,
		Indexer:  rootIndexer,
		Logf:     log.Printf,
	})
	if err := m.Register(ctx, o); err != nil {
		return err
	}
	viewers[o.Name()] = o
	return nil
}
