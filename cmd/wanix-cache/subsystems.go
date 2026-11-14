package main

import (
	"context"
	"flag"

	"github.com/progrium/wanix-cache/internal/fill"
)

// addSubsystemFlags adds each subsystem's serve flags. Each subsystem adds
// its own line here and keeps its flags in its own file.
func addSubsystemFlags(fl *flag.FlagSet) {
	addNixFlags(fl)
}

// registerSubsystems registers every subsystem with the fill manager. Each
// subsystem adds its own block here, constructing it with its config from
// dir (the cache directory), so subsystem threads touch only their lines.
func registerSubsystems(ctx context.Context, m *fill.LocalManager, dir string) error {
	_ = dir
	if err := registerNix(ctx, m); err != nil {
		return err
	}
	return nil
}
