package main

import (
	"context"

	"github.com/progrium/wanix-cache/internal/fill"
)

// registerSubsystems registers every subsystem with the fill manager. Each
// subsystem adds its own block here, constructing it with its config from
// dir (the cache directory), so subsystem threads touch only their lines.
func registerSubsystems(ctx context.Context, m *fill.LocalManager, dir string) error {
	_ = dir
	return nil
}
