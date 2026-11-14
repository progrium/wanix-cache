// Package subsys defines what a cache subsystem (APK, OCI, Nix) provides.
//
// A subsystem maps names in its part of the namespace to units, describes a
// unit before fetching it, and streams a unit's contents into a Sink. The
// core does everything else: single-flight faults, spooling, digest checks,
// ingest into the CAS, accounting, eviction and serving.
//
// # Conventions for subsystem packages
//
//   - Each subsystem lives in its own package, internal/subsys/<name>
//     (internal/subsys/apk, internal/subsys/oci, internal/subsys/nix), and
//     its Name is that same lower-case word. The name is also its namespace
//     root (/<name>/...) and the subsystem column on units.
//   - A subsystem's own tables are named <name>_<table> (apk_index,
//     nix_narinfo). It never writes core tables (inode, dirent, unit,
//     content) directly; it goes through Sink and the fill.Manager.
//   - Schema changes are numbered migrations returned by Migrations, applied
//     in order by meta.Migrate and recorded per owner in schema_migrations.
//     Versions start at 1 and are never edited or reused once merged: a
//     change is always a new, higher version. This lets the subsystem
//     threads change their schemas independently without conflicts.
//   - Keys are opaque to the core but must be stable and content-identifying
//     (an apk's checksum, a layer's diff_id, a store path's hash), because
//     "fetching again always works" relies on a key naming the same bytes.
package subsys

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
)

// ErrDir is returned by Resolve for a path that is not inside any unit but
// is a directory above unit mounts, such as /apk/x86_64 before any package
// is mounted under it. The manager creates the directory so a walk through
// it can reach the units below.
var ErrDir = errors.New("subsys: directory above unit mounts")

// Key identifies a unit within its subsystem.
type Key string

// Info describes a unit before it is fetched.
type Info struct {
	// Size is the expected logical size in bytes (the sum of file sizes
	// once unpacked), used to reserve quota before the fetch starts. -1
	// when unknown; the fill then reserves as it goes.
	Size int64
	// Digest, when set, is checked against Sink.Digest at the end of the
	// fill. Its format is the subsystem's ("sha256:...", a NarHash, ...);
	// the subsystem computes the matching value as it streams.
	Digest string
	// Deps are units this one pins while it is resident: an image's layers,
	// a store path's references.
	Deps []Key
	// Name and Version, for package-aware subsystems, key the usage history
	// so it survives eviction and upgrades. Empty for others.
	Name, Version string
	// Mount is where the unit's root directory goes, relative to /<sub>/,
	// slash separated, for example "x86_64/musl-1.2.5-r0". Default: the
	// key. Resolve must map paths under the mount back to this key.
	Mount string
}

// Subsystem is implemented by each subsystem package.
type Subsystem interface {
	// Name is the subsystem's namespace root and unit subsystem column.
	Name() string
	// Migrations returns the subsystem's schema migrations, in order.
	Migrations() []meta.Migration
	// Resolve maps a path under /<name>/ (without that prefix, slash
	// separated, no leading slash) to the unit that holds it and the path
	// inside that unit. It returns fs.ErrNotExist for names that are not
	// cache content, and ErrDir for directories above unit mounts.
	Resolve(ctx context.Context, p string) (key Key, rel string, err error)
	// Describe returns what is known about a unit without fetching it.
	Describe(ctx context.Context, key Key) (Info, error)
	// Fetch streams the unit into sink. It may be called again for the same
	// key after an eviction or a failed fill, and must produce the same tree.
	// It must return promptly when ctx is cancelled.
	Fetch(ctx context.Context, key Key, sink Sink) error
}

// Evicter is optionally implemented by subsystems that must react when one
// of their units is evicted, for example to run nix-store --delete for a
// guest-owned store. A non-nil error keeps the unit resident.
type Evicter interface {
	OnEvict(ctx context.Context, key Key) error
}

// Sink receives a unit's tree during a fill. Paths are relative to the unit
// root, slash separated, with parents created before children. Entries become
// visible to readers as soon as each call returns, and a regular file's
// bytes become readable as they are written.
type Sink interface {
	Dir(p string, mode fs.FileMode, mtime time.Time) error
	// File creates a regular file and returns a writer for its bytes. size
	// is the expected length, or -1. Close completes the file.
	File(p string, mode fs.FileMode, size int64, mtime time.Time) (io.WriteCloser, error)
	Symlink(p, target string, mtime time.Time) error
	// Hardlink adds p as another name for the existing file target.
	Hardlink(p, target string) error
	// Whiteout records that p hides lower layers' entries (OCI only).
	Whiteout(p string) error
	// Opaque marks directory p as hiding all lower layers' entries (OCI).
	Opaque(p string) error
	// Digest records the digest the subsystem computed over the unit's
	// upstream bytes, to be compared with Info.Digest.
	Digest(d string)
}

// EvictionOrderer is optionally implemented by subsystems with their own
// eviction order. The reclaimer passes a window of the subsystem's
// evictable (resident, unpinned) units in LRU order and evicts only the
// units returned, in the order returned; units left out are deferred to a
// later round. Nix uses it to evict from the leaves inward: a store path
// goes only once nothing live refers to it.
type EvictionOrderer interface {
	EvictionOrder(ctx context.Context, units []meta.Unit) ([]meta.Unit, error)
}
