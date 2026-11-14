// Package fill defines the read-through fill path: faulting units in,
// spooling them on plain disk, ingesting them into the file store, and
// accounting for the bytes in between. The implementation is LocalManager
// (manager.go).
//
// A fault runs:
//
//	Ensure(sub, key)
//	  absent|evicted -> fetching   quota reserved from Info.Size, unit pinned
//	  Subsystem.Fetch(key, sink)   entries appear in the namespace as written,
//	                               file bytes are readable from the spool
//	  digest checked               mismatch -> absent (or evicted), spool dropped
//	  fetching -> spooled          spool fsynced; survives a crash
//	  ingest workers               hash spool files; link each into the
//	                               store, or to an identical stored file
//	  spooled -> resident          inode content digests set, spool removed
//
// Eviction moves resident -> evicted through a Reclaimer the
// Manager calls when a reservation does not fit.
package fill

import (
	"context"
	"errors"
	"sync"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// ErrDigestMismatch fails a fill whose upstream bytes do not match the
// digest the subsystem described.
var ErrDigestMismatch = errors.New("fill: digest mismatch")

// Manager owns units and the fill path. It is safe for concurrent use.
type Manager interface {
	// Register adds a subsystem and applies its migrations.
	Register(ctx context.Context, s subsys.Subsystem) error
	// Subsystem returns a registered subsystem by name.
	Subsystem(name string) (subsys.Subsystem, bool)

	// Ensure returns the unit for (sub, key), faulting it in if it is
	// absent or evicted. Concurrent calls for one unit share one fetch. It
	// returns once the unit's manifest is complete (state spooled or
	// resident); with wait false it returns as soon as the fetch has started
	// and entries are being published (state fetching), for serving while
	// filling.
	Ensure(ctx context.Context, sub string, key subsys.Key, wait bool) (meta.Unit, error)

	// Open returns a file's bytes in whatever state its unit is in: from the
	// spool while fetching or spooled (reads past what has been written
	// block until it is written or the fill fails), from the store when
	// resident. An open handle pins the unit until Close.
	Open(ctx context.Context, in meta.Inode) (store.File, error)

	// OpenPath opens the file at rel (slash separated, inside the unit) of a
	// unit, faulting the unit in and waiting for the entry if needed. For
	// frontends that are not the 9P tree, such as the APK HTTP mirror.
	OpenPath(ctx context.Context, sub string, key subsys.Key, rel string) (store.File, meta.Inode, error)

	// WatchOpens registers fn to be called when a file is opened through
	// Open or OpenPath and again when it is closed, for usage tracking. It
	// runs on the opening goroutine, so it must be cheap.
	WatchOpens(fn func(OpenEvent))

	// Acquire pins a unit against eviction until release is called. Views
	// pin their units; fills and open handles pin implicitly.
	Acquire(unitID int64) (release func())
	// Pinned reports whether a unit is pinned.
	Pinned(unitID int64) bool

	// Watch registers fn to be called after every unit state change,
	// outside any lock. Eviction, stats and usage tracking hang off this.
	Watch(fn func(UnitEvent))

	// SetReclaimer installs the eviction policy (phase 5).
	SetReclaimer(r Reclaimer)
}

// UnitEvent describes one state change.
type UnitEvent struct {
	Unit     meta.Unit
	From, To meta.UnitState
	Err      error // set when a fill failed
}

// OpenEvent describes a file open (Closed false) or close (Closed true,
// with the bytes read through that handle).
type OpenEvent struct {
	Inode     meta.Inode
	Closed    bool
	BytesRead int64
	// Internal is set for opens made with a context from Internal: the
	// cache reading its own units (an apk layer unpacked from the cached
	// .apk), not a client. Usage stats skip them.
	Internal bool
}

type internalKey struct{}

// Internal marks ctx for opens the cache makes for itself, so their
// OpenEvents carry Internal.
func Internal(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalKey{}, true)
}

// IsInternal reports whether ctx was marked by Internal.
func IsInternal(ctx context.Context) bool {
	v, _ := ctx.Value(internalKey{}).(bool)
	return v
}

// Reclaimer frees stored bytes by evicting units. The Manager calls it when a
// quota reservation does not fit, asking for at least need bytes. It must
// not evict pinned units.
type Reclaimer interface {
	Reclaim(ctx context.Context, need int64) (freed int64, err error)
}

// Quota accounts for every byte the cache holds: stored file bytes (each
// file once, however many links it has), spool bytes, and reservations for
// fills in progress.
type Quota interface {
	// Reserve sets aside n bytes for a fill, calling the Reclaimer if they
	// do not fit. It fails with store.ErrFull if they still do not fit.
	Reserve(ctx context.Context, n int64) (Reservation, error)
	Usage() Usage
}

// Usage is a snapshot of the cache's accounted bytes.
type Usage struct {
	Store, Spool, Reserved, Max int64
}

// Reservation is quota held by one fill.
type Reservation interface {
	// Grow extends the reservation when a fill turns out larger than its
	// estimate (or had none).
	Grow(ctx context.Context, n int64) error
	// Release returns whatever the reservation still holds. Bytes that
	// landed in the spool or the store are accounted there instead.
	Release()
}

// Spool holds unit bytes on plain disk between fetch and ingest. Files are
// keyed by inode number.
type Spool interface {
	// Create makes the spool file for a new file inode.
	Create(unitID int64, ino uint64) (SpoolFile, error)
	// Open reads a spool file, blocking on reads past the bytes written so
	// far until they are written, the file is finished, or the fill fails.
	Open(ino uint64) (store.File, error)
	// Sync makes all of a unit's spool files durable (fetching -> spooled).
	Sync(unitID int64) error
	// Remove drops a unit's spool files after ingest or a failed fill.
	Remove(unitID int64) error
	// Units lists units with spool files, for crash recovery.
	Units() ([]int64, error)
	// Bytes is the total size of spool files, for the quota.
	Bytes() int64
}

// SpoolFile is a spool file being written.
type SpoolFile interface {
	Write(p []byte) (int, error)
	// Finish marks the file complete; blocked readers see EOF at its end.
	Finish() error
	// Fail wakes blocked readers with err.
	Fail(err error)
}

// Pins is a concurrency-safe pin count per unit. Managers can embed it.
type Pins struct {
	mu sync.Mutex
	n  map[int64]int
}

// Acquire pins unitID until the returned release is called (once).
func (p *Pins) Acquire(unitID int64) (release func()) {
	p.mu.Lock()
	if p.n == nil {
		p.n = map[int64]int{}
	}
	p.n[unitID]++
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			if p.n[unitID]--; p.n[unitID] <= 0 {
				delete(p.n, unitID)
			}
			p.mu.Unlock()
		})
	}
}

// Pinned reports whether unitID has any pins.
func (p *Pins) Pinned(unitID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n[unitID] > 0
}
