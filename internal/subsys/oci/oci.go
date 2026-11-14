// Package oci is the OCI image subsystem. Images are a distribution format
// for H9 root filesystems: nothing here runs them.
//
// Units are layers, keyed by diff_id (the digest of the uncompressed tar)
// and mounted at /oci/layers/<diff_id>/. Layers are stored unpacked, file
// by file, so identical files dedupe across layers, images and the other
// subsystems.
//
// Images are views. Walking /oci/<registry>/<repo>@<tag|digest>/ resolves
// the reference (manifest, then config), starts every layer's fill at once
// (top layer first, at most Client.PerRegistry downloads per registry),
// and serves the merged rootfs right away. Whiteouts and layer order are
// applied at lookup time: a name is looked up from the top layer down, and
// a layer still being fetched is waited on only when the name is not there
// yet, since it could still appear or be whited out.
//
// Eviction is per image: an image that has not been used releases its
// layers, and a layer can be evicted only when no live image uses it
// (OnEvict enforces this; Candidates and EvictImage are the policy hooks).
package oci

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Name is the subsystem name and namespace root.
const Name = "oci"

// LayersDir is where layer units are mounted under /oci/.
const LayersDir = "layers"

// Units is the part of fill.Manager the subsystem uses.
type Units interface {
	Ensure(ctx context.Context, sub string, key subsys.Key, wait bool) (meta.Unit, error)
	Acquire(unitID int64) (release func())
}

// Options configures the subsystem.
type Options struct {
	// Client talks to registries. Default: a Client with 4 downloads per
	// registry.
	Client *Client
	// Platform picks an image from a multi-platform index. Default linux/amd64.
	Platform Platform
	// TagTTL is how long a tag's resolution is trusted before the registry
	// is asked again. A registry that cannot be reached falls back to the
	// cached resolution. Default 24h; negative means never re-resolve.
	TagTTL time.Duration
	// IdleTimeout releases an image's layer pins after this long without a
	// lookup, making them candidates for eviction. Default 10m.
	IdleTimeout time.Duration
	// Indexer, if set, is asked to index an image's merged rootfs
	// (/.h9index) once every layer's listing is final.
	Indexer subsys.RootIndexer
	Logf    func(format string, args ...any)
}

// OCI implements subsys.Subsystem, subsys.Evicter and subsys.Viewer.
type OCI struct {
	db    *meta.DB
	st    store
	units Units
	opts  Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	resolves map[string]*resolveCall // ref + platform -> in-flight resolution
	views    map[string]*view        // ref path -> view
	nodes    map[uint64]vnode        // live virtual directories
	paths    map[uint64]string       // every virtual directory ever served
	touched  map[string]time.Time    // image digest -> last use, unflushed
	rootIno  uint64                  // the real /oci/ directory
}

var (
	_ subsys.Subsystem = (*OCI)(nil)
	_ subsys.Evicter   = (*OCI)(nil)
	_ subsys.Viewer    = (*OCI)(nil)
)

// New returns the subsystem. units is normally the fill.Manager it is
// registered with; it may be set later with SetUnits, before the first
// lookup. Close stops its background work.
func New(db *meta.DB, units Units, opts Options) *OCI {
	if opts.Client == nil {
		opts.Client = &Client{PerRegistry: 4}
	}
	if opts.Platform.OS == "" {
		opts.Platform = Platform{OS: "linux", Architecture: "amd64"}
	}
	if opts.TagTTL == 0 {
		opts.TagTTL = 24 * time.Hour
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 10 * time.Minute
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	ctx, cancel := context.WithCancel(context.Background())
	o := &OCI{
		db: db, st: store{db.SQL()}, units: units, opts: opts, ctx: ctx, cancel: cancel,
		resolves: map[string]*resolveCall{}, views: map[string]*view{},
		nodes: map[uint64]vnode{}, paths: map[uint64]string{}, touched: map[string]time.Time{},
	}
	o.wg.Add(1)
	go o.janitor()
	return o
}

// SetUnits sets the unit manager, for when the manager is built after the
// subsystem.
func (o *OCI) SetUnits(u Units) { o.units = u }

// Close releases pins, flushes access times and stops background work.
func (o *OCI) Close() error {
	o.cancel()
	o.wg.Wait()
	o.mu.Lock()
	for k, v := range o.views {
		v.release()
		delete(o.views, k)
	}
	o.mu.Unlock()
	return o.flush(context.Background())
}

func (o *OCI) Name() string                 { return Name }
func (o *OCI) Migrations() []meta.Migration { return migrations }

// Resolve maps layers/<diff_id>/<rel> to the layer unit. Image paths are
// views and are served through the Viewer methods instead.
func (o *OCI) Resolve(ctx context.Context, p string) (subsys.Key, string, error) {
	rest, ok := strings.CutPrefix(p, LayersDir+"/")
	if !ok {
		return "", "", fs.ErrNotExist
	}
	id, rel, _ := strings.Cut(rest, "/")
	if !digestRE.MatchString(id) {
		return "", "", fs.ErrNotExist
	}
	if _, err := o.st.blobFor(ctx, id); err != nil {
		if errors.Is(err, meta.ErrNotExist) {
			return "", "", fs.ErrNotExist
		}
		return "", "", err
	}
	return subsys.Key(id), rel, nil
}

// Describe implements subsys.Subsystem.
func (o *OCI) Describe(ctx context.Context, key subsys.Key) (subsys.Info, error) {
	id := string(key)
	if _, err := o.st.blobFor(ctx, id); err != nil {
		return subsys.Info{}, fmt.Errorf("oci: layer %s: %w", id, err)
	}
	return subsys.Info{
		Size:   o.st.unpackedSize(ctx, id),
		Digest: id,
		Mount:  LayersDir + "/" + id,
	}, nil
}

// Fetch downloads, verifies and unpacks a layer.
func (o *OCI) Fetch(ctx context.Context, key subsys.Key, sink subsys.Sink) error {
	id := string(key)
	b, err := o.st.blobFor(ctx, id)
	if err != nil {
		return fmt.Errorf("oci: layer %s: %w", id, err)
	}
	ref := Ref{Registry: b.Registry, Repo: b.Repo}
	body, err := o.opts.Client.Blob(ctx, ref, b.Digest)
	if err != nil {
		return fmt.Errorf("oci: layer %s from %s/%s: %w", id, b.Registry, b.Repo, err)
	}
	defer body.Close()
	st, err := unpackLayer(ctx, body, b.Digest, b.MediaType, sink)
	if err != nil {
		return err
	}
	sink.Digest(st.DiffID)
	if st.DiffID == id {
		if err := o.st.setUnpackedSize(ctx, id, st.Bytes); err != nil {
			o.opts.Logf("oci: layer %s: %v", id, err)
		}
	}
	return nil
}

func (o *OCI) janitor() {
	defer o.wg.Done()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-t.C:
		}
		if err := o.flush(o.ctx); err != nil && o.ctx.Err() == nil {
			o.opts.Logf("oci: flush access times: %v", err)
		}
		o.releaseIdle(time.Now())
	}
}

func (o *OCI) touch(digest string) {
	o.mu.Lock()
	o.touched[digest] = time.Now()
	o.mu.Unlock()
}

func (o *OCI) flush(ctx context.Context) error {
	o.mu.Lock()
	at := o.touched
	o.touched = map[string]time.Time{}
	o.mu.Unlock()
	return o.st.touch(ctx, at)
}

// releaseIdle unpins the layers of views not used for IdleTimeout. The
// views stay, so a later lookup pins them again.
func (o *OCI) releaseIdle(now time.Time) {
	o.mu.Lock()
	var idle []*view
	for _, v := range o.views {
		if now.Sub(v.lastUse()) > o.opts.IdleTimeout {
			idle = append(idle, v)
		}
	}
	o.mu.Unlock()
	for _, v := range idle {
		v.release()
	}
}
