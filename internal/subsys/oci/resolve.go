package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
)

type resolveCall struct {
	done chan struct{}
	im   imageRow
	err  error
}

// ResolveImage returns the image a reference names for the configured
// platform, fetching its manifest and config on first use. Concurrent
// calls for one reference share one resolution.
func (o *OCI) ResolveImage(ctx context.Context, r Ref) (imageRow, error) {
	k := r.String() + " " + o.opts.Platform.String()
	o.mu.Lock()
	c := o.resolves[k]
	if c == nil {
		c = &resolveCall{done: make(chan struct{})}
		o.resolves[k] = c
		go func() {
			// Detached from the caller: a cancelled walk must not fail the
			// others waiting on the same resolution.
			rctx, cancel := context.WithTimeout(o.ctx, 2*time.Minute)
			c.im, c.err = o.resolve(rctx, r)
			cancel()
			o.mu.Lock()
			delete(o.resolves, k)
			o.mu.Unlock()
			close(c.done)
		}()
	}
	o.mu.Unlock()
	select {
	case <-c.done:
		return c.im, c.err
	case <-ctx.Done():
		return imageRow{}, ctx.Err()
	}
}

func (o *OCI) resolve(ctx context.Context, r Ref) (imageRow, error) {
	plat := o.opts.Platform.String()
	if r.Digest != "" {
		if im, err := o.st.image(ctx, r.Digest); err == nil {
			return im, nil
		}
		// A digest of an index maps to a platform image like a tag does,
		// and never goes stale.
		if d, _, err := o.st.tagLike(ctx, r, plat); err == nil {
			return o.st.image(ctx, d)
		}
		return o.pull(ctx, r)
	}
	d, at, err := o.st.tagLike(ctx, r, plat)
	if err == nil && (o.opts.TagTTL < 0 || time.Since(at) < o.opts.TagTTL) {
		return o.st.image(ctx, d)
	}
	im, perr := o.pull(ctx, r)
	if perr != nil && err == nil && !errors.Is(perr, ErrNotFound) {
		o.opts.Logf("oci: %s: using cached resolution: %v", r, perr)
		return o.st.image(ctx, d)
	}
	return im, perr
}

// pull fetches a reference's manifest (through an index if there is one)
// and config and records the image.
func (o *OCI) pull(ctx context.Context, r Ref) (imageRow, error) {
	c := o.opts.Client
	body, mt, err := c.Manifest(ctx, r, r.Version())
	if err != nil {
		return imageRow{}, err
	}
	if r.Digest != "" && sha256Digest(body) != r.Digest {
		return imageRow{}, fmt.Errorf("oci: %s: manifest digest is %s", r, sha256Digest(body))
	}
	m, mt, err := parseManifest(body, mt)
	if err != nil {
		return imageRow{}, err
	}
	digest := sha256Digest(body)
	if isIndex(mt) {
		d, err := pick(m.Manifests, o.opts.Platform)
		if err != nil {
			return imageRow{}, fmt.Errorf("%s: %w", r, err)
		}
		if im, err := o.st.image(ctx, d.Digest); err == nil {
			// Known image under a new name: record the name only.
			return im, o.st.putImage(ctx, r, o.opts.Platform.String(), im, nil)
		}
		if body, mt, err = c.Manifest(ctx, r, d.Digest); err != nil {
			return imageRow{}, err
		}
		if got := sha256Digest(body); got != d.Digest {
			return imageRow{}, fmt.Errorf("oci: %s: manifest %s has digest %s", r, d.Digest, got)
		}
		if m, mt, err = parseManifest(body, mt); err != nil {
			return imageRow{}, err
		}
		digest = d.Digest
	}
	if !isImage(mt) {
		return imageRow{}, fmt.Errorf("oci: %s: unsupported manifest type %q", r, mt)
	}
	cfg, err := c.SmallBlob(ctx, r, m.Config.Digest, maxConfig)
	if err != nil {
		return imageRow{}, fmt.Errorf("oci: %s: config: %w", r, err)
	}
	if got := sha256Digest(cfg); got != m.Config.Digest {
		return imageRow{}, fmt.Errorf("oci: %s: config %s has digest %s", r, m.Config.Digest, got)
	}
	var ic imageConfig
	if err := json.Unmarshal(cfg, &ic); err != nil {
		return imageRow{}, fmt.Errorf("oci: %s: config: %w", r, err)
	}
	if len(ic.RootFS.DiffIDs) != len(m.Layers) {
		return imageRow{}, fmt.Errorf("oci: %s: %d layers but %d diff_ids", r, len(m.Layers), len(ic.RootFS.DiffIDs))
	}
	im := imageRow{Digest: digest, Registry: r.Registry, Repo: r.Repo, Manifest: body, Config: cfg}
	for i, l := range m.Layers {
		if _, err := compression(l.MediaType); err != nil {
			return imageRow{}, fmt.Errorf("oci: %s: layer %d: %w", r, i, err)
		}
		if !digestRE.MatchString(l.Digest) || !digestRE.MatchString(ic.RootFS.DiffIDs[i]) {
			return imageRow{}, fmt.Errorf("oci: %s: layer %d: only sha256 digests are supported", r, i)
		}
		im.Layers = append(im.Layers, layerRow{DiffID: ic.RootFS.DiffIDs[i], Blob: l.Digest})
	}
	if err := o.st.putImage(ctx, r, o.opts.Platform.String(), im, m.Layers); err != nil {
		return imageRow{}, err
	}
	return o.st.image(ctx, digest)
}

var _ = meta.ErrNotExist
