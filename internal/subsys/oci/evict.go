package oci

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// ErrLayerInUse keeps a layer resident while a live image uses it.
var ErrLayerInUse = errors.New("oci: layer is used by a live image")

// ErrImageInUse refuses to evict an image that is being served.
var ErrImageInUse = errors.New("oci: image is in use")

// OnEvict implements subsys.Evicter. Eviction is per image: a layer goes
// only once every image using it has been evicted with EvictImage (or it
// belongs to no image).
func (o *OCI) OnEvict(ctx context.Context, key subsys.Key) error {
	n, err := o.st.liveRefs(ctx, string(key))
	if err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%w (%d images)", ErrLayerInUse, n)
	}
	return nil
}

// Candidate is an image the eviction policy may evict, least recently used
// first.
type Candidate struct {
	Image      string // platform manifest digest
	Ref        string // registry/repo it was pulled from
	LastAccess time.Time
	// Layers are the layers no other live image uses: what evicting the
	// image would free. Shared layers stay.
	Layers []subsys.Key
}

// Candidates lists live images not being served, least recently used
// first, for the reclaimer.
func (o *OCI) Candidates(ctx context.Context) ([]Candidate, error) {
	if err := o.flush(ctx); err != nil {
		return nil, err
	}
	rows, err := o.st.db.QueryContext(ctx, `SELECT digest, registry || '/' || repo, last_access FROM oci_image WHERE evicted = 0 ORDER BY last_access, digest`)
	if err != nil {
		return nil, err
	}
	var out []Candidate
	for rows.Next() {
		var c Candidate
		var ns int64
		if err := rows.Scan(&c.Image, &c.Ref, &ns); err != nil {
			rows.Close()
			return nil, err
		}
		c.LastAccess = time.Unix(0, ns)
		out = append(out, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	keep := out[:0]
	for _, c := range out {
		if o.imageActive(c.Image) {
			continue
		}
		ls, err := o.exclusiveLayers(ctx, c.Image)
		if err != nil {
			return nil, err
		}
		c.Layers = ls
		keep = append(keep, c)
	}
	return keep, nil
}

func (o *OCI) exclusiveLayers(ctx context.Context, image string) ([]subsys.Key, error) {
	rows, err := o.st.db.QueryContext(ctx, `
		SELECT l.diff_id FROM oci_image_layer l WHERE l.image = ? AND NOT EXISTS (
			SELECT 1 FROM oci_image_layer l2 JOIN oci_image i ON i.digest = l2.image
			WHERE l2.diff_id = l.diff_id AND l2.image != l.image AND i.evicted = 0)
		GROUP BY l.diff_id ORDER BY min(l.pos) DESC`, image)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []subsys.Key
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, subsys.Key(d))
	}
	return out, rows.Err()
}

// EvictImage marks an image evicted, releasing its hold on its layers, and
// returns the layers no live image uses any more. The caller evicts those
// units through the core. Using the image again makes it live, and its
// layers fault back in on read.
func (o *OCI) EvictImage(ctx context.Context, image string) ([]subsys.Key, error) {
	if o.imageActive(image) {
		return nil, ErrImageInUse
	}
	ls, err := o.exclusiveLayers(ctx, image)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	delete(o.touched, image) // a use before now must not revive it
	o.mu.Unlock()
	res, err := o.st.db.ExecContext(ctx, `UPDATE oci_image SET evicted = 1 WHERE digest = ?`, image)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("oci: image %s: not found", image)
	}
	return ls, nil
}

// imageActive reports whether a view of the image has its layers pinned.
func (o *OCI) imageActive(digest string) bool {
	o.mu.Lock()
	var vs []*view
	for _, v := range o.views {
		if v.digest == digest {
			vs = append(vs, v)
		}
	}
	o.mu.Unlock()
	for _, v := range vs {
		if v.isActive() {
			return true
		}
	}
	return false
}

var _ subsys.EvictionOrderer = (*OCI)(nil)

// EvictionOrder implements subsys.EvictionOrderer, making eviction
// image-level: layers no live image uses go first; if there are none, the
// least recently used images not being served are evicted (EvictImage),
// oldest first, until some offered layer is no longer used.
func (o *OCI) EvictionOrder(ctx context.Context, units []meta.Unit) ([]meta.Unit, error) {
	orphans := func() ([]meta.Unit, error) {
		var out []meta.Unit
		for _, u := range units {
			n, err := o.st.liveRefs(ctx, u.Key)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				out = append(out, u)
			}
		}
		return out, nil
	}
	out, err := orphans()
	if err != nil || len(out) > 0 {
		return out, err
	}
	cs, err := o.Candidates(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		if _, err := o.EvictImage(ctx, c.Image); err != nil {
			if errors.Is(err, ErrImageInUse) {
				continue
			}
			return nil, err
		}
		o.opts.Logf("oci: evicting image %s@%s", c.Ref, c.Image)
		if out, err = orphans(); err != nil || len(out) > 0 {
			return out, err
		}
	}
	return nil, nil
}
