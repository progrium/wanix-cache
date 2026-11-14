package oci

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
)

var migrations = []meta.Migration{
	{Version: 1, SQL: `
-- tag is a tag, or the digest of an index, mapped to a platform image.
CREATE TABLE oci_tag (
	registry    TEXT    NOT NULL,
	repo        TEXT    NOT NULL,
	tag         TEXT    NOT NULL,
	platform    TEXT    NOT NULL,
	image       TEXT    NOT NULL, -- platform manifest digest
	resolved_at INTEGER NOT NULL,
	PRIMARY KEY (registry, repo, tag, platform)
) WITHOUT ROWID;

-- One row per platform image manifest. An image is the unit of eviction:
-- evicted images release their layers, and a layer is evictable only when
-- no live image references it.
CREATE TABLE oci_image (
	digest      TEXT    PRIMARY KEY, -- platform manifest digest
	registry    TEXT    NOT NULL,    -- where it was first pulled from
	repo        TEXT    NOT NULL,
	manifest    BLOB    NOT NULL,
	config      BLOB    NOT NULL,
	last_access INTEGER NOT NULL DEFAULT 0,
	evicted     INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

-- An image's layers, bottom (pos 0) to top.
CREATE TABLE oci_image_layer (
	image   TEXT    NOT NULL,
	pos     INTEGER NOT NULL,
	diff_id TEXT    NOT NULL,
	blob    TEXT    NOT NULL,
	PRIMARY KEY (image, pos)
) WITHOUT ROWID;
CREATE INDEX oci_image_layer_diff ON oci_image_layer (diff_id);

-- Compressed blob digest -> diff_id, with one place to fetch it from.
-- Several blobs (recompressions) can unpack to one diff_id.
CREATE TABLE oci_blob (
	digest     TEXT    PRIMARY KEY,
	diff_id    TEXT    NOT NULL,
	media_type TEXT    NOT NULL,
	size       INTEGER NOT NULL,
	registry   TEXT    NOT NULL,
	repo       TEXT    NOT NULL
) WITHOUT ROWID;
CREATE INDEX oci_blob_diff ON oci_blob (diff_id);

-- Unpacked size of a layer once fetched, so a refetch reserves quota up front.
CREATE TABLE oci_layer (
	diff_id       TEXT    PRIMARY KEY,
	unpacked_size INTEGER NOT NULL
) WITHOUT ROWID;
`},
}

type store struct{ db *sql.DB }

type imageRow struct {
	Digest, Registry, Repo string
	Manifest, Config       []byte
	Layers                 []layerRow // bottom to top
	Evicted                bool
}

type layerRow struct{ DiffID, Blob string }

// tagLike returns what a tag, or the digest of an index, resolved to.
func (s store) tagLike(ctx context.Context, r Ref, platform string) (image string, at time.Time, err error) {
	var ns int64
	err = s.db.QueryRowContext(ctx, `SELECT image, resolved_at FROM oci_tag WHERE registry = ? AND repo = ? AND tag = ? AND platform = ?`,
		r.Registry, r.Repo, r.Version(), platform).Scan(&image, &ns)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, meta.ErrNotExist
	}
	return image, time.Unix(0, ns), err
}

func (s store) image(ctx context.Context, digest string) (imageRow, error) {
	im := imageRow{Digest: digest}
	var ev int
	err := s.db.QueryRowContext(ctx, `SELECT registry, repo, manifest, config, evicted FROM oci_image WHERE digest = ?`, digest).
		Scan(&im.Registry, &im.Repo, &im.Manifest, &im.Config, &ev)
	if errors.Is(err, sql.ErrNoRows) {
		return im, meta.ErrNotExist
	}
	if err != nil {
		return im, err
	}
	im.Evicted = ev != 0
	rows, err := s.db.QueryContext(ctx, `SELECT diff_id, blob FROM oci_image_layer WHERE image = ? ORDER BY pos`, digest)
	if err != nil {
		return im, err
	}
	defer rows.Close()
	for rows.Next() {
		var l layerRow
		if err := rows.Scan(&l.DiffID, &l.Blob); err != nil {
			return im, err
		}
		im.Layers = append(im.Layers, l)
	}
	return im, rows.Err()
}

// putImage records an image, its layers and their blobs, and (for a tag
// reference) the tag, in one transaction.
func (s store) putImage(ctx context.Context, r Ref, platform string, im imageRow, blobs []Descriptor) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO oci_image (digest, registry, repo, manifest, config) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (digest) DO NOTHING`, im.Digest, im.Registry, im.Repo, im.Manifest, im.Config); err != nil {
		return err
	}
	for i, l := range im.Layers {
		if blobs == nil {
			break // a new name for a known image
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO oci_image_layer (image, pos, diff_id, blob) VALUES (?, ?, ?, ?)`,
			im.Digest, i, l.DiffID, l.Blob); err != nil {
			return err
		}
		b := blobs[i]
		// The latest source wins: it is the one most recently seen working.
		if _, err := tx.ExecContext(ctx, `INSERT INTO oci_blob (digest, diff_id, media_type, size, registry, repo) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (digest) DO UPDATE SET registry = excluded.registry, repo = excluded.repo`,
			b.Digest, l.DiffID, b.MediaType, b.Size, r.Registry, r.Repo); err != nil {
			return err
		}
	}
	if r.Version() != im.Digest {
		if _, err := tx.ExecContext(ctx, `INSERT INTO oci_tag (registry, repo, tag, platform, image, resolved_at) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (registry, repo, tag, platform) DO UPDATE SET image = excluded.image, resolved_at = excluded.resolved_at`,
			r.Registry, r.Repo, r.Version(), platform, im.Digest, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type blobRow struct {
	Digest, MediaType, Registry, Repo string
	Size                              int64
}

// blobFor returns a compressed blob that unpacks to diffID.
func (s store) blobFor(ctx context.Context, diffID string) (blobRow, error) {
	var b blobRow
	err := s.db.QueryRowContext(ctx, `SELECT digest, media_type, size, registry, repo FROM oci_blob WHERE diff_id = ?
		ORDER BY CASE media_type WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END LIMIT 1`, diffID, MediaOCILayerZstd, MediaOCILayerGzip).
		Scan(&b.Digest, &b.MediaType, &b.Size, &b.Registry, &b.Repo)
	if errors.Is(err, sql.ErrNoRows) {
		return b, meta.ErrNotExist
	}
	return b, err
}

// DiffID returns the diff_id a compressed layer blob unpacks to.
func (s store) diffID(ctx context.Context, blob string) (string, error) {
	var d string
	err := s.db.QueryRowContext(ctx, `SELECT diff_id FROM oci_blob WHERE digest = ?`, blob).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return "", meta.ErrNotExist
	}
	return d, err
}

func (s store) unpackedSize(ctx context.Context, diffID string) int64 {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT unpacked_size FROM oci_layer WHERE diff_id = ?`, diffID).Scan(&n); err != nil {
		return -1
	}
	return n
}

func (s store) setUnpackedSize(ctx context.Context, diffID string, n int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO oci_layer (diff_id, unpacked_size) VALUES (?, ?)
		ON CONFLICT (diff_id) DO UPDATE SET unpacked_size = excluded.unpacked_size`, diffID, n)
	return err
}

func (s store) touch(ctx context.Context, at map[string]time.Time) error {
	if len(at) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for d, t := range at {
		// Using an image makes it live again after an image-level eviction.
		if _, err := tx.ExecContext(ctx, `UPDATE oci_image SET last_access = max(last_access, ?), evicted = 0 WHERE digest = ?`, t.UnixNano(), d); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// liveRefs counts live (not evicted) images that use a layer.
func (s store) liveRefs(ctx context.Context, diffID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(DISTINCT l.image) FROM oci_image_layer l JOIN oci_image i ON i.digest = l.image
		WHERE l.diff_id = ? AND i.evicted = 0`, diffID).Scan(&n)
	return n, err
}
