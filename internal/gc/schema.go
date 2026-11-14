package gc

import "github.com/progrium/wanix-cache/internal/meta"

// Owner is the GC's name in schema_migrations.
const Owner = "gc"

// Migrations are the GC's numbered schema changes. Never edit or renumber a
// merged one; add the next version instead.
var Migrations = []meta.Migration{
	{Version: 1, SQL: `
-- One row per CAS blob that some content object is made of. refs counts
-- content rows that use the blob (not inodes, not units). A blob at refs 0
-- is doomed: the next sweep deletes it unless an ingest revives it first.
CREATE TABLE gc_blob (
	ref       TEXT    PRIMARY KEY,
	size      INTEGER NOT NULL,
	refs      INTEGER NOT NULL DEFAULT 0,
	doomed_at INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE INDEX gc_blob_doomed ON gc_blob (ref) WHERE refs <= 0;

-- Which blobs each content object (by its CAS ref) is made of. A content
-- row with no rows here has not been tracked yet; the GC walks its schema
-- and fills them in (backfill) before it trusts any refcount.
CREATE TABLE gc_content_blob (
	content TEXT NOT NULL,
	blob    TEXT NOT NULL,
	PRIMARY KEY (content, blob)
) WITHOUT ROWID;
CREATE INDEX gc_content_blob_blob ON gc_content_blob (blob);

-- Durable pins: units that are never evicted while pinned, for example a
-- subsystem's package indexes. Each reason is a separate pin.
CREATE TABLE gc_pin (
	unit   INTEGER NOT NULL,
	reason TEXT    NOT NULL,
	PRIMARY KEY (unit, reason)
) WITHOUT ROWID;

-- Eviction looks content up by CAS ref and walks units in LRU order.
CREATE INDEX gc_content_ref ON content (ref);
CREATE INDEX gc_unit_lru ON unit (state, last_access, id);
`},
}
