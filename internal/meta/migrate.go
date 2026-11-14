package meta

import (
	"context"
	"fmt"
	"sort"
)

// Migration is one numbered schema change owned by the core or by a
// subsystem. Versions start at 1, increase by one, and are never edited or
// reused once merged.
type Migration struct {
	Version int
	SQL     string
}

// coreMigrations change the core tables after the base schema in schemaSQL.
// Add new core changes here as the next version; never edit schemaSQL.
var coreMigrations = []Migration{
	// Whiteouts and opaque directories for OCI layers.
	{1, `ALTER TABLE inode ADD COLUMN flags INTEGER NOT NULL DEFAULT 0`},
}

// Inode flags.
const (
	FlagOpaque uint32 = 1 << 0 // directory hides lower layers' entries (OCI)
)

const migrationsSQL = `CREATE TABLE IF NOT EXISTS schema_migrations (
	owner   TEXT    NOT NULL,
	version INTEGER NOT NULL,
	PRIMARY KEY (owner, version)
) WITHOUT ROWID`

// Migrate applies the migrations of owner ("core" or a subsystem name) that
// have not been applied yet, each in its own transaction, in version order.
func (d *DB) Migrate(ctx context.Context, owner string, ms []Migration) error {
	if _, err := d.db.ExecContext(ctx, migrationsSQL); err != nil {
		return err
	}
	sorted := append([]Migration(nil), ms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })
	for i, m := range sorted {
		if m.Version != i+1 {
			return fmt.Errorf("meta: %s migrations must be numbered 1..n, found %d at position %d", owner, m.Version, i+1)
		}
	}
	var applied int
	if err := d.db.QueryRowContext(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations WHERE owner = ?`, owner).Scan(&applied); err != nil {
		return err
	}
	if applied > len(sorted) {
		return fmt.Errorf("meta: database has %s schema version %d, newer than this binary's %d", owner, applied, len(sorted))
	}
	for _, m := range sorted[applied:] {
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("meta: %s migration %d: %w", owner, m.Version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (owner, version) VALUES (?, ?)`, owner, m.Version); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
