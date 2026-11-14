package meta

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Queries and updates used by the fill path.

// UnitByID returns a unit by id.
func (d *DB) UnitByID(ctx context.Context, id int64) (Unit, error) {
	return scanUnit(d.db.QueryRowContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit WHERE id = ?`, id))
}

// UnitsInState lists units in one state.
func (d *DB) UnitsInState(ctx context.Context, s UnitState) ([]Unit, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit WHERE state = ? ORDER BY id`, s)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Unit
	for rows.Next() {
		u, err := scanUnit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// UnitFiles lists a unit's regular-file inodes.
func (d *DB) UnitFiles(ctx context.Context, unit int64) ([]Inode, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT i.ino, i.kind, i.mode, i.size, i.mtime, i.content, i.target, i.parent, i.unit, i.flags
		FROM inode i WHERE i.unit = ? AND i.kind = ? ORDER BY i.ino`, unit, KindFile)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Inode
	for rows.Next() {
		in, err := scanInode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// PathOf returns the absolute path of a directory, following parents.
func (d *DB) PathOf(ctx context.Context, dir uint64) (string, error) {
	var parts []string
	for dir != RootIno {
		var name string
		var parent uint64
		err := d.db.QueryRowContext(ctx, `SELECT d.name, d.parent FROM dirent d WHERE d.ino = ? LIMIT 1`, dir).Scan(&name, &parent)
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotExist
		} else if err != nil {
			return "", err
		}
		parts = append(parts, name)
		dir = parent
		if len(parts) > 4096 {
			return "", errors.New("meta: directory cycle")
		}
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return "/" + strings.Join(parts, "/"), nil
}

// Unit returns the unit with the given subsystem and key, inside the Tx.
func (t *Tx) Unit(subsystem, key string) (Unit, error) {
	return scanUnit(t.tx.QueryRowContext(t.ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit WHERE subsystem = ? AND key = ?`, subsystem, key))
}

// Lookup is DB.Lookup inside the Tx.
func (t *Tx) Lookup(parent uint64, name string) (Inode, error) {
	return scanInode(t.tx.QueryRowContext(t.ctx, `SELECT i.ino, i.kind, i.mode, i.size, i.mtime, i.content, i.target, i.parent, i.unit, i.flags
		FROM dirent d JOIN inode i ON i.ino = d.ino WHERE d.parent = ? AND d.name = ?`, parent, name))
}

// UpdateFile sets a file inode's mode, size and mtime, for a refault that
// reuses an inode from the kept manifest or a file whose size became known.
func (t *Tx) UpdateFile(ino uint64, mode uint32, size int64, mtime time.Time) error {
	_, err := t.tx.ExecContext(t.ctx, `UPDATE inode SET mode = ?, size = ?, mtime = ? WHERE ino = ?`, mode, size, mtime.UnixNano(), ino)
	return err
}

// SetContent sets a file inode's CAS ref ("" while its bytes are only in
// the spool or evicted).
func (t *Tx) SetContent(ino uint64, ref string) error {
	_, err := t.tx.ExecContext(t.ctx, `UPDATE inode SET content = ? WHERE ino = ?`, ref, ino)
	return err
}

// DeleteUnitTree removes every inode a unit owns and every dirent naming
// them, for a first fill that failed. It does not touch content refcounts.
func (t *Tx) DeleteUnitTree(unit int64) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM dirent WHERE ino IN (SELECT ino FROM inode WHERE unit = ?)`, unit); err != nil {
		return err
	}
	_, err := t.tx.ExecContext(t.ctx, `DELETE FROM inode WHERE unit = ?`, unit)
	return err
}
