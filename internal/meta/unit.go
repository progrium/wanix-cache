package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// UnitState is where a unit's bytes live.
//
//	absent   -> fetching            fault
//	fetching -> spooled | absent    fill verified | fill failed
//	spooled  -> resident            ingested into the file store
//	resident -> evicted             content evicted, manifest kept
//	evicted  -> fetching            refault
//	fetching -> evicted             refault failed (manifest still known)
type UnitState uint8

const (
	UnitAbsent UnitState = iota
	UnitFetching
	UnitSpooled
	UnitResident
	UnitEvicted
)

var unitStateNames = [...]string{"absent", "fetching", "spooled", "resident", "evicted"}

func (s UnitState) String() string {
	if int(s) < len(unitStateNames) {
		return unitStateNames[s]
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

var transitions = map[UnitState][]UnitState{
	UnitAbsent:   {UnitFetching},
	UnitFetching: {UnitSpooled, UnitAbsent, UnitEvicted},
	UnitSpooled:  {UnitResident},
	UnitResident: {UnitEvicted},
	UnitEvicted:  {UnitFetching},
}

// CanTransition reports whether a unit may move from one state to another.
func CanTransition(from, to UnitState) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Unit is one cached logical unit: an apk package, an OCI layer, a Nix
// store path, or (for now) an imported local tree.
type Unit struct {
	ID           int64
	Subsystem    string
	Key          string
	State        UnitState
	Root         uint64 // inode of the unit's top directory, once known
	LogicalBytes int64
	LastAccess   time.Time
}

// ErrBadTransition is returned for a state change the state machine forbids.
var ErrBadTransition = errors.New("meta: invalid unit state transition")

// Unit returns the unit with the given subsystem and key.
func (d *DB) Unit(ctx context.Context, subsystem, key string) (Unit, error) {
	return scanUnit(d.db.QueryRowContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit WHERE subsystem = ? AND key = ?`, subsystem, key))
}

// Units lists every unit of a subsystem ("" for all), in key order.
func (d *DB) Units(ctx context.Context, subsystem string) ([]Unit, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, subsystem, key, state, root, logical_bytes, last_access FROM unit
		WHERE ? = '' OR subsystem = ? ORDER BY subsystem, key`, subsystem, subsystem)
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

func scanUnit(s scanner) (Unit, error) {
	var u Unit
	var last int64
	err := s.Scan(&u.ID, &u.Subsystem, &u.Key, &u.State, &u.Root, &u.LogicalBytes, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return u, ErrNotExist
	}
	if last != 0 {
		u.LastAccess = time.Unix(0, last)
	}
	return u, err
}

// CreateUnit adds a unit in state absent and returns its id.
func (t *Tx) CreateUnit(subsystem, key string) (int64, error) {
	res, err := t.tx.ExecContext(t.ctx, `INSERT INTO unit (subsystem, key, state) VALUES (?, ?, ?)`, subsystem, key, UnitAbsent)
	if err != nil {
		if isUnique(err) {
			return 0, fmt.Errorf("meta: unit %s/%s: %w", subsystem, key, ErrExist)
		}
		return 0, err
	}
	return res.LastInsertId()
}

// SetUnitState moves a unit from one state to another. It fails if the
// unit is not currently in from, or the state machine forbids the move.
func (t *Tx) SetUnitState(id int64, from, to UnitState) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %v -> %v", ErrBadTransition, from, to)
	}
	res, err := t.tx.ExecContext(t.ctx, `UPDATE unit SET state = ? WHERE id = ? AND state = ?`, to, id, from)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: unit %d is not %v", ErrBadTransition, id, from)
	}
	return nil
}

// SetUnitManifest records a unit's root directory and logical size.
func (t *Tx) SetUnitManifest(id int64, root uint64, logicalBytes int64) error {
	_, err := t.tx.ExecContext(t.ctx, `UPDATE unit SET root = ?, logical_bytes = ? WHERE id = ?`, root, logicalBytes, id)
	return err
}

// TouchUnits records access times. Callers batch these in memory and flush
// them periodically so reads never write to the database.
func (d *DB) TouchUnits(ctx context.Context, at map[int64]time.Time) error {
	if len(at) == 0 {
		return nil
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := tx.PrepareContext(ctx, `UPDATE unit SET last_access = max(last_access, ?) WHERE id = ?`)
	if err != nil {
		return err
	}
	for id, t := range at {
		if _, err := st.ExecContext(ctx, t.UnixNano(), id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE")
}
