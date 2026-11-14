package h9index

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"time"

	"github.com/progrium/wanix-cache/internal/meta"
)

// Owner is the owner name of this package's migrations and the prefix of
// its tables.
const Owner = "h9index"

// Migrations are the package's schema changes, applied by Open. Never edit
// or renumber one once merged; add a new version.
var Migrations = []meta.Migration{
	// One generated index per root directory and variant. dir_mtime and
	// dir_unit pin the row to the directory it was generated for, so a
	// reused inode number never serves another tree's index. The text is
	// kept here, not in the CAS: it is metadata, survives eviction with the
	// manifest, and is a few hundred bytes a name.
	{Version: 1, SQL: `CREATE TABLE h9index_file (
		dir       INTEGER NOT NULL,
		variant   TEXT    NOT NULL,
		format    INTEGER NOT NULL,
		dir_mtime INTEGER NOT NULL,
		dir_unit  INTEGER NOT NULL,
		text      BLOB    NOT NULL,
		vers      INTEGER NOT NULL,
		mtime     INTEGER NOT NULL,
		entries   INTEGER NOT NULL,
		PRIMARY KEY (dir, variant)
	) WITHOUT ROWID`},
}

// Store keeps generated indexes in the metadata database.
type Store struct {
	db  *meta.DB
	get *sql.Stmt
}

// Open applies the package's migrations and returns the store.
func Open(ctx context.Context, db *meta.DB) (*Store, error) {
	if err := db.Migrate(ctx, Owner, Migrations); err != nil {
		return nil, err
	}
	get, err := db.SQL().PrepareContext(ctx, `SELECT f.vers, f.mtime, length(f.text) FROM h9index_file f LEFT JOIN inode i ON i.ino = f.dir
		WHERE f.dir = ? AND f.variant = ? AND f.format = ?
		AND (i.ino IS NULL AND f.dir_unit = -1 OR i.mtime = f.dir_mtime AND i.unit = f.dir_unit)`)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, get: get}, nil
}

// File is a stored index as the servers present it: a read-only file
// named Name in its root directory.
type File struct {
	Dir     uint64 // the root directory
	Variant Variant
	Ino     uint64 // qid.path; see FileIno
	Size    int64
	Vers    uint32 // changes whenever the text does
	Mtime   time.Time
}

// virtualBit marks the inode numbers of virtual view directories, which
// are served from a view rather than the inode table (subsys.VirtualBit).
const virtualBit = 1 << 61

func isVirtual(ino uint64) bool { return ino&^(virtualBit-1) == virtualBit }

// FileIno is the qid.path of directory dir's index for variant v. Inode
// numbers are SQLite rowids, and virtual view inodes use only bit 61 and
// below, so the top two bits are free: these can't collide with a real or
// virtual inode, and they are the same in every run.
func FileIno(dir uint64, v Variant) uint64 {
	ino := dir | 1<<63
	if v == DotL {
		ino |= 1 << 62
	}
	return ino
}

// IsFileIno reports whether ino names an index file, and for which root.
func IsFileIno(ino uint64) (dir uint64, v Variant, ok bool) {
	if ino&(1<<63) == 0 {
		return 0, "", false
	}
	v = NineP
	if ino&(1<<62) != 0 {
		v = DotL
	}
	return ino &^ (3 << 62), v, true
}

// Get returns directory dir's index for variant v, if one is stored for
// this directory in the current format.
func (s *Store) Get(ctx context.Context, dir uint64, v Variant) (File, bool, error) {
	f := File{Dir: dir, Variant: v, Ino: FileIno(dir, v)}
	var vers int64
	var mtime int64
	err := s.get.QueryRowContext(ctx, dir, string(v), FormatVersion).Scan(&vers, &mtime, &f.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return f, false, nil
	} else if err != nil {
		return f, false, err
	}
	f.Vers, f.Mtime = uint32(vers), time.Unix(0, mtime)
	return f, true, nil
}

// Read returns the text of directory dir's index for variant v.
func (s *Store) Read(ctx context.Context, dir uint64, v Variant) ([]byte, error) {
	var b []byte
	err := s.db.SQL().QueryRowContext(ctx, `SELECT text FROM h9index_file WHERE dir = ? AND variant = ? AND format = ?`,
		dir, string(v), FormatVersion).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, meta.ErrNotExist
	}
	return b, err
}

// Put stores the index text for directory dir and variant v. If the
// stored text is the same, nothing changes, so the file's qid and mtime
// stay put and h9's host cache keeps hitting.
func (s *Store) Put(ctx context.Context, dir uint64, v Variant, text []byte, entries int) (changed bool, err error) {
	// A real directory pins the row to its mtime and unit; a virtual view
	// directory (OCI), which has no inode row, is marked with unit -1.
	in := meta.Inode{Unit: -1}
	var stamp int64
	if !isVirtual(dir) {
		var err error
		if in, err = s.db.Get(ctx, dir); err != nil {
			return false, err
		}
		if in.Kind != meta.KindDir {
			return false, meta.ErrNotDir
		}
		stamp = in.Mtime.UnixNano()
	}
	tx, err := s.db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var old []byte
	var format, dmt, dunit int64
	err = tx.QueryRowContext(ctx, `SELECT text, format, dir_mtime, dir_unit FROM h9index_file WHERE dir = ? AND variant = ?`, dir, string(v)).
		Scan(&old, &format, &dmt, &dunit)
	if err == nil && format == FormatVersion && dmt == stamp && dunit == in.Unit && bytes.Equal(old, text) {
		return false, nil
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	h := fnv.New32a()
	h.Write(text)
	_, err = tx.ExecContext(ctx, `INSERT INTO h9index_file (dir, variant, format, dir_mtime, dir_unit, text, vers, mtime, entries)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (dir, variant) DO UPDATE SET format = excluded.format, dir_mtime = excluded.dir_mtime,
			dir_unit = excluded.dir_unit, text = excluded.text, vers = excluded.vers, mtime = excluded.mtime,
			entries = excluded.entries`,
		dir, string(v), FormatVersion, stamp, in.Unit, text, h.Sum32(), time.Now().UnixNano(), entries)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// Forget removes every index of directory dir, for when its tree is
// deleted from the metadata.
func (s *Store) Forget(ctx context.Context, dir uint64) error {
	_, err := s.db.SQL().ExecContext(ctx, `DELETE FROM h9index_file WHERE dir = ?`, dir)
	return err
}

// Roots lists the directories that have an index, in inode order.
func (s *Store) Roots(ctx context.Context) ([]uint64, error) {
	rows, err := s.db.SQL().QueryContext(ctx, `SELECT DISTINCT dir FROM h9index_file ORDER BY dir`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var d uint64
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
