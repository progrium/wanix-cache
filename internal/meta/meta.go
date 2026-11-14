// Package meta is the cache's metadata store: the namespace (inodes and
// directory entries), cached units and their states, and content objects.
//
// It replaces the prototype's index.json, which was rewritten in full on
// every change and scanned in full on every lookup. Here a lookup is one
// primary-key read on (parent, name), a directory listing is a range scan
// over that key, and writes are batched in transactions.
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// RootIno is the inode number of the namespace root.
const RootIno uint64 = 1

// Kind is the type of an inode.
type Kind uint8

const (
	KindDir Kind = iota + 1
	KindFile
	KindSymlink
	KindWhiteout // OCI: hides the same name in lower layers
)

func (k Kind) String() string {
	switch k {
	case KindDir:
		return "dir"
	case KindFile:
		return "file"
	case KindSymlink:
		return "symlink"
	case KindWhiteout:
		return "whiteout"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Inode is one namespace object. Hard links are several dirents that point
// at one inode; directories always have exactly one dirent and record it in
// Parent so ".." can be resolved.
type Inode struct {
	Ino     uint64
	Kind    Kind
	Mode    fs.FileMode // permission bits only
	Size    int64
	Mtime   time.Time
	Content string // CAS ref of a file's bytes; empty for an empty file
	Target  string // symlink target
	Parent  uint64 // directories only
	Unit    int64  // owning unit; 0 for namespace scaffolding
	Flags   uint32 // FlagOpaque, ...
}

// Dirent is a named entry in a directory listing.
type Dirent struct {
	Name string
	Inode
}

var (
	ErrNotExist = fs.ErrNotExist
	ErrExist    = fs.ErrExist
	ErrNotDir   = errors.New("not a directory")
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS unit (
	id            INTEGER PRIMARY KEY,
	subsystem     TEXT    NOT NULL,
	key           TEXT    NOT NULL,
	state         INTEGER NOT NULL,
	root          INTEGER NOT NULL DEFAULT 0,
	logical_bytes INTEGER NOT NULL DEFAULT 0,
	last_access   INTEGER NOT NULL DEFAULT 0,
	UNIQUE (subsystem, key)
);
CREATE TABLE IF NOT EXISTS inode (
	ino     INTEGER PRIMARY KEY,
	kind    INTEGER NOT NULL,
	mode    INTEGER NOT NULL,
	size    INTEGER NOT NULL DEFAULT 0,
	mtime   INTEGER NOT NULL DEFAULT 0,
	content TEXT    NOT NULL DEFAULT '',
	target  TEXT    NOT NULL DEFAULT '',
	parent  INTEGER NOT NULL DEFAULT 0,
	unit    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS inode_unit ON inode (unit);
CREATE TABLE IF NOT EXISTS dirent (
	parent INTEGER NOT NULL,
	name   TEXT    NOT NULL,
	ino    INTEGER NOT NULL,
	PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dirent_ino ON dirent (ino);
CREATE TABLE IF NOT EXISTS content (
	digest TEXT    PRIMARY KEY,
	ref    TEXT    NOT NULL,
	size   INTEGER NOT NULL,
	refs   INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
`

// DB is the metadata store. It is safe for concurrent use; SQLite in WAL
// mode lets readers run alongside one writer.
type DB struct {
	db *sql.DB

	// gen counts committed changes to the namespace (inodes and dirents),
	// for readers that cache it. See Gen.
	gen atomic.Uint64

	getInode *sql.Stmt
	lookup   *sql.Stmt
}

// Open opens or creates the metadata database at path.
func Open(path string) (*DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Keep idle connections. database/sql keeps 2 by default, so with more
	// concurrent readers (one per 9P request in flight) it closed and
	// reopened connections all the time, and each new SQLite connection
	// re-reads the schema and re-prepares its statements: most of the CPU
	// of a walk.
	sdb.SetMaxIdleConns(64)
	if _, err := sdb.Exec(schemaSQL); err != nil {
		sdb.Close()
		return nil, fmt.Errorf("meta: schema: %w", err)
	}
	if err := (&DB{db: sdb}).Migrate(context.Background(), "core", coreMigrations); err != nil {
		sdb.Close()
		return nil, err
	}
	d := &DB{db: sdb}
	if _, err := sdb.Exec(`INSERT OR IGNORE INTO inode (ino, kind, mode, mtime, parent) VALUES (?, ?, ?, ?, ?)`,
		RootIno, KindDir, 0o555, time.Now().UnixNano(), RootIno); err != nil {
		sdb.Close()
		return nil, err
	}
	const cols = `i.ino, i.kind, i.mode, i.size, i.mtime, i.content, i.target, i.parent, i.unit, i.flags`
	if d.getInode, err = sdb.Prepare(`SELECT ` + cols + ` FROM inode i WHERE i.ino = ?`); err != nil {
		sdb.Close()
		return nil, err
	}
	if d.lookup, err = sdb.Prepare(`SELECT ` + cols + ` FROM dirent d JOIN inode i ON i.ino = d.ino WHERE d.parent = ? AND d.name = ?`); err != nil {
		sdb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error { return d.db.Close() }

// Gen returns the namespace generation. It goes up after every committed
// transaction that changed an inode or a directory entry, and on Changed.
// A reader that caches the namespace reads Gen before it queries, keeps
// the result under that generation, and drops it once Gen moves on.
//
// It only sees this process's writes. Other processes that write the
// metadata (wanix-cache import, gc) add units, which a cache that keeps
// no negative lookups sees anyway, and clear content refs, which callers
// that open files must re-read uncached (as fill.Manager does).
func (d *DB) Gen() uint64 { return d.gen.Load() }

// Changed bumps the namespace generation, for code that writes inodes or
// dirents through SQL rather than through a Tx.
func (d *DB) Changed() { d.gen.Add(1) }

type scanner interface{ Scan(...any) error }

func scanInode(s scanner, extra ...any) (Inode, error) {
	var in Inode
	var mode uint32
	var mtime int64
	dst := append(extra, &in.Ino, &in.Kind, &mode, &in.Size, &mtime, &in.Content, &in.Target, &in.Parent, &in.Unit, &in.Flags)
	if err := s.Scan(dst...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return in, ErrNotExist
		}
		return in, err
	}
	in.Mode = fs.FileMode(mode).Perm()
	in.Mtime = time.Unix(0, mtime)
	return in, nil
}

// Get returns the inode with number ino.
func (d *DB) Get(ctx context.Context, ino uint64) (Inode, error) {
	return scanInode(d.getInode.QueryRowContext(ctx, ino))
}

// Lookup returns the inode named name in directory parent.
func (d *DB) Lookup(ctx context.Context, parent uint64, name string) (Inode, error) {
	return scanInode(d.lookup.QueryRowContext(ctx, parent, name))
}

// ReadDir lists up to limit entries of directory dir whose names sort after
// the given name ("" starts at the beginning), in name order.
func (d *DB) ReadDir(ctx context.Context, dir uint64, after string, limit int) ([]Dirent, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT d.name, i.ino, i.kind, i.mode, i.size, i.mtime, i.content, i.target, i.parent, i.unit, i.flags
		FROM dirent d JOIN inode i ON i.ino = d.ino
		WHERE d.parent = ? AND d.name > ? ORDER BY d.name LIMIT ?`, dir, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Dirent
	for rows.Next() {
		var de Dirent
		in, err := scanInode(rows, &de.Name)
		if err != nil {
			return nil, err
		}
		de.Inode = in
		out = append(out, de)
	}
	return out, rows.Err()
}

// Stats is a summary of the store's contents.
type Stats struct {
	Inodes       int64
	Units        int64
	Contents     int64
	ContentBytes int64 // logical bytes of distinct content objects
	FileBytes    int64 // logical bytes of all file inodes
}

func (d *DB) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := d.db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM inode),
		(SELECT count(*) FROM unit),
		(SELECT count(*) FROM content),
		(SELECT coalesce(sum(size), 0) FROM content),
		(SELECT coalesce(sum(size), 0) FROM inode WHERE kind = ?)`, KindFile).
		Scan(&s.Inodes, &s.Units, &s.Contents, &s.ContentBytes, &s.FileBytes)
	return s, err
}

// ReadDirAt lists up to limit entries of directory dir, skipping the first
// skip entries in name order. It serves clients whose directory cursors are
// positions rather than names (9P2000.L readdir offsets).
func (d *DB) ReadDirAt(ctx context.Context, dir uint64, skip uint64, limit int) ([]Dirent, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT d.name, i.ino, i.kind, i.mode, i.size, i.mtime, i.content, i.target, i.parent, i.unit, i.flags
		FROM dirent d JOIN inode i ON i.ino = d.ino
		WHERE d.parent = ? ORDER BY d.name LIMIT ? OFFSET ?`, dir, limit, skip)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Dirent
	for rows.Next() {
		var de Dirent
		in, err := scanInode(rows, &de.Name)
		if err != nil {
			return nil, err
		}
		de.Inode = in
		out = append(out, de)
	}
	return out, rows.Err()
}

// Nlink returns the number of directory entries that name an inode.
func (d *DB) Nlink(ctx context.Context, ino uint64) (uint64, error) {
	var n uint64
	err := d.db.QueryRowContext(ctx, `SELECT count(*) FROM dirent WHERE ino = ?`, ino).Scan(&n)
	return n, err
}

// SQL returns the underlying database handle, for subsystem-owned tables
// (<name>_<table>) and their queries. Subsystems must not write the core
// tables (inode, dirent, unit, content) through it.
func (d *DB) SQL() *sql.DB { return d.db }
