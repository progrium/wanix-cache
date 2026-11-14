package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"
)

// Tx is a write transaction. A unit's whole manifest is normally written in
// one Tx, so it appears in the namespace atomically.
type Tx struct {
	db   *DB
	tx   *sql.Tx
	ns   bool // wrote inodes or dirents: bump db.gen on commit
	ctx  context.Context
	dirs map[string]uint64 // MkdirAll cache, by cleaned path

	insInode  *sql.Stmt
	insDirent *sql.Stmt
}

// Begin starts a write transaction.
func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	t := &Tx{db: d, tx: tx, ctx: ctx, dirs: map[string]uint64{"/": RootIno}}
	if t.insInode, err = tx.PrepareContext(ctx, `INSERT INTO inode (kind, mode, size, mtime, content, target, parent, unit) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`); err != nil {
		tx.Rollback()
		return nil, err
	}
	if t.insDirent, err = tx.PrepareContext(ctx, `INSERT INTO dirent (parent, name, ino) VALUES (?, ?, ?)`); err != nil {
		tx.Rollback()
		return nil, err
	}
	return t, nil
}

func (t *Tx) Commit() error {
	err := t.tx.Commit()
	if t.ns {
		// Even after a failed commit: whether it landed is not certain.
		t.db.Changed()
	}
	return err
}

func (t *Tx) Rollback() error { return t.tx.Rollback() }

// exec runs a statement that writes inodes or dirents.
func (t *Tx) exec(q string, args ...any) (sql.Result, error) {
	t.ns = true
	return t.tx.ExecContext(t.ctx, q, args...)
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return fmt.Errorf("meta: invalid name %q", name)
	}
	return nil
}

func (t *Tx) kindOf(ino uint64) (Kind, error) {
	var k Kind
	err := t.tx.QueryRowContext(t.ctx, `SELECT kind FROM inode WHERE ino = ?`, ino).Scan(&k)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotExist
	}
	return k, err
}

func (t *Tx) create(parent uint64, name string, in Inode) (uint64, error) {
	if err := validName(name); err != nil {
		return 0, err
	}
	if k, err := t.kindOf(parent); err != nil {
		return 0, err
	} else if k != KindDir {
		return 0, ErrNotDir
	}
	if in.Kind == KindDir {
		in.Parent = parent
	}
	t.ns = true
	res, err := t.insInode.ExecContext(t.ctx, in.Kind, uint32(in.Mode.Perm()), in.Size, in.Mtime.UnixNano(), in.Content, in.Target, in.Parent, in.Unit)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := t.link(parent, name, uint64(id)); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

func (t *Tx) link(parent uint64, name string, ino uint64) error {
	t.ns = true
	if _, err := t.insDirent.ExecContext(t.ctx, parent, name, ino); err != nil {
		if isUnique(err) {
			return fmt.Errorf("meta: %q: %w", name, ErrExist)
		}
		return err
	}
	return nil
}

// Mkdir creates directory name in parent and returns its inode number.
func (t *Tx) Mkdir(parent uint64, name string, mode fs.FileMode, mtime time.Time, unit int64) (uint64, error) {
	return t.create(parent, name, Inode{Kind: KindDir, Mode: mode, Mtime: mtime, Unit: unit})
}

// File creates a regular file whose bytes are the CAS object content.
func (t *Tx) File(parent uint64, name string, mode fs.FileMode, size int64, mtime time.Time, content string, unit int64) (uint64, error) {
	return t.create(parent, name, Inode{Kind: KindFile, Mode: mode, Size: size, Mtime: mtime, Content: content, Unit: unit})
}

// Symlink creates a symbolic link.
func (t *Tx) Symlink(parent uint64, name, target string, mtime time.Time, unit int64) (uint64, error) {
	return t.create(parent, name, Inode{Kind: KindSymlink, Mode: 0o777, Size: int64(len(target)), Mtime: mtime, Target: target, Unit: unit})
}

// Link adds a hard link named name in parent to an existing non-directory.
func (t *Tx) Link(parent uint64, name string, ino uint64) error {
	if err := validName(name); err != nil {
		return err
	}
	k, err := t.kindOf(ino)
	if err != nil {
		return err
	}
	if k == KindDir {
		return errors.New("meta: hard link to a directory")
	}
	if pk, err := t.kindOf(parent); err != nil {
		return err
	} else if pk != KindDir {
		return ErrNotDir
	}
	return t.link(parent, name, ino)
}

// MkdirAll returns the directory at slash-separated path p, creating any
// missing directories with mode 0555 and no owning unit.
func (t *Tx) MkdirAll(p string) (uint64, error) {
	p = path.Clean("/" + p)
	if ino, ok := t.dirs[p]; ok {
		return ino, nil
	}
	parent, err := t.MkdirAll(path.Dir(p))
	if err != nil {
		return 0, err
	}
	name := path.Base(p)
	var ino uint64
	var k Kind
	err = t.tx.QueryRowContext(t.ctx, `SELECT i.ino, i.kind FROM dirent d JOIN inode i ON i.ino = d.ino WHERE d.parent = ? AND d.name = ?`, parent, name).Scan(&ino, &k)
	switch {
	case err == nil:
		if k != KindDir {
			return 0, fmt.Errorf("meta: %s: %w", p, ErrNotDir)
		}
	case errors.Is(err, sql.ErrNoRows):
		if ino, err = t.Mkdir(parent, name, 0o555, time.Now(), 0); err != nil {
			return 0, err
		}
	default:
		return 0, err
	}
	t.dirs[p] = ino
	return ino, nil
}

// ContentRef returns the CAS ref already stored for a whole-file digest.
func (t *Tx) ContentRef(digest string) (string, bool, error) {
	var ref string
	err := t.tx.QueryRowContext(t.ctx, `SELECT ref FROM content WHERE digest = ?`, digest).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return ref, err == nil, err
}

// RefContent records one more reference to a content object, creating it
// if it is new.
func (t *Tx) RefContent(digest, ref string, size int64) error {
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO content (digest, ref, size, refs) VALUES (?, ?, ?, 1)
		ON CONFLICT (digest) DO UPDATE SET refs = refs + 1`, digest, ref, size)
	return err
}

// ContentRef is the read-only form of Tx.ContentRef.
func (d *DB) ContentRef(ctx context.Context, digest string) (string, bool, error) {
	var ref string
	err := d.db.QueryRowContext(ctx, `SELECT ref FROM content WHERE digest = ?`, digest).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return ref, err == nil, err
}

// Whiteout records that name in parent hides lower layers' entries.
func (t *Tx) Whiteout(parent uint64, name string, unit int64) (uint64, error) {
	return t.create(parent, name, Inode{Kind: KindWhiteout, Unit: unit})
}

// SetFlags replaces an inode's flags, for example FlagOpaque on a directory.
func (t *Tx) SetFlags(ino uint64, flags uint32) error {
	_, err := t.exec(`UPDATE inode SET flags = ? WHERE ino = ?`, flags, ino)
	return err
}
