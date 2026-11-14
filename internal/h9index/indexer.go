package h9index

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/progrium/wanix-cache/internal/meta"
)

// ErrNotReady is returned for a root whose tree is still being filled: a
// unit under it is absent or fetching, so its listing in the metadata may
// be missing names, and an index would wrongly tell h9 they don't exist.
var ErrNotReady = errors.New("h9index: root is not ready")

// Indexer generates and stores the indexes of roots. It implements
// subsys.RootIndexer, the hook subsystems call once a root's tree is
// complete in the metadata.
type Indexer struct {
	DB      *meta.DB
	Store   *Store
	Sources map[Variant]Source
	// Resolve, if set, finds a root directory by namespace path, for roots
	// that aren't plain dirents (virtual view directories). Default: walk
	// the dirents without faulting anything in.
	Resolve func(ctx context.Context, p string) (uint64, error)
}

// Result describes one root's indexing.
type Result struct {
	Root    string
	Dir     uint64
	Stats   map[Variant]Stats
	Changed bool // some variant's text differs from what was stored
}

// IndexRoot is the subsystem hook: it indexes the root at namespace path
// root and discards the summary.
func (ix *Indexer) IndexRoot(ctx context.Context, root string) error {
	_, err := ix.Index(ctx, root)
	return err
}

// Index generates the index of every variant for the root directory at
// namespace path root ("/local/alpine") and stores it, where the servers
// serve it as root/.h9index. It reads metadata only: it never faults a unit
// in or reads file bytes, so an evicted root can be indexed. It fails with
// ErrNotReady while a unit under the root is absent or fetching.
func (ix *Indexer) Index(ctx context.Context, root string) (Result, error) {
	res := Result{Root: path.Clean("/" + root), Stats: map[Variant]Stats{}}
	dir, err := ix.resolve(ctx, res.Root)
	if err != nil {
		return res, err
	}
	res.Dir = dir
	if err := ix.ready(ctx, dir); err != nil {
		return res, fmt.Errorf("%w: %s: %v", ErrNotReady, res.Root, err)
	}
	for _, v := range Variants {
		src := ix.Sources[v]
		if src == nil {
			continue
		}
		text, st, err := Generate(ctx, src, dir)
		if err != nil {
			return res, err
		}
		changed, err := ix.Store.Put(ctx, dir, v, text, st.Entries)
		if err != nil {
			return res, err
		}
		res.Stats[v] = st
		res.Changed = res.Changed || changed
	}
	return res, nil
}

// IndexAll regenerates every stored root's index, for example after a
// format change. Roots that are no longer directories are forgotten.
func (ix *Indexer) IndexAll(ctx context.Context) ([]Result, error) {
	dirs, err := ix.Store.Roots(ctx)
	if err != nil {
		return nil, err
	}
	var out []Result
	for _, d := range dirs {
		p, err := ix.DB.PathOf(ctx, d)
		if errors.Is(err, meta.ErrNotExist) {
			if err := ix.Store.Forget(ctx, d); err != nil {
				return out, err
			}
			continue
		} else if err != nil {
			return out, err
		}
		r, err := ix.Index(ctx, p)
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

// resolve finds a directory by path without faulting anything in.
func (ix *Indexer) resolve(ctx context.Context, p string) (uint64, error) {
	if ix.Resolve != nil {
		return ix.Resolve(ctx, p)
	}
	cur, err := ix.DB.Get(ctx, meta.RootIno)
	if err != nil {
		return 0, err
	}
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		if name == "" {
			continue
		}
		if cur, err = ix.DB.Lookup(ctx, cur.Ino, name); err != nil {
			return 0, fmt.Errorf("h9index: %s: %w", p, err)
		}
	}
	if cur.Kind != meta.KindDir {
		return 0, fmt.Errorf("h9index: %s: %w", p, meta.ErrNotDir)
	}
	return cur.Ino, nil
}

// ready checks that no unit owning an inode under dir is absent or
// fetching. Spooled, resident and evicted units have their whole manifest.
func (ix *Indexer) ready(ctx context.Context, dir uint64) error {
	rows, err := ix.DB.SQL().QueryContext(ctx, `WITH RECURSIVE t(ino) AS (
			SELECT ?
			UNION
			SELECT d.ino FROM dirent d JOIN t ON d.parent = t.ino JOIN inode i ON i.ino = d.ino WHERE i.kind = ?
		)
		SELECT u.subsystem, u.key, u.state FROM unit u WHERE u.state IN (?, ?) AND u.id IN (
			SELECT i.unit FROM inode i WHERE i.ino IN t
			UNION
			SELECT i.unit FROM dirent d JOIN inode i ON i.ino = d.ino WHERE d.parent IN t
		) LIMIT 1`, dir, meta.KindDir, meta.UnitAbsent, meta.UnitFetching)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		var sub, key string
		var state meta.UnitState
		if err := rows.Scan(&sub, &key, &state); err != nil {
			return err
		}
		return fmt.Errorf("unit %s/%s is %v", sub, key, state)
	}
	return rows.Err()
}
