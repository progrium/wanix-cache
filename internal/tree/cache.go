package tree

import (
	"context"
	"sync"

	"github.com/progrium/wanix-cache/internal/meta"
)

// The namespace cache keeps lookups, inodes and link counts in memory so
// walks, stats and opens over 9P skip SQLite once a path has been seen.
// Each 9P walk otherwise costs a query per name, and a stat one more for
// the link count, which held small-file reads to a few thousand files a
// second.
//
// Entries are kept under the metadata generation (meta.DB.Gen) read
// before the query that produced them, and the whole cache is dropped when
// the generation moves on, so a commit that changes the namespace is seen
// by the next request. Misses are not cached: a name that is not there yet
// is looked up (and faulted in) every time.

// cacheMax bounds each map. A full map is emptied rather than trimmed:
// the working set refills it quickly, and this keeps the cache simple.
const cacheMax = 1 << 18

type dirName struct {
	dir  uint64
	name string
}

type nsCache struct {
	mu     sync.RWMutex
	gen    uint64
	lookup map[dirName]meta.Inode
	inode  map[uint64]meta.Inode
	nlink  map[uint64]uint64
}

// valid reports whether the cache holds generation gen. Callers hold mu.
func (c *nsCache) valid(gen uint64) bool { return c.lookup != nil && c.gen == gen }

// reset empties the cache for generation gen. Callers hold mu for writing.
func (c *nsCache) reset(gen uint64) {
	c.gen = gen
	c.lookup = map[dirName]meta.Inode{}
	c.inode = map[uint64]meta.Inode{}
	c.nlink = map[uint64]uint64{}
}

// put runs fn to add entries read under generation gen, unless the
// namespace changed since.
func (c *nsCache) put(db *meta.DB, gen uint64, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if db.Gen() != gen {
		return
	}
	if !c.valid(gen) || len(c.lookup) >= cacheMax || len(c.inode) >= cacheMax || len(c.nlink) >= cacheMax {
		c.reset(gen)
	}
	fn()
}

// metaLookup is Meta.Lookup through the cache.
func (f *FS) metaLookup(ctx context.Context, dir uint64, name string) (meta.Inode, error) {
	gen := f.Meta.Gen()
	k := dirName{dir, name}
	f.cache.mu.RLock()
	in, ok := f.cache.lookup[k]
	ok = ok && f.cache.valid(gen)
	f.cache.mu.RUnlock()
	if ok {
		return in, nil
	}
	in, err := f.Meta.Lookup(ctx, dir, name)
	if err != nil {
		return in, err
	}
	f.cache.put(f.Meta, gen, func() {
		f.cache.lookup[k] = in
		f.cache.inode[in.Ino] = in
	})
	return in, nil
}

// metaGet is Meta.Get through the cache.
func (f *FS) metaGet(ctx context.Context, ino uint64) (meta.Inode, error) {
	gen := f.Meta.Gen()
	f.cache.mu.RLock()
	in, ok := f.cache.inode[ino]
	ok = ok && f.cache.valid(gen)
	f.cache.mu.RUnlock()
	if ok {
		return in, nil
	}
	in, err := f.Meta.Get(ctx, ino)
	if err != nil {
		return in, err
	}
	f.cache.put(f.Meta, gen, func() { f.cache.inode[ino] = in })
	return in, nil
}

// metaNlink is Meta.Nlink through the cache.
func (f *FS) metaNlink(ctx context.Context, ino uint64) (uint64, error) {
	gen := f.Meta.Gen()
	f.cache.mu.RLock()
	n, ok := f.cache.nlink[ino]
	ok = ok && f.cache.valid(gen)
	f.cache.mu.RUnlock()
	if ok {
		return n, nil
	}
	n, err := f.Meta.Nlink(ctx, ino)
	if err != nil {
		return n, err
	}
	f.cache.put(f.Meta, gen, func() { f.cache.nlink[ino] = n })
	return n, nil
}
