package h9index

import (
	"context"
	"fmt"
	"path"

	"9fans.net/go/plan9"
)

// Child is a directory entry as one server shows it, with the stat(5)
// fields it reports. Dir.Name is ignored.
type Child struct {
	Name string
	Ino  uint64 // the inode to list next if this is a directory
	plan9.Dir
}

// IsDir reports whether the child is a directory.
func (c Child) IsDir() bool { return c.Mode&plan9.DMDIR != 0 }

// Source is a root's tree as one server shows it, read from metadata
// only: it must not fault content in or read file bytes. The tree package
// provides one per Variant, built on the same stat functions its servers
// answer with.
type Source interface {
	// Stat returns the entry for directory ino.
	Stat(ctx context.Context, ino uint64) (Child, error)
	// ReadDir returns up to limit entries of directory ino after the
	// cursor ("" = start), in any order, leaving out what the server hides,
	// the cursor to continue from, and whether the listing is finished.
	ReadDir(ctx context.Context, ino uint64, after string, limit int) (ents []Child, next string, done bool, err error)
}

// Stats summarizes a generated index.
type Stats struct {
	Entries   int
	Dirs      int
	Unindexed int // directories with a name that can't be listed
	Bytes     int
}

const page = 1024

// Generate builds the index of the tree rooted at directory root.
func Generate(ctx context.Context, src Source, root uint64) ([]byte, Stats, error) {
	var st Stats
	top, err := src.Stat(ctx, root)
	if err != nil {
		return nil, st, err
	}
	if !top.IsDir() {
		return nil, st, fmt.Errorf("h9index: root %d is not a directory", root)
	}
	entries := []Entry{{Path: "/", Dir: top.Dir}}
	var unindexed []string
	type dir struct {
		ino uint64
		p   string
	}
	queue := []dir{{root, "/"}}
	for len(queue) > 0 {
		d := queue[0]
		queue = queue[1:]
		st.Dirs++
		bad := false
		after := ""
		for {
			if err := ctx.Err(); err != nil {
				return nil, st, err
			}
			ents, next, done, err := src.ReadDir(ctx, d.ino, after, page)
			if err != nil {
				return nil, st, fmt.Errorf("h9index: %s: %w", d.p, err)
			}
			for _, c := range ents {
				if d.p == "/" && c.Name == Name {
					continue // the index doesn't list itself
				}
				if !ValidName(c.Name) {
					bad = true
					continue
				}
				p := path.Join(d.p, c.Name)
				entries = append(entries, Entry{Path: p, Dir: c.Dir})
				if c.IsDir() {
					queue = append(queue, dir{c.Ino, p})
				}
			}
			if done {
				break
			}
			if next == after {
				return nil, st, fmt.Errorf("h9index: %s: listing made no progress", d.p)
			}
			after = next
		}
		if bad {
			unindexed = append(unindexed, d.p)
		}
	}
	b := Encode(entries, unindexed)
	st.Entries, st.Unindexed, st.Bytes = len(entries), len(unindexed), len(b)
	if len(b) > MaxSize {
		return nil, st, fmt.Errorf("h9index: index is %d bytes, over h9's %d byte limit", len(b), MaxSize)
	}
	return b, st, nil
}
