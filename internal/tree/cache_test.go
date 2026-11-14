package tree_test

import (
	"context"
	"testing"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/meta"
)

// The namespace cache must not serve an entry after a commit changes it.
func TestCacheSeesCommits(t *testing.T) {
	ctx := context.Background()
	db, _, root := setupL(t)
	tx, _ := db.Begin(ctx)
	unit, err := tx.CreateUnit("test", "u")
	if err != nil {
		t.Fatal(err)
	}
	dir, err := tx.Mkdir(meta.RootIno, "d", 0o755, time.Now(), unit)
	if err != nil {
		t.Fatal(err)
	}
	ino, err := tx.File(dir, "f", 0o644, 5, time.Now(), "", unit)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	size := func() (uint64, uint64) {
		t.Helper()
		f := walk(t, root, "d/f")
		defer f.Close()
		_, _, a, err := f.GetAttr(p9.AttrMaskAll)
		if err != nil {
			t.Fatal(err)
		}
		return a.Size, uint64(a.NLink)
	}
	if s, n := size(); s != 5 || n != 1 {
		t.Fatalf("size, nlink = %d, %d", s, n)
	}
	size() // cached now

	// An in-place update and a new hard link.
	tx, _ = db.Begin(ctx)
	if err := tx.UpdateFile(ino, 0o644, 7, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Link(dir, "g", ino); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s, n := size(); s != 7 || n != 2 {
		t.Fatalf("after update: size, nlink = %d, %d", s, n)
	}

	// A deleted tree.
	tx, _ = db.Begin(ctx)
	if err := tx.DeleteUnitTree(unit); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, f, err := root.Walk([]string{"d", "f"}); err == nil {
		f.Close()
		t.Fatal("walk to a deleted file succeeded")
	}
}
