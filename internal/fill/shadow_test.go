package fill

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

func TestShadow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := newEnv(t, dir, 0, Options{Shadow: filepath.Join(dir, "shadow")})
	s := &fakeSub{}
	s.add("u", "bin/", "", "bin/tool", "binary", "bin/again", "=>bin/tool", "bin/alias", "->tool", "empty", "")
	s.add("f", "", "a single file")
	e.m.Register(ctx, s)
	for _, k := range []subsys.Key{"u", "f"} {
		if _, err := e.m.Ensure(ctx, "fake", k, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.m.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	u, _ := e.db.Unit(ctx, "fake", "u")
	root := ShadowPath(e.m.ShadowDir(), u)
	check := func() {
		t.Helper()
		if b, err := os.ReadFile(filepath.Join(root, "bin/tool")); err != nil || string(b) != "binary" {
			t.Fatalf("shadow bin/tool = %q, %v", b, err)
		}
		a, _ := os.Stat(filepath.Join(root, "bin/tool"))
		b, _ := os.Stat(filepath.Join(root, "bin/again"))
		if a == nil || b == nil || a.Sys().(*syscall.Stat_t).Ino != b.Sys().(*syscall.Stat_t).Ino {
			t.Error("hard link not kept as a link")
		}
		if l, err := os.Readlink(filepath.Join(root, "bin/alias")); err != nil || l != "tool" {
			t.Errorf("symlink = %q, %v", l, err)
		}
		if fi, err := os.Stat(filepath.Join(root, "empty")); err != nil || fi.Size() != 0 {
			t.Errorf("empty file: %v", err)
		}
	}
	check()
	uf, _ := e.db.Unit(ctx, "fake", "f")
	if b, err := os.ReadFile(ShadowPath(e.m.ShadowDir(), uf)); err != nil || string(b) != "a single file" {
		t.Fatalf("file-root shadow = %q, %v", b, err)
	}
	if e.spool.Bytes() != 0 {
		t.Errorf("spool counts %d bytes; the shadow must not", e.spool.Bytes())
	}

	// Evicted: SyncShadow drops the tree. Refaulted: ingest writes it again.
	evict(t, e, u)
	if w, rm, err := e.m.SyncShadow(ctx); err != nil || w != 0 || rm != 1 {
		t.Fatalf("sync after evict: wrote %d removed %d: %v", w, rm, err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("evicted unit's shadow still there: %v", err)
	}
	e.read(t, "/fake/u/bin/tool")
	e.m.Wait(ctx)
	check()

	// A resident unit without a tree (shadow turned on later) is written
	// from the CAS.
	os.RemoveAll(root)
	if w, rm, err := e.m.SyncShadow(ctx); err != nil || w != 1 || rm != 0 {
		t.Fatalf("sync of a missing tree: wrote %d removed %d: %v", w, rm, err)
	}
	check()
}

func evict(t *testing.T, e *env, u meta.Unit) {
	t.Helper()
	ctx := context.Background()
	tx, _ := e.db.Begin(ctx)
	files, _ := e.db.UnitFiles(ctx, u.ID)
	for _, f := range files {
		tx.SetContent(f.Ino, "")
	}
	if err := tx.SetUnitState(u.ID, meta.UnitResident, meta.UnitEvicted); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
