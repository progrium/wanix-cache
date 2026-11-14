package meta

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	v1 := []Migration{{1, `CREATE TABLE apk_index (name TEXT PRIMARY KEY)`}}
	if err := db.Migrate(ctx, "apk", v1); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, "apk", v1); err != nil {
		t.Fatalf("re-applying: %v", err)
	}
	v2 := append(v1, Migration{2, `ALTER TABLE apk_index ADD COLUMN version TEXT`})
	if err := db.Migrate(ctx, "apk", v2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`INSERT INTO apk_index (name, version) VALUES ('musl', '1.2.5-r0')`); err != nil {
		t.Fatalf("v2 column missing: %v", err)
	}
	if err := db.Migrate(ctx, "apk", v1); err == nil {
		t.Error("older binary accepted a newer schema")
	}
	if err := db.Migrate(ctx, "nix", []Migration{{2, `SELECT 1`}}); err == nil {
		t.Error("gap in versions accepted")
	}
	bad := []Migration{{1, `CREATE TABLE oci_x (a)`}, {2, `NOT SQL`}}
	if err := db.Migrate(ctx, "oci", bad); err == nil {
		t.Error("bad migration accepted")
	}
	db.Close()
	// Reopening applies nothing twice; the failed oci migration 2 retries.
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx, "oci", bad[:1]); err != nil {
		t.Fatalf("oci v1 should already be applied: %v", err)
	}
}

func TestWhiteoutAndFlags(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	tx, _ := db.Begin(ctx)
	d, _ := tx.MkdirAll("/layer/etc")
	if _, err := tx.Whiteout(d, "old.conf", 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetFlags(d, FlagOpaque); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	w, err := db.Lookup(ctx, d, "old.conf")
	if err != nil || w.Kind != KindWhiteout {
		t.Fatalf("whiteout = %+v, %v", w, err)
	}
	dir, _ := db.Get(ctx, d)
	if dir.Flags&FlagOpaque == 0 {
		t.Fatal("opaque flag not stored")
	}
}

// A cache made with the CAS keeps its manifests: resident units become
// evicted with their content cleared, so they refault into the file store.
func TestMigrateFromCAS(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin(ctx)
	id, _ := tx.CreateUnit("local", "a")
	root, _ := tx.MkdirAll("/local/a")
	ino, err := tx.File(root, "f", 0o644, 3, time.Unix(0, 0), "sha224-cafe", id)
	if err != nil {
		t.Fatal(err)
	}
	tx.RefContent("sha256-beef", 3)
	tx.SetUnitState(id, UnitAbsent, UnitFetching)
	tx.SetUnitState(id, UnitFetching, UnitSpooled)
	tx.SetUnitState(id, UnitSpooled, UnitResident)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Back to the CAS schema version.
	if _, err := db.db.Exec(`DELETE FROM schema_migrations WHERE owner = 'core' AND version = 2`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if db, err = Open(path); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	u, err := db.UnitByID(ctx, id)
	if err != nil || u.State != UnitEvicted {
		t.Fatalf("unit %+v, %v; want evicted", u, err)
	}
	in, err := db.Get(ctx, ino)
	if err != nil || in.Content != "" || in.Size != 3 {
		t.Fatalf("inode %+v, %v; want the manifest kept with no content", in, err)
	}
	if s, _ := db.Stats(ctx); s.Contents != 0 {
		t.Errorf("%d content rows left", s.Contents)
	}
}
