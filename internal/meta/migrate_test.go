package meta

import (
	"context"
	"path/filepath"
	"testing"
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
