package savings

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// memSub serves units of flat files from memory.
type memSub struct {
	name  string
	units map[subsys.Key]map[string][]byte
}

func (s *memSub) Name() string                 { return s.name }
func (s *memSub) Migrations() []meta.Migration { return nil }
func (s *memSub) Resolve(_ context.Context, p string) (subsys.Key, string, error) {
	k, rest, _ := strings.Cut(p, "/")
	if _, ok := s.units[subsys.Key(k)]; !ok {
		return "", "", fs.ErrNotExist
	}
	return subsys.Key(k), rest, nil
}
func (s *memSub) Describe(context.Context, subsys.Key) (subsys.Info, error) {
	return subsys.Info{Size: -1}, nil
}
func (s *memSub) Fetch(_ context.Context, k subsys.Key, sink subsys.Sink) error {
	names := make([]string, 0)
	for n := range s.units[k] {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w, err := sink.File(n, 0o644, int64(len(s.units[k][n])), time.Unix(0, 0))
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, bytes.NewReader(s.units[k][n])); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestMeasure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := cas.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := fill.OpenDiskSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	q := &fill.LocalQuota{CAS: func() int64 { u, _ := store.Usage(); return u }, Spool: sp.Bytes}
	shadow := filepath.Join(dir, "shadow")
	m, err := fill.NewManager(db, store, sp, q, fill.Options{Shadow: shadow, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	shared, own := randBytes(200<<10), randBytes(100<<10)
	aa := &memSub{name: "aa", units: map[subsys.Key]map[string][]byte{
		"u1": {"x": shared, "small": []byte("hello")},
		"u2": {"x": shared},
	}}
	bb := &memSub{name: "bb", units: map[subsys.Key]map[string][]byte{
		"v1": {"z": shared, "w": own},
	}}
	for _, s := range []*memSub{aa, bb} {
		if err := m.Register(ctx, s); err != nil {
			t.Fatal(err)
		}
		for k := range s.units {
			if _, err := m.Ensure(ctx, s.name, k, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	r, err := Measure(ctx, db, store, Options{Dir: dir, Shadow: shadow})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := r.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + buf.String())

	tot := r.Total
	logical := int64(3*len(shared) + 5 + len(own))
	distinct := int64(len(shared) + 5 + len(own))
	if tot.Units != 3 || tot.Files != 5 || tot.Logical != logical {
		t.Fatalf("units %d files %d logical %d, want 3, 5, %d", tot.Units, tot.Files, tot.Logical, logical)
	}
	if tot.Contents != 3 || tot.ContentBytes != distinct {
		t.Fatalf("contents %d / %d bytes, want 3 / %d", tot.Contents, tot.ContentBytes, distinct)
	}
	if tot.SchemaBlobs < 3 || tot.DataBlobs < 3 || tot.Blobs < tot.SchemaBlobs+tot.DataBlobs {
		t.Errorf("blobs: %d schema, %d data, %d on disk", tot.SchemaBlobs, tot.DataBlobs, tot.Blobs)
	}
	if tot.DataBytes != distinct {
		t.Errorf("unique chunk bytes %d, want %d (random data does not chunk-dedupe)", tot.DataBytes, distinct)
	}
	if tot.CASTotal < tot.BlobAllocated || tot.BlobOverhead <= 0 || tot.DB <= 0 {
		t.Errorf("CAS total %d, blob allocated %d, overhead %d, db %d", tot.CASTotal, tot.BlobAllocated, tot.BlobOverhead, tot.DB)
	}
	if tot.B2 == nil || tot.B2.Units != 3 || tot.B2.Files != 5 || tot.B2.Logical != logical {
		t.Fatalf("B2 = %+v", tot.B2)
	}
	// In a corpus this small the metadata DB dominates CAS total, so check
	// the store alone.
	if tot.StoreAllocated > tot.B2.Allocated*6/10 || tot.B3Estimate >= tot.B2.Allocated {
		t.Errorf("store %d, B3 estimate %d, B2 %d", tot.StoreAllocated, tot.B3Estimate, tot.B2.Allocated)
	}
	if want := 1 - float64(tot.CASTotal)/float64(tot.B2.Allocated); tot.Ratios.SavingsB2 != want {
		t.Errorf("savings vs B2 %.4f, want %.4f", tot.Ratios.SavingsB2, want)
	}

	sub := map[string]Subsystem{}
	for _, s := range r.Subsystems {
		sub[s.Name] = s
	}
	a, b := sub["aa"], sub["bb"]
	if a.Units != 2 || b.Units != 1 {
		t.Fatalf("units aa %d bb %d", a.Units, b.Units)
	}
	if a.SharedWith["bb"] < int64(len(shared)) || b.SharedWith["aa"] != a.SharedWith["bb"] {
		t.Errorf("shared: aa->bb %d, bb->aa %d", a.SharedWith["bb"], b.SharedWith["aa"])
	}
	if b.ExclusiveBytes < int64(len(own)) || a.ExclusiveBytes < 5 || a.ExclusiveBytes > int64(len(own)) {
		t.Errorf("exclusive: aa %d, bb %d", a.ExclusiveBytes, b.ExclusiveBytes)
	}
	if got := a.Attributed + b.Attributed; got > tot.BlobAllocated || got < tot.BlobAllocated-tot.Blobs {
		t.Errorf("attributed %d, blob allocated %d", got, tot.BlobAllocated)
	}
	if a.B2 == nil || a.SavingsB2 <= b.SavingsB2 {
		t.Errorf("aa (a duplicate within and across) should save more than bb: %.3f vs %.3f", a.SavingsB2, b.SavingsB2)
	}

	if len(r.TopShared) == 0 || r.TopShared[0].Files != 3 || strings.Join(r.TopShared[0].Subsystems, ",") != "aa,bb" || r.TopShared[0].Path == "" {
		t.Errorf("top shared = %+v", r.TopShared)
	}
	if len(r.TopExclusiveUnits) == 0 || r.TopExclusiveUnits[0].Key != "v1" {
		t.Errorf("top exclusive = %+v", r.TopExclusiveUnits)
	}
	if r.SizeClasses[0].Contents != 1 || r.SizeClasses[3].Contents != 2 {
		t.Errorf("size classes = %+v", r.SizeClasses)
	}
	if r.Broken != 0 {
		t.Errorf("%d broken contents", r.Broken)
	}
}
