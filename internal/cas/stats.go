package cas

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"syscall"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
)

// Parts splits content ref into its schema blobs (the file schema and any
// nested bytes schemas) and its data chunks, each once. Sizes are logical.
func (s *Store) Parts(ctx context.Context, ref string) (schemas, data []Blob, err error) {
	br, ok := blob.Parse(ref)
	if !ok {
		return nil, nil, fmt.Errorf("cas: bad blob ref %q", ref)
	}
	fr, err := schema.NewFileReader(ctx, s.q, br)
	if err != nil {
		return nil, nil, err
	}
	isSchema := map[blob.Ref]bool{br: true}
	isData := map[blob.Ref]bool{}
	err = fr.ForeachChunk(ctx, func(path []blob.Ref, p schema.BytesPart) error {
		for _, sr := range path {
			isSchema[sr] = true
		}
		if p.BlobRef.Valid() {
			isData[p.BlobRef] = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	refs := make([]blob.Ref, 0, len(isSchema)+len(isData))
	for r := range isSchema {
		refs = append(refs, r)
	}
	for r := range isData {
		if !isSchema[r] {
			refs = append(refs, r)
		}
	}
	n := 0
	err = s.q.Storage.StatBlobs(ctx, refs, func(sb blob.SizedRef) error {
		n++
		b := Blob{sb.Ref.String(), int64(sb.Size)}
		if isSchema[sb.Ref] {
			schemas = append(schemas, b)
		} else {
			data = append(data, b)
		}
		return nil
	})
	if err == nil && n != len(refs) {
		err = fmt.Errorf("cas: %s: %d of %d blobs missing: %w", br, len(refs)-n, len(refs), ErrMissing)
	}
	return schemas, data, err
}

// BlobFile is one blob as a file on disk.
type BlobFile struct {
	Ref       string
	Size      int64 // logical
	Allocated int64 // blocks the filesystem allocated, in bytes
}

// DiskTotals is the blob store's whole footprint on disk.
type DiskTotals struct {
	Allocated    int64 // everything under the store
	Dirs         int64 // localdisk's hash-prefix directories
	DirAllocated int64
}

// DiskUsage walks the blob store's files, calling fn for each blob file.
func (s *Store) DiskUsage(fn func(BlobFile)) (t DiskTotals, err error) {
	err = filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		a := Allocated(info)
		t.Allocated += a
		if d.IsDir() {
			t.Dirs++
			t.DirAllocated += a
		}
		name := d.Name()
		if !d.IsDir() && strings.HasSuffix(name, ".dat") && fn != nil {
			if br, ok := blob.Parse(strings.TrimSuffix(name, ".dat")); ok {
				fn(BlobFile{Ref: br.String(), Size: info.Size(), Allocated: a})
			}
		}
		return nil
	})
	return t, err
}

// Allocated returns the bytes a file occupies on disk (st_blocks × 512),
// or its size rounded up to 4 KiB where the platform does not say.
func Allocated(info fs.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512
	}
	return (info.Size() + 4095) &^ 4095
}
