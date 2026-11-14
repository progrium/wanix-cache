// Command ninebench reads every file of a settled savings-plan cache over
// 9P2000.L, with the file bytes coming from one of four stores, so the
// only difference between runs is where the bytes live:
//
//	files       plain files on disk (the debug shadow tree)
//	files+zstd  each distinct file zstd-compressed once, hard-linked into a
//	            copy of the shadow tree (savingsbench zlinks); decompressed
//	            whole at open
//	cas         a CAS (the cache's own, or -cas DIR: another store with the
//	            same contents, such as one with packing)
//	cas+zstd    a CAS with per-blob zstd (-cas DIR)
//
// Walks, stats and opens go through the production 9P2000.L server and the
// cache's metadata DB in every case.
//
//	ninebench -data CACHE -store files -files SHADOW [-j 8] [-cold]
//	ninebench -data CACHE -store zlinks -files ZLINKS/tree
//	ninebench -data CACHE -store cas -cas OTHERCACHE [-min-size N]
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/wanix-cache/bench"
	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/tree"
)

type file struct {
	ino  uint64
	path string // in the namespace, e.g. apk/x86_64/musl-1.2.5-r0/lib/ld-musl-x86_64.so.1
	rel  string // shadow/<subsystem>/<unit id>/<path in unit>
	ref  string
	size int64
}

func main() {
	log.SetFlags(0)
	data := flag.String("data", "", "cache directory whose metadata DB the server uses")
	store := flag.String("store", "cas", "files, zlinks or cas")
	filesDir := flag.String("files", "", "files: the shadow tree; zlinks: the zlinks tree/")
	casDir := flag.String("cas", "", "cas: the cache directory whose blob store serves the bytes (default -data)")
	par := flag.Int("j", 8, "concurrent readers, one 9P connection each")
	cold := flag.Bool("cold", false, "sync and drop the kernel caches before reading")
	minSize := flag.Int64("min-size", 0, "only read files of at least this many bytes")
	flag.Parse()
	ctx := context.Background()

	db, err := meta.Open(filepath.Join(*data, "meta.db"))
	if err != nil {
		log.Fatal(err)
	}
	files, err := list(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	byIno := map[uint64]file{}
	for _, f := range files {
		byIno[f.ino] = f
	}
	if *minSize > 0 {
		var big []file
		for _, f := range files {
			if f.size >= *minSize {
				big = append(big, f)
			}
		}
		files = big
	}

	if *casDir == "" {
		*casDir = *data
	}
	var st *cas.Store
	if *store == "cas" {
		if st, err = cas.Open(*casDir, 0); err != nil {
			log.Fatal(err)
		}
		defer st.Close()
	}
	dec, _ := zstd.NewReader(nil)
	open := func(ino uint64) (bench.Handle, bool, error) {
		f, ok := byIno[ino]
		if !ok {
			return nil, false, nil
		}
		switch *store {
		case "files":
			h, err := os.Open(filepath.Join(*filesDir, f.rel))
			return h, true, err
		case "zlinks":
			b, err := os.ReadFile(filepath.Join(*filesDir, f.rel))
			if err != nil || len(b) == 0 {
				return nopCloser{bytes.NewReader(nil)}, true, err
			}
			if b[0] == 0 {
				return nopCloser{bytes.NewReader(b[1:])}, true, nil
			}
			d, err := dec.DecodeAll(b[1:], nil)
			return nopCloser{bytes.NewReader(d)}, true, err
		default:
			if f.ref == "" {
				return nopCloser{bytes.NewReader(nil)}, true, nil
			}
			h, err := st.Open(ctx, f.ref)
			return h, true, err
		}
	}
	fs := &tree.FS{Meta: db, CAS: st}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go p9.NewServer(bench.DotLWith{DotL: tree.DotL{FS: fs}, Open: open}).Serve(ln)

	if *cold {
		syscall.Sync()
		if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0); err != nil {
			log.Fatal(err)
		}
	}
	work := make(chan file, 256)
	var nbytes, nfiles atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	for range *par {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			log.Fatal(err)
		}
		cl, err := p9.NewClient(c, p9.WithMessageSize(bench.Msize))
		if err != nil {
			log.Fatal(err)
		}
		root, err := cl.Attach("")
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cl.Close()
			buf := make([]byte, 128<<10)
			for f := range work {
				n, err := read(root, f.path, buf)
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("%s: %w", f.path, err))
					continue
				}
				nbytes.Add(n)
				nfiles.Add(1)
			}
		}()
	}
	for _, f := range files {
		work <- f
	}
	close(work)
	wg.Wait()
	el := time.Since(start).Seconds()
	if e, ok := firstErr.Load().(error); ok && e != nil {
		log.Fatal(e)
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{
		"store": *store, "cas": *casDir, "j": *par, "cold": *cold, "min_size": *minSize, "files": nfiles.Load(),
		"bytes": nbytes.Load(), "seconds": el, "mib_s": float64(nbytes.Load()) / (1 << 20) / el,
		"files_s": float64(nfiles.Load()) / el,
	})
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }

// read walks to p from the root, opens it, reads it whole and clunks it.
func read(root p9.File, p string, buf []byte) (int64, error) {
	names := strings.Split(p, "/")
	cur := root
	for len(names) > 0 {
		n := min(len(names), 16)
		_, next, err := cur.Walk(names[:n])
		if cur != root {
			cur.Close()
		}
		if err != nil {
			return 0, err
		}
		cur, names = next, names[n:]
	}
	defer cur.Close()
	if _, _, err := cur.Open(p9.ReadOnly); err != nil {
		return 0, err
	}
	var off int64
	for {
		k, err := cur.ReadAt(buf, off)
		off += int64(k)
		if k == 0 || err == io.EOF {
			return off, nil
		}
		if err != nil {
			return off, err
		}
	}
}

// list returns every file inode of a resident unit once, with its path in
// the namespace and in the shadow tree.
func list(ctx context.Context, db *meta.DB) ([]file, error) {
	units, err := db.UnitsInState(ctx, meta.UnitResident)
	if err != nil {
		return nil, err
	}
	type unitInfo struct {
		sub, root string
		id        int64
	}
	byID := map[int64]unitInfo{}
	for _, u := range units {
		p, err := db.PathOf(ctx, u.Root)
		if err != nil {
			return nil, err
		}
		byID[u.ID] = unitInfo{u.Subsystem, strings.Trim(p, "/"), u.ID}
	}
	rows, err := db.SQL().QueryContext(ctx, `SELECT ino, unit, content, size FROM inode WHERE kind = ? ORDER BY ino`, meta.KindFile)
	if err != nil {
		return nil, err
	}
	var out []file
	for rows.Next() {
		var f file
		var unit int64
		if err := rows.Scan(&f.ino, &unit, &f.ref, &f.size); err != nil {
			rows.Close()
			return nil, err
		}
		if u, ok := byID[unit]; ok {
			out = append(out, f)
			out[len(out)-1].rel = fmt.Sprintf("%s/%d", u.sub, u.id) // completed below
		}
	}
	rows.Close()
	for i := range out {
		p, err := db.PathOf(ctx, out[i].ino)
		if err != nil {
			return nil, err
		}
		p = strings.Trim(p, "/")
		var u unitInfo
		fmt.Sscanf(out[i].rel[strings.LastIndex(out[i].rel, "/")+1:], "%d", &u.id)
		u = byID[u.id]
		out[i].path = p
		out[i].rel = filepath.Join(u.sub, fmt.Sprint(u.id), strings.TrimPrefix(strings.TrimPrefix(p, u.root), "/"))
	}
	return out, nil
}
