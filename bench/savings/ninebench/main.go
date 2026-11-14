// Command ninebench reads every file of a settled cache over 9P2000.L,
// through the production 9P2000.L server and the cache's own metadata DB
// and file store, and prints one JSON line per pass.
//
//	ninebench -data CACHE [-j 8] [-cold] [-passes 2] [-min-size N] [-max-size N]
//
// The server keeps its namespace cache between passes, so the second of
// -passes 2 is the warm repeat read the performance log reports.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/store"
	"github.com/progrium/wanix-cache/internal/tree"
)

// msize is the 9P message size the clients ask for: 128 KiB, the Linux
// v9fs default.
const msize = 128 << 10

type file struct {
	path string // in the namespace, e.g. apk/x86_64/musl-1.2.5-r0/lib/ld-musl-x86_64.so.1
	size int64
}

func main() {
	log.SetFlags(0)
	data := flag.String("data", "", "cache directory")
	par := flag.Int("j", 8, "concurrent readers, one 9P connection each")
	cold := flag.Bool("cold", false, "sync and drop the kernel caches before reading")
	minSize := flag.Int64("min-size", 0, "only read files of at least this many bytes")
	maxSize := flag.Int64("max-size", 0, "only read files of at most this many bytes (0: no limit)")
	passes := flag.Int("passes", 1, "read every file this many times, one result line per pass (the server and its caches stay up between passes)")
	cpuprof := flag.String("cpuprofile", "", "write a CPU profile of the read phase to this file")
	flag.Parse()
	ctx := context.Background()

	db, err := meta.Open(filepath.Join(*data, "meta.db"))
	if err != nil {
		log.Fatal(err)
	}
	st, err := store.Open(*data)
	if err != nil {
		log.Fatal(err)
	}
	all, err := list(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	var files []file
	for _, f := range all {
		if f.size >= *minSize && (*maxSize == 0 || f.size <= *maxSize) {
			files = append(files, f)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go tree.NewDotLServer(&tree.FS{Meta: db, Store: st}).Serve(ln)

	if *cold {
		syscall.Sync()
		if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0); err != nil {
			log.Fatal(err)
		}
	}
	if *cpuprof != "" {
		pf, err := os.Create(*cpuprof)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(pf)
		defer pprof.StopCPUProfile()
	}
	for pass := 1; pass <= *passes; pass++ {
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
			cl, err := p9.NewClient(c, p9.WithMessageSize(msize))
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
			pprof.StopCPUProfile()
			log.Fatal(e)
		}
		json.NewEncoder(os.Stdout).Encode(map[string]any{
			"pass": pass, "zstd": st.Compression(), "j": *par, "cold": *cold, "min_size": *minSize, "max_size": *maxSize, "files": nfiles.Load(),
			"bytes": nbytes.Load(), "seconds": el, "mib_s": float64(nbytes.Load()) / (1 << 20) / el,
			"files_s": float64(nfiles.Load()) / el,
		})
	}
}

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
// the namespace.
func list(ctx context.Context, db *meta.DB) ([]file, error) {
	rows, err := db.SQL().QueryContext(ctx, `SELECT i.ino, i.size FROM inode i JOIN unit u ON u.id = i.unit
		WHERE i.kind = ? AND u.state = ? ORDER BY i.ino`, meta.KindFile, meta.UnitResident)
	if err != nil {
		return nil, err
	}
	type ino struct {
		n    uint64
		size int64
	}
	var inos []ino
	for rows.Next() {
		var i ino
		if err := rows.Scan(&i.n, &i.size); err != nil {
			rows.Close()
			return nil, err
		}
		inos = append(inos, i)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]file, 0, len(inos))
	for _, i := range inos {
		p, err := db.PathOf(ctx, i.n)
		if err != nil {
			return nil, err
		}
		out = append(out, file{strings.Trim(p, "/"), i.size})
	}
	return out, nil
}
