// Command savingsbench is the harness for the savings test plan: it serves
// a corpus built by corpus.py as the upstreams, drives reads through the
// cache's real paths, waits for the cache to settle, and measures what
// `wanix-cache stats -json` does not: B3 from the shadow tree and the V1
// (whole-file only) and V2 (zstd per chunk) variants from the blob store.
//
//	savingsbench serve-corpus -corpus DIR -apk ADDR -oci ADDR -nix ADDR
//	savingsbench apk     -mirror ADDR -corpus DIR MANIFEST...
//	savingsbench walk    -9p ADDR ROOT...
//	savingsbench settle  -data DIR [-quiet 5s]
//	savingsbench b3      -shadow DIR [-link]
//	savingsbench variants -data DIR
//	savingsbench zlinks  -shadow DIR -out DIR [-level 2]
//	savingsbench readall -mode dir|zlinks|cas -root DIR [-j N] [-cold]
//
// See bench/savings/README.md for how a run fits together.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"9fans.net/go/plan9"
	"9fans.net/go/plan9/client"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/wanix-cache/internal/cas"
	"github.com/progrium/wanix-cache/internal/meta"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: savingsbench serve-corpus|apk|walk|settle|b3|variants [flags]")
	}
	cmd, args := os.Args[1], os.Args[2:]
	fl := flag.NewFlagSet(cmd, flag.ExitOnError)
	var err error
	switch cmd {
	case "serve-corpus":
		dir := fl.String("corpus", "", "corpus directory")
		apk := fl.String("apk", "127.0.0.1:8401", "Alpine mirror address (serves CORPUS/alpine)")
		oci := fl.String("oci", "127.0.0.1:8402", "OCI registry address (serves CORPUS/oci)")
		nix := fl.String("nix", "127.0.0.1:8403", "Nix binary cache address (serves CORPUS/nix)")
		fl.Parse(args)
		err = serveCorpus(*dir, *apk, *oci, *nix)
	case "apk":
		mirror := fl.String("mirror", "127.0.0.1:8404", "the cache's -apk-mirror address")
		keepGoing := fl.Bool("k", false, "log packages the mirror fails on and go on")
		fl.Parse(args)
		err = driveAPK(*mirror, *keepGoing, fl.Args())
	case "walk":
		addr := fl.String("9p", "127.0.0.1:5641", "the cache's plain 9P2000 address")
		par := fl.Int("j", 8, "files read concurrently")
		fl.Parse(args)
		err = walk(*addr, *par, fl.Args())
	case "settle":
		data := fl.String("data", "", "cache directory")
		quiet := fl.Duration("quiet", 5*time.Second, "how long nothing may be fetching or spooled")
		timeout := fl.Duration("timeout", 2*time.Hour, "give up after")
		fl.Parse(args)
		err = settle(*data, *quiet, *timeout)
	case "b3":
		shadow := fl.String("shadow", "", "shadow tree (B2)")
		link := fl.Bool("link", false, "actually hard-link identical files in place (rdfind -makehardlinks style)")
		fl.Parse(args)
		err = b3(*shadow, *link)
	case "variants":
		data := fl.String("data", "", "cache directory")
		fl.Parse(args)
		err = variants(*data)
	case "zlinks":
		shadow := fl.String("shadow", "", "plain tree to convert (B2 or B3)")
		out := fl.String("out", "", "new directory: objects/ and tree/")
		level := fl.Int("level", 2, "zstd encoder level (1 to 4)")
		fl.Parse(args)
		err = zlinks(*shadow, *out, *level)
	case "readall":
		mode := fl.String("mode", "dir", "dir (plain files), zlinks (zlinks tree), cas (a cache's resident files)")
		root := fl.String("root", "", "the tree (dir, zlinks) or cache directory (cas)")
		par := fl.Int("j", 1, "files read concurrently")
		cold := fl.Bool("cold", false, "sync and drop the kernel caches first (root)")
		fl.Parse(args)
		err = readAll(*mode, *root, *par, *cold)
	default:
		log.Fatalf("savingsbench: unknown command %q", cmd)
	}
	if err != nil {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------- corpus

func serveCorpus(dir, apkAddr, ociAddr, nixAddr string) error {
	if dir == "" {
		return errors.New("serve-corpus: -corpus is required")
	}
	var reqs, bytes atomic.Int64
	count := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqs.Add(1)
			h.ServeHTTP(&countingWriter{w, &bytes}, r)
		})
	}
	errc := make(chan error, 3)
	listen := func(addr string, h http.Handler) {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			errc <- err
			return
		}
		log.Printf("corpus: serving %s", ln.Addr())
		errc <- http.Serve(ln, count(h))
	}
	go listen(apkAddr, http.FileServer(http.Dir(filepath.Join(dir, "alpine"))))
	go listen(nixAddr, http.FileServer(http.Dir(filepath.Join(dir, "nix"))))
	go listen(ociAddr, &registry{dir: filepath.Join(dir, "oci")})
	go func() {
		for range time.Tick(30 * time.Second) {
			log.Printf("corpus: %d requests, %d MiB served", reqs.Load(), bytes.Load()>>20)
		}
	}()
	return <-errc
}

type countingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

func (w *countingWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.n.Add(int64(n))
	return n, err
}

// registry serves the read side of the OCI distribution API over
// corpus.py's layout: blobs/sha256/<hex> (+ <hex>.type for manifests) and
// refs/<repo>/<tag> holding a digest. No auth.
type registry struct{ dir string }

func (g *registry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := path.Clean(r.URL.Path)
	if p == "/v2" {
		w.WriteHeader(http.StatusOK)
		return
	}
	rest, ok := strings.CutPrefix(p, "/v2/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	var repo, kind, ref string
	for _, k := range []string{"/manifests/", "/blobs/"} {
		if i := strings.LastIndex(rest, k); i > 0 {
			repo, kind, ref = rest[:i], strings.Trim(k, "/"), rest[i+len(k):]
			break
		}
	}
	if repo == "" {
		http.NotFound(w, r)
		return
	}
	digest := ref
	if !strings.HasPrefix(ref, "sha256:") {
		if kind != "manifests" {
			http.NotFound(w, r)
			return
		}
		b, err := os.ReadFile(filepath.Join(g.dir, "refs", filepath.FromSlash(repo), ref))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		digest = strings.TrimSpace(string(b))
	}
	hex := strings.TrimPrefix(digest, "sha256:")
	if strings.ContainsAny(hex, "/.") {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(filepath.Join(g.dir, "blobs", "sha256", hex))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	if kind == "manifests" {
		mt, err := os.ReadFile(filepath.Join(g.dir, "blobs", "sha256", hex+".type"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", string(mt))
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("Docker-Content-Digest", digest)
	st, _ := f.Stat()
	http.ServeContent(w, r, "", st.ModTime(), f)
}

// ---------------------------------------------------------------- drive

// driveAPK fetches every package of corpus.py apk manifests through the
// cache's HTTP mirror, as `apk add` in a guest would: the repositories'
// APKINDEX first, then each .apk.
func driveAPK(mirror string, keepGoing bool, manifests []string) error {
	type pkg struct{ Name, Version, Repo, Sha256 string }
	type man struct {
		Release  string
		Arch     string
		Packages []pkg
	}
	start := time.Now()
	var n, total, failed int64
	for _, mf := range manifests {
		b, err := os.ReadFile(mf)
		if err != nil {
			return err
		}
		var m man
		if err := json.Unmarshal(b, &m); err != nil {
			return fmt.Errorf("%s: %w", mf, err)
		}
		base := fmt.Sprintf("http://%s/v%s/", mirror, m.Release)
		for _, repo := range []string{"main", "community"} {
			if _, _, err := fetch(base+repo+"/"+m.Arch+"/APKINDEX.tar.gz", ""); err != nil {
				return err
			}
		}
		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		errs := make(chan error, len(m.Packages))
		for _, p := range m.Packages {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				sz, _, err := fetch(base+p.Repo+"/"+m.Arch+"/"+p.Name+"-"+p.Version+".apk", p.Sha256)
				if err != nil && keepGoing {
					log.Printf("apk: FAILED %s", err)
					atomic.AddInt64(&failed, 1)
					return
				}
				if err != nil {
					errs <- err
					return
				}
				atomic.AddInt64(&n, 1)
				atomic.AddInt64(&total, sz)
			}()
		}
		wg.Wait()
		close(errs)
		if err := <-errs; err != nil {
			return err
		}
	}
	log.Printf("apk: %d packages, %.1f MiB through the mirror in %s, %d failed", n, float64(total)/(1<<20), time.Since(start).Round(time.Millisecond), failed)
	return nil
}

func fetch(url, wantSHA string) (int64, string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, "", fmt.Errorf("GET %s: %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
	}
	h := sha256.New()
	n, err := io.Copy(h, resp.Body)
	if err != nil {
		return n, "", fmt.Errorf("GET %s: %w", url, err)
	}
	got := fmt.Sprintf("%x", h.Sum(nil))
	if wantSHA != "" && got != wantSHA {
		return n, got, fmt.Errorf("GET %s: sha256 %s, want %s", url, got, wantSHA)
	}
	return n, got, nil
}

// walk opens and reads every file under each root over plain 9P2000,
// which faults every unit under it in through the fill path.
func walk(addr string, par int, roots []string) error {
	start := time.Now()
	var files, bytes, dirs atomic.Int64
	for _, root := range roots {
		// One connection per reader: fids stay per connection, so no two
		// goroutines ever share a fid pool.
		conn, fsys, err := attach(addr)
		if err != nil {
			return err
		}
		paths := make(chan string, 1024)
		var wg sync.WaitGroup
		var firstErr atomic.Value
		for range par {
			wc, wfs, err := attach(addr)
			if err != nil {
				return err
			}
			defer wc.Close()
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 64<<10)
				for p := range paths {
					n, err := readFile(wfs, p, buf)
					if err != nil {
						firstErr.CompareAndSwap(nil, err)
						continue
					}
					files.Add(1)
					bytes.Add(n)
				}
			}()
		}
		var list func(dir string) error
		list = func(dir string) error {
			fid, err := fsys.Open(dir, plan9.OREAD)
			if err != nil {
				return fmt.Errorf("open %s: %w", dir, err)
			}
			ds, err := fid.Dirreadall()
			fid.Close()
			if err != nil {
				return fmt.Errorf("read dir %s: %w", dir, err)
			}
			dirs.Add(1)
			for _, d := range ds {
				p := dir + "/" + d.Name
				switch {
				case d.Mode&plan9.DMDIR != 0:
					if err := list(p); err != nil {
						return err
					}
				default:
					paths <- p
				}
			}
			return nil
		}
		err = list(strings.TrimRight(root, "/"))
		close(paths)
		wg.Wait()
		conn.Close()
		if err != nil {
			return err
		}
		if e, ok := firstErr.Load().(error); ok && e != nil {
			return e
		}
	}
	el := time.Since(start)
	log.Printf("walk: %d dirs, %d files, %.1f MiB in %s (%.1f MiB/s)", dirs.Load(), files.Load(),
		float64(bytes.Load())/(1<<20), el.Round(time.Millisecond), float64(bytes.Load())/(1<<20)/el.Seconds())
	return nil
}

func attach(addr string) (*client.Conn, *client.Fsys, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := client.NewConn(c)
	if err != nil {
		return nil, nil, err
	}
	fsys, err := conn.Attach(nil, "savingsbench", "")
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, fsys, nil
}

func readFile(fsys *client.Fsys, p string, buf []byte) (int64, error) {
	fid, err := fsys.Open(p, plan9.OREAD)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", p, err)
	}
	defer fid.Close()
	var off int64
	for {
		n, err := fid.ReadAt(buf, off)
		off += int64(n)
		if err == io.EOF || n == 0 {
			return off, nil
		}
		if err != nil {
			return off, fmt.Errorf("read %s: %w", p, err)
		}
	}
}

// settle waits until no unit is fetching or spooled for -quiet: every
// fault has landed and the ingest queue is empty.
func settle(dir string, quiet, timeout time.Duration) error {
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	start := time.Now()
	var calm time.Time
	for {
		us, err := db.Units(ctx, "")
		if err != nil {
			return err
		}
		var busy, resident int
		var logical int64
		for _, u := range us {
			switch u.State {
			case meta.UnitFetching, meta.UnitSpooled:
				busy++
			case meta.UnitResident:
				resident++
				logical += u.LogicalBytes
			}
		}
		if busy == 0 {
			if calm.IsZero() {
				calm = time.Now()
			}
			if time.Since(calm) >= quiet {
				log.Printf("settle: %d units resident, %.1f MiB logical, settled after %s", resident, float64(logical)/(1<<20), time.Since(start).Round(time.Second))
				return nil
			}
		} else {
			calm = time.Time{}
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("settle: %d units still busy after %s", busy, timeout)
		}
		time.Sleep(time.Second)
	}
}

// ---------------------------------------------------------------- B3

// B3 is the shadow tree with identical files hard-linked: what rdfind
// -makehardlinks, jdupes -L or nix-store --optimise leave on disk.
type B3 struct {
	Files      int64 `json:"files"`       // regular-file inodes in B2
	Distinct   int64 `json:"distinct"`    // distinct contents
	Linked     int64 `json:"linked"`      // inodes replaced by a link (with -link)
	B2         int64 `json:"b2"`          // allocated before
	B3         int64 `json:"b3"`          // allocated after hard-linking
	DirsLinks  int64 `json:"dirs_links"`  // directories and symlinks, allocated
	Measured   bool  `json:"measured"`    // true: B3 is du of the linked tree
	FileBlocks int64 `json:"file_blocks"` // B3's regular-file share
}

func b3(dir string, link bool) error {
	type key struct {
		size int64
		sum  [32]byte
	}
	seenIno := map[uint64]bool{}
	first := map[key]string{}
	var r B3
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		a := st.Blocks * 512
		if !info.Mode().IsRegular() {
			r.B2 += a
			r.DirsLinks += a
			return nil
		}
		if seenIno[st.Ino] {
			return nil
		}
		seenIno[st.Ino] = true
		r.Files++
		r.B2 += a
		k := key{size: info.Size()}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return err
		}
		copy(k.sum[:], h.Sum(nil))
		if prev, ok := first[k]; ok {
			if link {
				tmp := p + ".b3link"
				if err := os.Link(prev, tmp); err != nil {
					return err
				}
				if err := os.Rename(tmp, p); err != nil {
					return err
				}
				r.Linked++
			}
			return nil
		}
		first[k] = p
		r.Distinct++
		r.FileBlocks += a
		return nil
	})
	if err != nil {
		return err
	}
	r.B3 = r.FileBlocks + r.DirsLinks
	if link {
		var after int64
		seen := map[uint64]bool{}
		filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			st := info.Sys().(*syscall.Stat_t)
			if seen[st.Ino] {
				return nil
			}
			seen[st.Ino] = true
			after += st.Blocks * 512
			return nil
		})
		r.B3, r.Measured = after, true
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}

// ---------------------------------------------------------------- V1, V2

// Variants are the plan's V1 and V2, computed offline from the V0 store
// rather than by rerunning ingest.
type Variants struct {
	BlockSize int64 `json:"block_size"`

	DataBlobs     int64 `json:"data_blobs"`
	DataBytes     int64 `json:"data_bytes"`
	DataAllocated int64 `json:"data_allocated"`
	SchemaBlobs   int64 `json:"schema_blobs"`
	SchemaAlloc   int64 `json:"schema_allocated"`

	// V1: one blob per distinct content (no chunking), each rounded up to
	// a block, plus one file schema blob per content.
	V1Contents    int64   `json:"v1_contents"`
	V1DataAlloc   int64   `json:"v1_data_allocated"`
	V1DeltaVsV0   int64   `json:"v1_delta_vs_v0"` // V1 data − V0 data allocated (positive: chunking saves)
	ContentBytes  int64   `json:"content_bytes"`
	ChunkSavedRaw int64   `json:"chunk_saved_logical"` // content bytes − data bytes
	V2DataAlloc   int64   `json:"v2_data_allocated"`   // each data blob zstd-compressed (level 3), kept raw if that is not smaller
	V2DataBytes   int64   `json:"v2_data_bytes"`
	V2SavedAlloc  int64   `json:"v2_saved_allocated"` // V0 data allocated − V2 data allocated
	V2CompressRaw float64 `json:"v2_logical_ratio"`   // V2 data bytes ÷ V0 data bytes
}

func variants(dir string) error {
	ctx := context.Background()
	var v Variants
	var fsst syscall.Statfs_t
	if err := syscall.Statfs(dir, &fsst); err != nil {
		return err
	}
	bs := int64(fsst.Bsize)
	v.BlockSize = bs
	round := func(n int64) int64 {
		if n == 0 {
			return 0
		}
		return (n + bs - 1) / bs * bs
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	type job struct {
		path string
		size int64
		a    int64
	}
	jobs := make(chan job, 256)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst []byte
			for j := range jobs {
				b, err := os.ReadFile(j.path)
				if err != nil {
					log.Fatal(err)
				}
				if isSchema(b) {
					mu.Lock()
					v.SchemaBlobs++
					v.SchemaAlloc += j.a
					mu.Unlock()
					continue
				}
				dst = enc.EncodeAll(b, dst[:0])
				n := int64(len(dst))
				if n >= int64(len(b)) {
					n = int64(len(b))
				}
				mu.Lock()
				v.DataBlobs++
				v.DataBytes += j.size
				v.DataAllocated += j.a
				v.V2DataBytes += n
				v.V2DataAlloc += round(n)
				mu.Unlock()
			}
		}()
	}
	err = filepath.WalkDir(filepath.Join(dir, "blobs"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".dat") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		jobs <- job{p, info.Size(), info.Sys().(*syscall.Stat_t).Blocks * 512}
		return nil
	})
	close(jobs)
	wg.Wait()
	if err != nil {
		return err
	}
	v.V2SavedAlloc = v.DataAllocated - v.V2DataAlloc
	if v.DataBytes > 0 {
		v.V2CompressRaw = float64(v.V2DataBytes) / float64(v.DataBytes)
	}

	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.SQL().QueryContext(ctx, `SELECT DISTINCT i.content, i.size FROM inode i
		JOIN unit u ON u.id = i.unit WHERE i.kind = ? AND u.state = ? AND i.content != ''`, meta.KindFile, meta.UnitResident)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for rows.Next() {
		var ref string
		var size int64
		if err := rows.Scan(&ref, &size); err != nil {
			return err
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		v.V1Contents++
		v.ContentBytes += size
		v.V1DataAlloc += round(size)
	}
	rows.Close()
	v.V1DeltaVsV0 = v.V1DataAlloc - v.DataAllocated
	v.ChunkSavedRaw = v.ContentBytes - v.DataBytes
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(v)
}

func isSchema(b []byte) bool {
	return len(b) > 0 && b[0] == '{' && strings.Contains(string(b[:min(len(b), 64)]), `"camliVersion"`)
}

// ---------------------------------------------------------------- zlinks

// zlinks builds the hard-link alternative to the CAS with compression: each
// distinct file stored once as objects/<sha256>, one header byte (0 raw,
// 1 zstd) then the bytes, and tree/ mirroring the shadow with every file a
// hard link to its object. It prints the allocated size of the result.
func zlinks(shadow, out string, level int) error {
	if shadow == "" || out == "" {
		return errors.New("zlinks: -shadow and -out are required")
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	objs, tree := filepath.Join(out, "objects"), filepath.Join(out, "tree")
	if err := os.MkdirAll(objs, 0o755); err != nil {
		return err
	}
	byIno := map[uint64]string{}
	var files, objects, raw int64
	start := time.Now()
	err = filepath.WalkDir(shadow, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(shadow, p)
		dst := filepath.Join(tree, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.MkdirAll(dst, 0o755)
		case info.Mode()&fs.ModeSymlink != 0:
			t, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(t, dst)
		case !info.Mode().IsRegular():
			return nil
		}
		files++
		ino := info.Sys().(*syscall.Stat_t).Ino
		obj, ok := byIno[ino]
		if !ok {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(b)
			obj = filepath.Join(objs, fmt.Sprintf("%x", sum))
			if _, err := os.Lstat(obj); errors.Is(err, fs.ErrNotExist) {
				z := enc.EncodeAll(b, []byte{1})
				if len(z) >= len(b)+1 {
					z = append([]byte{0}, b...)
				}
				if err := os.WriteFile(obj, z, 0o444); err != nil {
					return err
				}
				objects++
				raw += int64(len(b))
			}
			byIno[ino] = obj
		}
		return os.Link(obj, dst)
	})
	if err != nil {
		return err
	}
	a, err := allocated(out)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"files": files, "objects": objects, "object_logical": raw, "allocated": a,
		"seconds": time.Since(start).Seconds(),
	})
}

// allocated sums st_blocks × 512 over the distinct inodes under dir.
func allocated(dir string) (int64, error) {
	seen := map[uint64]bool{}
	var n int64
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		if !seen[st.Ino] {
			seen[st.Ino] = true
			n += st.Blocks * 512
		}
		return nil
	})
	return n, err
}

// ---------------------------------------------------------------- readall

// readAll reads every file whole in 128 KiB reads, directly (no 9P), and
// prints the throughput: plain files, zlinks files (decompressed), or a
// cache's resident file inodes through cas.Store.Open.
func readAll(mode, root string, par int, cold bool) error {
	type item struct {
		path string
		ref  string
		size int64
	}
	var items []item
	var store *cas.Store
	switch mode {
	case "dir", "zlinks":
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() {
				items = append(items, item{path: p})
			}
			return nil
		})
		if err != nil {
			return err
		}
	case "cas":
		db, err := meta.Open(filepath.Join(root, "meta.db"))
		if err != nil {
			return err
		}
		rows, err := db.SQL().Query(`SELECT i.content, i.size FROM inode i JOIN unit u ON u.id = i.unit
			WHERE i.kind = ? AND u.state = ? AND i.content != '' ORDER BY i.ino`, meta.KindFile, meta.UnitResident)
		if err != nil {
			return err
		}
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.ref, &it.size); err != nil {
				return err
			}
			items = append(items, it)
		}
		rows.Close()
		db.Close()
		if store, err = cas.Open(root, 0); err != nil {
			return err
		}
		defer store.Close()
	default:
		return fmt.Errorf("readall: unknown mode %q", mode)
	}
	if cold {
		syscall.Sync()
		if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0); err != nil {
			return fmt.Errorf("readall: drop caches: %w", err)
		}
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	ctx := context.Background()
	var bytes atomic.Int64
	work := make(chan item, 256)
	var wg sync.WaitGroup
	var firstErr atomic.Value
	start := time.Now()
	for range par {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 128<<10)
			for it := range work {
				n, err := func() (int64, error) {
					switch mode {
					case "dir":
						f, err := os.Open(it.path)
						if err != nil {
							return 0, err
						}
						defer f.Close()
						return io.CopyBuffer(io.Discard, struct{ io.Reader }{f}, buf)
					case "zlinks":
						b, err := os.ReadFile(it.path)
						if err != nil || len(b) == 0 {
							return 0, err
						}
						if b[0] == 0 {
							return int64(len(b) - 1), nil
						}
						d, err := dec.DecodeAll(b[1:], nil)
						return int64(len(d)), err
					default:
						f, err := store.Open(ctx, it.ref)
						if err != nil {
							return 0, err
						}
						defer f.Close()
						var off int64
						for off < it.size {
							k, err := f.ReadAt(buf, off)
							off += int64(k)
							if err == io.EOF {
								break
							}
							if err != nil {
								return off, err
							}
						}
						return off, nil
					}
				}()
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("%s%s: %w", it.path, it.ref, err))
				}
				bytes.Add(n)
			}
		}()
	}
	for _, it := range items {
		work <- it
	}
	close(work)
	wg.Wait()
	if e, ok := firstErr.Load().(error); ok && e != nil {
		return e
	}
	el := time.Since(start).Seconds()
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"mode": mode, "root": root, "j": par, "cold": cold, "files": len(items),
		"bytes": bytes.Load(), "seconds": el, "mib_s": float64(bytes.Load()) / (1 << 20) / el,
		"files_s": float64(len(items)) / el,
	})
}
