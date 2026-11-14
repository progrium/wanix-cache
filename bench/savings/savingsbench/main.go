// Command savingsbench is the harness for the savings test plan: it serves
// a corpus built by corpus.py as the upstreams, drives reads through the
// cache's real paths, and waits for the cache to settle; `wanix-cache
// stats -json` then measures it.
//
//	savingsbench serve-corpus -corpus DIR -apk ADDR -oci ADDR -nix ADDR
//	savingsbench apk     -mirror ADDR -corpus DIR MANIFEST...
//	savingsbench walk    -9p ADDR ROOT...
//	savingsbench settle  -data DIR [-quiet 5s]
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
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"9fans.net/go/plan9"
	"9fans.net/go/plan9/client"

	"github.com/progrium/wanix-cache/internal/meta"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: savingsbench serve-corpus|apk|walk|settle [flags]")
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
