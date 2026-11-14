package nix

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
)

// BinaryCache serves the cache as a Nix binary cache over HTTP, so Nix in a
// guest can use it as a substituter:
//
//	nix-cache-info
//	<hash>.narinfo   the upstream narinfo, rewritten for an uncompressed NAR
//	nar/<hash>.nar   the NAR, rebuilt from the cached tree
//
// Every request reads through: a narinfo comes from upstream on first use,
// and a NAR faults the store path into the cache first. Upstream signatures
// are passed through unchanged; they cover the store path, NarHash, NarSize
// and references, all of which the rebuilt NAR preserves.
type BinaryCache struct {
	Nix *Subsystem
	// Priority is advertised in nix-cache-info; lower wins. Default 30,
	// ahead of cache.nixos.org's 40.
	Priority int
	Logf     func(string, ...any)
}

func (b *BinaryCache) logf(format string, args ...any) {
	if b.Logf != nil {
		b.Logf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (b *BinaryCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "read-only binary cache", http.StatusMethodNotAllowed)
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case p == "nix-cache-info":
		prio := b.Priority
		if prio == 0 {
			prio = 30
		}
		w.Header().Set("Content-Type", "text/x-nix-cache-info")
		fmt.Fprintf(w, "StoreDir: %s\nWantMassQuery: 1\nPriority: %d\n", StoreDir, prio)
	case strings.HasSuffix(p, ".narinfo") && !strings.Contains(p, "/"):
		b.serveNarInfo(w, r, strings.TrimSuffix(p, ".narinfo"))
	case strings.HasPrefix(p, "nar/") && strings.HasSuffix(p, ".nar"):
		b.serveNAR(w, r, strings.TrimSuffix(strings.TrimPrefix(p, "nar/"), ".nar"))
	default:
		http.NotFound(w, r)
	}
}

func validHash(h string) bool {
	if len(h) != HashLen {
		return false
	}
	for i := 0; i < len(h); i++ {
		if strings.IndexByte(base32Chars, h[i]) < 0 {
			return false
		}
	}
	return true
}

func (b *BinaryCache) narInfo(w http.ResponseWriter, r *http.Request, hash string) *NarInfo {
	if !validHash(hash) {
		http.NotFound(w, r)
		return nil
	}
	ni, err := b.Nix.NarInfoByHash(r.Context(), hash)
	if errors.Is(err, fs.ErrNotExist) {
		http.NotFound(w, r)
		return nil
	}
	if err != nil {
		b.logf("nix: binary cache: %s: %v", hash, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return nil
	}
	return ni
}

func (b *BinaryCache) serveNarInfo(w http.ResponseWriter, r *http.Request, hash string) {
	ni := b.narInfo(w, r, hash)
	if ni == nil {
		return
	}
	body := ni.Uncompressed("nar/" + hash + ".nar").String()
	w.Header().Set("Content-Type", "text/x-nix-narinfo")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodGet {
		w.Write([]byte(body))
	}
}

func (b *BinaryCache) serveNAR(w http.ResponseWriter, r *http.Request, hash string) {
	ni := b.narInfo(w, r, hash)
	if ni == nil {
		return
	}
	w.Header().Set("Content-Type", "application/x-nix-nar")
	w.Header().Set("Content-Length", strconv.FormatInt(ni.NarSize, 10))
	if r.Method == http.MethodHead {
		return
	}
	bw := bufio.NewWriterSize(w, 256<<10)
	n, err := b.Nix.NAR(r.Context(), ni.StorePath, bw)
	if err == nil {
		err = bw.Flush()
	}
	if err == nil && n != ni.NarSize {
		err = fmt.Errorf("rebuilt NAR is %d bytes, narinfo says %d", n, ni.NarSize)
	}
	if err != nil {
		if r.Context().Err() == nil {
			b.logf("nix: binary cache: %s: %v", ni.StorePath, err)
		}
		// The status line has gone out; cut the connection so the client
		// sees a truncated NAR rather than a complete wrong one.
		panic(http.ErrAbortHandler)
	}
}

// NixStoreDelete returns an Options.OnEvict hook that runs
// "nix-store --delete <path>" (with --store, if store is set), so a Nix
// database that tracks the mounted store forgets a path before the cache
// evicts it. A path the database does not know is not an error; a path
// Nix refuses to delete because it is still alive keeps the unit resident.
func NixStoreDelete(nixStore, store string) func(context.Context, StorePath) error {
	if nixStore == "" {
		nixStore = "nix-store"
	}
	return func(ctx context.Context, p StorePath) error {
		args := []string{"--delete"}
		if store != "" {
			args = append(args, "--store", store)
		}
		args = append(args, p.Abs())
		out, err := exec.CommandContext(ctx, nixStore, args...).CombinedOutput()
		if err == nil {
			return nil
		}
		msg := string(out)
		if strings.Contains(msg, "is not valid") || strings.Contains(msg, "not a valid") {
			return nil
		}
		return fmt.Errorf("nix: %s %s: %v: %s", nixStore, strings.Join(args, " "), err, strings.TrimSpace(msg))
	}
}
