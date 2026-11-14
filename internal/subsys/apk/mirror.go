package apk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// ServeHTTP serves the apk mirror. A guest's /etc/apk/repositories points
// at it, for example http://10.0.2.2:5680/v3.20/main, and apk requests
//
//	/<branch>/<repo>/<arch>/APKINDEX.tar.gz   revalidated after IndexTTL
//	/<branch>/<repo>/<arch>/<name>-<ver>.apk  immutable once cached
//
// The .apk is served from the cache while it fills: the first request for a
// package streams from upstream at download speed, later ones come from
// local disk with no upstream traffic.
func (a *APK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "read-only mirror", http.StatusMethodNotAllowed)
		return
	}
	p := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	repo, file := path.Split(p)
	repo = strings.TrimSuffix(repo, "/")
	if !validRepo(repo) {
		http.NotFound(w, r)
		return
	}
	switch {
	case file == "APKINDEX.tar.gz":
		a.serveIndex(w, r, repo)
	case strings.HasSuffix(file, ".apk"):
		a.servePackage(w, r, repo, strings.TrimSuffix(file, ".apk"))
	default:
		http.NotFound(w, r)
	}
}

func httpError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		http.NotFound(w, r)
	case errors.Is(err, context.Canceled):
		// client went away
	default:
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}

func (a *APK) serveIndex(w http.ResponseWriter, r *http.Request, repo string) {
	body, etag, mod, err := a.Index(r.Context(), repo)
	if err != nil {
		httpError(w, r, err)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "application/gzip")
	http.ServeContent(w, r, "APKINDEX.tar.gz", mod, bytes.NewReader(body))
}

func (a *APK) servePackage(w http.ResponseWriter, r *http.Request, repo, nv string) {
	ctx := r.Context()
	if a.mgr == nil {
		http.Error(w, "apk: mirror not attached to a fill manager", http.StatusInternalServerError)
		return
	}
	pkg, err := a.pkgInRepo(ctx, repo, nv)
	if errors.Is(err, fs.ErrNotExist) {
		// Not in the cached index (or none cached yet): refresh it, within
		// the TTL, and look again.
		if _, _, _, ierr := a.Index(ctx, repo); ierr != nil {
			httpError(w, r, ierr)
			return
		}
		pkg, err = a.pkgInRepo(ctx, repo, nv)
	}
	if err != nil {
		httpError(w, r, err)
		return
	}
	f, _, err := a.mgr.OpenPath(ctx, Name, rawKey(pkg.SHA1), pkg.NV+".apk")
	if err != nil {
		httpError(w, r, err)
		return
	}
	defer f.Close()
	if a.cfg.Unpack {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if _, err := a.mgr.Ensure(ctx, Name, filesKey(pkg.SHA1), true); err != nil {
				a.cfg.Logf("apk: unpack %s: %v", pkg.NV, err)
			}
		}()
	}
	w.Header().Set("ETag", `"`+pkg.SHA1+`"`)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, pkg.NV+".apk", time.Unix(pkg.BuildTime, 0), io.NewSectionReader(f, 0, pkg.Size))
}
