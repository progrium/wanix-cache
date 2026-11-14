package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ent is one tar entry of a fixture layer.
type ent struct {
	name string
	typ  byte // tar.TypeReg if 0
	body string
	link string
	mode int64
}

func file(name, body string) ent       { return ent{name: name, body: body} }
func dir(name string) ent              { return ent{name: name, typ: tar.TypeDir} }
func symlink(name, target string) ent  { return ent{name: name, typ: tar.TypeSymlink, link: target} }
func hardlink(name, target string) ent { return ent{name: name, typ: tar.TypeLink, link: target} }

type layer struct {
	blob      []byte
	digest    string // compressed
	diffID    string
	mediaType string
}

func makeLayer(t testing.TB, comp string, ents ...ent) layer {
	t.Helper()
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	for _, e := range ents {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: e.mode, ModTime: time.Unix(1700000000, 0), Linkname: e.link}
		if h.Typeflag == 0 {
			h.Typeflag = tar.TypeReg
		}
		if h.Mode == 0 {
			h.Mode = 0o644
			if h.Typeflag == tar.TypeDir {
				h.Mode = 0o755
			}
		}
		if h.Typeflag == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			tw.Write([]byte(e.body))
		}
	}
	tw.Close()
	l := layer{diffID: sha256Digest(raw.Bytes())}
	var out bytes.Buffer
	switch comp {
	case "gzip":
		zw := gzip.NewWriter(&out)
		zw.Write(raw.Bytes())
		zw.Close()
		l.mediaType = MediaOCILayerGzip
	case "zstd":
		zw, _ := zstd.NewWriter(&out)
		zw.Write(raw.Bytes())
		zw.Close()
		l.mediaType = MediaOCILayerZstd
	default:
		out = raw
		l.mediaType = MediaOCILayer
	}
	l.blob = out.Bytes()
	l.digest = sha256Digest(l.blob)
	return l
}

// registry is an in-memory registry with anonymous token auth.
type registry struct {
	t   testing.TB
	srv *httptest.Server

	mu        sync.Mutex
	manifests map[string][]byte // repo + "@" + tag|digest -> body
	types     map[string]string
	blobs     map[string][]byte // digest -> bytes
	gates     map[string]chan struct{}
	corrupt   map[string]bool

	tokens    atomic.Int32
	blobGets  sync.Map // digest -> *atomic.Int32
	inFlight  atomic.Int32
	maxFlight atomic.Int32
	slow      time.Duration
}

func newRegistry(t testing.TB) *registry {
	r := &registry{t: t, manifests: map[string][]byte{}, types: map[string]string{}, blobs: map[string][]byte{},
		gates: map[string]chan struct{}{}, corrupt: map[string]bool{}}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

// host is the registry's name in paths, e.g. 127.0.0.1:41234.
func (r *registry) host() string { return strings.TrimPrefix(r.srv.URL, "http://") }

func (r *registry) gate(digest string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := make(chan struct{})
	r.gates[digest] = g
	return g
}

func (r *registry) gets(digest string) int {
	v, ok := r.blobGets.Load(digest)
	if !ok {
		return 0
	}
	return int(v.(*atomic.Int32).Load())
}

// push adds an image (and returns its manifest digest and the layers).
func (r *registry) push(repo, tag string, platform Platform, layers ...layer) string {
	cfg := map[string]any{"architecture": platform.Architecture, "os": platform.OS,
		"rootfs": map[string]any{"type": "layers", "diff_ids": func() []string {
			var ids []string
			for _, l := range layers {
				ids = append(ids, l.diffID)
			}
			return ids
		}()}}
	cb, _ := json.Marshal(cfg)
	cd := sha256Digest(cb)
	m := manifest{SchemaVersion: 2, MediaType: MediaOCIManifest,
		Config: Descriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: cd, Size: int64(len(cb))}}
	r.mu.Lock()
	r.blobs[cd] = cb
	for _, l := range layers {
		r.blobs[l.digest] = l.blob
		m.Layers = append(m.Layers, Descriptor{MediaType: l.mediaType, Digest: l.digest, Size: int64(len(l.blob))})
	}
	r.mu.Unlock()
	mb, _ := json.Marshal(m)
	md := sha256Digest(mb)
	r.putManifest(repo, md, mb, MediaOCIManifest)
	if tag != "" {
		r.putManifest(repo, tag, mb, MediaOCIManifest)
	}
	return md
}

// index tags an index over platform images.
func (r *registry) index(repo, tag string, entries map[string]Platform) string {
	ix := manifest{SchemaVersion: 2, MediaType: MediaOCIIndex}
	for d, p := range entries {
		p := p
		r.mu.Lock()
		size := len(r.manifests[repo+"@"+d])
		r.mu.Unlock()
		ix.Manifests = append(ix.Manifests, Descriptor{MediaType: MediaOCIManifest, Digest: d, Size: int64(size), Platform: &p})
	}
	b, _ := json.Marshal(ix)
	d := sha256Digest(b)
	r.putManifest(repo, d, b, MediaOCIIndex)
	r.putManifest(repo, tag, b, MediaOCIIndex)
	return d
}

func (r *registry) putManifest(repo, ref string, b []byte, mt string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifests[repo+"@"+ref] = b
	r.types[repo+"@"+ref] = mt
}

func (r *registry) serve(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/token" {
		if !strings.HasPrefix(req.URL.Query().Get("scope"), "repository:") || req.URL.Query().Get("service") != "fixture" {
			http.Error(w, "bad token request", 400)
			return
		}
		r.tokens.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"token": "tok", "expires_in": 300})
		return
	}
	if req.Header.Get("Authorization") != "Bearer tok" {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fixture",scope="repository:x:pull"`, r.srv.URL))
		http.Error(w, "unauthorized", 401)
		return
	}
	p := strings.TrimPrefix(req.URL.Path, "/v2/")
	if i := strings.LastIndex(p, "/manifests/"); i >= 0 {
		k := p[:i] + "@" + p[i+len("/manifests/"):]
		r.mu.Lock()
		b, ok := r.manifests[k]
		mt := r.types[k]
		r.mu.Unlock()
		if !ok {
			http.Error(w, "manifest unknown", 404)
			return
		}
		w.Header().Set("Content-Type", mt)
		w.Write(b)
		return
	}
	if i := strings.LastIndex(p, "/blobs/"); i >= 0 {
		d := p[i+len("/blobs/"):]
		r.mu.Lock()
		b, ok := r.blobs[d]
		g := r.gates[d]
		bad := r.corrupt[d]
		r.mu.Unlock()
		if !ok {
			http.Error(w, "blob unknown", 404)
			return
		}
		v, _ := r.blobGets.LoadOrStore(d, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
		n := r.inFlight.Add(1)
		defer r.inFlight.Add(-1)
		for {
			m := r.maxFlight.Load()
			if n <= m || r.maxFlight.CompareAndSwap(m, n) {
				break
			}
		}
		if bad {
			b = append([]byte(nil), b...)
			b[len(b)/2] ^= 0xff
		}
		if g != nil {
			// Send half, then wait: the client sees a download in progress.
			w.Write(b[:len(b)/2])
			w.(http.Flusher).Flush()
			select {
			case <-g:
			case <-req.Context().Done():
				return
			}
			b = b[len(b)/2:]
		}
		if r.slow > 0 {
			time.Sleep(r.slow)
		}
		w.Write(b)
		return
	}
	http.NotFound(w, req)
}
