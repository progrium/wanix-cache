// Package apk is the Alpine package subsystem.
//
// It serves an HTTP mirror that in-guest apk uses as its repository, so no
// writable apk cache directory is ever shared into a guest, and it exposes
// every package as a read-only overlay layer for building roots outside the
// guest. Each package is two units:
//
//	<hex>.apk  the raw .apk, mounted at /apk/<arch>/.apk/<name>-<ver>/,
//	           which the mirror serves (it can't be rebuilt byte for byte:
//	           signature plus three gzip streams)
//	<hex>      the unpacked data files, mounted at /apk/<arch>/<name>-<ver>/,
//	           usable as an overlay layer; no install scripts are run
//
// <hex> is the SHA-1 in the APKINDEX C: checksum, so keys are stable and
// content-identifying. The unpacked unit is built from the cached raw unit,
// so a package is downloaded from upstream once.
//
// APKINDEX.tar.gz files are kept whole in apk_index (in the metadata
// database, so they are never evicted) and revalidated against upstream by
// ETag after IndexTTL. Their records go to apk_pkg, which is never pruned,
// so units for superseded packages can still be described and refetched
// while upstream still has them.
package apk

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/progrium/wanix-cache/internal/fill"
	"github.com/progrium/wanix-cache/internal/meta"
	"github.com/progrium/wanix-cache/internal/subsys"
)

// Name is the subsystem name and namespace root.
const Name = "apk"

// DefaultUpstream is the Alpine CDN.
const DefaultUpstream = "https://dl-cdn.alpinelinux.org/alpine"

// Config configures the subsystem.
type Config struct {
	// Upstream is the mirror base URL; repository paths such as
	// v3.20/main/x86_64 are appended. Default DefaultUpstream.
	Upstream string
	// IndexTTL is how long a cached APKINDEX is served before it is
	// revalidated upstream. Default 60s.
	IndexTTL time.Duration
	// Client fetches from upstream. Default http.DefaultClient.
	Client *http.Client
	// Unpack faults in a package's unpacked layer in the background after
	// the mirror serves its .apk, so its files dedupe against everything
	// else in the cache. Off, layers are unpacked only when a path under
	// /apk/<arch>/<name>-<ver>/ is looked up.
	Unpack bool
	// Indexer, if set, gets IndexRoot("/apk/<arch>/<name>-<ver>") when a
	// package's unpacked layer has its full manifest, so H9 can mount the
	// layer with a /.h9index.
	Indexer subsys.RootIndexer
	// Logf receives background errors. Default log.Printf.
	Logf func(string, ...any)
}

// APK implements subsys.Subsystem.
type APK struct {
	cfg Config
	db  *meta.DB
	sql *sql.DB
	mgr fill.Manager

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-repo index refresh
	bg    sync.WaitGroup          // background indexing and unpacking
	now   func() time.Time
}

var _ subsys.Subsystem = (*APK)(nil)

// New returns the subsystem. Call Attach with the manager it is registered
// with before serving the mirror.
func New(db *meta.DB, cfg Config) *APK {
	if cfg.Upstream == "" {
		cfg.Upstream = DefaultUpstream
	}
	cfg.Upstream = strings.TrimRight(cfg.Upstream, "/")
	if cfg.IndexTTL <= 0 {
		cfg.IndexTTL = 60 * time.Second
	}
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &APK{cfg: cfg, db: db, sql: db.SQL(), locks: map[string]*sync.Mutex{}, now: time.Now}
}

// Attach gives the subsystem the manager it is registered with, which the
// mirror and the unpacked units read raw .apk units through.
func (a *APK) Attach(m fill.Manager) {
	a.mgr = m
	if a.cfg.Indexer != nil {
		m.Watch(a.indexLayer)
	}
}

// indexLayer indexes an unpacked layer once its manifest is complete.
func (a *APK) indexLayer(ev fill.UnitEvent) {
	if ev.Unit.Subsystem != Name || ev.To != meta.UnitSpooled || !a.DerivedUnit(subsys.Key(ev.Unit.Key)) {
		return
	}
	a.bg.Go(func() {
		ctx := context.Background()
		info, err := a.Describe(ctx, subsys.Key(ev.Unit.Key))
		if err == nil {
			err = a.cfg.Indexer.IndexRoot(ctx, "/"+Name+"/"+info.Mount)
		}
		if err != nil {
			a.cfg.Logf("apk: indexing layer %s: %v", ev.Unit.Key, err)
		}
	})
}

// Wait blocks until background layer indexing and unpacking started so far
// have finished, for tests and orderly shutdown.
func (a *APK) Wait() { a.bg.Wait() }

func (a *APK) Name() string { return Name }

func (a *APK) Migrations() []meta.Migration {
	return []meta.Migration{
		{Version: 1, SQL: `
CREATE TABLE apk_index (
	repo          TEXT    PRIMARY KEY, -- v3.20/main/x86_64
	upstream_etag TEXT    NOT NULL DEFAULT '',
	last_modified TEXT    NOT NULL DEFAULT '',
	sha256        TEXT    NOT NULL,
	fetched_at    INTEGER NOT NULL,     -- unix nanos the body last changed
	checked_at    INTEGER NOT NULL,     -- unix nanos last revalidated
	body          BLOB    NOT NULL
);
CREATE TABLE apk_pkg (
	repo           TEXT    NOT NULL,
	nv             TEXT    NOT NULL,    -- name-version
	arch           TEXT    NOT NULL,
	name           TEXT    NOT NULL,
	version        TEXT    NOT NULL,
	sha1           TEXT    NOT NULL,    -- hex of the C: checksum
	size           INTEGER NOT NULL,
	installed_size INTEGER NOT NULL,
	build_time     INTEGER NOT NULL,
	indexed_at     INTEGER NOT NULL,
	PRIMARY KEY (repo, nv)
) WITHOUT ROWID;
CREATE INDEX apk_pkg_sha1 ON apk_pkg (sha1);
CREATE INDEX apk_pkg_arch_nv ON apk_pkg (arch, nv, indexed_at);
`},
	}
}

// Keys.

func rawKey(sha1hex string) subsys.Key   { return subsys.Key(sha1hex + ".apk") }
func filesKey(sha1hex string) subsys.Key { return subsys.Key(sha1hex) }

func parseKey(k subsys.Key) (sha1hex string, raw bool, err error) {
	s := string(k)
	raw = strings.HasSuffix(s, ".apk")
	s = strings.TrimSuffix(s, ".apk")
	if b, err := hex.DecodeString(s); err != nil || len(b) != 20 {
		return "", false, fmt.Errorf("apk: bad key %q: %w", k, fs.ErrNotExist)
	}
	return s, raw, nil
}

// pkgRow is one apk_pkg record.
type pkgRow struct {
	Repo, NV, Arch, Name, Version, SHA1 string
	Size, InstalledSize, BuildTime      int64
}

func (p pkgRow) checksum() string {
	b, _ := hex.DecodeString(p.SHA1)
	return Checksum(b)
}

const pkgCols = `repo, nv, arch, name, version, sha1, size, installed_size, build_time`

func scanPkg(row interface{ Scan(...any) error }) (pkgRow, error) {
	var p pkgRow
	err := row.Scan(&p.Repo, &p.NV, &p.Arch, &p.Name, &p.Version, &p.SHA1, &p.Size, &p.InstalledSize, &p.BuildTime)
	if errors.Is(err, sql.ErrNoRows) {
		err = fs.ErrNotExist
	}
	return p, err
}

func (a *APK) pkgBySHA1(ctx context.Context, sha1hex string) (pkgRow, error) {
	return scanPkg(a.sql.QueryRowContext(ctx, `SELECT `+pkgCols+` FROM apk_pkg WHERE sha1 = ? ORDER BY indexed_at DESC LIMIT 1`, sha1hex))
}

func (a *APK) pkgByNV(ctx context.Context, arch, nv string) (pkgRow, error) {
	return scanPkg(a.sql.QueryRowContext(ctx, `SELECT `+pkgCols+` FROM apk_pkg WHERE arch = ? AND nv = ? ORDER BY indexed_at DESC LIMIT 1`, arch, nv))
}

func (a *APK) pkgInRepo(ctx context.Context, repo, nv string) (pkgRow, error) {
	return scanPkg(a.sql.QueryRowContext(ctx, `SELECT `+pkgCols+` FROM apk_pkg WHERE repo = ? AND nv = ?`, repo, nv))
}

func (a *APK) knownArch(ctx context.Context, arch string) bool {
	var one int
	return a.sql.QueryRowContext(ctx, `SELECT 1 FROM apk_pkg WHERE arch = ? LIMIT 1`, arch).Scan(&one) == nil
}

// Resolve implements subsys.Subsystem. Paths are
//
//	<arch>/<name>-<ver>/<rel>         unpacked layer
//	<arch>/.apk/<name>-<ver>/<rel>    raw .apk (rel is <name>-<ver>.apk)
//
// for packages in an index the mirror has fetched.
func (a *APK) Resolve(ctx context.Context, p string) (subsys.Key, string, error) {
	parts := strings.SplitN(strings.Trim(p, "/"), "/", 4)
	arch := parts[0]
	if arch == "" || !a.knownArch(ctx, arch) {
		return "", "", fs.ErrNotExist
	}
	if len(parts) == 1 {
		return "", "", subsys.ErrDir
	}
	raw := parts[1] == ".apk"
	if raw {
		parts = parts[1:]
		if len(parts) == 1 {
			return "", "", subsys.ErrDir
		}
	}
	nv := parts[1]
	rel := ""
	if len(parts) > 2 {
		rel = strings.Join(parts[2:], "/")
	}
	pkg, err := a.pkgByNV(ctx, arch, nv)
	if err != nil {
		return "", "", err
	}
	if raw {
		return rawKey(pkg.SHA1), rel, nil
	}
	return filesKey(pkg.SHA1), rel, nil
}

// Describe implements subsys.Subsystem.
func (a *APK) Describe(ctx context.Context, key subsys.Key) (subsys.Info, error) {
	sha, raw, err := parseKey(key)
	if err != nil {
		return subsys.Info{}, err
	}
	pkg, err := a.pkgBySHA1(ctx, sha)
	if err != nil {
		return subsys.Info{}, fmt.Errorf("apk: %s: %w", key, err)
	}
	info := subsys.Info{
		Digest:  pkg.checksum(),
		Name:    pkg.Name,
		Version: pkg.Version,
	}
	if raw {
		info.Size = pkg.Size
		info.Mount = pkg.Arch + "/.apk/" + pkg.NV
	} else {
		info.Size = pkg.InstalledSize
		info.Mount = pkg.Arch + "/" + pkg.NV
	}
	return info, nil
}

// DerivedUnit reports that unpacked units are built from raw units already
// in the cache, so their fills are not upstream fetches (for usage stats).
func (a *APK) DerivedUnit(key subsys.Key) bool {
	_, raw, err := parseKey(key)
	return err == nil && !raw
}

// Fetch implements subsys.Subsystem.
func (a *APK) Fetch(ctx context.Context, key subsys.Key, sink subsys.Sink) error {
	sha, raw, err := parseKey(key)
	if err != nil {
		return err
	}
	pkg, err := a.pkgBySHA1(ctx, sha)
	if err != nil {
		return fmt.Errorf("apk: %s: %w", key, err)
	}
	if raw {
		return a.fetchRaw(ctx, pkg, sink)
	}
	return a.fetchFiles(ctx, pkg, sink)
}

func (a *APK) upstreamGet(ctx context.Context, pkg pkgRow) (io.ReadCloser, error) {
	u := a.cfg.Upstream + "/" + pkg.Repo + "/" + pkg.NV + ".apk"
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.cfg.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("apk: GET %s: %s", u, resp.Status)
	}
	return resp.Body, nil
}

// fetchRaw streams the .apk from upstream into one file, verifying it as it
// goes: the control stream against the index checksum (by the manager,
// through Sink.Digest) and the data stream against .PKGINFO's datahash.
func (a *APK) fetchRaw(ctx context.Context, pkg pkgRow, sink subsys.Sink) error {
	body, err := a.upstreamGet(ctx, pkg)
	if err != nil {
		return err
	}
	defer body.Close()
	w, err := sink.File(pkg.NV+".apk", 0o444, pkg.Size, time.Unix(pkg.BuildTime, 0))
	if err != nil {
		return err
	}
	p, err := Parse(io.TeeReader(body, w), nil)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("apk: %s: %w", pkg.NV, err)
	}
	sink.Digest(p.Checksum)
	return nil
}

// fetchFiles unpacks the data stream into the unit, reading the raw .apk
// from the cache (faulting it in if needed) so upstream is hit once.
func (a *APK) fetchFiles(ctx context.Context, pkg pkgRow, sink subsys.Sink) error {
	var r io.Reader
	if a.mgr != nil {
		f, in, err := a.mgr.OpenPath(fill.Internal(ctx), Name, rawKey(pkg.SHA1), pkg.NV+".apk")
		if err != nil {
			return err
		}
		defer f.Close()
		r = io.NewSectionReader(f, 0, in.Size)
	} else {
		body, err := a.upstreamGet(ctx, pkg)
		if err != nil {
			return err
		}
		defer body.Close()
		r = body
	}
	u := &unpacker{sink: sink, seen: map[string]bool{"": true}}
	p, err := Parse(r, u.entry)
	if err != nil {
		return fmt.Errorf("apk: %s: %w", pkg.NV, err)
	}
	sink.Digest(p.Checksum)
	return nil
}

// unpacker writes data tar entries into a sink, creating any parent
// directory the archive does not list before its children.
type unpacker struct {
	sink subsys.Sink
	seen map[string]bool
}

func (u *unpacker) parents(p string, mtime time.Time) error {
	dir := path.Dir(p)
	if dir == "." || u.seen[dir] {
		return nil
	}
	if err := u.parents(dir, mtime); err != nil {
		return err
	}
	u.seen[dir] = true
	return u.sink.Dir(dir, 0o755, mtime)
}

func (u *unpacker) entry(h *tar.Header, r io.Reader) error {
	p, err := cleanName(h.Name)
	if err != nil {
		return err
	}
	if u.seen[p] {
		return nil // root, or a repeated entry: keep the first
	}
	if err := u.parents(p, h.ModTime); err != nil {
		return err
	}
	mode := h.FileInfo().Mode().Perm()
	switch h.Typeflag {
	case tar.TypeDir:
		u.seen[p] = true
		return u.sink.Dir(p, mode, h.ModTime)
	case tar.TypeReg, tar.TypeRegA:
		u.seen[p] = true
		w, err := u.sink.File(p, mode, h.Size, h.ModTime)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return err
		}
		return w.Close()
	case tar.TypeSymlink:
		u.seen[p] = true
		return u.sink.Symlink(p, h.Linkname, h.ModTime)
	case tar.TypeLink:
		target, err := cleanName(h.Linkname)
		if err != nil {
			return err
		}
		u.seen[p] = true
		return u.sink.Hardlink(p, target)
	}
	return nil // devices, fifos: not cached
}

// Index returns the APKINDEX.tar.gz for repo (v3.20/main/x86_64), from the
// database while it is fresh and revalidated upstream by ETag after
// IndexTTL. If upstream fails, a stale copy is served. etag is the
// cache's own strong ETag for the body.
func (a *APK) Index(ctx context.Context, repo string) (body []byte, etag string, mod time.Time, err error) {
	if !validRepo(repo) {
		return nil, "", time.Time{}, fs.ErrNotExist
	}
	a.mu.Lock()
	l := a.locks[repo]
	if l == nil {
		l = new(sync.Mutex)
		a.locks[repo] = l
	}
	a.mu.Unlock()
	l.Lock()
	defer l.Unlock()

	var upEtag, lastMod, sum string
	var fetched, checked int64
	err = a.sql.QueryRowContext(ctx, `SELECT upstream_etag, last_modified, sha256, fetched_at, checked_at, body FROM apk_index WHERE repo = ?`, repo).
		Scan(&upEtag, &lastMod, &sum, &fetched, &checked, &body)
	have := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, "", time.Time{}, err
	}
	now := a.now()
	if have && now.Sub(time.Unix(0, checked)) < a.cfg.IndexTTL {
		return body, `"` + sum + `"`, time.Unix(0, fetched), nil
	}

	nb, ne, nl, rerr := a.revalidate(ctx, repo, upEtag, lastMod, have)
	switch {
	case rerr != nil && !have:
		return nil, "", time.Time{}, rerr
	case rerr != nil:
		a.cfg.Logf("apk: %s: serving stale APKINDEX: %v", repo, rerr)
		// Back off for a TTL rather than retrying on every request.
		_, err = a.sql.ExecContext(ctx, `UPDATE apk_index SET checked_at = ? WHERE repo = ?`, now.UnixNano(), repo)
		return body, `"` + sum + `"`, time.Unix(0, fetched), err
	case nb == nil: // 304
		_, err = a.sql.ExecContext(ctx, `UPDATE apk_index SET checked_at = ? WHERE repo = ?`, now.UnixNano(), repo)
		return body, `"` + sum + `"`, time.Unix(0, fetched), err
	}
	h := sha256.Sum256(nb)
	nsum := hex.EncodeToString(h[:])
	if have && nsum == sum {
		_, err = a.sql.ExecContext(ctx, `UPDATE apk_index SET checked_at = ?, upstream_etag = ?, last_modified = ? WHERE repo = ?`,
			now.UnixNano(), ne, nl, repo)
		return body, `"` + sum + `"`, time.Unix(0, fetched), err
	}
	if err := a.storeIndex(ctx, repo, nb, nsum, ne, nl, now); err != nil {
		return nil, "", time.Time{}, err
	}
	return nb, `"` + nsum + `"`, now, nil
}

// revalidate fetches repo's index, conditionally when have is set. A nil
// body with a nil error means not modified.
func (a *APK) revalidate(ctx context.Context, repo, etag, lastMod string, have bool) ([]byte, string, string, error) {
	u := a.cfg.Upstream + "/" + repo + "/APKINDEX.tar.gz"
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, "", "", err
	}
	if have && etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if have && lastMod != "" {
		req.Header.Set("If-Modified-Since", lastMod)
	}
	resp, err := a.cfg.Client.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotModified && have:
		return nil, "", "", nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", "", fmt.Errorf("apk: GET %s: %w", u, fs.ErrNotExist)
	case resp.StatusCode != http.StatusOK:
		return nil, "", "", fmt.Errorf("apk: GET %s: %s", u, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return nil, "", "", err
	}
	return b, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), nil
}

func (a *APK) storeIndex(ctx context.Context, repo string, body []byte, sum, etag, lastMod string, now time.Time) error {
	ents, err := ParseIndex(body)
	if err != nil {
		return fmt.Errorf("apk: %s: %w", repo, err)
	}
	arch := path.Base(repo)
	tx, err := a.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO apk_index (repo, upstream_etag, last_modified, sha256, fetched_at, checked_at, body)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (repo) DO UPDATE SET upstream_etag = excluded.upstream_etag, last_modified = excluded.last_modified,
			sha256 = excluded.sha256, fetched_at = excluded.fetched_at, checked_at = excluded.checked_at, body = excluded.body`,
		repo, etag, lastMod, sum, now.UnixNano(), now.UnixNano(), body); err != nil {
		return err
	}
	st, err := tx.PrepareContext(ctx, `INSERT INTO apk_pkg (`+pkgCols+`, indexed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (repo, nv) DO UPDATE SET arch = excluded.arch, sha1 = excluded.sha1, size = excluded.size,
			installed_size = excluded.installed_size, build_time = excluded.build_time, indexed_at = excluded.indexed_at
		WHERE sha1 != excluded.sha1`)
	if err != nil {
		return err
	}
	defer st.Close()
	for _, e := range ents {
		sha, err := ParseChecksum(e.Checksum)
		if err != nil {
			continue
		}
		ea := e.Arch
		if ea == "" || ea == "noarch" {
			ea = arch // noarch packages live in, and are mounted under, the repo's arch
		}
		if _, err := st.ExecContext(ctx, repo, e.NV(), ea, e.Name, e.Version, hex.EncodeToString(sha),
			e.Size, e.InstalledSize, e.BuildTime, now.UnixNano()); err != nil {
			return err
		}
	}
	return tx.Commit()
}
