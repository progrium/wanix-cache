# wanix-cache

A local read-through cache for H9 guests. Cached content lives in one
Perkeep content-addressed blob store, so identical files and chunks are
stored once across every subsystem (APK, OCI, Nix). Clients see a
**read-only** filesystem over 9P2000.L (Linux guests) and, optionally, plain
9P2000.

This is phase 1 of the plan: the core namespace, the CAS, and the 9P
servers. Fetching from upstream, the spool and async ingest, eviction and GC,
the debug savings mode, and the subsystems come in later phases.

## Layout

| Path | What |
|---|---|
| `internal/meta` | SQLite metadata: inodes, `(parent, name)` dirents, units and their states, content objects |
| `internal/cas` | Perkeep localdisk store with a quota on unique blob bytes (ported from the perkeep-dualfs prototype) |
| `internal/tree` | The namespace with bytes from the CAS: the 9P2000.L server (`dotl.go`) and the adapter for the plain 9P2000 server |
| `internal/ninep` | A small read-only 9P2000 server |
| `internal/ingest` | Imports a local directory as a unit (stand-in for subsystem fetchers) |
| `internal/subsys` | The `Subsystem` interface each subsystem implements, `Viewer` for virtual views, and the conventions below |
| `internal/subsys/apk` | Alpine packages: read-through HTTP mirror for in-guest apk, per-package overlay layers |
| `internal/usage` | Usage stats per package, content object and path, kept across eviction; never-opened report |
| `internal/subsys/oci` | OCI images: registry client with anonymous token auth, layer units, merged rootfs views |
| `internal/fill` | The fill path: unit manager, single-flight faults, spool, async ingest, crash recovery, quota, debug shadow tree |
| `internal/savings` | Debug savings mode: disk vs CAS savings per subsystem (`/.cache/stats`, `wanix-cache stats`) |
| `internal/h9index` | Generates, stores and serves each root's `/.h9index` (hyper9's mount-index) from metadata |
| `internal/gc` | Eviction and blob GC: global refcounts, LRU with watermarks, pins, sweep, audit |
| `bench` | Phase 2 read benchmark: CAS against plain disk, direct and over both 9P servers (`bench/cmd/readbench`); findings in [`bench/RESULTS.md`](bench/RESULTS.md) |
| `cmd/wanix-cache` | `serve`, `import`, `index`, `stats`, `units`, `usage`, `gc`, `evict`, `refault`, `audit`, `pin` |

## Try it

Needs Go 1.27.2 or newer (the pinned Perkeep commit requires it).

```sh
go run ./cmd/wanix-cache import -data ./cache -key alpine ./some-rootfs
go run ./cmd/wanix-cache stats -data ./cache
go run ./cmd/wanix-cache serve -data ./cache -9p 127.0.0.1:5640 -9p2000 127.0.0.1:5641
# Linux: mount -t 9p -o trans=tcp,port=5640,version=9p2000.L,ro 127.0.0.1 /mnt
```

Imported trees appear at `/local/<key>`, each with a generated `/.h9index`.
Attach to one as a root by giving its path as the attach name
(`aname=local/alpine`), and regenerate indexes with
`wanix-cache index -data ./cache /local/alpine` or `-all`.

## Design notes

- **Units and states.** Every cached thing is a unit (an apk, an OCI layer, a
  Nix store path) moving through `absent → fetching → spooled → resident →
  evicted`. A unit's manifest (its tree) stays in the DB after its content is
  evicted, so a later read can fault it back in.
- **Global dedup.** Content objects are keyed by whole-file SHA-256 with no
  subsystem column, so a file is chunked once no matter which subsystem
  fetched it. File schemas are written without a name or mtime, so identical
  bytes always get the identical ref.
- **Symlinks** are stored for every subsystem. 9P2000.L serves them; plain
  9P2000 has no way to, so that server hides them.
- **Read-only.** All client writes fail with EROFS. Only the cache writes.
- **Namespace cache.** The 9P servers keep lookups, inodes and link counts in
  memory (`internal/tree/cache.go`), so a walk to a path seen before makes no
  SQLite query. `meta.DB.Gen` goes up on every commit that touches inodes or
  dirents, and the whole cache is dropped when it does. Misses are not
  cached, so a name that appears later is found (and faulted in) as before.
  The metadata DB also keeps up to 64 idle SQLite connections: with the
  database/sql default of 2, concurrent 9P requests reopened a connection on
  most queries, re-reading the schema each time.

## Eviction and GC

- **Two refcounts, both global.** `content.refs` counts file inodes of
  resident units per content object; `gc_blob.refs` counts content objects
  per CAS blob. Neither has a subsystem column, so a blob shared by an apk
  and an OCI layer is freed only after the last reference anywhere goes.
- **Evict** moves a unit `resident -> evicted`, clears its file inodes'
  content refs (the manifest stays), and drops refcounts. Blobs left at zero
  are doomed; a **sweep** deletes them through the blobserver
  (`RemoveBlobs`; Perkeep has no GC of its own). The next read refaults the
  unit into the same inodes.
- **Policy.** One cap with high and low watermarks (default 95% and 85%).
  Victims are the least recently used resident units that are not pinned,
  except that units which would free nothing (all shared) wait until no
  unit that frees bytes is left. Subsystems can veto an eviction
  (`subsys.Evicter`) or order it (`EvictionOrderer`, e.g. Nix referrers
  first).
- **Evict before admit.** `fill.LocalQuota` and the CAS quota both call the
  collector when a write does not fit, so a write fails with ENOSPC only
  when nothing evictable remains.
- **Pins.** In-memory pins from the fill manager (fills, open handles,
  views), `Collector.Acquire`, and durable pins in `gc_pin` (package
  indexes): `wanix-cache pin -key K -reason R`.
- **Ingest contract.** Put blobs through a `cas.Session`, record
  `RefContent` in the transaction that makes the unit resident, and close
  the session after commit. The session holds every blob it wrote or found,
  so a sweep cannot delete a doomed chunk that new content just deduped
  against. The GC discovers new content rows itself and counts their blobs
  (backfill), so ingest never calls into it.
- **Audit** (`wanix-cache audit [-fix] [-deep]`) recomputes both refcounts,
  compares them with the blobs on disk, deletes orphans, and evicts units
  whose blobs are missing so they refetch.

## h9index for roots

Every root the cache serves can carry a generated
[`/.h9index`](https://github.com/progrium/hyper9/blob/main/design/mount-index.md),
so h9 answers walks, stats and directory reads from it and only opens and
reads reach the cache.

- **Attach names select roots.** Both servers treat the attach name as a
  path into the namespace (`local/alpine`, `nix/closure/<path>`), and `..`
  stops at the attach root. An empty attach name is the whole namespace,
  which has no index.
- **From metadata only.** `h9index.Generate` lists the tree through the same
  functions the servers answer `Tstat` and directory reads with
  (`ninep.Stat`, and `lqid`/`lattr` for .L), never reading file bytes or
  faulting anything in, so an evicted root can be indexed and its index
  stays served. `tree.FS.IndexSources` provides those listings.
- **One index per transport.** h9 can mount a root over plain 9P2000 or
  9P2000.L. The 9P2000 server hides symlinks, so its index (`9p`) leaves them
  out; the .L server's index (`dotl`) lists them with `DMSYMLINK` and the
  target's length, as h9's translator is expected to present them to
  devmnt. Each server serves its own variant at the same path, with its own
  qid.
- **Format.** `h9index 1`, then one line per name sorted by full path in byte
  order; a name with a space, a newline or a non-UTF-8 byte is left out and
  its directory gets an `!unindexed /dir` line. `/.h9index` doesn't list
  itself. It covers every file in the cache's tree; it doesn't know what h9
  binds over.
- **qid.vers is 0** for every file in both servers: unit trees never change
  in place, and a version derived from the CAS ref changed when ingest set
  it, which h9 would see as a stale file.
- **Storage and serving.** The text is kept in `h9index_file` (owner
  `h9index`, its own migrations), keyed by root directory and variant, and
  pinned to that directory's mtime and unit so a reused inode never serves
  another tree's index. The file is a synthetic `0444` entry in the root,
  listed by both servers (the .L server lists it after every real entry, at a fixed offset, so real offsets never shift), with qid.path `dir | 1<<63` (`| 1<<62` for .L),
  qid.vers the FNV-32a of the text, and mtime the time the text last
  changed. Regenerating an unchanged tree changes nothing, so h9's host cache
  keeps hitting. A generated index shadows a `.h9index` shipped in the tree.
- **Hook.** A subsystem calls `subsys.RootIndexer.IndexRoot(ctx, path)` once
  every unit under a root has its manifest, and again if the tree changes.
  While serving, `rootIndexer` in `cmd/wanix-cache` is set before
  `registerSubsystems`, so a subsystem's register function can hand it over.
  Indexing fails with `h9index.ErrNotReady` while a unit under the root is
  absent or fetching, since a partial listing would make h9 answer "does not
  exist" for names that are coming. Local imports are indexed when the
  import finishes.

## Subsystem conventions

- One package per subsystem: `internal/subsys/apk`, `internal/subsys/oci`,
  `internal/subsys/nix`. Its `Name()` is that word, its namespace root
  (`/<name>/`), and the `subsystem` column on units.
- Its own tables are named `<name>_<table>` (`apk_index`). It never writes the
  core tables (`inode`, `dirent`, `unit`, `content`) directly; it goes
  through `subsys.Sink` and `fill.Manager`.
- Schema changes are numbered `meta.Migration`s returned by `Migrations()`,
  applied by `meta.Migrate` and recorded per owner in `schema_migrations`.
  Versions are 1..n, and a merged migration is never edited or renumbered:
  a change is always a new version. Core migrations live in
  `internal/meta/migrate.go` under owner `core`.
- Unit keys must be stable and content-identifying (apk checksum, layer
  diff_id, store path hash), because refetching must produce the same tree.

## APK

`internal/subsys/apk` makes `apk add` inside a guest immediate for any
package used before on this machine, without sharing a writable apk cache
directory into the guest.

- `-apk-mirror ADDR` serves a mirror of `-apk-upstream` (the Alpine CDN by
  default). Point the guest at it: `http://ADDR/v3.20/main` in
  `/etc/apk/repositories`. It serves `/<branch>/<repo>/<arch>/APKINDEX.tar.gz`
  and `<name>-<ver>.apk`, read-only.
- **APKINDEX** is kept whole in `apk_index` (in the metadata database, so it
  is never evicted) and revalidated upstream by ETag after
  `-apk-index-ttl` (1 minute). If upstream is down the cached copy is
  served. Its records go to `apk_pkg`, which is never pruned, so a
  superseded package's units can still be described and refetched.
- **Each package is two units**, keyed by the SHA-1 in its APKINDEX `C:`
  checksum: the raw `.apk` (key `<sha1>.apk`, mounted at
  `/apk/<arch>/.apk/<name>-<ver>/`), which the mirror serves as an immutable
  file because it can't be rebuilt byte for byte; and its unpacked data
  files (key `<sha1>`) at `/apk/<arch>/<name>-<ver>/`, usable as an overlay
  layer, with no install scripts run and an `/.h9index`. Alpine rebuilds
  some packages per branch without a version bump, so one name-version can
  name several builds: the first one mounted keeps `<name>-<ver>`, later
  ones mount at `<name>-<ver>@<first 12 hex of the SHA-1>` (`apk_mount`). The layer is built
  from the cached `.apk`, so upstream is hit once; `-apk-unpack` (on) builds
  it in the background after the mirror serves the `.apk`.
- **Verified while streaming:** the control stream's SHA-1 must match the
  index checksum and the data stream's SHA-256 must match `.PKGINFO`'s
  `datahash`, or the fill fails and leaves nothing behind.
- **Exit check, against the real CDN:** `apk.static --root R --initdb add
  busybox musl-utils zlib` through the mirror took 1.5 s the first time and
  0.07 s the second, with no upstream requests.

## Usage stats

`internal/usage` runs inside `serve` for every subsystem. History is kept
in `usage_*` tables, separate from `unit`, so it survives eviction and
upgrades and never feeds the LRU.

| Level | Key | Counts |
|---|---|---|
| Package | subsystem, name, version (from `Info.Name`/`Version`) | opens, fetches, refetches, bytes read |
| Object | CAS content ref (one per distinct file content) | opens, bytes read, which paths hold it |
| Path | subsystem, package (or unit key), path in the unit | opens, bytes read |

- Each level has daily rows (packages kept 90 days, objects and paths 30)
  and running totals kept forever: first seen, last used, days used.
- A **refetch** is a fault on a unit that was evicted before, which
  measures eviction regret. Units a subsystem marks `usage.Derived` (an
  apk's unpacked layer, a Nix closure) are not counted as fetches, and
  opens made with `fill.Internal(ctx)` (the cache reading its own units)
  are not counted at all.
- **Hot path:** an open is one increment in a sharded in-memory map keyed
  by inode, and bytes read are added at close. A flush every 30 s maps
  inodes to units, contents and paths in one transaction.
- **Never opened:** `wanix-cache usage -untouched` lists the files in
  spooled or resident units with no recorded open, per unit with sizes and
  the never-read share. Hard links count as opened through any name.
- `wanix-cache usage [-subsystem S] [-days N] [-n N] [-untouched] [-json]`
  reads the database directly; `/.cache/usage` on the 9P root is the same
  report (with never-opened files), generated at lookup.
- Caveats: a guest's page cache can serve repeat opens without asking the
  server, so days used is the robust signal. Files apk installs from the
  mirror live in the guest's own root, so for APK only the `.apk` itself
  is seen; file-level stats come from overlay-layer use.

## Nix

`internal/subsys/nix` reads Nix store paths through from binary caches
(cache.nixos.org by default, `-nix-substituters` to change). No Nix is
needed on the cache side.

- `/nix/store/<hash>-<name>/` is one store path (unit key `<hash>-<name>`).
  Looking it up fetches the narinfo, streams the NAR (xz, zstd, bzip2, gzip
  or none), checks the NarHash over the stream, and unpacks it into the
  spool and then the CAS, so files dedupe across paths and subsystems.
- `/nix/closure/<hash>-<name>/` is the path's whole closure as a root, as
  a metadata-only view (`subsys.Viewer`): `nix/store/<member>/` for every
  path reachable through narinfo References, each the member's own unit.
  Using a view starts every member's fill, pins the members until it has
  been idle for 10 minutes, and gets the root a `/.h9index` once every
  member has its manifest.
- `-nix-cache ADDR` serves a binary cache (`nix-cache-info`, `<hash>.narinfo`,
  `nar/<hash>.nar`) with `Compression: none`. NARs are rebuilt from the tree
  and match the upstream NarHash; upstream signatures pass through unchanged,
  so a guest's Nix keeps verifying them. Use it as a substituter:
  `nix copy --from http://HOST:PORT /nix/store/...` or
  `--option substituters http://HOST:PORT`.
- Eviction goes from the leaves inward (`subsys.EvictionOrderer`): a path is
  a candidate only when no live path refers to it, and paths in a closure
  view in use are pinned.
  `-nix-evict-delete` runs `nix-store --delete` first (`subsys.Evicter`).
- Usage is per store path: the package name and version come from the store
  name (`hello-2.12.1` is `hello`, `2.12.1`).
- `WANIX_NIX_UPSTREAM=1 go test -run Upstream ./internal/subsys/nix` runs the
  end-to-end check: a cold 9P read of `hello`, a closure read, and real
  `nix copy` from the endpoint with signatures verified.

## OCI

Images are a distribution format for H9 roots; nothing here runs them.
Walking `/oci/<registry>/<repo>@<tag|digest>/` (for example
`/oci/docker.io/library/alpine@3.20/`) is the whole pull: H9 can mount
that path as a 9P root directly.

- **First access** resolves the reference (index, then the platform
  manifest for `-oci-platform`, then the config) and starts every layer's
  fill at once, top layer first, at most `-oci-downloads` per registry.
  Tags are trusted for `-oci-tag-ttl`; if the registry is unreachable the
  cached resolution is used. Digest references never re-resolve.
- **Layers are units** keyed by diff_id, mounted at `/oci/layers/<diff_id>/`,
  stored unpacked file by file (gzip, zstd and plain tar layers), so files
  dedupe across layers, images and subsystems. The compressed blob digest
  and the diff_id are both verified; `oci_blob` keeps the
  compressed-digest-to-diff_id mapping.
- **The merged rootfs is a view.** Its directories are virtual
  (`subsys.Viewer`, inode numbers with bit 61 set); its files are the real
  inodes of the winning layer, so reads go through the fill path and are
  served while the layer is still downloading. Whiteouts and opaque
  directories are applied at lookup time: a name is looked up from the top
  layer down, and a layer still being fetched is waited on only if the name
  is not there yet. A listing waits for the layers it merges.
- **Auth** is anonymous bearer tokens (Docker Hub, GHCR, Quay). Registries
  that need credentials are not supported yet.
- **Eviction is per image.** An image in use pins its layers (released
  after 10 minutes idle). Through `EvictionOrder` the reclaimer evicts
  layers no live image uses; when there are none, the least recently used
  image is marked evicted, releasing the layers only it used. `OnEvict`
  refuses a layer a live image still uses. Using an image again revives it.
- Once every layer's listing is final, the rootfs gets a `/.h9index`.
- Not yet: eStargz / zstd:chunked per-file lazy fetch, devices and FIFOs
  (skipped), setuid bits and ownership (the namespace keeps permission
  bits only), a repeated tar entry (the first one wins).

`WANIX_OCI_LIVE=1 go test -run Live ./internal/subsys/oci` pulls
alpine:3.20 from Docker Hub through the 9P2000.L server.

## Savings (debug mode)

`wanix-cache stats` and `/.cache/stats` report what the CAS saves, in
total and per subsystem. Sizes are allocated blocks (st_blocks × 512).
`stats -json` prints the same report as JSON, one result per run for the
savings test plan.

- **CAS total**: the blob store (blob files and localdisk's hash-prefix
  directories) plus the metadata DB.
- **Whole-file dedupe**: 1 − distinct content bytes ÷ logical bytes.
  **Chunk dedupe**: 1 − unique chunk bytes ÷ distinct content bytes, which
  is what chunking adds on top.
- **Schema blobs**, **per-blob overhead** (blob files' allocated minus
  logical bytes) and **store directories**, each as a share of CAS total,
  plus slack by content size class (< 4, 16, 64 KiB), which tells you
  whether small-file packing would pay.
- **Exclusive and shared** blob bytes per subsystem, with the bytes shared
  with each other subsystem. **Attributed** splits each shared blob evenly
  between the subsystems that reach it.
- **B2**, plain unpacked with no dedupe, needs the shadow tree:
  `serve -debug-shadow` keeps every resident unit unpacked under
  `DATA/shadow/<subsystem>/<unit id>`. Right after ingest the tree is
  hard-linked from the unit's spool files, so it costs no copy; missing
  trees are written from the CAS at startup and before each `/.cache/stats`.
  Eviction removes a unit's tree. The shadow is **not** counted by
  `-max-gib`, so leave headroom. **B3 (est.)** approximates the shadow with
  identical files hard-linked. Running `rdfind`/`jdupes` over a copy of the
  shadow gives the measured B3.
- The 20 units with the most exclusive bytes and the 20 most-shared content
  objects, which explain most surprises.

W0 (the Go source tree as one local unit, ext4):

| | |
|---|---|
| Logical | 126.3 MiB (12,054 files) |
| B2, plain unpacked | 162.3 MiB |
| CAS total | 289 to 297 MiB (store 281.2 MiB + DB 8 to 16 MiB with WAL) |
| Whole-file / chunk dedupe | 1.2% / 0.1% |
| Per-blob overhead | 74.8 MiB, 26% of CAS total (24,040 blob files) |
| Store directories | 79.7 MiB, 27% of CAS total (20,410 directories) |
| Slack from contents under 4 KiB | 68% of all slack |

So on small-file trees, localdisk's layout costs more than half the CAS:
one 4 KiB block per blob file (two per file) plus one per hash-prefix
directory. That is the "per-blob overhead above 10%" row of the test plan's
decision table, before the larger workloads have run.

**Small-blob packing (experimental).** `serve -pack-small N` stores blobs
of at most N bytes in Perkeep diskpacked pack files under `blobs/packed`
(LevelDB index, holes punched on delete) instead of one localdisk file
each; it is recorded in `blobs/packed/THRESHOLD` and stays on. The pack
index is locked by one process, so stop the server before `stats`, `gc` or
`audit`. On W7 of the savings plan (3.9 GiB logical): CAS 3.57 GiB
without it, 3.13 GiB at 64 KiB (4.9% below hard-linked plain disk), 2.98
GiB packing every blob (9.5% below), but diskpacked syncs every blob, so
packing everything doubled the time to ingest W7 (504 s against 261 s).

**Per-blob zstd (optional, off by default).** Read speed comes before
storage, so compression is opt-in. `serve -zstd L` (1 fastest to 4 best),
only on a new, empty cache, stores every blob as a header byte and either
its bytes or a zstd frame of them, whichever is smaller. Refs stay hashes
of the uncompressed bytes, so dedupe is unchanged. With `-pack-small 65536`
it took W7 to 1.45 GiB (56% below hard-linked plain disk) and 293 s to
ingest. The cost is reads: over 9P2000.L, with the namespace cache, a W7
re-read went from 12,487 to 8,016 files a second, and large files from
about 1.7 to 1.0 GiB/s. Blob sizes in `stats` are then stored
(compressed) bytes, so its chunk-dedupe ratio also counts compression.

## Measurements (phase 1)

- Lookup by `(parent, name)`: 7 µs at 1k entries, 12 µs at 100k entries
  (`go test -v -run LookupScales ./internal/meta`). The prototype's
  `index.json` was 37x slower at 11.6k entries.
- Importing the Go source tree (12k files, 126 MiB): 26 s the first time,
  1 s the second (whole-file digests hit, nothing re-chunked).
- On disk the CAS took 282 MiB for that tree against 163 MiB for the plain
  tree: one localdisk file per blob, at least two blobs per file. Small-file
  packing is a known follow-up.

## Tests

```sh
go test -race ./...
go test -short ./...   # skips the 100k-entry scaling test
```
