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
| `internal/subsys` | The `Subsystem` interface each subsystem implements, and the conventions below |
| `internal/fill` | The fill-path API: unit manager, spool, quota, reclaimer hook (implementation in progress) |
| `cmd/wanix-cache` | `serve`, `import`, `stats`, `units` |

## Try it

Needs Go 1.27.2 or newer (the pinned Perkeep commit requires it).

```sh
go run ./cmd/wanix-cache import -data ./cache -key alpine ./some-rootfs
go run ./cmd/wanix-cache stats -data ./cache
go run ./cmd/wanix-cache serve -data ./cache -9p 127.0.0.1:5640 -9p2000 127.0.0.1:5641
# Linux: mount -t 9p -o trans=tcp,port=5640,version=9p2000.L,ro 127.0.0.1 /mnt
```

Imported trees appear at `/local/<key>`.

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
