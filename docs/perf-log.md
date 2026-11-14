# Performance log

One entry per change that affects speed or space, newest first, with the
workload and the numbers before and after. Add an entry with every
performance change, in the same commit or the one right after it.

Unless an entry says otherwise, the setup is the same for all entries:

- **Machine:** a 4-CPU Firecracker VM, ext4 on virtio. The client and the
  server run in one process over loopback TCP.
- **W7:** the savings plan's mixed host (APK, OCI and Nix, 102,546 files,
  3.9 GiB). It is built by `bench/savings/run.sh`.
- **ninebench:** `bench/savings/ninebench`. It reads every file over
  9P2000.L with 8 readers.
  - **Warm, repeat read** is the second pass of `-passes 2`.
  - **Cold** drops the kernel caches first (`-cold`).
- **Raw results:** in the project files under `savings-results/`.

## 2026-11-14 · Plain files with hard links replace the CAS

**Commit:** the one that adds this entry ("Replace the CAS with plain files
and hard links")

The Perkeep CAS is gone. Each stored file is a plain file under
`DATA/files/<unit>/<inode>`, and identical files are hard-linked across
units and subsystems. Ingest links spool files into the store instead of
chunking and copying them. zstd is now per file and still off by default.

**W0** (the Go source tree as one local unit: 12,054 files, 126.3 MiB
logical), old build at cc1158f against the new store, ninebench with
`-passes 2`:

| | CAS (cc1158f) | Plain files |
|---|---:|---:|
| Import (s) | 23.7 | 22.7 |
| Store on disk (MiB) | 281.2 | 154.7 |
| Cache directory with DB (MiB) | 289.4 | 162.4 |
| First read (files/s) | 13.1k to 14.3k | 15.6k to 16.4k |
| Warm, repeat read (files/s) | 17.3k to 17.8k | 20.6k to 21.8k |
| Cold (files/s) | 5.9k to 6.9k | 10.3k to 11.7k |

With `serve -zstd 2` the new store holds W0 in 69.6 MiB of files and reads
12.9k files/s on a first pass and 16.5k warm.

**W7** (from the savings runs that led to the decision; there the CAS
store was compared with a hard-linked plain copy of the same tree):

| | Plain files + hard links | CAS |
|---|---:|---:|
| Store (MiB) | 3,372 | 3,013 |
| Store with zstd (MiB) | 1,519 | 1,293 |
| Whole tree over 9P, with the namespace cache (files/s) | 15.6k | 12.5k |

The CAS saved about 11% on W7 and cost about 20% of read speed, and on
small-file trees it was larger than plain files. Read speed comes first,
so plain files won.

**The entries below are from the CAS era.** Their numbers were measured on
the old store; packing, the blob cache and per-blob zstd no longer exist.

## 2026-11-14 · Compression read cost (zstd stays off by default)

**Commit:** 9a013b7 (the decision; per-blob zstd itself is f5e9e87)

This is a measurement, not a speedup. Read speed comes first, so
`serve -zstd L` stays optional and off by default.

W7 over ninebench, with the namespace cache in place. The CAS + zstd
store also packs blobs of 64 KiB or less. On the CAS side, read rows use
the unpacked store, and the size, settle-time and large-file rows use the
store packed at 64 KiB.

| | CAS | CAS + zstd |
|---|---:|---:|
| Whole tree, warm repeat read (files/s) | 12,487 | 8,016 |
| Whole tree, first read (files/s) | 9,935 | 6,729 |
| Files of 1 MiB or more, warm (GiB/s, before the namespace cache) | 1.7 | 1.0 |
| Store plus metadata DB on disk (MiB) | 3,210 | 1,488 |
| Time to fill and settle W7 (s) | 240 | 293 |

## 2026-11-14 · Namespace cache in the 9P servers

**Commit:** 5bc0c10

`internal/tree/cache.go` keeps lookups, inodes and link counts in memory.
It is dropped whenever a commit changes inodes or dirents (`meta.DB.Gen`).
Misses are not cached.

Workload: W7 over ninebench, unpacked CAS. The "before" column already
includes the SQLite idle connections fix below.

| | Before | After |
|---|---:|---:|
| Files of 64 KiB or less, first read (files/s) | 6,261 | 10,903 |
| Files of 64 KiB or less, warm repeat read (files/s) | n/a | 15,315 |

Both changes in this commit together, by store. The figures are W7 whole
tree, warm repeat read, in files per second:

| Store | Before | After |
|---|---:|---:|
| Files on disk | 2,244 | 15,615 |
| Files + zstd (hard links) | 2,105 | 11,646 |
| CAS | 2,125 | 12,487 |
| CAS + zstd | 2,089 | 8,016 |

## 2026-11-14 · Keep idle SQLite connections

**Commit:** 5bc0c10

`meta.Open` now calls `SetMaxIdleConns(64)`. The `database/sql` default of
2 idle connections meant that concurrent 9P requests opened a new SQLite
connection on most queries. Each new connection re-reads the schema and
re-prepares its statements, and a CPU profile showed that taking over a
third of the time.

| Workload | Before | After |
|---|---:|---:|
| W7, files of 64 KiB or less, first read, CAS (files/s) | 2,177 | 6,261 |
| Phase 2 Alpine tree, 155 packages, warm read-all with 8 workers, 9P2000.L (ms) | 3,906 | 2,285 |

## 2026-11-14 · Small-blob packing at 64 KiB (optional)

**Commit:** 1490c0e

`serve -pack-small 65536` stores blobs of 64 KiB or less in Perkeep
diskpacked pack files instead of one file each. It saves space and is about
neutral for reads.

Workload: W7.

| | Unpacked CAS | Packed at 64 KiB |
|---|---:|---:|
| Blob store on disk (MiB) | 3,467 | 3,013 |
| Time to fill and settle W7 (s) | 261 | 240 |
| Whole tree, warm (files/s, before the namespace cache) | 2,512 | 2,376 |
| Whole tree, cold (files/s, before the namespace cache) | 2,245 | 2,279 |
| Files of 1 MiB or more, warm (MiB/s) | 1,768 | 1,707 |

Packing every blob instead took the store to 2.98 GiB, but it doubled the
fill time to 504 s, because diskpacked syncs after every blob.

## 2026-11-14 · Blob cache and readahead (prototype, not in production)

**Commit:** 3ad3043 (`bench/cachedcas.go`). Full tables are in
`bench/RESULTS.md`.

This is a benchmark-only CAS read path with two parts:

- a shared 256 MiB LRU of blobs;
- an 8-way parallel readahead of 1 MiB on sequential reads.

It hasn't been moved into `internal/cas` yet.

Workload: phase 2 fixtures over 9P2000.L, whole-tree read-all in ms. The
times are before the SQLite and namespace cache fixes.

| Case | Disk | CAS | CAS + cache |
|---|---:|---:|---:|
| Alpine 155 packages, warm, 8 workers | 3,752 | 4,988 | 3,936 |
| Alpine 155 packages, cold, 8 workers | 4,248 | 6,107 | 4,734 |
| Nix closure, warm, 8 workers | 8,780 | 11,637 | 9,120 |
| Nix closure, cold, 8 workers | 9,205 | 12,662 | 10,016 |
| 100 MiB file, cold (MiB/s) | 702 | 167 | 388 |

It made cold reads that start at offset 0 for small files slightly worse.
Cold head4k on the Nix closure went from 1.39x to 1.49x of disk. Readahead
should start on the second sequential read.
