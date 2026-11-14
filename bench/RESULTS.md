# Phase 2: CAS reads against plain-disk reads

> **History.** This compares the old Perkeep CAS with plain disk. The CAS
> has since been removed: the cache now stores plain files with identical
> files hard-linked (see the README's "Why plain files" and
> `docs/perf-log.md`). The recommendation below was overtaken by the
> savings runs, and `bench/cmd/readbench` no longer exists.

**Recommendation: do not add a "hot units stay on plain disk" policy.**
Make the CAS read path faster instead. Clients only read through 9P, and
there the CAS costs 15–33% on warm whole-tree reads and 40–60% on cold ones.
A prototype with a blob cache and readahead (below) cuts that to 4–14% warm
and 9–38% cold. Plain-disk hot units would give up dedup and add a second
storage state to fill, eviction and GC to win back that margin.

The CAS is clearly slower in two cases: cold sequential reads of large files
(4x) and random 4 KiB reads (2.5x warm, 4–5x cold). The first one is
fixable. The second is a cost of storing each chunk as its own file. Neither
case shows up much in the package trees we cache. If a subsystem turns out to
serve large files with random access (SQLite databases, big mmapped
libraries), pin those files to disk one at a time rather than whole units.

The bigger cost per open is the 9P server's metadata path, not the CAS. A
warm open and read of a 4 KiB file over 9P2000.L takes about 370 µs from
plain disk. Of that, the CAS adds about 20 µs. Fix the metadata path first
(see the end of this file).

## What was measured

`go run ./bench/cmd/readbench` (needs root for cold runs). Every reader
uses the same SQLite metadata. Only the source of file bytes changes:

| reader | path |
|---|---|
| direct disk / direct CAS | no 9P: `os.Open` against `cas.Store.Open` |
| 9P2000.L disk / CAS | `tree.DotL` over loopback TCP, hugelgupf/p9 client, msize 128 KiB |
| 9P2000 disk / CAS | `ninep.Server` over loopback TCP, 9fans client, msize 128 KiB |
| CAS+cache (direct, 9P2000.L) | prototype read path, `bench/cachedcas.go` |

The disk-backed servers are the production servers with only `Open` and
`ReadAt` of regular files swapped (`bench/servers.go`).

Fixtures (`bench/fixtures/fetch.sh` rebuilds them; nothing downloaded is
executed):

| fixture | files | logical | plain disk | CAS on disk | CAS/plain |
|---|---:|---:|---:|---:|---:|
| synthetic, 1 KiB to 100 MiB, random bytes | 4,343 | 639 MiB | 642 MiB | 739 MiB | 1.15x |
| Alpine 3.22 minirootfs | 87 | 7.9 MiB | 8.6 MiB | 10.4 MiB | 1.21x |
| Alpine 3.22, 155 packages (build-base, python3, nodejs, git…) | 13,488 | 540 MiB | 566 MiB | 715 MiB | 1.26x |
| Nix closure of git, python3, nodejs, cmake, vim, gcc (138 store paths) | 32,189 | 1,054 MiB | 1,147 MiB | 1,295 MiB | 1.13x |

On real package trees the disk overhead is 1.13–1.26x. The 1.7x seen on
the Go source tree came from its very small files.

"Cold" drops the kernel page, dentry and inode caches before each run, so
blobs, source files and `meta.db` all come off the block device. "Warm"
follows one untimed pass. Every number is the median of 3 runs. Readers are
interleaved within each run. Workloads:

- **read-all**: open each file, read it whole in 128 KiB reads, close. Run
  with 1 and 8 workers.
- **head4k**: open each file, read its first 4 KiB, close. This is the open
  pattern of exec, ld.so and interpreter start-up.
- **random 4k**: open the files once, then make single reads at random
  4 KiB-aligned offsets (2,000 warm, 500 cold, per run).

Environment: 4-CPU Firecracker VM, ext4 on virtio, Go 1.27.4, main at
`9390ef9`. Client and server share one process. Cold "disk" reads probably
come partly from the host's page cache (2.1 GB/s for one file). So real
cold gaps on a laptop SSD are likely smaller for plain disk and the CAS
alike, and larger on spinning or network disks. Full tables are in
[`results/2026-11-14.md`](results/2026-11-14.md), with p99s and raw records
in the JSON next to it.

## Results through 9P2000.L

This is the path Linux guests use. x = CAS ÷ disk.

**Whole trees, read-all** (ms for the whole tree):

| case | disk | CAS | x | CAS+cache | x |
|---|---:|---:|---:|---:|---:|
| Alpine 155 pkgs, warm ×1 | 8,040 | 9,278 | 1.15 | 8,619 | 1.07 |
| Alpine 155 pkgs, warm ×8 | 3,752 | 4,988 | 1.33 | 3,936 | 1.05 |
| Alpine 155 pkgs, cold ×1 | 10,500 | 16,907 | 1.61 | 14,534 | 1.38 |
| Alpine 155 pkgs, cold ×8 | 4,248 | 6,107 | 1.44 | 4,734 | 1.11 |
| Nix closure, warm ×1 | 17,478 | 21,527 | 1.23 | 19,943 | 1.14 |
| Nix closure, warm ×8 | 8,780 | 11,637 | 1.33 | 9,120 | 1.04 |
| Nix closure, cold ×1 | 21,577 | 30,384 | 1.41 | 28,292 | 1.31 |
| Nix closure, cold ×8 | 9,205 | 12,662 | 1.38 | 10,016 | 1.09 |

**Whole trees, head4k** (ms):

| case | disk | CAS | x | CAS+cache | x |
|---|---:|---:|---:|---:|---:|
| Alpine 155 pkgs, warm | 6,226 | 7,356 | 1.18 | 7,090 | 1.14 |
| Alpine 155 pkgs, cold | 9,046 | 14,479 | 1.60 | 15,396 | 1.70 |
| Nix closure, warm | 16,029 | 18,713 | 1.17 | 18,334 | 1.14 |
| Nix closure, cold | 20,838 | 29,047 | 1.39 | 31,076 | 1.49 |

**Single files, read whole** (µs per file):

| size | warm disk | warm CAS | x | warm CAS+cache | x | cold disk | cold CAS | x | cold CAS+cache | x |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 KiB | 406 | 594 | 1.46 | 382 | 0.94 | 634 | 974 | 1.53 | 949 | 1.50 |
| 64 KiB | 473 | 753 | 1.59 | 519 | 1.10 | 767 | 1,332 | 1.74 | 1,242 | 1.62 |
| 1 MiB | 1,804 | 2,990 | 1.66 | 2,135 | 1.18 | 2,264 | 6,685 | 2.95 | 4,110 | 1.81 |
| 16 MiB | 22,128 | 34,470 | 1.56 | 26,975 | 1.22 | 25,188 | 100,179 | 3.98 | 45,456 | 1.80 |
| 100 MiB | 128,672 | 194,300 | 1.51 | 177,794 | 1.38 | 142,351 | 597,049 | 4.19 | 257,664 | 1.81 |

For a 100 MiB file that is 777 MiB/s from disk, 515 MiB/s from the CAS
and 562 MiB/s with the cache when warm. Cold, it is 702, 167 and 388 MiB/s.

**Random 4 KiB reads** (p50 µs; p99 in brackets):

| file size | warm disk | warm CAS | warm CAS+cache | cold disk | cold CAS | cold CAS+cache |
|---|---:|---:|---:|---:|---:|---:|
| 1 MiB | 39 (104) | 90 (316) | 63 (217) | 93 (244) | 349 (1,270) | 379 (1,245) |
| 16 MiB | 37 (97) | 85 (364) | 65 (219) | 92 (349) | 373 (1,714) | 387 (1,679) |
| 100 MiB | 38 (100) | 104 (351) | 80 (277) | 104 (236) | 502 (1,954) | 573 (2,214) |

Plain 9P2000 shows the same picture: CAS ÷ disk is 1.22–1.61 on whole
trees and 2.2–3.1 on cold large files (see the full tables).

## Why the CAS is slower

Measured directly, with no 9P:

- **Per open, about 20 µs warm and 150–200 µs cold.** `cas.Open` stats,
  opens, reads and JSON-parses the file schema blob before the first byte.
  For a 4 KiB file, a direct read takes 3 µs from disk and 20 µs from the CAS.
- **Per chunk, a stat, an open and a read of the whole chunk into a new
  buffer.** Perkeep's `FileReader` keeps only the last chunk. So a 4 KiB
  random read loads its whole chunk: median 16 KiB, mean 40 KiB, up to
  256 KiB for incompressible data. That is 35 µs warm against 0.8 µs from
  disk, and about 5x the bytes off the device cold.
- **No readahead across chunk files.** Each chunk is its own file, so the
  kernel's readahead stops at every chunk boundary. A cold large file is
  read one chunk file at a time at queue depth 1: 336 MiB/s direct, against
  2.1 GB/s for one plain file.

On the 9P2000.L CAS path, `lnode.Open` also does a `Meta.Get` that the
disk-backed bench server skips. Part of the "9P CAS" column for small files
is therefore that one query, not the blob store. A production plain-disk
path would need the same lookup to find the spool file. The CAS+cache
column does not do that `Get`, which is why it can beat disk on 1 KiB
files.

## The prototype (`bench/cachedcas.go`)

This is a benchmark-only read path over the same `blobs/` directory:

1. A shared LRU of blobs (256 MiB here), covering file schemas, bytes
   schemas and chunks. Repeat opens and nearby random reads stop going to
   disk.
2. Sequential readahead. A read that starts where the last one ended, or
   at offset 0, fetches the chunks for the next 1 MiB with 8 workers in
   parallel.

Through 9P2000.L this made warm whole-tree reads 1.04–1.14x of disk, and
cold ones with 8 workers 1.09–1.11x. Cold large-file throughput doubled
(167 → 388 MiB/s), and warm random reads got 23–30% faster.

Two things it does not fix, and one thing it makes worse:

- Cold random reads stay 4–5x slower, because each one still pulls a whole
  chunk file. Only smaller chunks or packed blobs with range reads (Perkeep
  `SubFetch` over `diskpacked` or a pack file) would change that.
- The LRU does not help scans of trees larger than itself (Alpine 715 MiB,
  Nix 1.3 GiB against 256 MiB). The direct "warm" numbers for those trees
  get slightly worse with the cache, which is LRU thrash plus the copy.
- Cold head4k got worse (1.39 → 1.49x on Nix), because the read at offset
  0 starts 1 MiB of readahead for a file that only needs 4 KiB. Readahead
  should start on the second sequential read, or grow from a small window
  the way the kernel's does.

## Suggested next steps

1. **CAS read path** (cas package): cache parsed file schemas by ref,
   keep a small chunk cache, and add a growing sequential readahead. The
   prototype is a working starting point.
2. **9P metadata path** (meta and tree, outside this benchmark's scope,
   but it is where the time goes). A profile of warm 9P2000.L read-all on
   the Alpine tree shows `Meta.Lookup` at about 50% of CPU. About 80% of
   that is `database/sql` re-preparing the lookup statement: the pool keeps
   only 2 idle connections, so a prepared statement is re-parsed on every
   new connection. `SetMaxIdleConns(64)` alone made 8-worker read-all
   1.7x faster (3,906 → 2,285 ms). The p9 server also walks one component
   at a time and calls `GetAttr` on each step, which runs an `Nlink`
   COUNT query. An in-memory dirent and inode cache in front of SQLite
   would cut most of the 370 µs per open.
3. **Small-file packing** matters for disk footprint (1.13–1.26x here) more
   than for read speed. If it is done with a pack file and range reads, it
   also fixes cold random reads.

## Also found

The 9fans plan9 client frees a fid number before its Tclunk is sent. The
`ninep` server handles requests concurrently, so a second goroutine on the
same connection can reuse that number in a Twalk that overtakes the clunk,
and gets "fid already in use". The bench works around it with one
connection per open file. A real client that pipelines would hit this too.
Handling Tclunk and Twalk for the same fid in order on the server would fix
it.
