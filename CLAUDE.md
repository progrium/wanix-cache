# Notes for Claude

- Commit straight to `main`; no pull requests.
- Every change that affects speed or space gets an entry in
  `docs/perf-log.md` (date, change, workload, before and after numbers,
  commit), in the same commit or the next one.
- Read speed matters more than storage: optimizations that save space at a
  read cost stay optional and off by default (like `serve -zstd`).
- The store is plain files with identical files hard-linked
  (`internal/store`). There is no CAS and no chunking: it was measured and
  removed (README, "Why plain files"). Don't bring one back without new
  numbers in `docs/perf-log.md`.
