# Savings test plan harness

Runs the workloads of the savings test plan against a local copy of the
upstreams, so the cache's real fill paths are exercised but the network is
only needed once, to build the corpus.

| File | What |
|---|---|
| `corpus.py` | Builds the corpus: an Alpine mirror tree (dependencies resolved from APKINDEX), Docker Hub images as blobs plus tag refs (linux/amd64), and Nix closures from a nixpkgs channel tarball (`nix-instantiate --eval`, no builds) copied from cache.nixos.org. Writes a pinned manifest per input set with the B1 sizes. |
| `savingsbench serve-corpus` | Serves the corpus as the three upstreams: Alpine mirror (`:8401`), OCI registry with no auth (`:8402`, plain HTTP because the OCI client uses HTTP for localhost), Nix binary cache (`:8403`). |
| `savingsbench apk` | Fetches each package of an apk manifest through the cache's HTTP mirror, as `apk add` would (`-k` logs failures and goes on). |
| `savingsbench walk` | Opens and reads every file under 9P roots (`/oci/...`, `/nix/closure/...`, `/apk/x86_64`), one connection per reader. |
| `savingsbench settle` | Waits until no unit is fetching or spooled. |
| `savingsbench b3` | B3 from the shadow tree: identical files hard-linked (`-link` does it in place, like `rdfind -makehardlinks` or `nix-store --optimise`, and measures with st_blocks). |
| `savingsbench variants` | V1 (one blob per distinct content, no chunking) and V2 (each data blob zstd level 3, kept raw when that isn't smaller), computed offline from the V0 store. |
| `run.sh` | One run: fresh cache with `-debug-shadow`, drive, settle, `stats -json`, B3, variants, `run.json` (commit, filesystem, timings, corpus manifest hashes). `AGAIN=1` drives the same inputs a second time into the same cache (W8). |
| `all.sh` | W0 to W8 in order. |
| `report.py` | Turns result directories into one JSON table and a Markdown summary. |

```sh
go build -o bin/wanix-cache ./cmd/wanix-cache
go build -o bin/savingsbench ./bench/savings/savingsbench
C=corpus; P="python3 -I bench/savings/corpus.py"
SET="build-base git curl openssh python3 nodejs go"
for r in 3.18 3.19 3.20 3.21; do $P apk $C $r $SET; done
$P oci $C w3 alpine:3.20 python:3.12-alpine3.20 node:22-alpine3.20 golang:1.23-alpine3.20 nginx:1.27-alpine
$P oci $C w4 python:3.12.{5,6,7}-slim-bookworm node:22.{9,10,11}.0-bookworm-slim nginx:1.27.{0,1,2}
$P oci $C w5 debian:bookworm-slim debian:bookworm ubuntu:22.04 ubuntu:24.04 \
  python:3.12-slim-bookworm buildpack-deps:bookworm-curl eclipse-temurin:21-jre-jammy eclipse-temurin:21-jre-noble
R=https://releases.nixos.org/nixos
$P nix $C 24.05-jul $R/24.05/nixos-24.05.2310.7dca15289a1c/nixexprs.tar.xz gcc python3 nodejs git
$P nix $C 24.05-sep $R/24.05/nixos-24.05.5218.23cbb250f3bf/nixexprs.tar.xz gcc python3 nodejs git
$P nix $C 24.05-oct $R/24.05/nixos-24.05.5809.4eb33fe664af/nixexprs.tar.xz gcc python3 nodejs git
$P nix $C 24.11-dec $R/24.11/nixos-24.11.711542.b47fd6fa00c6/nixexprs.tar.xz gcc python3 nodejs git

bin/savingsbench serve-corpus -corpus $C &
CORPUS=$C/manifests/.. GOSRC=$(go env GOROOT)/src bench/savings/all.sh
python3 -I bench/savings/report.py $C results > report.json
```

Tags float, so a corpus built later differs; the manifests pin what a run
used, and `run.json` records their hashes.

## Deviations from the plan

- W4 uses the `-slim` Python and Node images (nginx is the Debian default)
  to fit the session's disk; W6's dev shell is the union of the runtime
  closures of `gcc`, `python3`, `nodejs` and `git`, not a `mkShell`.
- W6 has four revisions: 24.05 in July, September and October, and 24.11
  in December. September and October evaluate to identical store paths for
  all four packages (no rebuild in the stable channel that month), so July
  was added to get a within-channel generation that differs.
- B3 is measured by hard-linking identical files in the shadow tree itself
  (`b3 -link`), not with `rdfind` on a copy.
- V1 and V2 are computed from the V0 store, not by rerunning ingest.
- Only ext4 (4 KiB blocks); W1 ran three times, the others once.
