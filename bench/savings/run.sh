#!/usr/bin/env bash
# One run of the savings test plan: a fresh cache, fed from the local corpus
# through the real fill paths, settled and measured.
#
#   bench/savings/run.sh NAME [apk:MANIFEST.json]... [oci:REPO@TAG]... [nix:STOREPATH]... [local:DIR]...
#
# Env: CORPUS (corpus dir, default ./corpus), WORK (cache dirs, default
# ./work), OUT (results, default ./results), BIN (dir with wanix-cache and
# savingsbench), KEEP=1 keeps the cache dir, ZSTD=L serves with -zstd L,
# AGAIN=1 drives every input a second time into the same cache after
# measuring (W8).
#
# The corpus server (savingsbench serve-corpus) must already be running on
# its default ports.
set -euo pipefail
name=$1; shift
CORPUS=${CORPUS:-./corpus}; WORK=${WORK:-./work}; OUT=${OUT:-./results}; BIN=${BIN:-./bin}
D=$WORK/$name; R=$OUT/$name
rm -rf "$D" "$R"; mkdir -p "$D" "$R"
P9L=127.0.0.1:5640; P9=127.0.0.1:5641; MIRROR=127.0.0.1:8404

"$BIN/wanix-cache" serve -data "$D" -max-gib 0 ${ZSTD:+-zstd $ZSTD} -9p $P9L -9p2000 $P9 \
  -apk-upstream http://127.0.0.1:8401 -apk-mirror $MIRROR -apk-index-ttl 1h \
  -nix-substituters http://127.0.0.1:8403/ >"$R/serve.log" 2>&1 &
srv=$!
trap 'kill $srv 2>/dev/null || true' EXIT
for _ in $(seq 50); do nc -z 127.0.0.1 5641 2>/dev/null && break; sleep 0.2; done

drive() {
  local apks=() roots=()
  for a in "$@"; do
    case $a in
      apk:*) apks+=("${a#apk:}") ;;
      oci:*) roots+=("/oci/localhost:8402/${a#oci:}") ;;
      nix:*) roots+=("/nix/closure/$(basename "${a#nix:}")") ;;
      local:*) "$BIN/wanix-cache" import -data "$D" -key "$(basename "${a#local:}")" "${a#local:}" ;;
    esac
  done
  if ((${#apks[@]})); then "$BIN/savingsbench" apk -k -mirror $MIRROR "${apks[@]}"; fi
  if ((${#roots[@]})); then "$BIN/savingsbench" walk -9p $P9 "${roots[@]}"; fi
  "$BIN/savingsbench" settle -data "$D"
  # The unpacked apk layers are built in the background once the mirror
  # served each .apk; read them too, as an overlay user would.
  if ((${#apks[@]})); then "$BIN/savingsbench" walk -9p $P9 /apk/x86_64 && "$BIN/savingsbench" settle -data "$D"; fi
}

t0=$(date +%s.%N)
drive "$@" 2>&1 | tee "$R/drive.log"
t1=$(date +%s.%N)
kill -INT $srv; wait $srv || true; trap - EXIT

"$BIN/wanix-cache" stats -data "$D" -json -n 20 >"$R/stats.json"
"$BIN/wanix-cache" stats -data "$D" -n 20 >"$R/stats.txt"
store=$(du -sB1 "$D/files" | cut -f1)

again=null
if [[ ${AGAIN:-} == 1 ]]; then
  before=$(du -sB1 "$D" | cut -f1)
  "$BIN/wanix-cache" serve -data "$D" -max-gib 0 -9p $P9L -9p2000 $P9 \
    -apk-upstream http://127.0.0.1:8401 -apk-mirror $MIRROR -apk-index-ttl 1h \
    -nix-substituters http://127.0.0.1:8403/ >"$R/serve-again.log" 2>&1 &
  srv=$!; trap 'kill $srv 2>/dev/null || true' EXIT
  for _ in $(seq 50); do nc -z 127.0.0.1 5641 2>/dev/null && break; sleep 0.2; done
  ta=$(date +%s.%N)
  drive "$@" 2>&1 | tee "$R/drive-again.log"
  tb=$(date +%s.%N)
  kill -INT $srv; wait $srv || true; trap - EXIT
  after=$(du -sB1 "$D" | cut -f1)
  again="{\"before\": $before, \"after\": $after, \"seconds\": $(echo "$tb - $ta" | bc)}"
fi

cat >"$R/run.json" <<EOF
{"name": "$name", "inputs": $(printf '%s\n' "$@" | jq -R . | jq -s .),
 "commit": "$(git -C "$(dirname "$0")" rev-parse HEAD)",
 "fs": "$(stat -f -c %T "$D")", "block_size": $(stat -f -c %S "$D"),
 "seconds": $(echo "$t1 - $t0" | bc), "ingest_workers": 4,
 "du_files": $store, "again": $again,
 "corpus_manifests": $(cd "$CORPUS/manifests" && sha256sum *.json | jq -R 'split("  ") | {(.[1]): .[0]}' | jq -s add)}
EOF
[[ ${KEEP:-} == 1 ]] || rm -rf "$D"
echo "result: $R"
