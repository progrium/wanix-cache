#!/usr/bin/env bash
# Every workload of the savings test plan, in order. Same env as run.sh.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
M=${CORPUS:-./corpus}/manifests
oci() { jq -r '.images[] | "oci:\(.repo)@\(.tag)"' "$M/oci-$1.json"; }
nix() { jq -r '.roots[] | "nix:\(.)"' "$M/nix-$1.json"; }
apk() { echo "apk:$M/apk-$1.json"; }
W1=$(apk 3.20)
W2="$(apk 3.18) $(apk 3.19) $(apk 3.20) $(apk 3.21)"
W3=$(oci w3); W4=$(oci w4); W5=$(oci w5)
W6="$(nix 24.05-jul) $(nix 24.05-sep) $(nix 24.05-oct) $(nix 24.11-dec)"
run() { "$here/run.sh" "$@" || echo "RUN FAILED: $1"; }
[[ -n ${GOSRC:-} ]] && run w0 "local:$GOSRC"
[[ -n ${SKIP_W1:-} ]] || for i in 1 2 3; do run w1-r$i $W1; done
run w2 $W2
run w3 $W3
run w4 $W4
run w5 $W5
run w6 $W6
AGAIN=1 run w7 $W1 $W3 $W5 $W6
