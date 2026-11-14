#!/bin/sh
# Builds the three real-tree fixtures readbench was run on, in $1
# (default ./fixtures). Nothing downloaded is executed: the scripts parse
# APKINDEX / narinfo files and unpack archives with tar and a NAR reader.
# nixclosure.py needs the zstandard module for zstd-compressed NARs.
set -e
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-./fixtures}
mkdir -p "$out" && cd "$out"

# Alpine minirootfs (520 entries, 8.6 MB)
curl -sSLO https://dl-cdn.alpinelinux.org/alpine/v3.22/releases/x86_64/alpine-minirootfs-3.22.8-x86_64.tar.gz
mkdir -p alpine-mini && tar -xzf alpine-minirootfs-3.22.8-x86_64.tar.gz -C alpine-mini

# A fuller Alpine root: 155 packages, 16k entries, 566 MB
for r in main community; do
  curl -sSL https://dl-cdn.alpinelinux.org/alpine/v3.22/$r/x86_64/APKINDEX.tar.gz | tar -xzO APKINDEX > APKINDEX.$r
done
python3 -I "$here/apkroot.py" . alpine-full alpine-base build-base python3 py3-pip git perl \
  openssl curl bash vim gcc g++ cmake nodejs npm

# A Nix closure from cache.nixos.org (nixos-25.05): git, python3, nodejs,
# cmake, vim and the gcc wrapper. 138 store paths, 39k entries, 1.2 GB.
python3 -I "$here/nixclosure.py" nix-store \
  f59f8y1bn2nzqqwpfax7wk0kbdjdic84-git-2.50.1 \
  jd20rkmqmkfkcvk2wl2lmzz7acq4svlr-python3-3.12.12 \
  4ml9d607wkfa05rg5j3w39yj4jinbprh-nodejs-22.20.0 \
  7lll2iy8mdsmxnizxrrbkhcyp1nr5i5c-cmake-3.31.6 \
  ij6giknyxkqsw3srd2xi4wmkvyziy6gd-vim-9.1.1566 \
  2qm7q037197agxdz2ysngj34vv5llwm0-gcc-wrapper-14.3.0
