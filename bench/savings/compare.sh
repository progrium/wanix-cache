#!/usr/bin/env bash
# The CAS against the hard-link alternatives with compression, on one
# workload: sizes and direct read throughput. Run after
#
#   KEEP=1 LINK=1 run.sh w7-plain ...            (B3: the shadow, hard-linked)
#   KEEP=1 PACK=65536 run.sh w7-pack ...
#   KEEP=1 PACK=65536 ZSTD=2 run.sh w7-zstd ...
#
# Needs root (loop mount, drop_caches) and mkfs.btrfs. Env as run.sh, plus
# REPS (default 3).
set -euo pipefail
WORK=${WORK:-./work}; BIN=${BIN:-./bin}; REPS=${REPS:-3}
B3=$WORK/w7-plain/shadow
mkdir -p "$WORK/btrfs"
truncate -s 8G "$WORK/btrfs.img"
mkfs.btrfs -q -f "$WORK/btrfs.img"
mount -o loop,compress-force=zstd:3 "$WORK/btrfs.img" "$WORK/btrfs"
trap 'umount "$WORK/btrfs" || true' EXIT
btrfs filesystem sync "$WORK/btrfs"; u0=$(df -B1 --output=used "$WORK/btrfs" | tail -1)
cp -a "$B3" "$WORK/btrfs/b3"   # one cp keeps the hard links
sync; btrfs filesystem sync "$WORK/btrfs"; sleep 5
u1=$(df -B1 --output=used "$WORK/btrfs" | tail -1)
echo "{\"variant\":\"btrfs-zstd-b3\",\"used\":$((u1 - u0))}"
"$BIN/savingsbench" zlinks -shadow "$B3" -out "$WORK/zlinks" -level 2
for rep in $(seq "$REPS"); do
  for v in "ext4-b3 -mode dir -root $B3" "btrfs-zstd-b3 -mode dir -root $WORK/btrfs/b3" \
           "zlinks -mode zlinks -root $WORK/zlinks/tree" "cas -mode cas -root $WORK/w7-plain" \
           "cas-pack64k -mode cas -root $WORK/w7-pack" "cas-zstd-pack64k -mode cas -root $WORK/w7-zstd"; do
    set -- $v; name=$1; shift
    for c in "-cold -j 1" "-cold -j 8" "-j 8"; do
      echo "{\"variant\":\"$name\",\"rep\":$rep,\"r\":$("$BIN/savingsbench" readall "$@" $c)}"
    done
  done
done
