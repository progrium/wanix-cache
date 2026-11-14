#!/usr/bin/env python3
"""Turn a set of run.sh result directories into one comparison table.

    report.py CORPUS RESULTS_DIR > report.json

Prints JSON (one row per run) on stdout and a Markdown table on stderr.
B1 comes from the corpus manifests: the distinct fetched objects of the
run's inputs (.apk files, compressed layer blobs, .nar.xz files).
"""
import glob
import json
import os
import sys

MiB = 1 << 20


def load(p):
    with open(p) as f:
        return json.load(f)


def b1(corpus, inputs):
    """Distinct compressed downloads for a run's inputs, per subsystem."""
    man = os.path.join(corpus, "manifests")
    seen, out = set(), {}
    for inp in inputs:
        kind, _, arg = inp.partition(":")
        if kind == "apk":
            for p in load(arg if os.path.isabs(arg) else os.path.join(man, os.path.basename(arg)))["packages"]:
                if p["sha256"] not in seen:
                    seen.add(p["sha256"])
                    out["apk"] = out.get("apk", 0) + p["size"]
        elif kind == "oci":
            repo, tag = arg.split("@")
            for mf in glob.glob(os.path.join(man, "oci-*.json")):
                for im in load(mf)["images"]:
                    if im["repo"] == repo and im["tag"] == tag:
                        for l in im["layers"]:
                            if l["digest"] not in seen:
                                seen.add(l["digest"])
                                out["oci"] = out.get("oci", 0) + l["size"]
                        break
                else:
                    continue
                break
        elif kind == "nix":
            for mf in glob.glob(os.path.join(man, "nix-*.json")):
                m = load(mf)
                if arg in m["roots"]:
                    for p in m["paths"]:
                        if p["url"] not in seen:
                            seen.add(p["url"])
                            out["nix"] = out.get("nix", 0) + p["file_size"]
                    break
    return out


def sav(cas, base):
    return None if not base else 1 - cas / base


def row(corpus, d):
    run = load(os.path.join(d, "run.json"))
    st = load(os.path.join(d, "stats.json"))
    b3 = load(os.path.join(d, "b3.json"))
    v = load(os.path.join(d, "variants.json"))
    t = st["total"]
    cas = t["cas_total"]
    b1s = b1(corpus, run["inputs"])
    B1 = sum(b1s.values()) or None
    B2 = t["b2"]["allocated"]
    B3 = b3["b3"]
    sc = st["size_classes"]
    slack = sum(c["slack"] for c in sc) or 1
    small16 = sum(c["slack"] for c in sc if c["max"] and c["max"] <= 16384) / slack
    subs = {}
    blob_bytes = t["blob_bytes"] or 1
    for s in st["subsystems"]:
        subs[s["name"]] = {
            "logical": s["logical"], "exclusive": s["exclusive_bytes"], "shared": s["shared_bytes"],
            "shared_share": s["shared_bytes"] / max(1, s["exclusive_bytes"] + s["shared_bytes"]),
            "shared_with": s.get("shared_with", {}), "attributed": s["attributed"],
            "b2": (s.get("b2") or {}).get("allocated"), "b1": b1s.get(s["name"]),
        }
    shared_any = max((s["shared"] for s in subs.values()), default=0)
    v1 = cas - v["data_allocated"] + v["v1_data_allocated"]
    v2 = cas - v["v2_saved_allocated"]
    apk_raw = b1s.get("apk")
    out = {
        "name": run["name"], "inputs": run["inputs"], "commit": run["commit"],
        "fs": run["fs"], "block_size": run["block_size"], "seconds": run["seconds"],
        "files": t["files"], "units": t["resident_units"], "logical": t["logical"],
        "b1": B1, "b1_by_subsystem": b1s, "b2": B2, "b3": B3, "b3_measured": b3["measured"],
        "cas_total": cas, "db": t["db"],
        "savings_vs_b1": sav(cas, B1), "savings_vs_b2": sav(cas, B2), "savings_vs_b3": sav(cas, B3),
        "b3_vs_b2": sav(B3, B2),
        "whole_file_dedupe": t["ratios"]["whole_file_dedupe"], "chunk_dedupe": t["ratios"]["chunk_dedupe"],
        "schema_share": t["ratios"]["schema_share"], "blob_overhead_share": t["ratios"]["blob_overhead_share"],
        "store_dir_share": t["ratios"]["store_dir_share"], "db_share": t["db"] / cas,
        "layout_overhead_share": (t["blob_overhead"] + t["store_dir_allocated"]) / cas,
        "slack_under_16k_share": small16, "blobs": t["blobs"],
        "subsystems": subs, "cross_subsystem_shared_share": shared_any / blob_bytes,
        "raw_apk_share_of_apk_logical": (apk_raw / subs["apk"]["logical"]) if apk_raw and "apk" in subs else None,
        "v1_total": v1, "chunking_gain_vs_v1": 1 - cas / v1,
        "v2_total": v2, "v2_saving_vs_v0": 1 - v2 / cas, "v2_zstd_ratio": v["v2_logical_ratio"],
        "ingest_mib_s": t["logical"] / MiB / run["seconds"],
        "again": run.get("again"),
    }
    return out


def pct(x):
    return "n/a" if x is None else f"{x * 100:.1f}%"


def main():
    corpus, res = sys.argv[1], sys.argv[2]
    rows = [row(corpus, d) for d in sorted(glob.glob(os.path.join(res, "*"))) if os.path.exists(os.path.join(d, "b3.json"))]
    json.dump(rows, sys.stdout, indent=1)
    hdr = ["run", "logical", "B1", "B2", "B3", "CAS", "vs B1", "vs B2", "vs B3", "whole-file", "chunk",
           "blob ovh", "dirs", "DB", "schema", "V2 saves", "ingest MiB/s"]
    print("| " + " | ".join(hdr) + " |", file=sys.stderr)
    print("|" + "---|" * len(hdr), file=sys.stderr)
    for r in rows:
        m = lambda n: "n/a" if n is None else f"{n / MiB:.0f}"
        print("| " + " | ".join([r["name"], m(r["logical"]), m(r["b1"]), m(r["b2"]), m(r["b3"]), m(r["cas_total"]),
                                 pct(r["savings_vs_b1"]), pct(r["savings_vs_b2"]), pct(r["savings_vs_b3"]),
                                 pct(r["whole_file_dedupe"]), pct(r["chunk_dedupe"]), pct(r["blob_overhead_share"]),
                                 pct(r["store_dir_share"]), pct(r["db_share"]), pct(r["schema_share"]),
                                 pct(r["v2_saving_vs_v0"]), f"{r['ingest_mib_s']:.1f}"]) + " |", file=sys.stderr)


if __name__ == "__main__":
    main()
