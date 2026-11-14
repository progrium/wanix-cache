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


def sav(store, base):
    return None if not base else 1 - store / base


def row(corpus, d):
    run = load(os.path.join(d, "run.json"))
    st = load(os.path.join(d, "stats.json"))
    t = st["total"]
    total = t["total"]
    b1s = b1(corpus, run["inputs"])
    B1 = sum(b1s.values()) or None
    B2 = t["no_dedupe_estimate"]
    sc = st["size_classes"]
    slack = sum(c["slack"] for c in sc) or 1
    small16 = sum(c["slack"] for c in sc if c["max"] and c["max"] <= 16384) / slack
    subs = {}
    content = t["content_bytes"] or 1
    for s in st["subsystems"]:
        subs[s["name"]] = {
            "logical": s["logical"], "exclusive": s["exclusive_bytes"], "shared": s["shared_bytes"],
            "shared_share": s["shared_bytes"] / max(1, s["exclusive_bytes"] + s["shared_bytes"]),
            "shared_with": s.get("shared_with", {}), "attributed": s["attributed"], "b1": b1s.get(s["name"]),
        }
    shared_any = max((s["shared"] for s in subs.values()), default=0)
    apk_raw = b1s.get("apk")
    return {
        "name": run["name"], "inputs": run["inputs"], "commit": run["commit"],
        "fs": run["fs"], "block_size": run["block_size"], "seconds": run["seconds"],
        "compression": st.get("compression", 0),
        "files": t["files"], "units": t["resident_units"], "logical": t["logical"],
        "b1": B1, "b1_by_subsystem": b1s, "no_dedupe": B2,
        "total": total, "store_allocated": t["store_allocated"], "store_files": t["store_files"],
        "store_links": t["store_links"], "db": t["db"],
        "savings_vs_b1": sav(total, B1), "savings_vs_no_dedupe": t["ratios"]["savings_vs_no_dedupe"],
        "whole_file_dedupe": t["ratios"]["whole_file_dedupe"], "compression_saves": t["ratios"]["compression"],
        "dir_share": t["ratios"]["dir_share"], "db_share": t["db"] / total,
        "slack_under_16k_share": small16, "copies": t["copies"],
        "subsystems": subs, "cross_subsystem_shared_share": shared_any / content,
        "raw_apk_share_of_apk_logical": (apk_raw / subs["apk"]["logical"]) if apk_raw and "apk" in subs else None,
        "ingest_mib_s": t["logical"] / MiB / run["seconds"],
        "again": run.get("again"),
    }


def pct(x):
    return "n/a" if x is None else f"{x * 100:.1f}%"


def main():
    corpus, res = sys.argv[1], sys.argv[2]
    rows = [row(corpus, d) for d in sorted(glob.glob(os.path.join(res, "*"))) if os.path.exists(os.path.join(d, "stats.json"))]
    json.dump(rows, sys.stdout, indent=1)
    hdr = ["run", "logical", "B1", "no dedupe", "store+DB", "vs B1", "vs no dedupe", "whole-file", "zstd",
           "dirs", "DB", "ingest MiB/s"]
    print("| " + " | ".join(hdr) + " |", file=sys.stderr)
    print("|" + "---|" * len(hdr), file=sys.stderr)
    for r in rows:
        m = lambda n: "n/a" if n is None else f"{n / MiB:.0f}"
        z = pct(r["compression_saves"]) if r["compression"] else "off"
        print("| " + " | ".join([r["name"], m(r["logical"]), m(r["b1"]), m(r["no_dedupe"]), m(r["total"]),
                                 pct(r["savings_vs_b1"]), pct(r["savings_vs_no_dedupe"]),
                                 pct(r["whole_file_dedupe"]), z, pct(r["dir_share"]), pct(r["db_share"]),
                                 f"{r['ingest_mib_s']:.1f}"]) + " |", file=sys.stderr)


if __name__ == "__main__":
    main()
