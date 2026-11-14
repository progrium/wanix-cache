#!/usr/bin/env python3
"""Build the savings test plan's corpus once (step 2 of the plan).

    corpus.py apk  OUT RELEASE PKG...     Alpine mirror tree + manifest
    corpus.py oci  OUT REF...             registry blob store + manifest
    corpus.py nix  OUT NAME URL ATTR...   Nix binary cache dir + manifest

Everything lands under OUT laid out as the upstream serves it, so
`savingsbench serve-corpus OUT` can stand in for the real upstreams:

    OUT/alpine/v3.20/main/x86_64/{APKINDEX.tar.gz,*.apk}
    OUT/oci/blobs/sha256/<hex>, OUT/oci/refs/<repo>/<tag>  (manifest digest)
    OUT/nix/{nix-cache-info,<hash>.narinfo,nar/*}

Each call also writes OUT/manifests/<kind>-<name>.json, the pinned input
list (names, versions, digests, compressed sizes = baseline B1). Objects
already present with the right digest are not fetched again.
"""
import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import urllib.parse
import urllib.request

ALPINE = "https://dl-cdn.alpinelinux.org/alpine"
NIXCACHE = "https://cache.nixos.org"


def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read()


def download(url, dest, headers=None, sha256=None):
    """Fetch url into dest (atomically); skip if dest exists and matches."""
    if os.path.exists(dest):
        if sha256 is None or file_sha256(dest) == sha256:
            return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    req = urllib.request.Request(url, headers=headers or {})
    h = hashlib.sha256()
    tmp = dest + ".part"
    with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
        while True:
            b = r.read(1 << 20)
            if not b:
                break
            h.update(b)
            f.write(b)
    if sha256 is not None and h.hexdigest() != sha256:
        os.unlink(tmp)
        raise SystemExit(f"{url}: sha256 {h.hexdigest()} != {sha256}")
    os.rename(tmp, dest)


def file_sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def write_manifest(out, name, doc):
    os.makedirs(os.path.join(out, "manifests"), exist_ok=True)
    p = os.path.join(out, "manifests", name + ".json")
    with open(p, "w") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    print("wrote", p, file=sys.stderr)


# ---------------------------------------------------------------- APK

def parse_apkindex(data):
    tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
    text = tf.extractfile("APKINDEX").read().decode()
    pkgs = []
    for rec in text.split("\n\n"):
        p = {}
        for line in rec.splitlines():
            if len(line) > 2 and line[1] == ":":
                p.setdefault(line[0], []).append(line[2:])
        if "P" in p:
            pkgs.append({k: (v if k in "Dpi" else v[0]) for k, v in p.items()})
    return pkgs


def strip_ver(tok):
    for op in ("><", ">=", "<=", "~", ">", "<", "="):
        i = tok.find(op)
        if i > 0:
            tok = tok[:i]
    return tok


def apk_resolve(pkgs, want):
    """The set apk add would install: dependencies closed over provides.
    install_if triggers are not followed."""
    byname, provides = {}, {}
    for p in pkgs:
        byname.setdefault(p["P"], p)
        for pr in (p.get("p") or [""])[0].split():
            provides.setdefault(strip_ver(pr), []).append(p)

    def pick(tok):
        if tok in byname:
            return byname[tok]
        cands = provides.get(tok)
        if not cands:
            raise SystemExit(f"apk: nothing provides {tok}")
        return max(cands, key=lambda c: (int(c.get("k", "0")), c["P"] == tok))

    out, todo = {}, list(want)
    while todo:
        tok = todo.pop()
        if tok.startswith("!"):
            continue
        p = pick(strip_ver(tok))
        if p["P"] in out:
            continue
        out[p["P"]] = p
        todo.extend((p.get("D") or [""])[0].split())
    return out


def cmd_apk(out, release, want):
    arch = "x86_64"
    allpkgs = []
    for repo in ("main", "community"):
        d = os.path.join(out, "alpine", f"v{release}", repo, arch)
        os.makedirs(d, exist_ok=True)
        idx = os.path.join(d, "APKINDEX.tar.gz")
        if not os.path.exists(idx):
            download(f"{ALPINE}/v{release}/{repo}/{arch}/APKINDEX.tar.gz", idx)
        with open(idx, "rb") as f:
            for p in parse_apkindex(f.read()):
                p["repo"] = repo
                allpkgs.append(p)
    sel = apk_resolve(allpkgs, want)
    rows = []
    for name in sorted(sel):
        p = sel[name]
        fn = f"{p['P']}-{p['V']}.apk"
        dest = os.path.join(out, "alpine", f"v{release}", p["repo"], arch, fn)
        download(f"{ALPINE}/v{release}/{p['repo']}/{arch}/{fn}", dest)
        rows.append({"name": p["P"], "version": p["V"], "repo": p["repo"],
                     "checksum": p["C"], "sha256": file_sha256(dest),
                     "size": os.path.getsize(dest), "installed_size": int(p.get("I", 0))})
    write_manifest(out, f"apk-{release}", {"release": release, "arch": arch, "want": want,
                                            "packages": rows,
                                            "b1_bytes": sum(r["size"] for r in rows)})


# ---------------------------------------------------------------- OCI

MT_INDEX = ("application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json")
ACCEPT = ",".join(MT_INDEX + ("application/vnd.oci.image.manifest.v1+json",
                              "application/vnd.docker.distribution.manifest.v2+json"))


def split_ref(ref):
    # docker.io/library/python:3.12-alpine  ->  repo, tag
    ref = ref.removeprefix("docker.io/")
    repo, tag = ref.rsplit(":", 1)
    if "/" not in repo:
        repo = "library/" + repo
    return repo, tag


def hub_token(repo):
    q = urllib.parse.urlencode({"service": "registry.docker.io", "scope": f"repository:{repo}:pull"})
    return json.loads(get(f"https://auth.docker.io/token?{q}"))["token"]


def cmd_oci(out, name, refs):
    reg = "https://registry-1.docker.io"
    blobs = os.path.join(out, "oci", "blobs", "sha256")
    os.makedirs(blobs, exist_ok=True)
    images = []
    for ref in refs:
        repo, tag = split_ref(ref)
        hdr = {"Authorization": "Bearer " + hub_token(repo), "Accept": ACCEPT}

        def put_manifest(reference):
            req = urllib.request.Request(f"{reg}/v2/{repo}/manifests/{reference}", headers=hdr)
            with urllib.request.urlopen(req, timeout=120) as r:
                body, mt = r.read(), r.headers["Content-Type"].split(";")[0]
            dg = hashlib.sha256(body).hexdigest()
            with open(os.path.join(blobs, dg), "wb") as f:
                f.write(body)
            with open(os.path.join(blobs, dg + ".type"), "w") as f:
                f.write(mt)
            return dg, mt, json.loads(body)

        top, mt, doc = put_manifest(tag)
        mdg = top
        if mt in MT_INDEX:
            m = next(m for m in doc["manifests"]
                     if m.get("platform", {}).get("os") == "linux"
                     and m["platform"].get("architecture") == "amd64")
            mdg, mt, doc = put_manifest(m["digest"])
        rdir = os.path.join(out, "oci", "refs", repo)
        os.makedirs(rdir, exist_ok=True)
        with open(os.path.join(rdir, tag), "w") as f:
            f.write("sha256:" + top)
        layers = []
        for d in [doc["config"]] + doc["layers"]:
            hexd = d["digest"].split(":", 1)[1]
            download(f"{reg}/v2/{repo}/blobs/{d['digest']}", os.path.join(blobs, hexd), hdr, hexd)
            layers.append({"digest": d["digest"], "size": d["size"], "mediaType": d["mediaType"]})
        images.append({"ref": f"docker.io/{repo}:{tag}", "repo": repo, "tag": tag,
                       "digest": "sha256:" + top, "platform_manifest": "sha256:" + mdg,
                       "config": layers[0], "layers": layers[1:]})
        print(ref, "sha256:" + top, len(layers) - 1, "layers", file=sys.stderr)
    seen, b1 = set(), 0
    for im in images:
        for l in im["layers"]:
            if l["digest"] not in seen:
                seen.add(l["digest"])
                b1 += l["size"]
    write_manifest(out, f"oci-{name}", {"images": images, "b1_bytes": b1,
                                         "b1_note": "distinct compressed layer blobs (a registry mirror holds each once)"})


# ---------------------------------------------------------------- Nix

def parse_narinfo(text):
    d = {}
    for line in text.splitlines():
        k, _, v = line.partition(": ")
        d[k] = v
    return d


def cmd_nix(out, name, nixexprs_url, attrs):
    """Evaluate ATTRs' output paths from a nixpkgs channel tarball (no
    builds), then copy their runtime closures from cache.nixos.org."""
    src = os.path.join(out, "nixpkgs", name)
    if not os.path.isdir(src):
        os.makedirs(src)
        tgz = src + ".tar.xz"
        download(nixexprs_url, tgz)
        subprocess.run(["tar", "xJf", tgz, "-C", src, "--strip-components=1"], check=True)
        os.unlink(tgz)
    expr = "let p = import %s { system = \"x86_64-linux\"; config = {}; overlays = []; }; in [ %s ]" % (
        src, " ".join(f"p.{a}.outPath" for a in attrs))
    res = subprocess.run(["nix-instantiate", "--eval", "--strict", "--json", "-E", expr],
                         check=True, capture_output=True, text=True)
    roots = json.loads(res.stdout)
    cache = os.path.join(out, "nix")
    os.makedirs(os.path.join(cache, "nar"), exist_ok=True)
    with open(os.path.join(cache, "nix-cache-info"), "w") as f:
        f.write("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n")
    seen, todo, paths = set(), [os.path.basename(r) for r in roots], []
    while todo:
        base = todo.pop()
        if base in seen:
            continue
        seen.add(base)
        h = base.split("-", 1)[0]
        ni_path = os.path.join(cache, h + ".narinfo")
        if not os.path.exists(ni_path):
            body = get(f"{NIXCACHE}/{h}.narinfo")
            with open(ni_path, "wb") as f:
                f.write(body)
        with open(ni_path) as f:
            ni = parse_narinfo(f.read())
        fh = ni["FileHash"].split(":", 1)
        nar = os.path.join(cache, ni["URL"])
        download(f"{NIXCACHE}/{ni['URL']}", nar)  # FileHash is nix base32; checked by the cache on read
        paths.append({"path": ni["StorePath"], "url": ni["URL"], "file_hash": ni["FileHash"],
                      "file_size": int(ni["FileSize"]), "nar_hash": ni["NarHash"], "nar_size": int(ni["NarSize"])})
        todo.extend(r for r in ni.get("References", "").split() if r)
    write_manifest(out, f"nix-{name}", {"nixexprs": nixexprs_url, "attrs": attrs, "roots": roots,
                                         "paths": sorted(paths, key=lambda p: p["path"]),
                                         "b1_bytes": sum(p["file_size"] for p in paths)})


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    kind, out, rest = sys.argv[1], sys.argv[2], sys.argv[3:]
    if kind == "apk":
        cmd_apk(out, rest[0], rest[1:])
    elif kind == "oci":
        cmd_oci(out, rest[0], rest[1:])
    elif kind == "nix":
        cmd_nix(out, rest[0], rest[1], rest[2:])
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
