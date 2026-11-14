# Resolve Alpine packages from APKINDEX files and unpack them into a root.
# Data only: downloads .apk archives and extracts them; runs nothing.
import sys, os, re, subprocess, urllib.request
base = "https://dl-cdn.alpinelinux.org/alpine/v3.22"
idx_dir, root, want = sys.argv[1], sys.argv[2], sys.argv[3:]
pkgs, provides = {}, {}
for repo in ("main", "community"):
    cur = {}
    for line in open(os.path.join(idx_dir, "APKINDEX." + repo)):
        line = line.rstrip("\n")
        if not line:
            if cur: 
                cur["repo"] = repo
                pkgs.setdefault(cur["P"], cur)
            cur = {}
            continue
        k, v = line[0], line[2:]
        cur[k] = v
for p in pkgs.values():
    provides.setdefault(p["P"], p["P"])
    for pr in p.get("p", "").split():
        provides.setdefault(re.split(r"[=<>~]", pr)[0], p["P"])
seen, order, stack = set(), [], list(want)
while stack:
    d = stack.pop()
    if d.startswith("!"): continue
    name = provides.get(re.split(r"[=<>~]", d)[0])
    if not name or name in seen: continue
    seen.add(name); order.append(name)
    stack.extend(pkgs[name].get("D", "").split())
os.makedirs(root, exist_ok=True)
cache = os.path.join(idx_dir, "apks"); os.makedirs(cache, exist_ok=True)
for name in order:
    p = pkgs[name]
    fn = f"{name}-{p['V']}.apk"
    path = os.path.join(cache, fn)
    if not os.path.exists(path):
        urllib.request.urlretrieve(f"{base}/{p['repo']}/x86_64/{fn}", path)
    subprocess.run(["tar", "-xzf", path, "-C", root, "--no-same-owner",
                    "--exclude=.PKGINFO", "--exclude=.SIGN.*", "--exclude=.pre-*", "--exclude=.post-*", "--exclude=.trigger"],
                   check=True, stderr=subprocess.DEVNULL)
print(len(order), "packages")
