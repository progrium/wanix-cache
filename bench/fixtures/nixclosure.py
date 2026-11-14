# Fetch the closure of some Nix store paths from cache.nixos.org and unpack
# each NAR into OUT/<hash-name>. Data only: nothing downloaded is executed.
import sys, os, lzma, urllib.request, concurrent.futures as cf, threading, io
C = "https://cache.nixos.org"
out, roots = sys.argv[1], sys.argv[2:]
os.makedirs(out, exist_ok=True)
def get(u):
    with urllib.request.urlopen(u, timeout=120) as r: return r.read()
def narinfo(h):
    d = {}
    for line in get(f"{C}/{h}.narinfo").decode().splitlines():
        k, _, v = line.partition(": "); d[k] = v
    return d
class R:
    def __init__(s, f): s.f = f
    def n(s, k):
        b = s.f.read(k)
        assert len(b) == k; return b
    def u64(s): return int.from_bytes(s.n(8), "little")
    def s(s_):
        k = s_.u64(); b = s_.n(k); s_.n((8 - k % 8) % 8); return b
def unnar(r, path):
    assert r.s() == b"("
    assert r.s() == b"type"; t = r.s()
    if t == b"regular":
        tag = r.s(); ex = False
        if tag == b"executable": r.s(); ex = True; tag = r.s()
        assert tag == b"contents"
        k = r.u64()
        with open(path, "wb") as f:
            left = k
            while left:
                b = r.f.read(min(left, 1 << 20)); f.write(b); left -= len(b)
        r.n((8 - k % 8) % 8)
        os.chmod(path, 0o555 if ex else 0o444)
        assert r.s() == b")"
    elif t == b"symlink":
        assert r.s() == b"target"; os.symlink(r.s().decode(), path); assert r.s() == b")"
    elif t == b"directory":
        os.mkdir(path)
        while True:
            tag = r.s()
            if tag == b")": break
            assert tag == b"entry"; assert r.s() == b"("
            assert r.s() == b"name"; name = r.s().decode()
            assert r.s() == b"node"; unnar(r, os.path.join(path, name)); assert r.s() == b")"
        os.chmod(path, 0o555)
def fetch(base):
    h = base.split("-")[0]
    ni = narinfo(h)
    dest = os.path.join(out, base)
    if not os.path.lexists(dest):
        data = get(f"{C}/{ni['URL']}")
        comp = ni.get("Compression", "none")
        if comp == "xz": data = lzma.decompress(data)
        elif comp == "zstd":
            import zstandard; data = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()
        elif comp != "none": raise SystemExit(f"{base}: compression {comp}")
        r = R(io.BytesIO(data)); assert r.s() == b"nix-archive-1"
        unnar(r, dest + ".tmp"); os.rename(dest + ".tmp", dest)
    return [x for x in ni.get("References", "").split() if x]
seen, lock = set(), threading.Lock()
with cf.ThreadPoolExecutor(16) as ex:
    pend = set()
    def sub(b):
        with lock:
            if b in seen: return
            seen.add(b)
        pend.add(ex.submit(fetch, b))
    for r in roots: sub(os.path.basename(r))
    while pend:
        done, pend = cf.wait(pend, return_when=cf.FIRST_COMPLETED)
        for f in done:
            for b in f.result(): sub(b)
print(len(seen), "store paths")
