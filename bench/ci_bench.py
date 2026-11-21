#!/usr/bin/env python3
"""CI A/B bench: builds the Dockerfile image of two commits, runs them interleaved on the same dataset.
usage: ci_bench.py DATA_DIR WORKDIR   (reads bench/request.json {"a","b","pairs"})"""
import json, os, statistics as st, subprocess, sys, time, shutil, threading
from pathlib import Path

DATA = Path(sys.argv[1]).resolve(); WORK = Path(sys.argv[2]).resolve(); WORK.mkdir(parents=True, exist_ok=True)
REQ = json.load(open("bench/request.json"))
A, B, PAIRS = REQ["a"], REQ["b"], int(REQ.get("pairs", 7))
WARM = int(REQ.get("warm", 3))
NCPU = min(8, os.cpu_count() or 4)
MEM = "12g"
sh = lambda c, **k: subprocess.run(c, shell=True, check=True, text=True, **k)

def build(sha, tag):
    t = time.time()
    if subprocess.run(f"git cat-file -e {sha}^{{commit}}", shell=True).returncode:
        sh(f"git fetch -q origin {sha}")
    sh(f"git archive {sha} | docker build -q -t {tag} - ")
    print(f"built {tag} from {sha[:10]} in {time.time()-t:.0f}s", flush=True)

def cg_read(path):
    try: return open(path).read()
    except OSError: return None

def run_once(tag, out, cold):
    shutil.rmtree(out, ignore_errors=True) if not os.system(f"sudo rm -rf {out}") else None
    scratch = WORK / "scratch"; os.system(f"sudo rm -rf {scratch}")
    out.mkdir(parents=True); scratch.mkdir(parents=True)
    os.chmod(out, 0o777); os.chmod(scratch, 0o777)
    name = f"bench-{os.getpid()}-{time.time_ns()}"
    if cold: os.system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null")
    cid_holder = {}
    samples = {"cpu": None, "mem": None}
    stop = threading.Event()
    def sampler():
        while not stop.is_set():
            cid = subprocess.run(f"docker inspect -f '{{{{.Id}}}}' {name} 2>/dev/null", shell=True, capture_output=True, text=True).stdout.strip()
            if cid:
                base = f"/sys/fs/cgroup/system.slice/docker-{cid}.scope"
                while not stop.is_set():
                    c = cg_read(base + "/cpu.stat"); m = cg_read(base + "/memory.peak")
                    if c is None: break
                    samples["cpu"] = c
                    if m: samples["mem"] = int(m)
                    time.sleep(0.05)
                return
            time.sleep(0.02)
    th = threading.Thread(target=sampler, daemon=True); th.start()
    cmd = (f"docker run --rm --name {name} --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 "
           f"--cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpus {NCPU} --memory {MEM} --memory-swap {MEM} "
           f"--user 1000:1000 -v {DATA}:/data:ro -v {out}:/output -v {scratch}:/scratch "
           f"-e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU={NCPU} -e MEM_GB=12 {tag}")
    t0 = time.perf_counter()
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    wall = time.perf_counter() - t0
    stop.set(); th.join(timeout=2)
    if r.returncode: print("RUN FAILED", r.stderr[-2000:], flush=True); sys.exit(1)
    d = {"wall": wall, "user": None, "sys": None, "rss_mb": None}
    if samples["cpu"]:
        kv = dict(l.split() for l in samples["cpu"].splitlines())
        d["user"] = int(kv["user_usec"]) / 1e6; d["sys"] = int(kv["system_usec"]) / 1e6
    if samples["mem"]: d["rss_mb"] = samples["mem"] / 2**20
    return d

same = A == B
build(A, "bench-a")
if same: sh("docker tag bench-a bench-b")
else: build(B, "bench-b")
outs = {"a": WORK / "out_a", "b": WORK / "out_b"}
res = {"cold": [], "warm": []}
run_once("bench-a", WORK / "warmup", False)  # make sure the image/dataset path works (not counted)
for mode, n in (("cold", PAIRS), ("warm", WARM)):
    for i in range(n):
        order = ["a", "b"] if i % 2 == 0 else ["b", "a"]
        row = {}
        for s in order:
            keep = outs[s] if (mode == "cold" and i == 0) else WORK / "tmp_out"
            row[s] = run_once(f"bench-{s}", keep, mode == "cold")
        res[mode].append(row)
        print(mode, i, " ".join(f"{s}={row[s]['wall']:.3f}s" for s in "ab"), flush=True)

lines = []
def med(x): return st.median(x) if x else float("nan")
lines.append(f"## CI bench\nA=`{A[:10]}` B=`{B[:10]}` NCPU={NCPU} (runner has {os.cpu_count()} cpus)\n")
for mode in ("cold", "warm"):
    rows = res[mode]
    if not rows: continue
    wa = [r["a"]["wall"] for r in rows]; wb = [r["b"]["wall"] for r in rows]
    d = [(r["b"]["wall"] / r["a"]["wall"] - 1) * 100 for r in rows]
    wins = sum(1 for r in rows if r["b"]["wall"] < r["a"]["wall"])
    lines.append(f"### {mode} ({len(rows)} pairs)")
    lines.append(f"- median wall A {med(wa):.3f}s (min {min(wa):.3f} max {max(wa):.3f}); B {med(wb):.3f}s (min {min(wb):.3f} max {max(wb):.3f})")
    lines.append(f"- paired delta B vs A: median {med(d):+.1f}%  range [{min(d):+.1f}%, {max(d):+.1f}%]  B wins {wins}/{len(rows)}")
    for k in ("user", "sys", "rss_mb"):
        va = [r["a"][k] for r in rows if r["a"][k] is not None]; vb = [r["b"][k] for r in rows if r["b"][k] is not None]
        if va and vb: lines.append(f"- median {k}: A {med(va):.2f} B {med(vb):.2f}")
    lines.append(f"- noise (A, max/min-1): {(max(wa)/min(wa)-1)*100:.1f}%")
# output identity
cmp = subprocess.run(f"python3 scripts/compare.py {outs['a']} {outs['b']}", shell=True, capture_output=True, text=True)
ident = cmp.stdout.strip().splitlines()[-1] if cmp.stdout.strip() else "compare failed: " + cmp.stderr[-300:]
if cmp.returncode and "DIFFER" not in cmp.stdout:
    h = lambda p: subprocess.run(f"cd {p} && find . -type f | sort | xargs sha256sum | sha256sum", shell=True, capture_output=True, text=True).stdout.strip()
    ident += f" | fallback tree hash {'SAME' if h(outs['a'])==h(outs['b']) else 'DIFF'}"
lines.append(f"\n### Output check: {ident}\n```\n{cmp.stdout[-1500:]}\n```")
txt = "\n".join(lines); print(txt)
open(WORK / "summary.md", "w").write(txt)
json.dump(res, open(WORK / "results.json", "w"), indent=1)
