import os, re, shutil, statistics, subprocess, sys, time, pathlib
HOME = pathlib.Path(sys.argv[1]); WORK = pathlib.Path(sys.argv[2]); SHA = sys.argv[3]
ROUNDS = 3
ROOT = pathlib.Path.cwd()
CONFIGS = [("IO0", {"BLUE2_IO_THREADS": "0"}), ("IO4", {"BLUE2_IO_THREADS": "4"}), ("IO16", {"BLUE2_IO_THREADS": "16"}),
           ("IO32", {"BLUE2_IO_THREADS": "32"}), ("IO64", {"BLUE2_IO_THREADS": "64"}),
           ("POP16", {"BLUE2_IO_THREADS": "16", "BLUE2_WARM_POPULATE": "1"}), ("POP32", {"BLUE2_IO_THREADS": "32", "BLUE2_WARM_POPULATE": "1"})]
def drop(): subprocess.run(["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
src = WORK / "src"; src.mkdir(parents=True)
a = subprocess.Popen(["git", "archive", SHA], cwd=ROOT, stdout=subprocess.PIPE)
subprocess.run(["tar", "-x", "-C", str(src)], stdin=a.stdout, check=True)
env0 = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64-v3")
subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path", str(src / "rust/Cargo.toml")], check=True, env=env0)
binary = next((src / "rust/target/release").glob("rproto"))
data = HOME / "v5-20m"
def run(name, extra, tag, cold=True):
    out, scr = WORK / "o" / tag, WORK / "s" / tag
    for d in (out, scr):
        if d.exists(): shutil.rmtree(d)
        d.mkdir(parents=True)
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scr), NCPU="8", TIMING="1", **extra)
    if cold: drop()
    t = time.perf_counter(); r = subprocess.run([str(binary)], text=True, capture_output=True, env=env); ms = (time.perf_counter() - t) * 1000
    shutil.rmtree(out, ignore_errors=True)
    if r.returncode: raise SystemExit(f"{tag} failed: {r.stderr[-1500:]}")
    laps = {n.strip(): float(m) for m, n in re.findall(r"^\s*([0-9.]+) ms\s+(.+)$", r.stderr, re.M)}
    return ms, laps
res = {n: [] for n, _ in CONFIGS}
for rd in range(ROUNDS):
    order = CONFIGS[rd % len(CONFIGS):] + CONFIGS[:rd % len(CONFIGS)]
    for n, extra in order:
        res[n].append(run(n, extra, f"{n}-{rd}"))
cats = []
for rd in range(ROUNDS):
    drop(); t = time.perf_counter()
    subprocess.run("cat " + str(data) + "/* > /dev/null 2>&1; true", shell=True)
    # cat on directories fails harmlessly; read files recursively
    subprocess.run(f"find {data} -type f -print0 | xargs -0 cat > /dev/null", shell=True)
    cats.append((time.perf_counter() - t) * 1000)
warm = run("warm", {}, "warm", cold=False)  # leave cache as-is after the cat
lines = [f"# cold IO sweep, 20M, `{SHA}`", "", f"raw cold cat of input (ms, 3 rounds): {[round(c) for c in cats]} median {statistics.median(cats):.0f}", "",
         "| config | cold median ms | rounds | key laps (median ms) |", "|---|---:|---|---|"]
for n, _ in CONFIGS:
    rows = res[n]; med = statistics.median(r[0] for r in rows)
    keys = sorted({k for r in rows for k in r[1]})
    lap = "; ".join(f"{k}: {statistics.median(r[1].get(k, 0) for r in rows):.0f}" for k in keys)
    lines.append(f"| {n} | {med:.0f} | {', '.join(str(round(r[0])) for r in rows)} | {lap} |")
lines.append(f"\nwarm (cache hot from cat) default: {warm[0]:.0f} ms")
(WORK / "summary.md").write_text("\n".join(lines)); print("\n".join(lines))
