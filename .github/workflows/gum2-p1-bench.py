#!/usr/bin/env python3
"""Usage: bench.py DATA LABEL BINS REPO OUTDIR. Compare base vs p1 outputs (fail on mismatch), then time 3 interleaved runs each."""
import os, re, shutil, statistics, subprocess, sys, time
from pathlib import Path

data, label, bins, repo, outdir = sys.argv[1:6]
bins, repo, outdir = Path(bins), Path(repo), Path(outdir)
outdir.mkdir(parents=True, exist_ok=True)
work = Path(os.environ["HOME"]) / f"run-{label}"
LAP = re.compile(r"^\s*([0-9.]+) ms  (\S.*)$")

def run(name, rep):
    out = work / f"{name}-{rep}"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR=str(out), NCPU=os.environ.get("NCPU", "8"), TIMING="1")
    t = time.perf_counter()
    p = subprocess.run([str(bins / name)], env=env, capture_output=True, text=True)
    wall = (time.perf_counter() - t) * 1000
    if p.returncode != 0:
        sys.exit(f"{name} exited {p.returncode}\n{p.stderr[-2000:]}")
    if "declined" in p.stderr:
        print(f"::warning::{name} {label}: fast engine declined (slow fallback)")
    laps = [(m.group(2), float(m.group(1))) for m in map(LAP.match, p.stderr.splitlines()) if m]
    return out, wall, laps

# Correctness first: first run of each, exact compare.
b_out, bw, bl = run("base", 0)
p_out, pw, pl = run("p1", 0)
cmp = subprocess.run([sys.executable, str(repo / "scripts/compare.py"), str(b_out), str(p_out)], capture_output=True, text=True)
print(cmp.stdout)
if cmp.returncode != 0 or not cmp.stdout.strip().endswith("MATCH"):
    sys.exit(f"{label}: outputs DIFFER\n{cmp.stderr[-2000:]}")
print(f"{label}: MATCH")
shutil.rmtree(b_out); shutil.rmtree(p_out)

# Three interleaved timing runs per binary (the compared runs above act as warm-up).
res = {"base": [], "p1": []}
for rep in range(1, 4):
    for name in ("base", "p1"):
        out, wall, laps = run(name, rep)
        res[name].append((wall, laps))
        shutil.rmtree(out)

def med(xs): return statistics.median(xs)
labels = []
for name in res:
    for _, laps in res[name]:
        for l, _ in laps:
            if l not in labels: labels.append(l)
def lapmed(name, l):
    v = [dict(laps).get(l) for _, laps in res[name]]
    v = [x for x in v if x is not None]
    return med(v) if v else None
rows = [f"### {label} events: median of 3 runs, NCPU={os.environ.get('NCPU','8')}, cumulative ms", "",
        "| stage | base 80f9e55 | P1 5801b8f |", "|---|---:|---:|"]
f = lambda x: "-" if x is None else f"{x:.1f}"
rows.append(f"| **wall (process)** | {f(med([w for w,_ in res['base']]))} | {f(med([w for w,_ in res['p1']]))} |")
for l in labels:
    rows.append(f"| {l} | {f(lapmed('base', l))} | {f(lapmed('p1', l))} |")
text = "\n".join(rows) + "\n"
print(text)
(outdir / f"table-{label}.md").write_text(text)
