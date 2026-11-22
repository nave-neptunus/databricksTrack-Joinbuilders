#!/usr/bin/env python3
"""Ablation timing: ABL knobs skip pieces of the Nova per-row work (outputs are wrong)."""
import json, re, statistics, subprocess, sys, os, time
from pathlib import Path
sys.argv = [sys.argv[0], *sys.argv[1:3]]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_bench as cb

KNOBS = ["", "scan", "evkey", "ingest", "time", "user", "name", "price", "prefetch", "person", "push", "claim",
         "evkey,user", "person,push,claim", "evkey,ingest,time,user,name,price",
         "evkey,ingest,time,user,name,price,prefetch,person,push"]
ROUNDS = int(os.environ.get("ABL_ROUNDS", "5"))
sha = cb.REQUEST["candidate"]
binary = cb.build(sha)
data = cb.HOME / "v5-20m"
res = {k: [] for k in KNOBS}
for r in range(ROUNDS + 1):  # round 0 = warm-up
    order = KNOBS[r % len(KNOBS):] + KNOBS[:r % len(KNOBS)]
    for k in order:
        out, scratch = cb.WORK / "o", cb.WORK / "s"
        cb.fresh(out); cb.fresh(scratch)
        env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch), NCPU=cb.NCPU, TIMING="1", ABL=k)
        p = subprocess.run([str(binary)], text=True, capture_output=True, env=env)
        laps = {}
        for ms, name in re.findall(r"^\s*([0-9.]+) ms\s+(.+)$", p.stderr, re.M):
            n = "events parsed" if name.startswith("events parsed (") else "links ready" if name.startswith("links: ") else name
            laps[n] = float(ms)
        if "events parsed" not in laps or "links ready" not in laps or "fast engine declined" in p.stderr:
            print(f"[{k}] no parse lap rc={p.returncode}: {p.stderr[-600:]}", flush=True); continue
        if r:
            res[k].append(laps["events parsed"] - laps["links ready"])
lines = ["| knob | median events-parse ms | min | max | delta vs none |", "|---|---:|---:|---:|---:|"]
base = statistics.median(res[""]) if res[""] else 0
for k in KNOBS:
    v = res[k]
    if v:
        lines.append(f"| {k or 'none'} | {statistics.median(v):.0f} | {min(v):.0f} | {max(v):.0f} | {statistics.median(v)-base:+.0f} |")
(cb.WORK / "summary.md").write_text("\n".join(lines))
print(f"binary sha {sha}\n" + "\n".join(lines))
