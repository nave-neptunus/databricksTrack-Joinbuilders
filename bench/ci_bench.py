#!/usr/bin/env python3
"""Exact x86 CI gate and cold A/B timer for two Dockerfile refs."""
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

DATA = Path(sys.argv[1]).resolve()
WORK = Path(sys.argv[2]).resolve()
BASE = os.environ.get("BASELINE_REF", "ed40599")
CAND = os.environ.get("CANDIDATE_REF", "6fafb87")
PAIRS = int(os.environ.get("PAIRS", "6"))
NCPU = int(os.environ.get("NCPU", "8"))
WORK.mkdir(parents=True, exist_ok=True)


def call(args, *, capture=False, env=None):
    print("+", " ".join(map(str, args)), flush=True)
    return subprocess.run(args, check=True, text=True, capture_output=capture, env=env)


def ref(ref):
    subprocess.run(["git", "fetch", "--no-tags", "origin", ref], check=False)
    return call(["git", "rev-parse", ref], capture=True).stdout.strip()


def build(commit, tag):
    archive = subprocess.Popen(["git", "archive", "--format=tar", commit], stdout=subprocess.PIPE)
    assert archive.stdout is not None
    result = subprocess.run(["docker", "build", "--platform", "linux/amd64", "--load", "--tag", tag, "-"], stdin=archive.stdout)
    archive.stdout.close()
    if archive.wait() or result.returncode:
        raise SystemExit(f"build failed for {commit}")


def clean(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    path.chmod(0o777)


def drop_caches():
    call(["sync"])
    subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null", shell=True, check=True)


def run(tag, name, *, cold=False, extra_env=None):
    out = WORK / "outputs" / name
    scratch = WORK / "scratch" / name
    clean(out)
    clean(scratch)
    if cold:
        drop_caches()
    command = [
        "docker", "run", "--rm", "--network", "none", "--read-only",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--memory", "12g", "--memory-swap", "12g",
        "--user", "1000:1000", "-v", f"{DATA}:/data:ro", "-v", f"{out}:/output",
        "-v", f"{scratch}:/scratch", "-e", f"NCPU={NCPU}", "-e", "DATA_DIR=/data",
        "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
    ]
    for key, value in (extra_env or {}).items():
        command.extend(["-e", f"{key}={value}"])
    command.append(tag)
    started = time.perf_counter()
    result = subprocess.run(command, text=True, capture_output=True)
    elapsed = time.perf_counter() - started
    (WORK / "logs").mkdir(exist_ok=True)
    (WORK / "logs" / f"{name}.stdout").write_text(result.stdout)
    (WORK / "logs" / f"{name}.stderr").write_text(result.stderr)
    if result.returncode:
        raise SystemExit(f"{name} failed ({result.returncode}):\n{result.stderr[-4000:]}")
    return out, elapsed


def compare(actual, expected, label):
    result = subprocess.run(["python3", "scripts/compare.py", str(actual), str(expected)], text=True, capture_output=True)
    print(f"{label}: {result.stdout}", flush=True)
    if result.returncode or not result.stdout.rstrip().endswith("MATCH"):
        raise SystemExit(f"{label}: output mismatch")


def fixture_gate(tag, label):
    fixtures = sorted(p for p in Path("fixtures").iterdir() if p.is_dir())
    for fixture in fixtures:
        data = fixture / "input"
        out = WORK / "fixture-outputs" / label / fixture.name
        scratch = WORK / "fixture-scratch" / label / fixture.name
        clean(out)
        clean(scratch)
        command = ["docker", "run", "--rm", "--network", "none", "--read-only",
                   "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
                   "--security-opt", "no-new-privileges", "--memory", "12g", "--user", "1000:1000",
                   "-v", f"{data.resolve()}:/data:ro", "-v", f"{out}:/output", "-v", f"{scratch}:/scratch",
                   "-e", f"NCPU={NCPU}", "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch", tag]
        result = subprocess.run(command, text=True, capture_output=True)
        if result.returncode:
            raise SystemExit(f"fixture {label}/{fixture.name} failed: {result.stderr[-2000:]}")
        compare(out, fixture / "expected", f"fixture {label}/{fixture.name}")


base_sha, cand_sha = ref(BASE), ref(CAND)
print(f"BASELINE={base_sha} CANDIDATE={cand_sha}", flush=True)
build(base_sha, "g7-bench-baseline")
build(cand_sha, "g7-bench-candidate")
fixture_gate("g7-bench-baseline", "baseline")
fixture_gate("g7-bench-candidate", "candidate")
base_out, _ = run("g7-bench-baseline", "big20-reference")
cand_out, _ = run("g7-bench-candidate", "big20-candidate-gate")
compare(cand_out, base_out, "big20 candidate vs baseline")
_, _ = run("g7-bench-baseline", "big20-xl-timing", extra_env={"XL_TIMING": "1"})

observations = []
for pair in range(1, PAIRS + 1):
    order = [("baseline", "g7-bench-baseline"), ("candidate", "g7-bench-candidate")]
    if pair % 2 == 0:
        order.reverse()
    row = {"pair": pair, "order": "-".join(x[0] for x in order)}
    for label, image in order:
        _, seconds = run(image, f"cold-{pair}-{label}", cold=True)
        row[label] = seconds
    observations.append(row)
    print(f"PAIR {pair} {row['order']} baseline={row['baseline']:.6f}s candidate={row['candidate']:.6f}s", flush=True)

base_times = [row["baseline"] for row in observations]
cand_times = [row["candidate"] for row in observations]
savings_ms = [(row["baseline"] - row["candidate"]) * 1000 for row in observations]
savings_pct = [(row["baseline"] - row["candidate"]) / row["baseline"] * 100 for row in observations]
summary = [
    "# g7 x86 cold benchmark",
    f"baseline={base_sha}", f"candidate={cand_sha}", f"pairs={PAIRS}, NCPU={NCPU}",
    "", "| pair | order | baseline seconds | candidate seconds | saving ms |", "|---:|---|---:|---:|---:|",
]
summary += [f"| {r['pair']} | {r['order']} | {r['baseline']:.6f} | {r['candidate']:.6f} | {(r['baseline']-r['candidate'])*1000:+.3f} |" for r in observations]
summary += ["", f"baseline median: {statistics.median(base_times):.6f}s", f"candidate median: {statistics.median(cand_times):.6f}s", f"median paired saving: {statistics.median(savings_ms):+.3f} ms ({statistics.median(savings_pct):+.3f}%)", "", "Correctness: fixtures MATCH; big20 candidate vs baseline MATCH.", "See logs/big20-xl-timing.stderr for XL_TIMING stage data."]
(WORK / "summary.md").write_text("\n".join(summary) + "\n")
(WORK / "observations.json").write_text(json.dumps(observations, indent=2) + "\n")
print((WORK / "summary.md").read_text(), flush=True)
