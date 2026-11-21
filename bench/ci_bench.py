#!/usr/bin/env python3
"""Exact x86 CI gate plus paired cold timing for two Dockerfile refs.

XL_TIMING is deliberately enabled for every timed run: its "exit at" lap is
the wall clock inside the pipeline process, while container_seconds includes
Docker/container setup and teardown.
"""
import json
import os
import re
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
PAIRS = int(os.environ.get("PAIRS", "10"))
NCPU = int(os.environ["NCPU"])
CANDIDATE_ENV = os.environ.get("CANDIDATE_ENV", "")
WORK.mkdir(parents=True, exist_ok=True)


def call(args, *, capture=False):
    print("+", " ".join(map(str, args)), flush=True)
    return subprocess.run(args, check=True, text=True, capture_output=capture)


def ref(name):
    subprocess.run(["git", "fetch", "--no-tags", "origin", name], check=False)
    return call(["git", "rev-parse", name], capture=True).stdout.strip()


def build(commit, tag):
    archive = subprocess.Popen(["git", "archive", "--format=tar", commit], stdout=subprocess.PIPE)
    assert archive.stdout is not None
    result = subprocess.run(["docker", "build", "--platform", "linux/amd64", "--load", "--tag", tag, "-"], stdin=archive.stdout)
    archive.stdout.close()
    if archive.wait() or result.returncode:
        raise SystemExit(f"build failed for {commit}")


def clean(path):
    if path.exists():
        subprocess.run(["sudo", "rm", "-rf", str(path)], check=True)
    path.mkdir(parents=True)
    path.chmod(0o777)


def remove(path):
    """Remove a known child of the CI-created work directory (may be UID 1000-owned)."""
    subprocess.run(["sudo", "rm", "-rf", str(path)], check=True)


def drop_caches():
    call(["sync"])
    subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null", shell=True, check=True)


def candidate_env():
    if not CANDIDATE_ENV:
        return {}
    key, sep, value = CANDIDATE_ENV.partition("=")
    if not sep or not key.startswith("XL_"):
        raise SystemExit(f"CANDIDATE_ENV must be one XL_ setting, got {CANDIDATE_ENV!r}")
    return {key: value}


def proc_minflt(pid):
    try:
        # /proc/PID/stat's comm can contain spaces; field 3 begins after ") ".
        rest = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
        return int(rest[7])  # field 10: minflt
    except (FileNotFoundError, IndexError, ValueError):
        return None


def timing(stderr):
    exit_match = re.search(r"\[v2b\] exit at ([0-9.]+) ms", stderr)
    rss_match = re.search(r"peak_rss_kb=(\d+)", stderr)
    if not exit_match:
        raise SystemExit("XL_TIMING did not print an internal exit-at lap")
    return float(exit_match.group(1)), int(rss_match.group(1)) if rss_match else None


def run(tag, name, *, cold=False, extra_env=None, preserve=False):
    out = WORK / "outputs" / name
    scratch = WORK / "scratch" / name
    clean(out)
    clean(scratch)
    if cold:
        drop_caches()
    container_name = f"g7-{os.getpid()}-{time.time_ns()}"
    command = [
        "docker", "run", "--rm", "--name", container_name, "--network", "none", "--read-only",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--memory", "12g", "--memory-swap", "12g",
        "--user", "1000:1000", "-v", f"{DATA}:/data:ro", "-v", f"{out}:/output",
        "-v", f"{scratch}:/scratch", "-e", f"NCPU={NCPU}", "-e", "DATA_DIR=/data",
        "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch", "-e", "XL_TIMING=1",
    ]
    for key, value in (extra_env or {}).items():
        command.extend(["-e", f"{key}={value}"])
    command.append(tag)
    started = time.perf_counter()
    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    max_minflt = None
    while process.poll() is None:
        inspected = subprocess.run(["docker", "inspect", "--format", "{{.State.Pid}}", container_name], text=True, capture_output=True)
        if inspected.returncode == 0 and inspected.stdout.strip().isdigit():
            value = proc_minflt(int(inspected.stdout))
            if value is not None:
                max_minflt = value if max_minflt is None else max(max_minflt, value)
        time.sleep(0.002)
    stdout, stderr = process.communicate()
    container_seconds = time.perf_counter() - started
    logs = WORK / "logs"
    logs.mkdir(exist_ok=True)
    (logs / f"{name}.stdout").write_text(stdout)
    (logs / f"{name}.stderr").write_text(stderr)
    if process.returncode:
        raise SystemExit(f"{name} failed ({process.returncode}):\n{stderr[-4000:]}")
    engine_ms, peak_rss_kb = timing(stderr)
    metrics = {"container_seconds": container_seconds, "engine_ms": engine_ms,
               "peak_rss_kb": peak_rss_kb, "minflt": max_minflt}
    if not preserve:
        remove(out)
        remove(scratch)
    return out, metrics


def compare(actual, expected, label):
    result = subprocess.run(["python3", "scripts/compare.py", str(actual), str(expected)], text=True, capture_output=True)
    print(f"{label}: {result.stdout}", flush=True)
    if result.returncode or not result.stdout.rstrip().endswith("MATCH"):
        raise SystemExit(f"{label}: output mismatch")


def fixture_gate(tag, label, env):
    for fixture in sorted(p for p in Path("fixtures").iterdir() if p.is_dir()):
        data = fixture / "input"
        out = WORK / "fixture-outputs" / label / fixture.name
        scratch = WORK / "fixture-scratch" / label / fixture.name
        clean(out)
        clean(scratch)
        command = ["docker", "run", "--rm", "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777",
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "12g", "--user", "1000:1000",
                   "-v", f"{data.resolve()}:/data:ro", "-v", f"{out}:/output", "-v", f"{scratch}:/scratch",
                   "-e", f"NCPU={NCPU}", "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch"]
        for key, value in env.items():
            command.extend(["-e", f"{key}={value}"])
        result = subprocess.run(command + [tag], text=True, capture_output=True)
        if result.returncode:
            raise SystemExit(f"fixture {label}/{fixture.name} failed: {result.stderr[-2000:]}")
        compare(out, fixture / "expected", f"fixture {label}/{fixture.name}")
        remove(out)
        remove(scratch)


base_sha, cand_sha = ref(BASE), ref(CAND)
print(f"BASELINE={base_sha} CANDIDATE={cand_sha} NCPU={NCPU} CANDIDATE_ENV={CANDIDATE_ENV or '<none>'}", flush=True)
build(base_sha, "g7-bench-baseline")
if cand_sha == base_sha:
    call(["docker", "tag", "g7-bench-baseline", "g7-bench-candidate"])
else:
    build(cand_sha, "g7-bench-candidate")
env = candidate_env()
fixture_gate("g7-bench-baseline", "baseline", {})
fixture_gate("g7-bench-candidate", "candidate", env)
base_out, _ = run("g7-bench-baseline", "big20-reference", preserve=True)
cand_out, _ = run("g7-bench-candidate", "big20-candidate-gate", extra_env=env, preserve=True)
compare(cand_out, base_out, "big20 candidate vs baseline")
remove(base_out)
remove(cand_out)
_, baseline_profile = run("g7-bench-baseline", "baseline-profile", cold=True)
print("BASELINE_METRICS " + json.dumps(baseline_profile, sort_keys=True), flush=True)

observations = []
for pair in range(1, PAIRS + 1):
    order = [("baseline", "g7-bench-baseline", {}), ("candidate", "g7-bench-candidate", env)]
    if pair % 2 == 0:
        order.reverse()
    row = {"pair": pair, "order": "-".join(x[0] for x in order)}
    for label, image, overrides in order:
        _, metrics = run(image, f"cold-{pair}-{label}", cold=True, extra_env=overrides)
        row[label] = metrics
    observations.append(row)
    print(f"PAIR {pair} {row['order']} "
          f"baseline={row['baseline']['engine_ms']:.1f}ms/{row['baseline']['container_seconds']:.3f}s "
          f"candidate={row['candidate']['engine_ms']:.1f}ms/{row['candidate']['container_seconds']:.3f}s", flush=True)

def med(label, metric):
    return statistics.median(row[label][metric] for row in observations)

engine_savings = [row["baseline"]["engine_ms"] - row["candidate"]["engine_ms"] for row in observations]
container_savings = [(row["baseline"]["container_seconds"] - row["candidate"]["container_seconds"]) * 1000 for row in observations]
summary = [
    "# g7 x86 cold benchmark", f"baseline={base_sha}", f"candidate={cand_sha}",
    f"candidate_env={CANDIDATE_ENV or '<none>'}", f"pairs={PAIRS}, NCPU={NCPU}",
    "", "| pair | order | baseline engine ms | candidate engine ms | engine saving ms | baseline container s | candidate container s |",
    "|---:|---|---:|---:|---:|---:|---:|",
]
summary += [f"| {r['pair']} | {r['order']} | {r['baseline']['engine_ms']:.1f} | {r['candidate']['engine_ms']:.1f} | {r['baseline']['engine_ms']-r['candidate']['engine_ms']:+.1f} | {r['baseline']['container_seconds']:.6f} | {r['candidate']['container_seconds']:.6f} |" for r in observations]
summary += ["", f"baseline engine median: {med('baseline', 'engine_ms'):.1f} ms", f"candidate engine median: {med('candidate', 'engine_ms'):.1f} ms", f"median paired engine saving: {statistics.median(engine_savings):+.3f} ms", f"baseline container median: {med('baseline', 'container_seconds'):.6f} s", f"candidate container median: {med('candidate', 'container_seconds'):.6f} s", f"median paired container saving: {statistics.median(container_savings):+.3f} ms", f"baseline profile: peak_rss_kb={baseline_profile['peak_rss_kb']} minflt={baseline_profile['minflt']} engine_ms={baseline_profile['engine_ms']:.1f}", "", "Correctness: fixtures MATCH; big20 candidate vs baseline MATCH."]
(WORK / "summary.md").write_text("\n".join(summary) + "\n")
(WORK / "observations.json").write_text(json.dumps(observations, indent=2) + "\n")
print((WORK / "summary.md").read_text(), flush=True)
