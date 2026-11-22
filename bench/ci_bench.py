#!/usr/bin/env python3
"""Reproducible x86 gate for gum v5; request.json names every tested SHA."""
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

HOME = Path(sys.argv[1]).resolve()
WORK = Path(sys.argv[2]).resolve()
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from run_fixtures import SCHEMAS, load_actual

REQUEST = json.loads((ROOT / "bench/request.json").read_text())
BASE, CAND, TIMING, ROUNDS = (REQUEST[k] for k in ("baseline", "candidate", "timing", "rounds"))
GATE_BASELINES = [BASE, *REQUEST.get("additional_gate_baselines", [])]
NCPU = os.environ.get("NCPU", "8")
WORK.mkdir(parents=True, exist_ok=True)
GATE_FAILURES = []


def call(args, *, cwd=ROOT, capture=False, env=None):
    print("+", " ".join(map(str, args)), flush=True)
    return subprocess.run(args, cwd=cwd, text=True, check=True, capture_output=capture, env=env)


def source_for(sha):
    dest = WORK / "sources" / sha
    if dest.exists():
        return dest
    dest.mkdir(parents=True)
    archive = subprocess.Popen(["git", "archive", sha], cwd=ROOT, stdout=subprocess.PIPE)
    assert archive.stdout is not None
    tar = subprocess.run(["tar", "-x", "-C", str(dest)], stdin=archive.stdout)
    archive.stdout.close()
    if archive.wait() or tar.returncode:
        raise SystemExit(f"git archive failed: {sha}")
    return dest


def build(sha):
    source, target = source_for(sha), WORK / "target" / sha
    env = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64-v3", CARGO_TARGET_DIR=str(target))
    call(["cargo", "build", "--release", "--locked", "--manifest-path", str(source / "rust/Cargo.toml")], env=env)
    binary = target / "release/rproto"
    if not binary.is_file():
        raise SystemExit(f"missing binary after build: {binary}")
    return binary


def cargo_test(sha):
    source, target = source_for(sha), WORK / "target-test" / sha
    # AVX2/BMI2 scanner tests are cfg(target_feature) gated: build them like the image does.
    env = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64-v3", CARGO_TARGET_DIR=str(target))
    call(["cargo", "test", "--release", "--locked", "--manifest-path", str(source / "rust/Cargo.toml")], env=env)


def fresh(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def run(binary, data, label, *, timing=False):
    out, scratch = WORK / "outputs" / label, WORK / "scratch" / label
    fresh(out); fresh(scratch)
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch), NCPU=NCPU)
    if timing:
        env["TIMING"] = "1"
    started = time.perf_counter()
    result = subprocess.run([str(binary)], text=True, capture_output=True, env=env)
    elapsed = (time.perf_counter() - started) * 1000
    (WORK / "logs").mkdir(exist_ok=True)
    (WORK / "logs" / f"{label}.stderr").write_text(result.stderr)
    if result.returncode:
        raise SystemExit(f"{label} failed ({result.returncode}):\n{result.stderr[-4000:]}")
    if "fast engine declined" in result.stderr:
        GATE_FAILURES.append(f"{label}: fast engine declined")
    for table in ("user_daily", "campaign_daily"):
        if not list((out / table).glob("*.parquet")):
            raise SystemExit(f"{label}: no parquet for {table}")
    laps = [(float(ms), name) for ms, name in re.findall(r"^\s*([0-9.]+) ms\s+(.+)$", result.stderr, re.M)]
    return out, elapsed, laps


def compare(actual, expected, label):
    r = subprocess.run([sys.executable, "scripts/compare.py", str(actual), str(expected)], cwd=ROOT, text=True, capture_output=True)
    print(f"{label}: {r.stdout.strip()}", flush=True)
    ok = not r.returncode and r.stdout.rstrip().endswith("MATCH")
    if not ok:
        GATE_FAILURES.append(f"{label}: output mismatch")
    return ok


def parquet_equal(left, right, label):
    ok = True
    for table, schema in SCHEMAS.items():
        # The scorer permits arbitrary file boundaries and row order. Keep
        # every row, including repeated rows, in the canonical comparison.
        actual = load_actual(left / table, schema).sort(list(schema))
        expected = load_actual(right / table, schema).sort(list(schema))
        matches = actual.equals(expected, null_equal=True)
        print(f"{label} {table}: {actual.height} vs {expected.height} rows, "
              f"{'MATCH' if matches else 'DIFFER'}", flush=True)
        ok = ok and matches
    if not ok:
        GATE_FAILURES.append(f"{label}: canonical parquet rows differ")
    return ok


def fixture_gate(binary):
    for fx in sorted(p for p in (ROOT / "fixtures").iterdir() if p.is_dir()):
        out, _, _ = run(binary, fx / "input", f"fixture-{fx.name}")
        compare(out, fx / "expected", f"fixture {fx.name}")


def time_size(binaries, size, rounds):
    rows = []
    for number in range(rounds):
        order = TIMING[number % len(TIMING):] + TIMING[:number % len(TIMING)]
        row = {"round": number + 1, "order": order}
        for sha in order:
            _, elapsed, laps = run(binaries[sha], HOME / f"v5-{size}", f"{size}-{number + 1}-{sha[:8]}", timing=True)
            row[sha] = (elapsed, laps)
        rows.append(row)
    return rows


def stages(rows, sha):
    keys = sorted({name for row in rows for _, name in row[sha][1]})
    return {key: statistics.median(next((ms for ms, name in row[sha][1] if name == key), 0.0) for row in rows) for key in keys}


def main():
    if len(set(TIMING)) != 3 or BASE not in TIMING or CAND not in TIMING:
        raise SystemExit("request must name baseline, candidate, and one comparison SHA")
    if len(GATE_BASELINES) != len(set(GATE_BASELINES)) or CAND in GATE_BASELINES or not set(GATE_BASELINES) <= set(TIMING):
        raise SystemExit("gate baselines must be distinct, non-candidate SHAs listed in timing")
    for sha in TIMING:
        call(["git", "cat-file", "-e", f"{sha}^{{commit}}"])
    cargo_test(CAND)
    binaries = {sha: build(sha) for sha in TIMING}
    fixture_gate(binaries[CAND])
    dev, _, _ = run(binaries[CAND], HOME / "v5-dev", "candidate-1m", timing=True)
    expected = WORK / "dev-answer"
    call(["unzip", "-q", str(ROOT / "bench/assets/dev-answer.zip"), "-d", str(expected)])
    compare(dev, expected / "dev-answer", "1M candidate vs dev-answer")
    for size, data in (("1m", "v5-dev"), ("20m", "v5-20m"), ("60m", "v5-60m")):
        corpus = HOME / data
        candidate = dev
        if size != "1m":
            candidate, _, _ = run(binaries[CAND], corpus, f"candidate-{size}", timing=True)
        for sha in GATE_BASELINES:
            baseline, _, _ = run(binaries[sha], corpus, f"baseline-{size}-{sha[:8]}", timing=True)
            parquet_equal(candidate, baseline, f"{size} candidate vs {sha}")
    results = {size: time_size(binaries, size, int(ROUNDS[size])) for size in ("20m", "60m")}
    gate = "PASS" if not GATE_FAILURES else "FAIL — " + "; ".join(GATE_FAILURES)
    baselines = ", ".join(f"`{sha}`" for sha in GATE_BASELINES)
    lines = ["# gum v5 gate and timing", f"Host vendor {os.environ.get('RUNNER_VENDOR', 'unknown')}, logical CPUs {os.environ.get('RUNNER_VCPUS', 'unknown')}", "", f"- gate baselines: {baselines}", f"- candidate: `{CAND}`", f"- NCPU: `{NCPU}`; x86-64-v3; TIMING=1", f"- gate: {gate}", ""]
    for size, rows in results.items():
        lines += [f"## {size} ({len(rows)} interleaved rounds)", "", "| round | order | " + " | ".join(s[:8] for s in TIMING) + " |", "|---:|---|" + "---:|" * len(TIMING)]
        for row in rows:
            lines.append(f"| {row['round']} | {' → '.join(s[:8] for s in row['order'])} | " + " | ".join(f"{row[s][0]:.1f} ms" for s in TIMING) + " |")
        lines += ["", "| SHA | median wall ms | stage laps (median ms) |", "|---|---:|---|"]
        for sha in TIMING:
            med = statistics.median(row[sha][0] for row in rows)
            stage_text = "; ".join(f"{k}: {v:.1f}" for k, v in stages(rows, sha).items()) or "(no laps)"
            lines.append(f"| `{sha}` | {med:.1f} | {stage_text} |")
        lines.append("")
    (WORK / "summary.md").write_text("\n".join(lines))
    (WORK / "observations.json").write_text(json.dumps(results, indent=2))
    print((WORK / "summary.md").read_text())
    if GATE_FAILURES:
        raise SystemExit("gate failed: " + "; ".join(GATE_FAILURES))


if __name__ == "__main__":
    main()
