#!/usr/bin/env python3
"""Canonical v5 verification and interleaved wall-clock timing."""
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

REQUEST = json.loads((ROOT / "bench/request.json").read_text())
BASE = REQUEST["baseline"]
CAND = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
TIMING, ROUNDS = [BASE, CAND], REQUEST["rounds"]
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
    if sha == CAND:
        # Use the actual musl artifact from the Dockerfile for native timing.
        image = f"express-v5-bench:{sha[:12]}"
        call(["docker", "build", "--target", "build", "-t", image, str(source)])
        container = call(["docker", "create", image], capture=True).stdout.strip()
        target.mkdir(parents=True)
        binary = target / "pipeline"
        try:
            call(["docker", "cp", f"{container}:/pipeline", str(binary)])
            source_root = "/src"
            call(["docker", "run", "--rm", image, "cargo", "test", "--release",
                  "--target", "x86_64-unknown-linux-musl", "--manifest-path", f"{source_root}/Cargo.toml"])
        finally:
            call(["docker", "rm", container])
    else:
        env = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64-v3", CARGO_TARGET_DIR=str(target))
        call(["cargo", "build", "--release", "--locked", "--manifest-path", str(source / "rust/Cargo.toml")], env=env)
        binary = target / "release/rproto"
    if not binary.is_file():
        raise SystemExit(f"missing binary after build: {binary}")
    return binary


def fresh(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def run(binary, data, label, *, candidate=False, timing=False, require_fast=True):
    out, scratch = WORK / "outputs" / label, WORK / "scratch" / label
    fresh(out); fresh(scratch)
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch), NCPU=NCPU)
    if candidate:
        if timing:
            env["XL_TIMING"] = "1"
    else:
        env.update(GLIBC_TUNABLES="glibc.malloc.hugetlb=1", BLUE2_IO_THREADS="32")
        if timing:
            env["TIMING"] = "1"
    started = time.perf_counter()
    result = subprocess.run([str(binary)], text=True, capture_output=True, env=env)
    elapsed = (time.perf_counter() - started) * 1000
    (WORK / "logs").mkdir(exist_ok=True)
    (WORK / "logs" / f"{label}.stderr").write_text(result.stderr)
    if result.returncode:
        raise SystemExit(f"{label} failed ({result.returncode}):\n{result.stderr[-4000:]}")
    if candidate:
        fast = "[v5] FAST path complete" in result.stderr and "FALLBACK:" not in result.stderr
        print(f"{label}: fast path used {'yes' if fast else 'no'}", flush=True)
        if require_fast and not fast:
            GATE_FAILURES.append(f"{label}: v5 fast path did not complete")
            print(result.stderr[-2000:], flush=True)
    for table in ("user_daily", "campaign_daily"):
        if not list((out / table).glob("*.parquet")):
            raise SystemExit(f"{label}: no parquet for {table}")
    laps = [(float(ms), name) for ms, name in re.findall(r"^\s*([0-9.]+) ms\s+(.+)$", result.stderr, re.M)]
    if candidate:
        timing_line = next((line for line in result.stderr.splitlines() if line.startswith("[v5] ms: ")), "")
        laps = [(float(ms), name) for name, ms in re.findall(r"([\w.]+)=([0-9.]+)", timing_line)]
    return out, elapsed, laps


def compare(actual, expected, label):
    r = subprocess.run([sys.executable, str(source_for(BASE) / "scripts/compare.py"), str(actual), str(expected)], cwd=ROOT, text=True, capture_output=True)
    print(f"{label}: {r.stdout.strip()}", flush=True)
    ok = not r.returncode and r.stdout.rstrip().endswith("MATCH")
    if not ok:
        GATE_FAILURES.append(f"{label}: output mismatch")
    return ok


def parquet_equal(left, right, label):
    ok = True
    from run_fixtures import SCHEMAS, load_actual

    for table, schema in SCHEMAS.items():
        # The scorer permits arbitrary file boundaries and row order. Keep
        # every row, including repeated rows, in the canonical comparison.
        actual = load_actual(left / table, schema).sort(list(schema))
        expected = load_actual(right / table, schema).sort(list(schema))
        matches = actual.equals(expected, null_equal=True)
        print(f"{label} {table}: {actual.height} vs {expected.height} rows, "
              f"{'MATCH' if matches else 'DIFFER'}", flush=True)
        if not matches:
            cols = list(schema)
            for side, a, b in (("candidate", actual, expected), ("baseline", expected, actual)):
                diff = a.join(b, on=cols, how="anti", nulls_equal=True)
                print(f"{label} {table}: {side} has {diff.height} exclusive rows\n{diff.head(4)}", flush=True)
        ok = ok and matches
    if not ok:
        GATE_FAILURES.append(f"{label}: canonical parquet rows differ")
    return ok


def fixture_gate(binary):
    for fx in sorted(p for p in (source_for(BASE) / "fixtures").iterdir() if p.is_dir()):
        data = fx / "input" if (fx / "input").is_dir() else fx / "data"
        out, _, _ = run(binary, data, f"fixture-{fx.name}", candidate=True, require_fast=False)
        compare(out, fx / "expected", f"fixture {fx.name}")


def time_size(binaries, size, rounds):
    rows = []
    for number in range(rounds):
        order = TIMING[number % len(TIMING):] + TIMING[:number % len(TIMING)]
        row = {"round": number + 1, "order": order}
        for sha in order:
            out, elapsed, laps = run(binaries[sha], HOME / f"v5-{size}", f"{size}-{number + 1}-{sha[:8]}", candidate=sha == CAND, timing=True)
            row[sha] = (elapsed, laps)
            print(f"{size} {sha[:8]} wall {elapsed:.1f} ms", flush=True)
            shutil.rmtree(out)
        rows.append(row)
    return rows


def stages(rows, sha):
    keys = sorted({name for row in rows for _, name in row[sha][1]})
    return {key: statistics.median(next((ms for ms, name in row[sha][1] if name == key), 0.0) for row in rows) for key in keys}


def main():
    sys.path.insert(0, str(source_for(BASE) / "scripts"))
    binaries = {sha: build(sha) for sha in reversed(TIMING)}
    fixture_gate(binaries[CAND])
    dev, _, _ = run(binaries[CAND], HOME / "v5-dev", "candidate-1m", candidate=True, timing=True)
    expected = WORK / "dev-answer"
    call(["unzip", "-q", str(ROOT / "bench/assets/dev-answer.zip"), "-d", str(expected)])
    compare(dev, expected / "dev-answer", "1M candidate vs dev-answer")
    for size, data in (("1m", "v5-dev"), ("20m", "v5-20m"), ("60m", "v5-60m")):
        corpus = HOME / data
        candidate = dev
        if size != "1m":
            candidate, _, _ = run(binaries[CAND], corpus, f"candidate-{size}", candidate=True, timing=True)
        baseline, _, _ = run(binaries[BASE], corpus, f"baseline-{size}-{BASE[:8]}", timing=True)
        parquet_equal(candidate, baseline, f"{size} candidate vs {BASE}")
        shutil.rmtree(baseline)
        shutil.rmtree(candidate)
    results = {size: time_size(binaries, size, int(ROUNDS[size])) for size in ("20m", "60m")}
    gate = "PASS" if not GATE_FAILURES else "FAIL — " + "; ".join(GATE_FAILURES)
    lines = ["# Express v5 gate and timing", "", f"- baseline: `{BASE}`", f"- candidate: `{CAND}`", f"- NCPU: `{NCPU}`; candidate Dockerfile build", f"- gate: {gate}", ""]
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
