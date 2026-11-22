#!/usr/bin/env python3
"""express-cl gate: canonical parquet compare vs baseline engine, fast-path check, timing."""
import os, re, shutil, statistics, subprocess, sys, time
from pathlib import Path

HOME, WORK, BASE_BIN, CAND_BIN = (Path(a).resolve() for a in sys.argv[1:5])
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))
from run_fixtures import SCHEMAS, load_actual

NCPU = os.environ.get("NCPU", "8")
ROUNDS = {"20m": int(os.environ.get("R20", "3")), "60m": int(os.environ.get("R60", "2"))}
FAILS = []
WORK.mkdir(parents=True, exist_ok=True)


def fresh(p):
    shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True)


def run(binary, data, label):
    out, scratch = WORK / "out" / label, WORK / "scratch" / label
    fresh(out); fresh(scratch)
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch), NCPU=NCPU, TIMING="1")
    t = time.perf_counter()
    r = subprocess.run([str(binary)], text=True, capture_output=True, env=env)
    ms = (time.perf_counter() - t) * 1000
    (WORK / "logs").mkdir(exist_ok=True)
    (WORK / "logs" / f"{label}.stderr").write_text(r.stderr)
    if r.returncode:
        print(f"{label} FAILED rc={r.returncode}\n{r.stderr[-3000:]}", flush=True)
        FAILS.append(f"{label}: rc={r.returncode}")
    fb = "FALLBACK" in r.stderr
    return out, ms, fb, r.stderr


def equal(a, b, label):
    ok = True
    for table, schema in SCHEMAS.items():
        x = load_actual(a / table, schema).sort(list(schema))
        y = load_actual(b / table, schema).sort(list(schema))
        m = x.equals(y, null_equal=True)
        print(f"{label} {table}: {x.height} vs {y.height} rows {'MATCH' if m else 'DIFFER'}", flush=True)
        ok &= m
    if not ok:
        FAILS.append(f"{label}: DIFFER")
    return ok


def main():
    exp = WORK / "dev-answer"
    fresh(exp)
    subprocess.run(["unzip", "-q", str(ROOT / "bench/assets/dev-answer.zip"), "-d", str(exp)], check=True)
    for size, data in (("1m", "v5-dev"), ("20m", "v5-20m"), ("60m", "v5-60m")):
        d = HOME / data
        c, cms, cfb, cerr = run(CAND_BIN, d, f"cand-{size}")
        b, bms, bfb, _ = run(BASE_BIN, d, f"base-{size}")
        print(f"[{size}] cand {cms:.0f} ms fallback={cfb} | base {bms:.0f} ms", flush=True)
        if size == "1m":
            r = subprocess.run([sys.executable, str(ROOT / "bench/compare.py"), str(c), str(next(exp.rglob("user_daily")).parent)], capture_output=True, text=True)
            print("1M vs dev-answer:", r.stdout.strip()[-200:], flush=True)
            if not r.stdout.rstrip().endswith("MATCH"):
                FAILS.append("1m dev-answer mismatch")
        equal(c, b, f"{size} cand vs base")
        if cfb:
            FAILS.append(f"{size}: candidate fell back")
        print("\n".join(l for l in cerr.splitlines() if re.search(r"FALLBACK|v5|fast", l))[:1500], flush=True)
    rows = {}
    for size in ("20m", "60m"):
        d = HOME / f"v5-{size}"
        cs, bs = [], []
        for i in range(ROUNDS[size]):
            order = [("c", CAND_BIN), ("b", BASE_BIN)] if i % 2 == 0 else [("b", BASE_BIN), ("c", CAND_BIN)]
            for tag, bin_ in order:
                _, ms, _, err = run(bin_, d, f"t-{size}-{i}-{tag}")
                (cs if tag == "c" else bs).append(ms)
                if tag == "c" and i == ROUNDS[size] - 1:
                    print(f"--- cand laps {size} ---\n" + err[-2500:], flush=True)
        rows[size] = (statistics.median(cs), statistics.median(bs), cs, bs)
        print(f"TIMING {size}: cand median {rows[size][0]:.0f} ms vs base {rows[size][1]:.0f} ms | cand {[round(x) for x in cs]} base {[round(x) for x in bs]}", flush=True)
    print("GATE:", "PASS" if not FAILS else "FAIL " + "; ".join(FAILS), flush=True)
    sys.exit(1 if FAILS else 0)


main()
