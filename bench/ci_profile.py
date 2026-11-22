#!/usr/bin/env python3
"""Run v5 stage timing, cold-IO controls and Linux perf on the same host."""
import argparse
import bisect
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_HEAD = re.compile(r"^\s*\d+\s+[\d.]+:\s+(\d+)\b")
REQUEST = json.loads((ROOT / "bench/request.json").read_text())
EVENTS = (
    "task-clock,cycles,instructions,cache-misses,LLC-load-misses,"
    "page-faults,context-switches,cpu-migrations,cache-references,minor-faults,major-faults"
)
LAP = re.compile(r"^\s*([0-9.]+) ms\s+(.+)$")
SAMPLE = re.compile(
    r"^\s*(\d+)\s+(\d+\.\d+):\s+(\d+)\s+[\da-f]+\s+(.*?)\s+\((\S+)\)(.*)$"
)
LOCATION = re.compile(r"([^\s]+:\d+)(?:\b|$)")


def build(work, evidence):
    digests = {}
    for sha in REQUEST["timing"]:
        # Isolate Cargo fingerprints for same-name packages from separate git
        # archives. Their checked-in mtimes can predate an existing build.
        target = work / "engine-target" / sha
        source = work / "sources" / sha
        source.mkdir(parents=True, exist_ok=True)
        with subprocess.Popen(["git", "archive", sha, "rust"], cwd=ROOT,
                              stdout=subprocess.PIPE) as archive:
            subprocess.run(["tar", "-x", "-C", source], stdin=archive.stdout, check=True)
            if archive.wait():
                raise RuntimeError(f"git archive failed for {sha}")
        env = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64-v3",
                   CARGO_PROFILE_RELEASE_DEBUG="1", CARGO_TARGET_DIR=str(target))
        print(f"Building {sha} with release debug=1", flush=True)
        subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path",
                        source / "rust/Cargo.toml"], env=env, check=True)
        binary = work / "bin" / sha / "rproto"
        binary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target / "release/rproto", binary)
        with binary.open("rb") as stream:
            digests[sha] = hashlib.file_digest(stream, "sha256").hexdigest()
        shutil.rmtree(target)
    (evidence / "binaries.json").write_text(json.dumps(digests, indent=2) + "\n")
    for sha, digest in digests.items():
        print(f"{sha} binary SHA-256 {digest}", flush=True)
    if len(set(digests.values())) != len(digests):
        raise RuntimeError("The requested revisions produced identical profiling binaries")


def drop_caches():
    subprocess.run(["sync"], check=True)
    subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)


def cat_all(inputs):
    start = time.monotonic()
    subprocess.run(["cat", *inputs], check=True, stdout=subprocess.DEVNULL)
    return (time.monotonic() - start) * 1000


def stage_label(label):
    for prefix in ("links:", "events parsed", "merged:"):
        if label.startswith(prefix):
            return prefix
    return label


def measure(work, evidence, data, sha, label, perf=()):
    scratch = work / "run" / label
    out = scratch / "out"
    out.mkdir(parents=True)
    env = dict(os.environ, NCPU="8", TIMING="1", DATA_DIR=str(data),
               OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch / "scratch"))
    binary = work / "bin" / sha / "rproto"
    laps = []
    with (evidence / f"{label}.stderr").open("w") as log:
        started = time.monotonic()
        with subprocess.Popen([*perf, binary], env=env, text=True, stdout=subprocess.DEVNULL,
                              stderr=subprocess.PIPE) as proc:
            for line in proc.stderr:
                received = time.monotonic()
                log.write(line)
                match = LAP.match(line)
                if match:
                    laps.append({"stage": stage_label(match[2]), "engine_ms": float(match[1]),
                                 "received": received})
            code = proc.wait()
        ended = time.monotonic()
    diagnostics = (evidence / f"{label}.stderr").read_text()
    valid = (code == 0 and laps and laps[-1]["stage"] == "parquet written"
             and "fast engine declined" not in diagnostics and "events parsed" in
             {lap["stage"] for lap in laps}
             and all(list((out / table).glob("*.parquet"))
                     for table in ("campaign_daily", "user_daily")))
    if not valid:
        print(f"{label}: invalid run (exit {code})\n{diagnostics[-3000:]}", flush=True)
        shutil.rmtree(scratch)
        if not perf:
            raise RuntimeError(f"{label}: missing Nova timing or output")
        return None
    shutil.rmtree(scratch)
    row = {
        "wall_ms": (ended - started) * 1000,
        "first_ms": (laps[0]["received"] - started) * 1000,
        "exit_ms": (ended - laps[-1]["received"]) * 1000,
        "laps": laps,
    }
    print(f"{label} {row['wall_ms']:.2f} ms (first {row['first_ms']:.2f}, "
          f"exit {row['exit_ms']:.2f})", flush=True)
    return row


def stage_times(row):
    first = row["first_ms"]
    elapsed = row["laps"][0]["engine_ms"]
    offsets = [first + lap["engine_ms"] - elapsed for lap in row["laps"]]
    return {
        **{lap["stage"]: end - start
           for lap, start, end in zip(row["laps"], [0, *offsets], offsets)},
        "exit": row["exit_ms"],
    }


def perf_stat(work, evidence, data, sha):
    label = f"{sha[:8]}-stat"
    stat = evidence / f"{label}.csv"
    for events in (EVENTS, "task-clock,page-faults,context-switches,cpu-migrations,minor-faults,major-faults"):
        if measure(work, evidence, data, sha, label,
                   ["perf", "stat", "-x", ",", "-e", events, "-o", stat, "--"]):
            break
        label += "-software"
        stat = evidence / f"{label}.csv"
    metrics = {}
    if stat.exists():
        print(stat.read_text(), flush=True)
        for line in stat.read_text().splitlines():
            columns = line.split(",")
            if len(columns) >= 3 and columns[2] in EVENTS.split(","):
                value, unit, event = columns[:3]
                metrics[event] = {"value": float(value) if not value.startswith("<") else None,
                                  "unit": unit, "status": value,
                                  "running_pct": columns[4] if len(columns) > 4 else ""}
    return metrics


def attribute_callers(evidence, record, total):
    """Aggregate DWARF call chains: who calls interning and the unresolved libc code."""
    frame = re.compile(r"^\s+[0-9a-f]+\s+(.*?)\s+\(([^)]*)\)\s*$")
    groups = {"intern": Counter(), "libc": Counter(), "person": Counter()}
    proc = subprocess.Popen(["perf", "script", "-i", record, "-F", "tid,time,period,ip,sym,dso",
                             "--max-stack", "12"], stdout=subprocess.PIPE, text=True)
    def flush(period, frames):
        if not frames:
            return
        leaf, dso = frames[0]
        if "ExactIds>::intern" in leaf:
            name = "intern"
        elif dso.endswith("libc.so.6"):
            name = "libc"
        elif "Users>::person" in leaf:
            name = "person"
        else:
            return
        chain = []
        for sym, d in frames[1:]:
            if sym == leaf or (name == "libc" and d.endswith("libc.so.6")):
                continue
            chain.append(sym[:90])
            if len(chain) == 4:
                break
        groups[name][(leaf[:60], *chain)] += period
    period, frames = 0, []
    for line in proc.stdout:
        match = SAMPLE_HEAD.match(line)
        if match:
            flush(period, frames)
            period, frames = int(match[1]), []
            continue
        found = frame.match(line)
        if found:
            frames.append((found[1], found[2]))
    flush(period, frames)
    proc.wait()
    with (evidence / "callers.txt").open("w") as out:
        for name, counter in groups.items():
            out.write(f"== {name}: {100 * counter.total() / total:.2f}% of samples\n")
            for chain, weight in counter.most_common(25):
                out.write(f"{100 * weight / total:6.2f}%  " + "  <-  ".join(chain) + "\n")
    print((evidence / "callers.txt").read_text()[:6000], flush=True)


def summarize_samples(evidence, label, record, profile, event, detailed):
    # The producer and perf use CLOCK_MONOTONIC. The earliest consistent
    # receipt minus the Rust lap avoids positive reader scheduling delays.
    origin = min(lap["received"] - lap["engine_ms"] / 1000 for lap in profile["laps"])
    bounds = [origin + lap["engine_ms"] / 1000 for lap in profile["laps"]]
    stages = [*(lap["stage"] for lap in profile["laps"]), "exit"]
    windows = defaultdict(Counter)
    symbols = Counter()
    symbol_stages = defaultdict(Counter)
    locations = defaultdict(Counter)
    comm = ["perf", "script", "-i", record, "-G", "-F", "tid,time,period,ip,sym,dso,srcline"]
    with (evidence / f"{label}-samples.txt").open("w") as stream:
        subprocess.run(comm, stdout=stream, check=True)
    with (evidence / f"{label}-samples.txt").open() as stream:
        previous_sample = None
        for line in stream:
            match = SAMPLE.match(line)
            if not match:
                # perf script emits source lines on a separate indented line.
                location = LOCATION.fullmatch(line.strip())
                if location and previous_sample:
                    key, weight = previous_sample
                    locations[key][location[1]] += weight
                previous_sample = None
                continue
            tid, seconds, period, symbol, dso, rest = match.groups()
            stage = stages[bisect.bisect_left(bounds, float(seconds))]
            weight = int(period)
            key = (symbol, Path(dso).name)
            windows[stage][tid] += weight
            symbol_stages[stage][key] += weight
            symbols[key] += weight
            location = LOCATION.search(rest)
            if location:
                locations[key][location[1]] += weight
            previous_sample = (key, weight)
    total = symbols.total()
    top = []
    for (symbol, dso), weight in symbols.most_common(30):
        key = (symbol, dso)
        top.append({"symbol": symbol, "dso": dso, "percent": 100 * weight / total,
                    "locations": locations[key].most_common(3),
                    "stages": {s: counts[key] for s, counts in symbol_stages.items()
                               if counts[key]}})
    event_stage = symbol_stages["events parsed"]
    event_symbols = [
        {"symbol": key[0], "dso": key[1], "percent": 100 * weight / event_stage.total(),
         "period": weight, "locations": locations[key].most_common(3)}
        for key, weight in event_stage.most_common(30)
    ]
    result = {"event": event, "period_total": total, "by_stage_and_thread": windows,
              "top_symbols": top, "event_parse_symbols": event_symbols,
              "lap_clock_origin": origin, "lap_clock_boundaries": dict(zip(stages, bounds))}
    if not total:
        print(f"{label}: perf script had no parseable samples", flush=True)
    if detailed:
        with (evidence / "perf-report.txt").open("w") as report:
            subprocess.run(["perf", "report", "--no-children", "--percent-limit", "0.5",
                            "--stdio", "-i", record], stdout=report, check=True)
        print("\n".join((evidence / "perf-report.txt").read_text().splitlines()[:150]),
              flush=True)
        attribute_callers(evidence, record, total)
        with (evidence / "perf-srcline.txt").open("w") as report:
            subprocess.run(["perf", "report", "--no-children", "--percent-limit", "0",
                            "--stdio", "--sort", "symbol,srcline", "-i", record],
                           stdout=report, check=True)
    return result


def perf_record(work, evidence, data, sha, counters, detailed):
    label = f"{sha[:8]}-profile"
    record = work / f"{data.name}-{label}.data"
    event = "cycles" if counters.get("cycles", {}).get("value") else "cpu-clock"
    flags = ["-g", "--call-graph", "dwarf"] if detailed else []
    try:
        for sample_event in dict.fromkeys((event, "cpu-clock")):
            profile = measure(work, evidence, data, sha, label,
                              ["perf", "record", "-e", sample_event, "-F", "999",
                               "--clockid", "mono", "--no-buildid-cache", *flags,
                               "-o", record, "--"])
            if profile and record.exists():
                try:
                    samples = summarize_samples(evidence, label, record, profile, sample_event,
                                                detailed)
                except subprocess.CalledProcessError as error:
                    print(f"{label}: {error}", flush=True)
                    continue
                if samples["period_total"]:
                    return samples
        return {}
    finally:
        record.unlink(missing_ok=True)


def format_summary(size, results):
    lines = [f"## Intel v5 {size}, seed 17", "",
             f"Cold input cat: {results['bytes']:,} B in {results['cat_ms']:.1f} ms", ""]
    for sha, cohort in results["cohorts"].items():
        warm = cohort["warm"]
        median = statistics.median(row["wall_ms"] for row in warm)
        runs = ", ".join(f"{row['wall_ms']:.1f}" for row in warm)
        startup = statistics.median(row["first_ms"] for row in warm)
        exit_ms = statistics.median(row["exit_ms"] for row in warm)
        lines.extend([f"### `{sha}`", "",
                      f"Warm wall {median:.1f} ms ({runs}). Cold {cohort['cold']['wall_ms']:.1f} ms.",
                      f"Launch to first lap {startup:.2f} ms. Last lap to exit {exit_ms:.2f} ms.",
                      "", "| Stage | Median ms | % wall | Threads sampled |",
                      "|---|---:|---:|---:|"])
        stages = [stage_times(row) for row in warm]
        by_stage = cohort["samples"].get("by_stage_and_thread", {})
        for stage in stages[0]:
            elapsed = statistics.median(row[stage] for row in stages)
            lines.append(f"| {stage} | {elapsed:.2f} | {elapsed / median:.1%} | "
                         f"{len(by_stage.get(stage, {}))} |")
        lines.extend(["", "| Perf stat event | Value | Unit |", "|---|---:|---|"])
        for name, metric in cohort["counters"].items():
            lines.append(f"| {name} | {metric['status']} | {metric['unit']} |")
        if size == "20m" and sha == REQUEST["candidate"]:
            lines.extend(["", f"Samples weighted by {cohort['samples'].get('event', 'unavailable')}.",
                          "", "| Self % | Symbol | DSO | Sampled source |", "|---:|---|---|---|"])
            for symbol in cohort["samples"].get("top_symbols", []):
                source = ", ".join(f"`{loc}`" for loc, _ in symbol["locations"][:2])
                lines.append(f"| {symbol['percent']:.2f} | `{symbol['symbol']}` | "
                             f"{symbol['dso']} | {source} |")
        lines.append("")
    return "\n".join(lines)


def run_size(work, evidence, size):
    data = work / f"data-{size}"
    inputs = sorted(path for path in data.rglob("*") if path.is_file())
    if not inputs:
        raise RuntimeError(f"No inputs in {data}")
    counts = subprocess.check_output(["wc", "-l", *[path for path in inputs
                                                   if path.suffix == ".csv"]], text=True)
    csv_rows = Counter()
    for line in counts.splitlines():
        number, name = line.split(maxsplit=1)
        if name != "total":
            csv_rows[Path(name).parent.name if Path(name).parent != data else
                     Path(name).stem] += int(number) - 1
    drop_caches()
    results = {"bytes": sum(path.stat().st_size for path in inputs),
               "cat_ms": cat_all(inputs), "csv_rows": csv_rows,
               "cohorts": {sha: {"warm": []} for sha in REQUEST["timing"]}}
    cohorts = results["cohorts"]

    for sha, cohort in cohorts.items():
        drop_caches()
        cohort["cold"] = measure(work, evidence, data, sha, f"{sha[:8]}-cold")
    cat_all(inputs)
    for round_number in range(REQUEST["rounds"][size]):
        order = list(cohorts) if round_number % 2 == 0 else list(reversed(cohorts))
        for sha in order:
            cohorts[sha]["warm"].append(measure(work, evidence, data, sha,
                                               f"{sha[:8]}-warm-{round_number + 1}"))
    for sha, cohort in cohorts.items():
        cohort["counters"] = perf_stat(work, evidence, data, sha)
        cohort["samples"] = perf_record(work, evidence, data, sha, cohort["counters"],
                                        size == "20m" and sha == REQUEST["candidate"])
    summary = format_summary(size, results)
    (evidence / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (evidence / "summary.md").write_text(summary)
    print(summary, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["build", "20m", "60m"])
    parser.add_argument("work", type=Path)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    if args.action == "build":
        build(args.work.resolve(), args.evidence.resolve())
    else:
        evidence = args.evidence.resolve() / args.action
        evidence.mkdir(parents=True, exist_ok=True)
        run_size(args.work.resolve(), evidence, args.action)


if __name__ == "__main__":
    main()
