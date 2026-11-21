#!/usr/bin/env python3
"""Convert TIMING=1 logs into a warm-run median stage breakdown."""
from __future__ import annotations

import os
import re
import statistics
import sys
from pathlib import Path

LINE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?) ms\s+(.+?)\s*$")


def read_run(path: Path) -> list[tuple[str, float]]:
    stages: list[tuple[str, float]] = []
    for line in path.read_text(errors="replace").splitlines():
        matched = LINE.match(line)
        if matched:
            stages.append((matched.group(2), float(matched.group(1))))
    if not stages:
        raise SystemExit(f"no TIMING stages found in {path}")
    return stages


def main() -> None:
    logs = [Path(arg) for arg in sys.argv[1:]]
    if not logs:
        raise SystemExit("usage: summarize_timings.py WARM_LOG ...")
    runs = [read_run(path) for path in logs]
    labels = [label for label, _ in runs[0]]
    if any([label for label, _ in run] != labels for run in runs[1:]):
        raise SystemExit("TIMING stage labels differ across warm runs")

    cumulative = [[value for _, value in run] for run in runs]
    print("## Warm 20M stage breakdown")
    print()
    print(f"`NCPU={os.environ.get('NCPU', 'nproc')}`, {len(runs)} measured warm runs; values are median milliseconds.")
    print()
    print("| stage | stage ms | cumulative ms |")
    print("| --- | ---: | ---: |")
    prior = 0.0
    for index, label in enumerate(labels):
        at = statistics.median(run[index] for run in cumulative)
        print(f"| {label} | {at - prior:.1f} | {at:.1f} |")
        prior = at


if __name__ == "__main__":
    main()
