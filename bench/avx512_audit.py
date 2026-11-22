#!/usr/bin/env python3
"""Count 256- and 512-bit vector instructions in selected emitted functions.

The release profile uses fat LTO and many source helpers are deliberately
``inline(always)``.  A source helper with no emitted symbol is therefore shown
as ``inlined/no emitted symbol`` rather than being silently attributed to an
unrelated function.  Counts are instruction counts, not vector-lane counts.
"""
from __future__ import annotations

import argparse
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


HOT = [
    ("k_free::run_events", re.compile(r"(?:^|::)k_free::run_events")),
    ("k_free::ev_s", re.compile(r"(?:^|::)k_free::ev_s")),
    ("k_free::ev_d", re.compile(r"(?:^|::)k_free::ev_d")),
    ("k_free::ev_purchase", re.compile(r"(?:^|::)k_free::ev_purchase")),
    ("k_free::cl_loop", re.compile(r"(?:^|::)k_free::cl_loop")),
    ("parse::ev_batch", re.compile(r"(?:^|::)parse::.*ev_batch")),
    ("parse::ev_vec", re.compile(r"(?:^|::)parse::ev_vec")),
    ("parse::ck_vec", re.compile(r"(?:^|::)parse::ck_vec")),
    ("parse::push_many", re.compile(r"(?:^|::)parse::.*push_many")),
    ("events::stage3 closures", re.compile(r"(?:^|::)events::stage3.*(?:closure|\{\{closure\})")),
]
HEAVY = {"vpmaddubsw", "vpmaddwd", "vpmulld", "vpmullq", "vpmuludq"}
HEADER = re.compile(r"^[0-9a-fA-F]+ <(.+)>:$")
INSN = re.compile(r"^\s*[0-9a-fA-F]+:\s+(.+)$")
MNEMONIC = re.compile(r"^([a-z][a-z0-9.]*)\b", re.I)


@dataclass
class Counts:
    functions: list[str] = field(default_factory=list)
    zmm: int = 0
    ymm: int = 0
    heavy: Counter[str] = field(default_factory=Counter)

    def heavy_text(self) -> str:
        return ", ".join(f"{op}×{self.heavy[op]}" for op in sorted(self.heavy)) or "—"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--objdump", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    counts = {name: Counts() for name, _ in HOT}
    active: list[Counts] = []
    for raw in args.objdump.read_text(errors="replace").splitlines():
        header = HEADER.match(raw)
        if header:
            symbol = header.group(1)
            active = [counts[name] for name, pattern in HOT if pattern.search(symbol)]
            for item in active:
                item.functions.append(symbol)
            continue
        if not active:
            continue
        insn = INSN.match(raw)
        if not insn:
            continue
        text = insn.group(1)
        mnemonic = MNEMONIC.match(text)
        for item in active:
            item.zmm += len(re.findall(r"%zmm\d+\b", text)) > 0
            item.ymm += len(re.findall(r"%ymm\d+\b", text)) > 0
            if mnemonic and mnemonic.group(1).lower() in HEAVY:
                item.heavy[mnemonic.group(1).lower()] += 1

    lines = [
        f"# AVX-512 audit: {args.commit}",
        "",
        "Built through the Dockerfile's fast-path build (`x86_64-unknown-linux-musl`); "
        "the repository `.cargo/config.toml` supplies `-C target-cpu=cascadelake`.",
        "Counts are emitted assembly instruction counts. A missing source helper is normally "
        "fat-LTO `inline(always)` code, so its instructions appear in its caller's row.",
        "",
        "| source hot function / emitted aggregation | emitted symbols | %zmm instructions | %ymm instructions | heavy integer multiplies |",
        "|---|---:|---:|---:|---|",
    ]
    for name, _ in HOT:
        item = counts[name]
        symbols = str(len(item.functions)) if item.functions else "inlined/no emitted symbol"
        lines.append(f"| {name} | {symbols} | {item.zmm} | {item.ymm} | {item.heavy_text()} |")
    emitted = [(name, item) for name, item in counts.items() if item.functions]
    lines += ["", "## Matched emitted symbols", ""]
    for name, item in emitted:
        lines.append(f"- `{name}`: " + "; ".join(f"`{symbol}`" for symbol in item.functions))
    lines.append("")
    args.output.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
