"""Exact gates and interleaved timing for isolated rewrite variants."""
import json
import os
import shutil
import statistics
from pathlib import Path

import ci_bench as bench

variants = json.loads((bench.ROOT / 'bench/variants.json').read_text())
shared_target = bench.WORK / 'target-shared'


def build_variant(label, config, test=False):
    source = bench.source_for(config['sha'])
    env = dict(os.environ, RUSTFLAGS='-C target-cpu=x86-64-v3', CARGO_TARGET_DIR=str(shared_target))
    command = ['cargo', 'test' if test else 'build', '--release', '--locked', '--manifest-path', str(source / 'rust/Cargo.toml')]
    if config['features']:
        command += ['--features', ','.join(config['features'])]
    bench.call(command, env=env)
    if test:
        return
    destination = bench.WORK / 'binaries' / label
    destination.parent.mkdir(exist_ok=True)
    shutil.copy2(shared_target / 'release/rproto', destination)
    return destination


# Exercise both the unchanged path and the new table, including alternate
# routing bits. Every production variant is also checked on all three sizes.
build_variant('counts', variants['counts'], test=True)
build_variant('hex-users', variants['hex-users'], test=True)
binaries = {label: build_variant(label, config) for label, config in variants.items()}
for label in ('counts', 'hex-users'):
    bench.fixture_gate(binaries[label])
expected = bench.WORK / 'dev-answer'
bench.call(['unzip', '-q', str(bench.ROOT / 'bench/assets/dev-answer.zip'), '-d', str(expected)])
for size, corpus in [('1m', 'v5-dev'), ('20m', 'v5-20m'), ('60m', 'v5-60m')]:
    reference, _, _ = bench.run(binaries['control'], bench.HOME / corpus, f'gate-{size}-control', timing=True)
    if size == '1m':
        bench.compare(reference, expected / 'dev-answer', 'control public dev-answer')
    for label, binary in binaries.items():
        if label == 'control':
            continue
        output, _, _ = bench.run(binary, bench.HOME / corpus, f'gate-{size}-{label}', timing=True)
        bench.parquet_equal(output, reference, f'{label} {size}')
        shutil.rmtree(output)
    shutil.rmtree(reference)
if bench.GATE_FAILURES:
    raise SystemExit('gate failed: ' + '; '.join(bench.GATE_FAILURES))

results = {}
labels = list(variants)
for size, rounds in [('20m', 7), ('60m', 5)]:
    rows = []
    for number in range(rounds):
        order = labels[number % len(labels):] + labels[:number % len(labels)]
        measurements = {}
        for label in order:
            output, elapsed, laps = bench.run(binaries[label], bench.HOME / f'v5-{size}', f'timing-{size}-{number}-{label}', timing=True)
            measurements[label] = {'wall_ms': elapsed, 'laps': laps}
            shutil.rmtree(output)
        rows.append({'order': order, 'measurements': measurements})
        print(f'{size} round {number + 1}: ' + ', '.join(f'{label}={measurements[label]["wall_ms"]:.1f}ms' for label in labels), flush=True)
    results[size] = rows

lines = ['# Rewrite comparison', '', f"Host vendor {os.environ.get('RUNNER_VENDOR', 'unknown')}, logical CPUs {os.environ.get('RUNNER_VCPUS', 'unknown')}", '', 'Canonical gates MATCH at 1M, 20M and 60M for every variant.', '', '| Variant | SHA | Features | 20M median ms | 60M median ms |', '|---|---|---|---:|---:|']
for label, config in variants.items():
    medians = [statistics.median(row['measurements'][label]['wall_ms'] for row in results[size]) for size in ('20m', '60m')]
    lines.append(f'| {label} | {config["sha"]} | {",".join(config["features"])} | {medians[0]:.1f} | {medians[1]:.1f} |')
(bench.WORK / 'summary.md').write_text('\n'.join(lines) + '\n')
(bench.WORK / 'observations.json').write_text(json.dumps({'variants': variants, 'results': results}, indent=2))
print('\n'.join(lines))
if bench.GATE_FAILURES:
    raise SystemExit('timing run declined: ' + '; '.join(bench.GATE_FAILURES))
