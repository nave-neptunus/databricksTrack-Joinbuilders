#!/usr/bin/env sh
# Complete CI harness: fixtures, independent-reference exact gate, then a
# 20M warm-cache profile.  Data and outputs stay beneath .ci-bench so Actions
# cleanup can reclaim them after the job.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

: "${IMAGE:?set IMAGE to the image tag}"
BENCH_EVENTS=${BENCH_EVENTS:-20000000}
BENCH_CLICKS=${BENCH_CLICKS:-5000000}
VERIFY_EVENTS=${VERIFY_EVENTS:-300000}
VERIFY_CLICKS=${VERIFY_CLICKS:-75000}
WARM_RUNS=${WARM_RUNS:-3}
NCPU=${NCPU:-$(nproc)}
export NCPU

work="$root/.ci-bench"
rm -rf "$work"
mkdir -p "$work/logs"
gen="$root/generator/target/release/generate"

echo "== exact fixture gate"
python3 scripts/run_fixtures.py fixtures -- scripts/ci_docker_run.sh

echo "== independent-reference gate (${VERIFY_EVENTS} events)"
"$gen" --seed 249 --events "$VERIFY_EVENTS" --clicks "$VERIFY_CLICKS" --output "$work/verify-data"
DATA_DIR="$work/verify-data" OUTPUT_DIR="$work/reference-output" python3 reference/reference.py
scripts/ci_docker_run.sh "$work/verify-data" "$work/verify-output"
python3 scripts/compare.py "$work/verify-output" "$work/reference-output"

echo "== generate 20M benchmark data"
"$gen" --seed 3 --events "$BENCH_EVENTS" --clicks "$BENCH_CLICKS" --output "$work/bench-data"

run_one() {
    name=$1
    out="$work/$name-output"
    log="$work/logs/$name.log"
    rm -rf "$out"
    echo "== ${name} (NCPU=${NCPU})"
    { /usr/bin/time -f 'HARNESS_WALL_SECONDS %e' env TIMING=1 scripts/ci_docker_run.sh "$work/bench-data" "$out"; } 2>&1 | tee "$log"
}

# This run faults input pages and is intentionally excluded from the summary.
run_one warmup

logs=''
i=1
while [ "$i" -le "$WARM_RUNS" ]; do
    name="warm-$i"
    run_one "$name"
    logs="$logs $work/logs/$name.log"
    i=$((i + 1))
done

# shellcheck disable=SC2086 -- log paths contain no spaces and are generated above.
python3 scripts/summarize_timings.py $logs | tee "$work/timing-summary.md"
