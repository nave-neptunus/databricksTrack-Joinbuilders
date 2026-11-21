#!/usr/bin/env bash
# Regenerate the checked-in x86-64-v3 PGO profile from the current source.
# Run on an x86-64 Linux host with Rust 1.100.0 / LLVM 23.1.5 and llvm-tools-preview:
#   ./scripts/regen-pgo.sh
set -euo pipefail

. /root/.cargo/env

repo=$(cd "$(dirname "$0")/.." && pwd)
data=${PGO_DATA_DIR:-/root/shopping-bench/big20}
shipped=${PGO_SHIPPED_DIR:-/root/shopping-bench/out-shipped}
py=${PGO_PYTHON:-/root/shopping-bench/check-env/bin/python}

toolchain=$(rustup show active-toolchain | cut -d' ' -f1)
rustup component add llvm-tools-preview --toolchain "$toolchain"
rustc -vV
host=$(rustc -vV | sed -n 's/^host: //p')
[[ $host == x86_64-unknown-linux-gnu ]] || { echo "expected x86_64-unknown-linux-gnu, got $host" >&2; exit 1; }
profdata=$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata
"$profdata" --version

root=$(mktemp -d /root/oc-pgo.XXXXXX)
echo "PGO_ROOT=$root"
exec > >(tee "$root/run.log") 2>&1
trap 'echo "PGO_END status=$? root=$root"' EXIT
mkdir -p "$root/raw" "$root/bin" "$repo/rust/pgo"

flags='-C target-cpu=x86-64-v3'
export CARGO_TARGET_DIR=$root/target CARGO_INCREMENTAL=0
unset TIMING FORCE_SLOW LLVM_PROFILE_FILE
export TMPDIR=$root

exec 9>/tmp/lb.lock
echo 'PGO_WAIT_LOCK'
flock 9

echo 'PGO_BUILD baseline'
/usr/bin/time -f 'PGO_BUILD baseline seconds=%e rss_kib=%M' env RUSTFLAGS="$flags" \
    cargo build --manifest-path "$repo/rust/Cargo.toml" --locked --release -j4
cp "$CARGO_TARGET_DIR/release/rproto" "$root/bin/base"

echo 'PGO_BUILD instrumented'
/usr/bin/time -f 'PGO_BUILD instrumented seconds=%e rss_kib=%M' env RUSTFLAGS="$flags -C profile-generate=$root/raw" \
    cargo build --manifest-path "$repo/rust/Cargo.toml" --locked --release -j4
cp "$CARGO_TARGET_DIR/release/rproto" "$root/bin/instrumented"

run_training() (
    out=$(mktemp -d "$root/train.XXXXXX")
    trap 'rm -r "$out"' EXIT
    DATA_DIR=$1 OUTPUT_DIR=$out NCPU=8 LLVM_PROFILE_FILE="$root/raw/%m-%p.profraw" \
        taskset -c 0-7 "$root/bin/instrumented"
)

echo 'PGO_TRAIN big20'
run_training "$data"
echo 'PGO_TRAIN fixtures A1 A12 E10 L3'
for fixture in A1 A12 E10 L3; do
    fixture_dir=$repo/fixtures/$fixture/data
    if [[ ! -d "$fixture_dir" ]]; then fixture_dir=$repo/fixtures/$fixture/input; fi
    run_training "$fixture_dir"
done

"$profdata" merge --sparse "$root/raw" -o "$root/merged.profdata"
"$profdata" show "$root/merged.profdata" | head -20
size=$(stat -c %s "$root/merged.profdata")
(( size < 10000000 )) || { echo "profile too large: $size bytes" >&2; exit 1; }
cp "$root/merged.profdata" "$repo/rust/pgo/merged.profdata"
sha256sum "$repo/rust/pgo/merged.profdata"
echo "PGO_PROFILE_SIZE=$size"

echo 'PGO_BUILD optimized'
/usr/bin/time -f 'PGO_BUILD optimized seconds=%e rss_kib=%M' env \
    RUSTFLAGS="$flags -Cprofile-use=$repo/rust/pgo/merged.profdata -Cllvm-args=-no-pgo-warn-mismatch" \
    cargo build --manifest-path "$repo/rust/Cargo.toml" --locked --release -j4 >"$root/profile-build.log" 2>&1 || {
    tail -40 "$root/profile-build.log"; exit 1;
}
tail -7 "$root/profile-build.log"
cp "$CARGO_TARGET_DIR/release/rproto" "$root/bin/pgo"
sha256sum "$root/bin/base" "$root/bin/pgo"

check_big20() (
    out=$(mktemp -d "$root/check.XXXXXX")
    trap 'rm -r "$out"' EXIT
    DATA_DIR=$data OUTPUT_DIR=$out NCPU=8 TIMING=1 taskset -c 0-7 "$root/bin/$1" 2>"$root/check-$1.log"
    if grep -qi declined "$root/check-$1.log"; then cat "$root/check-$1.log"; exit 1; fi
    (cd "$repo/scripts" && "$py" compare.py "$out" "$shipped") | tee "$root/compare-$1.log"
    [[ $(tail -1 "$root/compare-$1.log") == MATCH ]] || exit 1
    echo "PGO_CHECK big20 $1 MATCH"
)
check_big20 base
check_big20 pgo

"$py" "$repo/scripts/run_fixtures.py" "$repo/fixtures" -- "$root/bin/pgo" > "$root/fixtures.log"
tail -3 "$root/fixtures.log"

flock -u 9
echo "PGO_COMPLETE root=$root profile=$repo/rust/pgo/merged.profdata"
