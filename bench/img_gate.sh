#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOME_DIR=${1:?need corpus home}
WORK=${2:?need work dir}
REQ="$ROOT/bench/img_request.json"
BASE=$(jq -r .baseline "$REQ")
NCPU=8
SUMMARY="$WORK/summary.md"
LOGS="$WORK/logs"
mkdir -p "$WORK/src" "$LOGS"
: > "$SUMMARY"
log() { echo "$*" | tee -a "$SUMMARY"; }

declare -A SHA IMAGE STATIC ENV_OK CMD_OK USER_OK STATUS
SHA[img1]=$(jq -r .img1 "$REQ")
SHA[img2]=$(jq -r .img2 "$REQ")

log "baseline $BASE; img1 ${SHA[img1]}; img2 ${SHA[img2]}"
log "runner arch $(uname -m); avx512=$(grep -qw avx512f /proc/cpuinfo && echo yes || echo no)"

# Exact expected output: the current best binary, built like bench/ci_bench.py.
mkdir -p "$WORK/src/base"
git -C "$ROOT" archive "$BASE" | tar -x -C "$WORK/src/base"
RUSTFLAGS="-C target-cpu=x86-64-v3" CARGO_TARGET_DIR="$WORK/target-base" \
  cargo build --release --locked --manifest-path "$WORK/src/base/rust/Cargo.toml"
BASE_BIN="$WORK/target-base/release/rproto"

build_image() {
  local name=$1 sha=${SHA[$1]} src="$WORK/src/$1"
  mkdir -p "$src"
  git -C "$ROOT" archive "$sha" | tar -x -C "$src"
  IMAGE[$name]="gum-$name:$sha"
  docker build --platform linux/amd64 -f "$src/Dockerfile" -t "${IMAGE[$name]}" "$src"
}

verify_image() {
  local name=$1 image=${IMAGE[$1]} cid
  cid=$(docker create "$image")
  docker cp "$cid:/app/rproto" "$WORK/$name-rproto" > /dev/null
  docker rm "$cid" > /dev/null
  if file "$WORK/$name-rproto" | grep -qi 'statically linked'; then STATIC[$name]=PASS; else STATIC[$name]=FAIL; fi
  docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$image" > "$WORK/$name.env"
  ENV_OK[$name]=PASS
  grep -qx 'GLIBC_TUNABLES=glibc.malloc.hugetlb=1' "$WORK/$name.env" || ENV_OK[$name]=FAIL
  grep -qx 'BLUE2_IO_THREADS=32' "$WORK/$name.env" || ENV_OK[$name]=FAIL
  if test "$(docker inspect -f '{{json .Config.Cmd}}' "$image")" = '["/app/rproto"]'; then CMD_OK[$name]=PASS; else CMD_OK[$name]=FAIL; fi
  if test "$(docker inspect -f '{{.Config.User}}' "$image")" = '1000:1000'; then USER_OK[$name]=PASS; else USER_OK[$name]=FAIL; fi
  log "- $name image: static=${STATIC[$name]} env=${ENV_OK[$name]} cmd=${CMD_OK[$name]} user=${USER_OK[$name]}"
}

run_binary() {
  local data=$1 out=$2 label=$3
  rm -rf "$out"; mkdir -p "$out"; chmod 777 "$out"
  DATA_DIR="$data" OUTPUT_DIR="$out" SCRATCH_DIR="$out/scratch" NCPU="$NCPU" BLUE2_IO_THREADS=32 \
    "$BASE_BIN" > "$LOGS/$label.out" 2> "$LOGS/$label.err"
}

run_image() {
  local name=$1 data=$2 out=$3 label=$4
  rm -rf "$out"; mkdir -p "$out" "$out/scratch"; chmod -R 777 "$out"
  docker run --rm --network none --user 1000:1000 \
    -v "$data:/data:ro" -v "$out:/output" \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/output/scratch -e NCPU="$NCPU" \
    "${IMAGE[$name]}" > "$LOGS/$label.out" 2> "$LOGS/$label.err"
}

compare_outputs() {
  local actual=$1 expected=$2 label=$3
  if python3 "$ROOT/scripts/compare.py" "$actual" "$expected" > "$LOGS/$label.compare" 2>&1 \
      && grep -q '^MATCH$' "$LOGS/$label.compare"; then
    echo PASS
  else
    echo FAIL
  fi
}

for name in img1 img2; do
  build_image "$name"
  verify_image "$name"
done

for spec in "1m:$HOME_DIR/v5-1m" "20m:$HOME_DIR/v5-20m"; do
  size=${spec%%:*}; data=${spec#*:}
  run_binary "$data" "$WORK/out/base-$size" "baseline-$size"
done

for name in img1 img2; do
  if test "$name" = img2 && ! grep -qw avx512f /proc/cpuinfo; then
    STATUS[$name]=SKIPPED
    log "$name ${SHA[$name]}: 1M SKIPPED; 20M SKIPPED (no avx512f on runner)"
    continue
  fi
  if test "${STATIC[$name]}" != PASS || test "${ENV_OK[$name]}" != PASS \
      || test "${CMD_OK[$name]}" != PASS || test "${USER_OK[$name]}" != PASS; then
    STATUS[$name]=FAIL
    log "$name ${SHA[$name]}: FAIL (image checks)"
    continue
  fi
  bits=()
  ok=PASS
  for spec in "1m:$HOME_DIR/v5-1m" "20m:$HOME_DIR/v5-20m"; do
    size=${spec%%:*}; data=${spec#*:}
    run_image "$name" "$data" "$WORK/out/$name-$size" "$name-$size"
    result=$(compare_outputs "$WORK/out/$name-$size" "$WORK/out/base-$size" "$name-$size-vs-base")
    bits+=("${size^^} $result")
    test "$result" = PASS || ok=FAIL
  done
  STATUS[$name]=$ok
  log "$name ${SHA[$name]}: ${bits[*]}"
done

log "RESULT img1 ${SHA[img1]} ${STATUS[img1]}"
log "RESULT img2 ${SHA[img2]} ${STATUS[img2]}"
if test "${STATUS[img1]}" = FAIL || test "${STATUS[img2]}" = FAIL; then exit 1; fi
