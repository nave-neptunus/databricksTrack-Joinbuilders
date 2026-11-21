#!/usr/bin/env sh
# Scorer-like Docker invocation for GitHub Actions.  Arguments are DATA_DIR
# and OUTPUT_DIR; TIMING=1 is intentionally forwarded for stage telemetry.
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 DATA_DIR OUTPUT_DIR" >&2
    exit 2
fi

: "${IMAGE:?set IMAGE to the image tag}"
data=$(cd "$1" && pwd)
mkdir -p "$2"
out=$(cd "$2" && pwd)
scratch=$(mktemp -d)
chmod 777 "$out" "$scratch"

cleanup() {
    rm -rf "$scratch"
}
trap cleanup EXIT

ncpu=${NCPU:-$(nproc)}
docker run --rm \
    --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges \
    --pids-limit 4096 --cpus "$ncpu" --memory "24g" --memory-swap "24g" \
    --user 1000:1000 \
    -v "$data":/data:ro -v "$out":/output -v "$scratch":/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="$ncpu" -e MEM_GB=24 \
    -e TIMING \
    "$IMAGE"
