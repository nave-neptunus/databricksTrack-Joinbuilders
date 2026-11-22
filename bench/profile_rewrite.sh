#!/usr/bin/env bash
# Diagnostic sampling runs after the correctness and timing gates.
set -euo pipefail
mkdir -p bench-work/logs
if ! timeout 180 sudo apt-get install -y -qq linux-tools-common linux-tools-generic > bench-work/logs/perf-install.txt 2>&1; then
  echo 'Profiling tools unavailable' > bench-work/logs/profile.txt
  exit 0
fi
perf_binary=$(rg --files /usr/lib/linux-tools | rg '/perf$' | head -1)
if test -z "$perf_binary"; then
  echo 'Profiling binary unavailable' > bench-work/logs/profile.txt
  exit 0
fi
mkdir -p bench-work/profile-output bench-work/profile-scratch
sudo sysctl -w kernel.perf_event_paranoid=-1 > bench-work/logs/perf-sysctl.txt 2>&1 || true
if sudo env DATA_DIR="$HOME/v5-20m" OUTPUT_DIR="$PWD/bench-work/profile-output" SCRATCH_DIR="$PWD/bench-work/profile-scratch" NCPU=8 TIMING=1 \
  "$perf_binary" record -e cpu-clock:u -F 199 --call-graph dwarf -o "$PWD/bench-work/profile.data" -- "$PWD/bench-work/binaries/combined1024" \
  > bench-work/logs/profile-stdout.txt 2> bench-work/logs/profile-stderr.txt; then
  sudo "$perf_binary" report -i bench-work/profile.data --stdio --no-children --percent-limit 0.5 > bench-work/logs/profile.txt
else
  echo 'Sampling unavailable on this runner' > bench-work/logs/profile.txt
fi
