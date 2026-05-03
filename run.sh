#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -x ./target/release/mexc_lag ]; then
  echo "Release binary not found. Building..."
  cargo build --release
fi
./target/release/mexc_lag
