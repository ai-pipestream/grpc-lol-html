#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required to build and start grpc-lol-html" >&2
  exit 1
fi

echo "Building grpc-lol-html in release mode..."
cargo build --release --locked

echo "Starting grpc-lol-html..."
exec "$SCRIPT_DIR/target/release/grpc-lol-html"
