#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$project_root"

echo "Building QuantaDB workspace..."
cargo build --workspace --release

echo "Build completed successfully."
echo "Server: target/release/quantadb-server"
