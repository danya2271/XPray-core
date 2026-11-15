#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$native_dir/../.." && pwd)
output=${1:-"$repo_dir/build_assets/xray-native"}

cd "$repo_dir"
cargo build --locked --release --features engine --lib --manifest-path "$native_dir/Cargo.toml"
mkdir -p -- "$(dirname -- "$output")"
# Go's cache does not track modifications inside external static archives.
native_hash=$(sha256sum "$native_dir/target/release/libxhysteria.a")
native_hash=${native_hash%% *}
CGO_ENABLED=1 go build -tags xhysteria_native -trimpath -buildvcs=false -ldflags="-s -w -buildid=xhysteria-$native_hash" -o "$output" ./main
printf 'Native Xray binary: %s\n' "$output"
