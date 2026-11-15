#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$native_dir/../.." && pwd)

cd "$repo_dir"
cargo test --locked --features engine --manifest-path "$native_dir/Cargo.toml"
cargo clippy --locked --all-targets --features engine --manifest-path "$native_dir/Cargo.toml" -- -D warnings
cargo build --locked --release --features engine --lib --manifest-path "$native_dir/Cargo.toml"
cargo build --locked --features probe --bins --manifest-path "$native_dir/Cargo.toml"
native_hash=$(sha256sum "$native_dir/target/release/libxhysteria.a")
native_hash=${native_hash%% *}
CGO_ENABLED=1 go test -race -tags 'xhysteria_native xhysteria_interop' -ldflags="-buildid=xhysteria-$native_hash" ./transport/internet/xhysteria ./transport/internet/hysteria ./native/libxhysteria/interop -count=1 -timeout=180s
