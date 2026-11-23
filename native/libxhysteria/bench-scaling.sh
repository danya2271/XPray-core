#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH='' cd -- "$native_dir/../.." && pwd)
output=${1:-"$repo_dir/build_assets/core-scaling"}
mkdir -p -- "$output"
output=$(CDPATH='' cd -- "$output" && pwd)
binary=${XHYS_SCALING_BINARY:-"$output/interop.test"}
cd "$repo_dir"
if [[ -z "${XHYS_SCALING_BINARY:-}" ]]; then
    cargo build --locked --release --features engine --lib \
        --manifest-path "$native_dir/Cargo.toml" > "$output/build.log" 2>&1
    native_hash=$(sha256sum "$native_dir/target/release/libxhysteria.a")
    printf '%s\n' "${native_hash%% *}" > "$output/native-archive.sha256"
    bash "$native_dir/link-go.sh" "$native_dir/target/release" "${OBJCOPY:-objcopy}" \
        env CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' \
        -ldflags="-buildid=xhysteria-${native_hash%% *}" -o "$binary" \
        ./native/libxhysteria/interop >> "$output/build.log" 2>&1
fi
binary=$(realpath -- "$binary")
test -x "$binary"
git rev-parse HEAD > "$output/revision.txt"
args=(
    --binary "$binary" --output "$output"
    --cores "${XHYS_SCALING_CORES:-1,2,4,6,8,10,12,14,16}"
    --search-repeats "${XHYS_SCALING_SEARCH_REPEATS:-2}"
    --final-repeats "${XHYS_SCALING_FINAL_REPEATS:-3}"
)
if [[ -n "${XHYS_SCALING_CPUS:-}" ]]; then
    args+=(--cpus "$XHYS_SCALING_CPUS")
fi
if [[ -n "${XHYS_SCALING_FINALISTS:-}" ]]; then
    args+=(--finalists "$XHYS_SCALING_FINALISTS")
fi
if [[ "${XHYS_SCALING_SKIP_SCREEN:-0}" == 1 ]]; then
    args+=(--skip-screen)
fi
exec python3 "$native_dir/bench-scaling.py" "${args[@]}"
