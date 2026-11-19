#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$native_dir/../.." && pwd)
ndk=${1:-${ANDROID_NDK_ROOT:-${ANDROID_NDK_HOME:-}}}
api=${2:-26}
output=${3:-"$repo_dir/build_assets/xray-native-android-arm64"}

if [[ -z "$ndk" ]]; then
    printf 'Usage: bash %s <Android NDK directory> [API level] [output]\n' "$0" >&2
    exit 1
fi
if [[ ! "$api" =~ ^[0-9]+$ ]] || (( api < 21 )); then
    printf 'Android arm64 requires a numeric API level >= 21: %s\n' "$api" >&2
    exit 1
fi
case "$(uname -s)" in
    Linux) host=linux-x86_64 ;;
    Darwin) host=darwin-x86_64 ;;
    *) printf 'Build on Linux or macOS with the Android NDK.\n' >&2; exit 1 ;;
esac
toolchain="$ndk/toolchains/llvm/prebuilt/$host/bin"
cc="$toolchain/aarch64-linux-android${api}-clang"
if [[ ! -x "$cc" || ! -x "$toolchain/llvm-ar" || ! -x "$toolchain/llvm-objcopy" ]]; then
    printf 'Android NDK toolchain not found: %s\n' "$cc" >&2
    exit 1
fi

if [[ -n "${CARGO:-}" ]]; then
    cargo=$CARGO
elif [[ -x "${HOME:-}/.cargo/bin/cargo" ]]; then
    cargo="$HOME/.cargo/bin/cargo"
else
    cargo=cargo
fi
go=${GO:-go}
target=aarch64-linux-android

export CC="$cc"
export CXX="$toolchain/aarch64-linux-android${api}-clang++"
export AR="$toolchain/llvm-ar"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$cc"
export CC_aarch64_linux_android="$cc"
export AR_aarch64_linux_android="$AR"
export RUSTFLAGS="${RUSTFLAGS:-} -C relocation-model=pic"

cd "$repo_dir"
"$cargo" build --locked --release --features engine --lib \
    --target "$target" --target-dir "$native_dir/target" \
    --manifest-path "$native_dir/Cargo.toml"

# A content-derived build ID invalidates Go's linker cache when only Rust changed.
archive="$native_dir/target/$target/release/libxhysteria.a"
native_hash=$(shasum -a 256 "$archive")
native_hash=${native_hash%% *}
mkdir -p -- "$(dirname -- "$output")"
bash "$native_dir/link-go.sh" "$native_dir/target/$target/release" \
    "$toolchain/llvm-objcopy" env CGO_ENABLED=1 GOOS=android GOARCH=arm64 \
    CGO_LDFLAGS="${CGO_LDFLAGS:-} -Wl,-z,max-page-size=16384" \
    "$go" build -tags xhysteria_native -buildmode=pie -trimpath -buildvcs=false \
    -ldflags="-s -w -checklinkname=0 -buildid=xhysteria-$native_hash" \
    -o "$output" ./main
printf 'Native Android arm64 Xray binary: %s\n' "$output"
