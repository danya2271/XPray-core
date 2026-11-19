#!/usr/bin/env bash
set -euo pipefail

release=${1:?Rust release directory required}
objcopy=${2:?objcopy executable required}
shift 2
if (( $# == 0 )); then
    printf 'A linker command is required.\n' >&2
    exit 1
fi
if ! command -v "$objcopy" >/dev/null 2>&1; then
    printf 'objcopy executable not found: %s\n' "$objcopy" >&2
    exit 1
fi

backup=$(mktemp -d "${TMPDIR:-/tmp}/xhysteria-link.XXXXXX")
saved=false
cleanup() {
    local result=$?
    trap - EXIT
    if "$saved" && ! tar -xf "$backup/archives.tar" -C "$release"; then
        printf 'Failed to restore Rust archives; backup retained at %s\n' "$backup" >&2
        exit 1
    fi
    rm -rf -- "$backup"
    exit "$result"
}
trap cleanup EXIT

archives=()
while IFS= read -r -d '' archive; do
    archives+=("$archive")
done < <(cd "$release" && find . -type f -name '*.a' -print0)
if (( ${#archives[@]} == 0 )); then
    printf 'No Rust static archives found in %s\n' "$release" >&2
    exit 1
fi
tar -cf "$backup/archives.tar" -C "$release" -- "${archives[@]}"
saved=true
for archive in "${archives[@]}"; do
    "$objcopy" --remove-section=.llvmbc --remove-section=.llvmcmd "$release/$archive"
done
"$@"
