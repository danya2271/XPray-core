#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$native_dir/../.." && pwd)
output=${1:-"$repo_dir/build_assets/native-server-perf"}
duration=${XHYS_BENCH_TIME:-2s}
count=${XHYS_BENCH_COUNT:-3}
cpus=${XHYS_BENCH_CPUS:-4}
scenarios=${XHYS_BENCH_SCENARIOS:-"Transfer Parallel Upload Download Latency LoadedLatency UDPLatency"}
clients=${XHYS_BENCH_CLIENTS:-"go shared_pool2"}
masks=${XHYS_BENCH_MASKS:-"0 1"}

mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
cd "$repo_dir"
cargo build --locked --release --features engine --lib --manifest-path "$native_dir/Cargo.toml"
native_hash=$(sha256sum "$native_dir/target/release/libxhysteria.a")
CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' \
    -ldflags="-buildid=xhysteria-${native_hash%% *}" \
    -o "$output/interop.test" ./native/libxhysteria/interop
git rev-parse HEAD > "$output/revision.txt"
printf '%s\n' "${native_hash%% *}" > "$output/native-archive.sha256"
git diff --binary -- native/libxhysteria transport/internet/xhysteria \
    transport/internet/hysteria/native.go transport/internet/hysteria/native_test.go \
    > "$output/source.patch"

# Every sample is a new process: no surviving pools, runtimes or RSS high-water
# marks from a previous benchmark. Alternate server order between repetitions.
for ((sample=1; sample<=count; sample++)); do
    servers="0 1"
    if (( sample % 2 == 0 )); then servers="1 0"; fi
    for masked in $masks; do
        for scenario in $scenarios; do
            for client in $clients; do
                for server in $servers; do
                    name="$scenario-mask$masked-client$client-server$server-$sample"
                    XHYS_BENCH_NATIVE_SERVER="$server" XHYS_BENCH_SALAMANDER="$masked" \
                        "$output/interop.test" -test.run '^$' \
                        -test.bench "^BenchmarkNative${scenario}$/${client}$" \
                        -test.benchtime "$duration" -test.count 1 -test.cpu "$cpus" \
                        -test.benchmem -test.timeout 120s > "$output/$name.txt" 2>&1
                    printf '%s ' "$name"
                    rg 'ns/op' "$output/$name.txt"
                done
            done
        done
    done
done
