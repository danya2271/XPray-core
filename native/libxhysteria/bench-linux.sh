#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$native_dir/../.." && pwd)
output=${1:-"$repo_dir/build_assets/native-perf"}
duration=${XHYS_BENCH_TIME:-1s}
count=${XHYS_BENCH_COUNT:-3}
cpus=${XHYS_BENCH_CPUS:-4}

mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
cd "$repo_dir"
cargo build --locked --release --features engine --lib --manifest-path "$native_dir/Cargo.toml"
native_hash=$(sha256sum "$native_dir/target/release/libxhysteria.a")
CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' \
    -ldflags="-buildid=xhysteria-${native_hash%% *}" \
    -o "$output/interop.test" ./native/libxhysteria/interop

for scenario in OpenClose Transfer Parallel; do
    for mode in go socket_perflow socket_pool2 shared_perflow shared_pool2; do
        name="$scenario-$mode"
        "$output/interop.test" -test.run '^$' \
            -test.bench "^BenchmarkNative${scenario}$/${mode}$" \
            -test.benchtime "$duration" -test.count "$count" -test.cpu "$cpus" \
            -test.timeout 180s -test.benchmem -test.v \
            -test.cpuprofile "$output/$name.cpu.pprof" \
            -test.memprofile "$output/$name.mem.pprof" > "$output/$name.txt" 2>&1
        go tool pprof -top -nodecount=15 "$output/interop.test" "$output/$name.cpu.pprof" \
            > "$output/$name.cpu.txt"
        go tool pprof -top -alloc_space -nodecount=15 "$output/interop.test" "$output/$name.mem.pprof" \
            > "$output/$name.alloc.txt"
        if [[ "${XHYS_BENCH_PERF:-0}" == 1 && "$scenario" == Transfer && "$mode" == shared_pool2 ]]; then
            perf record -g --call-graph dwarf,16384 -F 99 -e cpu-clock:u \
                -o "$output/$name.perf.data" -- "$output/interop.test" -test.run '^$' \
                -test.bench "^BenchmarkNative${scenario}$/${mode}$" \
                -test.benchtime "$duration" -test.count 1 -test.cpu "$cpus" \
                > "$output/$name.perf-run.txt" 2>&1
            perf report --stdio --no-children --sort comm,dso,symbol \
                -i "$output/$name.perf.data" > "$output/$name.perf.txt"
        fi
        printf '%s\n' "$name"
        rg -e '^Benchmark' -e '^--- SKIP' -e 'no valid result' "$output/$name.txt"
    done
done
printf 'Benchmark results and Go profiles: %s\n' "$output"
