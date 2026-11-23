#!/usr/bin/env bash
set -euo pipefail

native_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH='' cd -- "$native_dir/../.." && pwd)
output=${1:-"$repo_dir/build_assets/throughput-profile"}
count=${XHYS_BENCH_COUNT:-7}
cpus=${XHYS_BENCH_CPUS:-4}
latency=${XHYS_BENCH_LATENCY:-1}
if [[ ! "$count" =~ ^[1-9][0-9]*$ || ! "$cpus" =~ ^[1-9][0-9]*$ || ! "$latency" =~ ^[01]$ ]]; then
    printf 'Count/CPUs must be positive integers; XHYS_BENCH_LATENCY must be 0 or 1.\n' >&2
    exit 2
fi
mkdir -p -- "$output"
output=$(CDPATH='' cd -- "$output" && pwd)
if [[ -e "$output/profile-r1-workers4-upload.log" || -e "$output/summary.json" ]]; then
    printf 'Choose a new output directory to preserve existing measurements: %s\n' "$output" >&2
    exit 2
fi
runner=()
if [[ -n "${XHYS_BENCH_AFFINITY:-}" ]]; then
    runner=(taskset -c "$XHYS_BENCH_AFFINITY")
fi
cd "$repo_dir"
binary=${XHYS_BENCH_BINARY:-"$output/interop.test"}
binary_origin=provided
if [[ -z "${XHYS_BENCH_BINARY:-}" ]]; then
    binary_origin=built
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
sha256sum "$binary" > "$output/interop.sha256"
printf 'cpu_affinity=%s\ngomaxprocs=%s\nrepeats=%s\nbulk_bytes=536870912\npaced_MiBps=32\nworkers_per_role=4,1\nbinary_origin=%s\nlatency_enabled=%s\n' \
    "${XHYS_BENCH_AFFINITY:-unrestricted}" "$cpus" "$count" "$binary_origin" "$latency" > "$output/methodology.txt"
cat > "$output/measure.py" <<'PY'
import os
import sys
pid = os.fork()
if pid == 0:
    os.execvp(sys.argv[1], sys.argv[1:])
_, status, usage = os.wait4(pid, 0)
print(f"process_peak_rss_KiB={usage.ru_maxrss}", flush=True)
sys.exit(os.waitstatus_to_exitcode(status))
PY

# Each sample is a fresh process, with one QUIC session and the same 512 MiB.
# This compares explicit runtime settings, not different transport binaries.
for ((repeat=1; repeat<=count; repeat++)); do
    profiles='4 1'
    if ((repeat%2==0)); then profiles='1 4'; fi
    for workers in $profiles; do
        kinds='upload download'
        if [[ "$latency" == 1 ]]; then kinds+=' tcp-idle udp-idle tcp-paced'; fi
        for kind in $kinds; do
            case "$kind" in
                upload) benchmark=BenchmarkNativePacketUpload; iterations=8192x ;;
                download) benchmark=BenchmarkNativePacketDownload; iterations=8192x ;;
                tcp-idle) benchmark=BenchmarkNativePacketLatency; iterations=30000x ;;
                udp-idle) benchmark=BenchmarkNativePacketUDPLatency; iterations=30000x ;;
                tcp-paced) benchmark=BenchmarkNativePacketPacedLatency; iterations=30000x ;;
            esac
            log="$output/profile-r${repeat}-workers${workers}-${kind}.log"
            env XRAY_HYSTERIA_CLIENT_WORKERS="$workers" XRAY_HYSTERIA_SERVER_WORKERS="$workers" \
                XHYS_BENCH_PACED_MIBPS=32 python3 "$output/measure.py" "${runner[@]}" \
                "$binary" -test.run '^$' -test.bench "^${benchmark}$/native$" \
                -test.benchtime "$iterations" -test.cpu "$cpus" -test.count 1 \
                -test.timeout 120s > "$log" 2>&1
            printf 'repeat=%s workers=%s kind=%s\n' "$repeat" "$workers" "$kind"
        done
    done
done
python3 - "$output" "$count" "$latency" <<'PY'
from pathlib import Path
from statistics import median
import json
import re
import sys
p, count, latency = Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
kinds = ["upload", "download"] + (["tcp-idle", "udp-idle", "tcp-paced"] if latency else [])
result = []
for workers in (4, 1):
    for kind in kinds:
        rows = []
        for repeat in range(1, count + 1):
            path = p / f"profile-r{repeat}-workers{workers}-{kind}.log"
            text = path.read_text()
            assert "\nPASS\n" in text, path
            words = next(line for line in text.splitlines() if line.startswith("Benchmark")).split()
            row = {words[i + 1]: float(words[i]) for i in range(2, len(words), 2)}
            assert int(words[1]) == (8192 if kind in ("upload", "download") else 30000), path
            row["RSS-MiB"] = int(re.search(r"process_peak_rss_KiB=(\d+)", text)[1]) / 1024
            rows.append(row)
        summary = {key: {"median": median(row[key] for row in rows),
                         "min": min(row[key] for row in rows),
                         "max": max(row[key] for row in rows)} for key in rows[0]}
        result.append(dict(workers=workers, kind=kind, samples=rows, summary=summary))
        selected = {key: round(values["median"], 3) for key, values in summary.items()
                    if key in ("MB/s", "process-cpu-ns/op", "RSS-MiB", "p99-us", "background-MiB/s")}
        print(f"workers={workers} {kind}: {selected}")
(p / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
PY
printf 'Complete observations and summary: %s\n' "$output"
