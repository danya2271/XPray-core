#!/usr/bin/env python3
"""Tune an explicit end-to-end CPU budget; never change runtime defaults."""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import signal
from statistics import median
import subprocess
import sys
import time

BENCHMARKS = {
    "duplex": "BenchmarkNativeScalingDuplex",
    "upload": "BenchmarkNativeScalingUpload",
    "download": "BenchmarkNativeScalingDownload",
    "small": "BenchmarkNativeScalingSmall",
    "paced": "BenchmarkNativeScalingPacedLatency",
}
SIZES = {"duplex": 8192, "upload": 8192, "download": 8192, "small": 65536, "paced": 10000}


def physical_cpus():
    """One allowed logical CPU per physical core; exclude SMT siblings."""
    seen, selected = set(), []
    for cpu in sorted(os.sched_getaffinity(0)):
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        identity = ((root / "physical_package_id").read_text().strip(),
                    (root / "core_id").read_text().strip())
        if identity not in seen:
            seen.add(identity)
            selected.append(cpu)
    return selected


def parse_log(path):
    text = path.read_text()
    if "\nPASS\n" not in text:
        raise RuntimeError(f"Benchmark did not pass: {path}")
    line = next(line for line in text.splitlines() if line.startswith("Benchmark"))
    words = line.split()
    row = {words[i + 1]: float(words[i]) for i in range(2, len(words), 2)}
    row["iterations"] = int(words[1])
    row["RSS-MiB"] = int(re.search(r"process_peak_rss_KiB=(\d+)", text)[1]) / 1024
    return row


def summarize(rows):
    return {key: dict(median=median(row[key] for row in rows),
                      min=min(row[key] for row in rows), max=max(row[key] for row in rows))
            for key in rows[0]}


class Matrix:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.binary = args.binary.resolve()
        self.cpus = physical_cpus()
        if args.cpus:
            requested = [int(value) for value in args.cpus.split(",")]
            if len(set(requested)) != len(requested) or not set(requested) <= set(self.cpus):
                raise ValueError("--cpus must list distinct allowed physical-core representatives")
            self.cpus = requested
        if max(args.cores) > len(self.cpus):
            raise ValueError(f"Need {max(args.cores)} physical cores, have {len(self.cpus)}")
        digest = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        manifest = dict(binary=str(self.binary), sha256=digest, physical_cpus=self.cpus,
                        methodology="v4-drained-calibration-persistent-streams-driver-drop-gate",
                        cores=args.cores, flows=32, model="combined client/server/target CPU budget",
                        search_iterations=SIZES["duplex"], final_iterations=SIZES,
                        search_repeats=args.search_repeats, final_repeats=args.final_repeats)
        if args.finalists:
            manifest["finalist_source"] = str(args.finalists.resolve())
            manifest["finalist_sha256"] = hashlib.sha256(args.finalists.read_bytes()).hexdigest()
        path = self.output / "manifest.json"
        if path.exists() and json.loads(path.read_text()) != manifest:
            raise RuntimeError("Output contains a different benchmark manifest; use a new directory")
        path.write_text(json.dumps(manifest, indent=2) + "\n")
        self.rows = []
        invocation = dict(argv=sys.argv, skip_stream_screen=getattr(args, "skip_screen", False),
                          driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        invocation_path = self.output / "invocation.json"
        if invocation_path.exists():
            previous = json.loads(invocation_path.read_text())
            if previous["skip_stream_screen"] != invocation["skip_stream_screen"]:
                raise RuntimeError("Output contains a different screening mode; use a new directory")
        else:
            invocation_path.write_text(json.dumps(invocation, indent=2) + "\n")
        failures = self.output / "failures.json"
        self.failures = json.loads(failures.read_text()) if failures.exists() else []

    def run(self, cores, config, kind, phase, repeat):
        client, server, pool = config
        name = f"{phase}-n{cores}-c{client}-s{server}-p{pool}-r{repeat}-{kind}"
        path = self.output / (name + ".log")
        iterations = SIZES[kind]
        if not path.exists():
            env = os.environ.copy()
            env.update(XRAY_HYSTERIA_CLIENT_WORKERS=str(client),
                       XRAY_HYSTERIA_SERVER_WORKERS=str(server),
                       XHYS_SCALING_POOL=str(pool), XRAY_NET_DIAG="0")
            command = [sys.executable, str(Path(__file__).resolve()), "--measure",
                       "taskset", "-c", ",".join(map(str, self.cpus[:cores])),
                       str(self.binary), "-test.run", "^$",
                       "-test.bench", f"^{BENCHMARKS[kind]}/native$",
                       "-test.benchtime", f"{iterations}x", "-test.cpu", str(cores),
                       "-test.count", "1", "-test.timeout", "120s"]
            began = time.time()
            (self.output / (name + ".command.json")).write_text(json.dumps(dict(
                argv=command, env={key: env[key] for key in (
                    "XRAY_HYSTERIA_CLIENT_WORKERS", "XRAY_HYSTERIA_SERVER_WORKERS",
                    "XHYS_SCALING_POOL", "XRAY_NET_DIAG")},
                started_unix=began, loadavg=os.getloadavg()), indent=2) + "\n")
            with path.open("w") as output:
                process = subprocess.Popen(command, env=env, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = process.wait(timeout=140)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=10)
                    raise RuntimeError(f"Timed out; log preserved: {path}") from None
            if code and not re.search(r"scaling fixture kernel UDP drops: \d+", path.read_text()):
                raise RuntimeError(f"Benchmark exit {code}; log preserved: {path}")
        dropped = re.search(r"scaling fixture kernel UDP drops: (\d+)", path.read_text())
        if dropped:
            failure = dict(cores=cores, client_workers=client, server_workers=server,
                           pool=pool, kind=kind, phase=phase, repeat=repeat,
                           log=path.name, kernel_udp_drops=int(dropped[1]))
            if not any(row["log"] == path.name for row in self.failures):
                self.failures.append(failure)
                (self.output / "failures.json").write_text(
                    json.dumps(self.failures, indent=2) + "\n")
            print(f"rejected drop gate: {name} drops={dropped[1]}", flush=True)
            return None
        metrics = parse_log(path)
        if metrics["iterations"] != iterations or metrics["tcp-flows"] != 32:
            raise RuntimeError(f"Workload changed: {path}")
        # Validate the pool even when the sample fails its UDP-drop gate.
        if pool != 1 and metrics.get("native-client-engines") != pool:
            raise RuntimeError(f"QUIC pool was not populated: {path}")
        drops = metrics.get("kernel-udp-drops")
        if drops is None or drops < 0:
            raise RuntimeError(f"Missing/invalid UDP counter: {path}")
        if drops:
            failure = dict(cores=cores, client_workers=client, server_workers=server,
                           pool=pool, kind=kind, phase=phase, repeat=repeat,
                           log=path.name, kernel_udp_drops=int(drops), metrics=metrics)
            if not any(row["log"] == path.name for row in self.failures):
                self.failures.append(failure)
                (self.output / "failures.json").write_text(json.dumps(self.failures, indent=2) + "\n")
            print(f"rejected drop gate: {name} drops={int(drops)}", flush=True)
            return None
        row = dict(cores=cores, client_workers=client, server_workers=server,
                   pool=pool, kind=kind, phase=phase, repeat=repeat,
                   log=path.name, metrics=metrics)
        self.rows.append(row)
        (self.output / "observations.json").write_text(json.dumps(self.rows, indent=2) + "\n")
        return metrics

    def search_summary(self, cores):
        configs = {}
        for row in self.rows:
            if row["cores"] == cores and row["kind"] == "duplex" and row["phase"] != "final":
                key = (row["client_workers"], row["server_workers"], row["pool"])
                configs.setdefault(key, []).append(row["metrics"])
        return {config: summarize(rows) for config, rows in configs.items()}

    def rank(self, cores):
        summaries = self.search_summary(cores)
        return sorted(summaries, key=lambda config: (
            -summaries[config]["MB/s"]["median"],
            summaries[config]["process-cpu-ns/op"]["median"],
            summaries[config]["RSS-MiB"]["median"]))

    @staticmethod
    def default(cores):
        return min(4, cores), min(4, cores), 4 if cores >= 8 else 2

    def search(self):
        # Search ordering alternates and is reproducibly shuffled per budget.
        for repeat in range(1, self.args.search_repeats + 1):
            budgets = self.args.cores if repeat % 2 else list(reversed(self.args.cores))
            for cores in budgets:
                workers = [n for n in (1, 2, 4, 6, 8, 10, 12, 14, 16) if n <= cores]
                random.Random(1000 * cores + repeat).shuffle(workers)
                configs = [(n, n, 8) for n in workers]
                default = self.default(cores)
                if default not in configs:
                    configs.append(default)
                for config in configs:
                    self.run(cores, config, "duplex", "balanced", repeat)
                best = self.rank(cores)[0]
                print(f"balanced cores={cores} repeat={repeat} best={best}", flush=True)
        for cores in self.args.cores:
            anchors = self.rank(cores)[:2]
            configs = set()
            for client, server, _ in anchors:
                clients = {1, client, min(cores, max(2, client * 2))}
                servers = {1, server, min(cores, max(2, server * 2))}
                configs.update((c, s, 8) for c in clients for s in servers)
            known = self.search_summary(cores)
            configs = sorted(configs - set(known))
            random.Random(2000 + cores).shuffle(configs)
            for config in configs:
                self.run(cores, config, "duplex", "asymmetric", 1)
            print(f"asymmetric cores={cores} best={self.rank(cores)[0]}", flush=True)
        for cores in self.args.cores:
            anchors = self.rank(cores)[:2]
            configs = {(c, s, pool) for c, s, _ in anchors for pool in (1, 2, 4, 8)}
            configs.add(self.default(cores))
            for repeat in range(1, self.args.search_repeats + 1):
                ordered = sorted(configs)
                random.Random(3000 * cores + repeat).shuffle(ordered)
                for config in ordered:
                    self.run(cores, config, "duplex", "pool", repeat)
                print(f"pool cores={cores} repeat={repeat} best={self.rank(cores)[0]}", flush=True)
        finalists = {}
        for cores in self.args.cores:
            # Recheck two fastest search configurations and the actual default.
            configs = self.rank(cores)[:2]
            default = self.default(cores)
            if default not in configs:
                configs.append(default)
            finalists[str(cores)] = configs
        (self.output / "finalists.json").write_text(json.dumps(finalists, indent=2) + "\n")
        return finalists

    def final(self, finalists):
        for repeat in range(1, self.args.final_repeats + 1):
            budgets = self.args.cores if repeat % 2 else list(reversed(self.args.cores))
            for cores in budgets:
                configs = [tuple(config) for config in finalists[str(cores)]]
                if repeat % 2 == 0:
                    configs.reverse()
                for config in configs:
                    for kind in ("duplex", "upload", "download", "paced"):
                        self.run(cores, config, kind, "final", repeat)
                print(f"final cores={cores} repeat={repeat}", flush=True)

    def screen(self, finalists):
        """A duplex leader may overflow UDP during uninterrupted streaming."""
        selected, missing = {}, []
        for cores in self.args.cores:
            candidates = [tuple(config) for config in finalists[str(cores)]]
            candidates.extend([(1, 1, 1), (1, 1, 2)])
            if cores >= 2:
                candidates.extend([(2, 2, 1), (2, 2, 2),
                                   (2, min(4, cores), 2), (2, min(8, cores), 2)])
                candidates.extend((client, server, pool)
                                  for client, server in ((1, 2), (2, 1)) for pool in (1, 2))
            clean = []
            for config in dict.fromkeys(candidates):
                values = [self.run(cores, config, kind, "stream-screen", 1)
                          for kind in ("upload", "download")]
                if all(value is not None for value in values):
                    speed = math.sqrt(values[0]["MB/s"] * values[1]["MB/s"])
                    clean.append((speed, config))
            configs = [config for _, config in sorted(clean, reverse=True)[:2]]
            if not clean:
                missing.append(cores)
            default = self.default(cores)
            if default not in configs:
                configs.append(default)
            selected[str(cores)] = configs
            print(f"stream-screen cores={cores} clean={sorted(clean, reverse=True)} "
                  f"finalists={configs}", flush=True)
        (self.output / "finalists.json").write_text(json.dumps(selected, indent=2) + "\n")
        if missing:
            raise RuntimeError(f"No fully loss-free screening candidate for core budgets {missing}")
        return selected

    def results(self):
        final_rows, recommendations = [], []
        for cores in self.args.cores:
            grouped = {}
            valid = {}
            for row in self.rows:
                if row["cores"] == cores and row["phase"] == "final":
                    key = (row["client_workers"], row["server_workers"], row["pool"])
                    grouped.setdefault(key, {}).setdefault(row["kind"], []).append(row["metrics"])
                    counts = valid.setdefault(key, {})
                    counts[row["kind"]] = counts.get(row["kind"], 0) + 1
            for row in self.failures:
                if row["cores"] == cores and row["phase"] == "final" and "metrics" in row:
                    key = (row["client_workers"], row["server_workers"], row["pool"])
                    grouped.setdefault(key, {}).setdefault(row["kind"], []).append(row["metrics"])
            summaries = {}
            counts, failed = {}, {}
            for config, kinds in grouped.items():
                counts[config] = valid.get(config, {})
                failed[config] = sum(
                    row["cores"] == cores and row["phase"] == "final"
                    and (row["client_workers"], row["server_workers"], row["pool"]) == config
                    for row in self.failures)
                for kind in ("duplex", "upload", "download", "paced"):
                    failures = sum(
                        row["cores"] == cores and row["phase"] == "final" and row["kind"] == kind
                        and (row["client_workers"], row["server_workers"], row["pool"]) == config
                        for row in self.failures)
                    if counts[config].get(kind, 0) + failures != self.args.final_repeats:
                        raise RuntimeError(f"Incomplete finalist: {cores} {config} {kind}")
                summaries[config] = {kind: summarize(rows) for kind, rows in kinds.items()}
                final_rows.append(dict(cores=cores, client_workers=config[0],
                                       server_workers=config[1], pool=config[2],
                                       failed_samples=failed[config], valid_samples=counts[config],
                                       summary=summaries[config]))
            baseline = summaries[self.default(cores)]
            baseline_p99 = baseline.get("paced", {}).get("p99-us", {}).get("median")
            qualified = [config for config in summaries
                         if failed[config] == 0 and len(counts[config]) == 4
                         and all(n == self.args.final_repeats for n in counts[config].values())]
            loss_free = bool(qualified)
            if not qualified:
                # Preserve an honest overload result for this CPU budget,
                # without publishing it as a qualified environment profile.
                qualified = list(summaries)
            # Constrain latency at the same attained load. Within 3% of peak
            # geometric-mean streaming throughput, favor lower CPU then RSS.
            eligible = [config for config in qualified
                        if baseline_p99 is not None
                        and summaries[config]["paced"]["p99-us"]["median"] <= baseline_p99 * 1.10
                        and summaries[config]["paced"]["background-MiB/s"]["median"] >= 57.6]
            if not eligible:
                eligible = qualified
            speed = {config: math.sqrt(values["upload"]["MB/s"]["median"]
                                       * values["download"]["MB/s"]["median"])
                     for config, values in summaries.items()
                     if "upload" in values and "download" in values}
            fastest = max(speed, key=speed.get)
            peak = max(speed[config] for config in eligible)
            near = [config for config in eligible if speed[config] >= peak * .97]
            chosen = min(near, key=lambda config: (
                sum(summaries[config][kind]["process-cpu-ns/op"]["median"]
                    for kind in ("upload", "download")),
                max(summaries[config][kind]["RSS-MiB"]["median"]
                    for kind in ("upload", "download"))))
            values = summaries[chosen]
            record = dict(cores=cores, client_workers=chosen[0], server_workers=chosen[1],
                          pool=chosen[2], affinity=self.cpus[:cores],
                          upload_MBps=values["upload"]["MB/s"]["median"],
                          download_MBps=values["download"]["MB/s"]["median"],
                          duplex_MBps=values["duplex"]["MB/s"]["median"],
                          upload_cpu_us=values["upload"]["process-cpu-ns/op"]["median"] / 1000,
                          download_cpu_us=values["download"]["process-cpu-ns/op"]["median"] / 1000,
                          paced_p99_us=values["paced"]["p99-us"]["median"],
                          background_MiBps=values["paced"]["background-MiB/s"]["median"],
                          peak_RSS_MiB=max(values[kind]["RSS-MiB"]["median"]
                                           for kind in ("duplex", "upload", "download", "paced")),
                          default_upload_MBps=baseline.get("upload", {}).get("MB/s", {}).get("median"),
                          default_download_MBps=baseline.get("download", {}).get("MB/s", {}).get("median"),
                          default_paced_p99_us=baseline_p99,
                          default_failed_samples=failed[self.default(cores)],
                          default_valid_samples=counts[self.default(cores)],
                          loss_free_qualification_pass=loss_free,
                          selected_failed_samples=failed[chosen],
                          fastest_measured_config=list(fastest),
                          fastest_measured_upload_MBps=summaries[fastest]["upload"]["MB/s"]["median"],
                          fastest_measured_download_MBps=summaries[fastest]["download"]["MB/s"]["median"],
                          fastest_measured_failed_samples=failed[fastest],
                          latency_gate_pass=(baseline_p99 is not None
                                             and values["paced"]["p99-us"]["median"] <= baseline_p99 * 1.10),
                          load_gate_pass=values["paced"]["background-MiB/s"]["median"] >= 57.6)
            recommendations.append(record)
            print("recommended", json.dumps(record, sort_keys=True), flush=True)
        result = dict(method="best measured finalist; coordinate search, not exhaustive global optimum",
                      scope="combined end-to-end physical CPU budget, one host",
                      manifest=json.loads((self.output / "manifest.json").read_text())
                      if (self.output / "manifest.json").exists() else None,
                      final_repeats=self.args.final_repeats, configurations=final_rows,
                      rejected_samples=self.failures, recommendations=recommendations)
        (self.output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
        with (self.output / "recommendations.csv").open("w") as output:
            writer = csv.DictWriter(output, fieldnames=list(recommendations[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(recommendations)
        profiles = self.output / "profiles"
        profiles.mkdir(exist_ok=True)
        for row in recommendations:
            suffix = ".env" if row["loss_free_qualification_pass"] else ".candidate.env"
            stale_suffix = ".candidate.env" if row["loss_free_qualification_pass"] else ".env"
            (profiles / f"cores-{row['cores']}{stale_suffix}").unlink(missing_ok=True)
            (profiles / f"cores-{row['cores']}{suffix}").write_text(
                "# Measured combined client/server/target CPU budget on one host.\n"
                "# Remeasure for isolated client/server deployment; set before runtime startup.\n"
                f"# Loss-free qualification passed: {row['loss_free_qualification_pass']}.\n"
                f"# Latency/load gates passed: {row['latency_gate_pass']}/{row['load_gate_pass']}.\n"
                f"GOMAXPROCS={row['cores']}\n"
                f"XRAY_HYSTERIA_CLIENT_WORKERS={row['client_workers']}\n"
                f"XRAY_HYSTERIA_SERVER_WORKERS={row['server_workers']}\n"
                f"XRAY_HYSTERIA_POOL_SIZE={row['pool']}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cores", type=lambda text: [int(v) for v in text.split(",")],
                        default=[1, 2, 4, 6, 8, 10, 12, 14, 16])
    parser.add_argument("--cpus", help="ordered physical-core CPU representatives")
    parser.add_argument("--search-repeats", type=int, default=2)
    parser.add_argument("--final-repeats", type=int, default=3)
    parser.add_argument("--finalists", type=Path,
                        help="qualify existing candidate settings with fresh measurements")
    parser.add_argument("--skip-screen", action="store_true",
                        help="qualify every supplied finalist without another streaming screen")
    args = parser.parse_args()
    if (not args.cores or min(args.cores) < 1 or len(set(args.cores)) != len(args.cores)
            or args.search_repeats < 1 or args.final_repeats < 1):
        parser.error("Core budgets must be distinct positive integers; repeats must be positive")
    if args.skip_screen and not args.finalists:
        parser.error("--skip-screen requires --finalists")
    matrix = Matrix(args)
    if args.finalists:
        finalists = json.loads(args.finalists.read_text())
        for cores in args.cores:
            configs = finalists.get(str(cores), [])
            if (not configs or any(len(config) != 3
                                   or any(type(value) is not int for value in config)
                                   or not 1 <= config[0] <= 16 or not 1 <= config[1] <= 16
                                   or not 1 <= config[2] <= 8 for config in configs)
                    or len({tuple(config) for config in configs}) != len(configs)
                    or list(matrix.default(cores)) not in configs):
                parser.error(f"Invalid finalist configurations for {cores} cores")
        (matrix.output / "finalists.json").write_text(json.dumps(finalists, indent=2) + "\n")
    else:
        finalists = matrix.search()
    if not args.skip_screen:
        finalists = matrix.screen(finalists)
    matrix.final(finalists)
    matrix.results()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--measure":
        pid = os.fork()
        if pid == 0:
            os.execvp(sys.argv[2], sys.argv[2:])
        _, status, usage = os.wait4(pid, 0)
        print(f"process_peak_rss_KiB={usage.ru_maxrss}", flush=True)
        sys.exit(os.waitstatus_to_exitcode(status))
    main()
