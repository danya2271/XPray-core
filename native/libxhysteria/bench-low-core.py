#!/usr/bin/env python3
"""Compare native interop harnesses under the same physical CPU budget.

Keep nonzero UDP-drop samples as overload evidence, not loss-free qualification.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

DRIVER = Path(__file__).resolve()
ENVIRONMENT_KEYS = {
    "GOMAXPROCS", "GOMEMLIMIT", "GOGC", "GODEBUG", "XRAY_NET_DIAG",
}

spec = importlib.util.spec_from_file_location(
    "scaling", DRIVER.with_name("bench-scaling.py"))
scaling = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scaling)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cores", default="1,2")
    parser.add_argument("--cpus", help="ordered physical-core representatives")
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()
    cores = [int(value) for value in args.cores.split(",")]
    available = scaling.physical_cpus()
    cpus = [int(value) for value in args.cpus.split(",")] if args.cpus else available
    if (args.repeats < 1 or not cores or min(cores) < 1 or max(cores) > 16
            or len(set(cores)) != len(cores) or len(set(cpus)) != len(cpus)
            or not set(cpus) <= set(available) or max(cores) > len(cpus)):
        parser.error("Require positive distinct budgets/repeats and allowed physical CPUs")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    binaries = {"before": args.before.resolve(), "after": args.after.resolve()}
    environment = {key: value for key, value in os.environ.items()
                   if key in ENVIRONMENT_KEYS
                   or key.startswith(("XRAY_HYSTERIA_", "XHYS_SCALING_"))}
    environment.update(XHYS_SCALING_POOL="2", XRAY_NET_DIAG="0")
    for key in ("XRAY_HYSTERIA_CLIENT_WORKERS", "XRAY_HYSTERIA_SERVER_WORKERS"):
        environment.pop(key, None)  # Each sample explicitly sets its core budget.
    manifest = dict(
        method="alternating fresh-process A/B; client/server/target share CPU budget",
        cores=cores, physical_cpus=cpus, repeats=args.repeats,
        workers="client/server workers equal CPU budget", pool=2, flows=32,
        iterations=scaling.SIZES,
        environment=environment,
        drivers={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in (DRIVER, DRIVER.with_name("bench-scaling.py"))},
        binaries={name: dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
                  for name, path in binaries.items()})
    manifest_path = output / "manifest.json"
    if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
        raise RuntimeError("Different binary/method manifest; choose a new output directory")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    rows = []
    for repeat in range(1, args.repeats + 1):
        budgets = cores if repeat % 2 else list(reversed(cores))
        revisions = ["before", "after"] if repeat % 2 else ["after", "before"]
        for budget in budgets:
            for revision in revisions:
                for kind in ("duplex", "upload", "download", "paced"):
                    name = f"n{budget}-r{repeat}-{revision}-{kind}"
                    log = output / (name + ".log")
                    env = os.environ.copy()
                    env.update(XRAY_HYSTERIA_CLIENT_WORKERS=str(budget),
                               XRAY_HYSTERIA_SERVER_WORKERS=str(budget),
                               XHYS_SCALING_POOL="2", XRAY_NET_DIAG="0")
                    command = [
                        sys.executable, str(DRIVER.with_name("bench-scaling.py")),
                        "--measure", "taskset", "-c", ",".join(map(str, cpus[:budget])),
                        str(binaries[revision]), "-test.run", "^$",
                        "-test.bench", f"^{scaling.BENCHMARKS[kind]}/native$",
                        "-test.benchtime", f"{scaling.SIZES[kind]}x",
                        "-test.cpu", str(budget), "-test.count", "1",
                        "-test.timeout", "120s"]
                    if not log.exists():
                        (output / (name + ".command.json")).write_text(json.dumps(dict(
                            argv=command, loadavg=os.getloadavg(),
                            env={**environment,
                                 "XRAY_HYSTERIA_CLIENT_WORKERS": str(budget),
                                 "XRAY_HYSTERIA_SERVER_WORKERS": str(budget)}),
                            indent=2) + "\n")
                        with log.open("w") as stream:
                            process = subprocess.Popen(
                                command, env=env, stdout=stream,
                                stderr=subprocess.STDOUT, start_new_session=True)
                            try:
                                code = process.wait(timeout=140)
                            except subprocess.TimeoutExpired:
                                os.killpg(process.pid, signal.SIGTERM)
                                try:
                                    process.wait(timeout=10)
                                except subprocess.TimeoutExpired:
                                    os.killpg(process.pid, signal.SIGKILL)
                                    process.wait()
                                raise RuntimeError(f"Timed out; log retained: {log}") from None
                            stream.write(f"\nbenchmark_driver_exit_code={code}\n")
                            if code:
                                raise RuntimeError(f"Benchmark exit {code}; log retained: {log}")
                    if "\nbenchmark_driver_exit_code=0\n" not in log.read_text():
                        raise RuntimeError(f"Incomplete or unsuccessful invocation: {log}")
                    metrics = scaling.parse_log(log)
                    if (metrics["iterations"] != scaling.SIZES[kind]
                            or metrics["tcp-flows"] != 32 or metrics["native-client-engines"] != 2
                            or metrics.get("kernel-udp-drops", -1) < 0):
                        raise RuntimeError(f"Workload/counter changed: {log}")
                    rows.append(dict(cores=budget, repeat=repeat, revision=revision,
                                     kind=kind, metrics=metrics, log=log.name))
            print(f"completed cores={budget} repeat={repeat}", flush=True)
    summaries = []
    for budget in cores:
        for kind in ("duplex", "upload", "download", "paced"):
            entry = dict(cores=budget, kind=kind)
            for revision in binaries:
                selected = [row["metrics"] for row in rows
                            if (row["cores"], row["kind"], row["revision"])
                            == (budget, kind, revision)]
                entry[revision] = scaling.summarize(selected)
                entry[revision + "_drop_samples"] = sum(row["kernel-udp-drops"] > 0
                                                       for row in selected)
            summaries.append(entry)
    (output / "results.json").write_text(json.dumps(
        dict(manifest=manifest, observations=rows, summary=summaries), indent=2) + "\n")
    print(output / "results.json")


if __name__ == "__main__":
    main()
