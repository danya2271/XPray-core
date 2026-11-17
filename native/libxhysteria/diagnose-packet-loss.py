#!/usr/bin/env python3

import argparse
import json
import math
import os
from pathlib import Path
import time


def snapshot(pid):
    process = Path(f"/proc/{pid}")
    lines = (process / "net/snmp").read_text().splitlines()
    counters = {}
    for index in range(0, len(lines), 2):
        names = lines[index].split()
        values = lines[index + 1].split()
        if names[0] == "Udp:":
            counters = dict(zip(names[1:], map(int, values[1:])))
    sockets = set()
    for descriptor in (process / "fd").iterdir():
        try:
            target = descriptor.readlink().as_posix()
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            sockets.add(target[8:-1])
    drops = {}
    for table in ("udp", "udp6"):
        for line in (process / f"net/{table}").read_text().splitlines()[1:]:
            fields = line.split()
            if fields[9] in sockets:
                drops[fields[9]] = {
                    "local_hex": fields[1],
                    "rx_queue_bytes": int(fields[4].split(":")[1], 16),
                    "dropped_total": int(fields[-1]),
                }
    interfaces = {}
    for line in (process / "net/dev").read_text().splitlines()[2:]:
        interface, values = line.split(":", 1)
        fields = list(map(int, values.split()))
        interfaces[interface.strip()] = dict(zip(
            ("rx_packets", "rx_errors", "rx_dropped", "tx_packets", "tx_errors", "tx_dropped"),
            (fields[1], fields[2], fields[3], fields[9], fields[10], fields[11]),
        ))
    fields = (process / "stat").read_text().rsplit(")", 1)[1].split()
    return {
        "sampled": time.monotonic(),
        "start_ticks": int(fields[19]),
        "cpu_ticks": int(fields[11]) + int(fields[12]),
        "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
        "udp": counters,
        "sockets": drops,
        "interfaces": interfaces,
    }


def difference(before, after):
    return {name: value - before.get(name, value) for name, value in after.items()}


def report(pid, before, after):
    if before["start_ticks"] != after["start_ticks"]:
        raise RuntimeError("PID was reused; restart sampling with the new Xray PID")
    elapsed = after["sampled"] - before["sampled"]
    sockets = []
    for inode, current in after["sockets"].items():
        previous = before["sockets"].get(inode)
        sockets.append({
            "inode": inode,
            **current,
            "dropped_delta": None if previous is None else current["dropped_total"] - previous["dropped_total"],
        })
    return {
        "pid": pid,
        "sample_seconds": round(elapsed, 3),
        "cpu_percent_one_core": round(100 * (after["cpu_ticks"] - before["cpu_ticks"]) / os.sysconf("SC_CLK_TCK") / elapsed, 2),
        "rss_mib": round(after["rss_bytes"] / (1 << 20), 2),
        "namespace_udp_delta": difference(before["udp"], after["udp"]),
        "process_udp_sockets": sockets,
        "namespace_interface_delta": {
            name: difference(before["interfaces"].get(name, {}), values)
            for name, values in after["interfaces"].items()
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Read-only Linux UDP drop/CPU/RSS sampler; namespace counters include other processes.")
    parser.add_argument("--pid", type=int, required=True, help="current Xray process ID")
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--samples", type=int, default=6)
    arguments = parser.parse_args()
    if arguments.pid < 1 or not math.isfinite(arguments.interval) or arguments.interval <= 0 or arguments.samples < 1:
        parser.error("PID, interval and sample count must be positive")
    try:
        previous = snapshot(arguments.pid)
        for _ in range(arguments.samples):
            time.sleep(arguments.interval)
            current = snapshot(arguments.pid)
            print(json.dumps(report(arguments.pid, previous, current)), flush=True)
            previous = current
    except (OSError, RuntimeError) as error:
        parser.exit(1, f"sampling failed: {error}\n")


if __name__ == "__main__":
    main()
