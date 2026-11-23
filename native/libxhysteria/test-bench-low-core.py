#!/usr/bin/env python3
"""Checks for A/B manifests, resumption, overload evidence and process cleanup."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "low_core", Path(__file__).with_name("bench-low-core.py"))
low_core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(low_core)


class LowCoreDriverTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.before, self.after = self.root / "before", self.root / "after"
        self.before.write_bytes(b"baseline")
        self.after.write_bytes(b"candidate")
        self.output = self.root / "results"
        self.argv = [
            "bench-low-core.py", "--before", str(self.before),
            "--after", str(self.after), "--output", str(self.output),
            "--cpus", "4,5", "--cores", "1,2", "--repeats", "2",
        ]
        self.commands = []
        self.engines = 2
        self.exit_code = 0

    def spawn(self, command, **kwargs):
        self.commands.append(command)
        count = int(command[command.index("-test.benchtime") + 1][:-1])
        drops = 7 if str(self.after) in command else 0
        kwargs["stdout"].write(
            f"BenchmarkExample {count} 42 ns/op {drops} kernel-udp-drops "
            f"{self.engines} native-client-engines 32 tcp-flows "
            "100 process-cpu-ns/op\nPASS\nprocess_peak_rss_KiB=65536\n")
        return mock.Mock(wait=mock.Mock(return_value=self.exit_code))

    def run_driver(self, spawn=None):
        with (mock.patch.object(sys, "argv", self.argv),
              mock.patch.object(low_core.scaling, "physical_cpus", return_value=[4, 5]),
              mock.patch.object(low_core.subprocess, "Popen", side_effect=spawn or self.spawn),
              contextlib.redirect_stdout(io.StringIO()),
              contextlib.redirect_stderr(io.StringIO())):
            low_core.main()

    def test_alternation_drops_and_resumption(self):
        self.run_driver()
        result = json.loads((self.output / "results.json").read_text())
        self.assertEqual(len(result["observations"]), 32)
        self.assertEqual(len(self.commands), 32)
        for summary in result["summary"]:
            self.assertEqual(summary["before_drop_samples"], 0)
            self.assertEqual(summary["after_drop_samples"], 2)
        order = [(command[command.index("-test.cpu") + 1],
                  "after" if str(self.after) in command else "before")
                 for command in self.commands[::4]]
        self.assertEqual(order, [
            ("1", "before"), ("1", "after"), ("2", "before"), ("2", "after"),
            ("2", "after"), ("2", "before"), ("1", "after"), ("1", "before"),
        ])
        command = json.loads(next(self.output.glob("*.command.json")).read_text())
        self.assertEqual(command["env"]["XHYS_SCALING_POOL"], "2")
        self.assertIn("loadavg", command)
        self.assertEqual(set(result["manifest"]["drivers"]),
                         {"bench-low-core.py", "bench-scaling.py"})
        self.run_driver()
        self.assertEqual(len(self.commands), 32)

    def test_changed_binary_cannot_resume(self):
        self.run_driver()
        self.after.write_bytes(b"changed candidate")
        with self.assertRaisesRegex(RuntimeError, "Different binary/method manifest"):
            self.run_driver()
        self.assertEqual(len(self.commands), 32)

    def test_changed_environment_cannot_resume(self):
        with mock.patch.dict(low_core.os.environ, {"GOGC": "100"}):
            self.run_driver()
        with (mock.patch.dict(low_core.os.environ, {"GOGC": "50"}),
              self.assertRaisesRegex(RuntimeError, "Different binary/method manifest")):
            self.run_driver()

    def test_invalid_budget_is_rejected(self):
        for value in ("0", "17", "1,1", "3"):
            with self.subTest(value=value):
                self.argv[self.argv.index("--cores") + 1] = value
                with self.assertRaises(SystemExit):
                    self.run_driver()
        self.assertFalse(self.output.exists())

    def test_unpopulated_pool_stops_results(self):
        self.engines = 1
        with self.assertRaisesRegex(RuntimeError, "Workload/counter changed"):
            self.run_driver()
        self.assertFalse((self.output / "results.json").exists())
        self.assertEqual(len(list(self.output.glob("*.log"))), 1)

    def test_failed_exit_preserves_log_without_results(self):
        self.exit_code = 1
        with self.assertRaisesRegex(RuntimeError, "Benchmark exit 1"):
            self.run_driver()
        self.assertFalse((self.output / "results.json").exists())
        self.assertEqual(len(list(self.output.glob("*.command.json"))), 1)
        with self.assertRaisesRegex(RuntimeError, "Incomplete or unsuccessful invocation"):
            self.run_driver()
        self.assertEqual(len(self.commands), 1)

    def test_timeout_terminates_then_kills_whole_group(self):
        process = mock.Mock(pid=12345)
        process.wait.side_effect = [
            subprocess.TimeoutExpired("benchmark", 140),
            subprocess.TimeoutExpired("benchmark", 10), -9,
        ]
        with (mock.patch.object(low_core.os, "killpg") as killpg,
              self.assertRaisesRegex(RuntimeError, "Timed out")):
            self.run_driver(spawn=lambda *args, **kwargs: process)
        self.assertEqual(killpg.call_args_list, [
            mock.call(12345, signal.SIGTERM), mock.call(12345, signal.SIGKILL),
        ])
        self.assertFalse((self.output / "results.json").exists())


if __name__ == "__main__":
    unittest.main()
