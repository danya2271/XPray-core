#!/usr/bin/env python3
"""Unit tests for the measurement parser and recommendation gates."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import types
import unittest

spec = importlib.util.spec_from_file_location(
    "scaling", Path(__file__).with_name("bench-scaling.py"))
scaling = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scaling)


class ScalingResultsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.matrix = scaling.Matrix.__new__(scaling.Matrix)
        self.matrix.args = types.SimpleNamespace(cores=[4], final_repeats=3)
        self.matrix.output = Path(self.directory.name)
        self.matrix.cpus = [0, 1, 2, 3]
        self.matrix.rows = []
        self.matrix.failures = []

    def sample(self, config, speed, cpu, p99=100, failed_kind=None):
        for kind in ("duplex", "upload", "download", "paced"):
            for repeat in range(1, 4):
                metrics = {"MB/s": speed, "process-cpu-ns/op": cpu,
                           "RSS-MiB": 64, "p99-us": p99,
                           "background-MiB/s": 64, "kernel-udp-drops": 0}
                row = dict(cores=4, client_workers=config[0],
                           server_workers=config[1], pool=config[2],
                           kind=kind, phase="final", repeat=repeat,
                           log=f"{config}-{kind}-{repeat}.log", metrics=metrics)
                if kind == failed_kind and repeat == 1:
                    metrics["kernel-udp-drops"] = 7
                    row["kernel_udp_drops"] = 7
                    self.matrix.failures.append(row)
                else:
                    self.matrix.rows.append(row)

    def result(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.matrix.results()
        return json.loads((self.matrix.output / "results.json").read_text())

    def test_drop_failure_retains_metrics_but_disqualifies_profile(self):
        self.sample((4, 4, 2), 500, 100, failed_kind="upload")
        self.sample((2, 2, 2), 450, 90)
        result = self.result()
        row = result["recommendations"][0]
        self.assertEqual(row["client_workers"], 2)
        self.assertEqual(row["default_upload_MBps"], 500)
        self.assertEqual(row["default_failed_samples"], 1)
        self.assertEqual(row["default_valid_samples"]["upload"], 2)
        self.assertEqual(row["fastest_measured_config"], [4, 4, 2])
        self.assertEqual(row["fastest_measured_failed_samples"], 1)

    def test_lower_cpu_wins_within_three_percent(self):
        self.sample((4, 4, 2), 500, 100)
        self.sample((2, 2, 2), 490, 90)
        self.assertEqual(self.result()["recommendations"][0]["client_workers"], 2)

    def test_latency_gate_rejects_faster_profile(self):
        self.sample((4, 4, 2), 500, 100)
        self.sample((2, 2, 2), 600, 90, p99=111)
        self.assertEqual(self.result()["recommendations"][0]["client_workers"], 4)

    def test_incomplete_matrix_cannot_publish(self):
        self.sample((4, 4, 2), 500, 100)
        self.matrix.rows.pop()
        with self.assertRaisesRegex(RuntimeError, "Incomplete finalist"):
            self.result()
        self.assertFalse((self.matrix.output / "results.json").exists())

    def test_no_clean_finalist_publishes_only_marked_candidate(self):
        self.sample((4, 4, 2), 500, 100, failed_kind="download")
        row = self.result()["recommendations"][0]
        self.assertFalse(row["loss_free_qualification_pass"])
        self.assertEqual(row["selected_failed_samples"], 1)
        self.assertFalse((self.matrix.output / "profiles/cores-4.env").exists())
        self.assertTrue((self.matrix.output / "profiles/cores-4.candidate.env").exists())

    def test_reclassification_removes_stale_qualified_profile(self):
        self.sample((4, 4, 2), 500, 100)
        self.result()
        row = self.matrix.rows.pop()
        row["metrics"]["kernel-udp-drops"] = 7
        row["kernel_udp_drops"] = 7
        self.matrix.failures.append(row)
        self.result()
        self.assertFalse((self.matrix.output / "profiles/cores-4.env").exists())
        self.assertTrue((self.matrix.output / "profiles/cores-4.candidate.env").exists())

    def test_parser_requires_success_and_preserves_drop_counter(self):
        log = self.matrix.output / "sample.log"
        log.write_text("BenchmarkExample 8192 42 ns/op 1 kernel-udp-drops\n"
                       "PASS\nprocess_peak_rss_KiB=65536\n")
        self.assertEqual(scaling.parse_log(log)["RSS-MiB"], 64)
        self.assertEqual(scaling.parse_log(log)["kernel-udp-drops"], 1)
        log.write_text(log.read_text().replace("PASS", "FAIL"))
        with self.assertRaisesRegex(RuntimeError, "did not pass"):
            scaling.parse_log(log)


if __name__ == "__main__":
    unittest.main()
