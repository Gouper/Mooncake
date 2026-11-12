#!/usr/bin/env python3
# Copyright 2026 KVCache.AI
# SPDX-License-Identifier: Apache-2.0
"""Check benchmark exit status without the optional NPU Python dependencies."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / "benchmark/nds/benchmark_nds.py"
spec = importlib.util.spec_from_file_location("benchmark_nds", SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
with patch.dict(
    sys.modules,
    {name: ModuleType(name) for name in ("numpy", "tent", "torch", "torch_npu")},
):
    spec.loader.exec_module(benchmark)


class BenchmarkResultTest(unittest.TestCase):
    def check_result(self, runner, expected_status):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            args = SimpleNamespace(json=output)
            with (
                patch.object(benchmark, "arguments", return_value=args),
                patch.object(benchmark, "mooncake_git_sha", return_value="test"),
                patch.object(benchmark, "run", side_effect=runner),
                patch.object(benchmark.os, "umask"),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(benchmark.main(), expected_status)
            return json.loads(output.read_text())

    def test_success_requires_run_to_return(self):
        result = self.check_result(lambda args, result: None, 0)
        self.assertTrue(result["success"])
        self.assertNotIn("error", result)

    def test_failure_records_error_and_returns_nonzero(self):
        def fail(args, result):
            raise RuntimeError("transfer failed")

        result = self.check_result(fail, 1)
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "RuntimeError: transfer failed")

    def test_cleanup_failure_overrides_early_success(self):
        def fail_cleanup(args, result):
            result["success"] = True
            raise RuntimeError("unregister failed")

        result = self.check_result(fail_cleanup, 1)
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "RuntimeError: unregister failed")


if __name__ == "__main__":
    unittest.main()
