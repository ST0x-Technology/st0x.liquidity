"""Regression tests for the deployed close-window budget."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_pre_close_cutoff import validate_config


class CutoffTests(unittest.TestCase):
    def test_accepts_budget_boundary(self):
        validate_config({"broker": {"extended_hours_close_flatten_window_secs": 900}})
        validate_config({"broker": {"extended_hours_close_flatten_window_secs": 300}})

    def test_rejects_unsafe_values(self):
        for value in [True, False, 0, -1, 901, 900.0, "900", None]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_config({"broker": {"extended_hours_close_flatten_window_secs": value}})

    def test_missing_field(self):
        with self.assertRaises(ValueError):
            validate_config({"broker": {}})
        with self.assertRaises(ValueError):
            validate_config({})

    def test_cli_checks_every_file_and_fails_closed(self):
        checker = Path(__file__).with_name("check_pre_close_cutoff.py")
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.toml"
            candidate.write_text("[broker]\nextended_hours_close_flatten_window_secs = 900\n")
            valid = subprocess.run([sys.executable, checker, candidate], capture_output=True)
            self.assertEqual(valid.returncode, 0, valid.stderr.decode())
            for content in ["[broker]\nextended_hours_close_flatten_window_secs = 901\n", "not valid toml"]:
                candidate.write_text(content)
                rejected = subprocess.run([sys.executable, checker, candidate], capture_output=True)
                self.assertNotEqual(rejected.returncode, 0)
            candidate.write_text("[broker]\nextended_hours_close_flatten_window_secs = 900\n")
            missing = Path(directory) / "missing.toml"
            rejected = subprocess.run([sys.executable, checker, candidate, missing], capture_output=True)
            self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
