"""Unit tests for deterministic quality-gate checks."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import quality_gate


class QualityGateUnitTests(unittest.TestCase):
    def _temporary_file(self, name: str, content: str):
        runtime_directory = quality_gate.ROOT / ".ralph-state"
        runtime_directory.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=str(runtime_directory))
        path = Path(temporary.name) / name
        path.write_text(content, encoding="utf-8")
        self.addCleanup(temporary.cleanup)
        return path

    def test_python_parser_rejects_invalid_source(self):
        path = self._temporary_file("invalid.py", "def broken(:\n")
        with self.assertRaises(SyntaxError):
            quality_gate._check_python([path])

    def test_json_parser_rejects_invalid_document(self):
        path = self._temporary_file("invalid.json", "{not-json}\n")
        with self.assertRaises(ValueError):
            quality_gate._check_json([path])

    def test_whitespace_check_rejects_trailing_space(self):
        path = self._temporary_file("invalid.md", "bad \n")
        with self.assertRaisesRegex(RuntimeError, "trailing whitespace"):
            quality_gate._check_whitespace([path])

    def test_whitespace_check_requires_final_newline(self):
        path = self._temporary_file("invalid.md", "missing newline")
        with self.assertRaisesRegex(RuntimeError, "missing final newline"):
            quality_gate._check_whitespace([path])

    def test_repository_inventory_excludes_ignored_runtime_state(self):
        paths = [path.relative_to(quality_gate.ROOT).as_posix() for path in quality_gate._repository_files()]
        self.assertFalse(any(path.startswith(".ralph-state/") for path in paths))
        self.assertNotIn("config/config.ini", paths)

    def test_secret_bearing_paths_are_prohibited(self):
        for raw in (
            ".env",
            ".env.production",
            "secrets/token.txt",
            "tls/private.key",
            "tls/private.pem",
        ):
            self.assertTrue(quality_gate._is_prohibited_path(Path(raw)), raw)
        self.assertFalse(quality_gate._is_prohibited_path(Path("src/config.py")))


if __name__ == "__main__":
    unittest.main()
