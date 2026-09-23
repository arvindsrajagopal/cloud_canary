"""Contract regressions for the portable Python module entry point."""

import signal
import subprocess
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import call, patch

import src.main as main


ROOT = Path(__file__).parents[2]
README = ROOT / "README.md"
MISSING_CONFIG = ROOT / "tests" / "contract" / "does-not-exist.ini"


class RuntimeEntrypointContractTests(TestCase):
    def test_readme_documents_portable_module_command(self):
        readme = README.read_text(encoding="utf-8")

        self.assertIn("\npython -m src.main\n", readme)
        self.assertNotIn(".venv/bin/python", readme)

    def test_module_execution_reaches_application_and_preserves_failure_exit(self):
        completed = subprocess.run(
            [sys.executable, "-m", "src.main"],
            cwd=ROOT,
            env={"CANARY_CONFIG_FILE": str(MISSING_CONFIG)},
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        self.assertEqual(1, completed.returncode)
        self.assertIn("Startup operation failed", completed.stderr)

    def test_run_owns_signals_and_uses_production_daemon_boundary(self):
        with (
            patch.object(main.signal, "signal") as register_signal,
            patch.object(main, "_run_behind_daemon_boundary") as boundary,
        ):
            main.run()

        self.assertEqual(
            [
                call(signal.SIGINT, main._handle_signal),
                call(signal.SIGTERM, main._handle_signal),
            ],
            register_signal.call_args_list,
        )
        boundary.assert_called_once_with(main._run_lifecycle)


if __name__ == "__main__":
    import unittest

    unittest.main()
