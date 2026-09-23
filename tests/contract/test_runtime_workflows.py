"""Contract regressions for portable application and monitoring workflows."""

import subprocess
import tempfile
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).parents[2]


class RuntimeWorkflowContractTests(TestCase):
    def setUp(self):
        self.helper = (ROOT / "run.sh").read_text(encoding="utf-8")
        self.readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.production_compose = (ROOT / "docker-compose.example.yml").read_text(
            encoding="utf-8"
        )
        self.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    def run_with_fake_docker(self, body):
        with tempfile.TemporaryDirectory(dir=ROOT) as temporary_directory:
            temporary_path = Path(temporary_directory)
            calls_path = temporary_path / "calls"
            docker_path = temporary_path / "docker"
            docker_path.write_text(
                "#!/bin/bash\n"
                f"printf '%s\\n' \"$*\" >> {str(calls_path)!r}\n"
                f"{body}\n",
                encoding="utf-8",
            )
            docker_path.chmod(0o755)
            completed = subprocess.run(
                ["/bin/bash", str(ROOT / "run.sh"), "start"],
                cwd=ROOT,
                env={"PATH": str(temporary_path)},
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            calls = calls_path.read_text(encoding="utf-8").splitlines()
        return completed, calls

    def test_monitoring_helper_is_independent_and_does_not_manage_runtime(self):
        self.assertIn('start|stop) action="$1"', self.helper)
        self.assertIn('docker compose -f "$COMPOSE_FILE" up -d', self.helper)
        self.assertIn('docker compose -f "$COMPOSE_FILE" down', self.helper)
        self.assertNotIn('"$SCRIPT_DIR/.venv/bin/python"', self.helper)
        self.assertNotIn("colima start", self.helper)
        self.assertNotIn("docker start", self.helper)
        self.assertNotIn("brew install", self.helper)
        self.assertNotIn("apt install", self.helper)

    def test_missing_cli_reports_actionable_error_without_mutation(self):
        completed = subprocess.run(
            ["/bin/bash", str(ROOT / "run.sh"), "start"],
            cwd=ROOT,
            env={"PATH": ""},
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )

        self.assertEqual(1, completed.returncode)
        self.assertIn("Docker CLI is required", completed.stderr)
        self.assertIn("Install and start", completed.stderr)

    def test_missing_compose_reports_actionable_error_without_mutation(self):
        completed, calls = self.run_with_fake_docker("exit 1")

        self.assertEqual(1, completed.returncode)
        self.assertIn("Docker Compose V2 is required", completed.stderr)
        self.assertIn("Install the Compose V2 plugin", completed.stderr)
        self.assertEqual(["compose version"], calls)

    def test_unavailable_runtime_reports_actionable_error_without_mutation(self):
        completed, calls = self.run_with_fake_docker(
            'if [ "$1" = "compose" ]; then exit 0; fi\nexit 1'
        )

        self.assertEqual(1, completed.returncode)
        self.assertIn("Docker runtime is unavailable", completed.stderr)
        self.assertIn(
            "Start your container runtime outside this helper", completed.stderr
        )
        self.assertEqual(["compose version", "info"], calls)

    def test_production_example_uses_versioned_image_without_monitoring(self):
        self.assertIn("image: cloud-canary:1.2.3", self.production_compose)
        for example in (self.production_compose, self.readme, self.dockerfile):
            self.assertNotIn("cloud-canary:latest", example)
        self.assertNotIn("\n  prometheus:", self.production_compose.lower())
        self.assertNotIn("\n  grafana:", self.production_compose.lower())

    def test_documentation_separates_all_three_workflows(self):
        self.assertIn("### Python workflow", self.readme)
        self.assertIn("### Versioned OCI-image workflow", self.readme)
        self.assertIn("## Monitoring Stack (Prometheus + Grafana)", self.readme)
        self.assertIn("python -m src.main", self.readme)
        self.assertIn("./run.sh start", self.readme)
        self.assertIn("./run.sh stop", self.readme)


if __name__ == "__main__":
    import unittest

    unittest.main()
