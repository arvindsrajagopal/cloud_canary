"""Contract regressions for portable application and monitoring workflows."""

from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).parents[2]


class RuntimeWorkflowContractTests(TestCase):
    def setUp(self):
        self.readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.production_compose = (ROOT / "docker-compose.example.yml").read_text(
            encoding="utf-8"
        )
        self.dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    def test_obsolete_combined_launcher_is_removed(self):
        self.assertFalse((ROOT / "run.sh").exists())

    def test_prerequisite_checks_are_explicit_and_non_mutating(self):
        self.assertIn("docker compose version", self.readme)
        self.assertIn("docker info", self.readme)
        self.assertIn("failures are non-mutating", self.readme)

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
        compose = "docker compose -f monitoring/docker-compose.yml"
        self.assertIn(f"{compose} up -d", self.readme)
        self.assertIn(f"{compose} down", self.readme)


if __name__ == "__main__":
    import unittest

    unittest.main()
