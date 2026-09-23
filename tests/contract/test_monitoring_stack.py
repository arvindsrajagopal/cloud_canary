"""Contract regressions for the development monitoring boundary."""

from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).parents[2]


class MonitoringStackContractTests(TestCase):
    def setUp(self):
        self.compose = (ROOT / "monitoring" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.readme = (ROOT / "README.md").read_text(encoding="utf-8")

    def test_published_monitoring_ports_bind_to_loopback(self):
        self.assertIn('"127.0.0.1:9090:9090"', self.compose)
        self.assertIn('"127.0.0.1:3000:3000"', self.compose)
        self.assertNotIn('- "9090:9090"', self.compose)
        self.assertNotIn('- "3000:3000"', self.compose)

    def test_prometheus_administrative_api_is_not_enabled(self):
        self.assertNotIn("--web.enable-admin-api", self.compose)
        self.assertNotIn("--web.enable-lifecycle", self.compose)

    def test_bundled_stack_and_credentials_are_development_only(self):
        self.assertIn("for local development and testing", self.readme)
        self.assertIn("unsuitable for production", self.readme)
        self.assertIn("credential is development-only", self.readme)
        self.assertIn("not production-safe", self.readme)

    def test_production_monitoring_is_external_and_pull_only(self):
        self.assertIn(
            "externally managed, secure\nPrometheus-compatible scraper and "
            "visualization system",
            self.readme,
        )
        self.assertIn("does not accept or require\na Prometheus server URL", self.readme)
        self.assertIn("does not push metrics", self.readme)
        self.assertIn("retention, high availability, alerting, access control", self.readme)
        self.assertIn("authentication, network isolation, audit requirements", self.readme)
        self.assertIn("dashboard security", self.readme)


if __name__ == "__main__":
    import unittest

    unittest.main()
