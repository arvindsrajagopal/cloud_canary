"""Contracts for static-role metrics, dashboard counts, and manager alerting."""

import json
import unittest
from pathlib import Path

from prometheus_client import generate_latest

from src.main import INSTANCE_ROLE, set_instance_role


ROOT = Path(__file__).parents[2]
DASHBOARD = ROOT / "monitoring" / "grafana" / "dashboards" / "cloud_canary.json"
ALERT_RULE = (
    ROOT
    / "monitoring"
    / "grafana"
    / "provisioning"
    / "alerting"
    / "cloud-canary.yml"
)
MONITORING_COMPOSE = ROOT / "monitoring" / "docker-compose.yml"


class RoleObservabilityContractTests(unittest.TestCase):
    def test_instance_exports_exactly_one_bounded_role_without_identity(self):
        for role in ("manage", "observe"):
            with self.subTest(role=role):
                set_instance_role(role)
                samples = [
                    sample
                    for sample in INSTANCE_ROLE.collect()[0].samples
                    if sample.name == "canary_instance_role"
                ]
                self.assertEqual(1, len(samples))
                self.assertEqual({"role": role}, samples[0].labels)
                self.assertEqual(1, samples[0].value)

                exposition = generate_latest().decode("utf-8")
                role_lines = [
                    line for line in exposition.splitlines()
                    if line.startswith("canary_instance_role{")
                ]
                self.assertEqual(1, len(role_lines))
                self.assertNotIn("host=", role_lines[0])
                self.assertNotIn("instance=", role_lines[0])

        with self.assertRaises(ValueError):
            set_instance_role("auto")

    def test_dashboard_role_counts_are_scoped_and_joined_to_up(self):
        dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        variables = {item["name"]: item for item in dashboard["templating"]["list"]}
        self.assertEqual(
            "label_values(up, job)", variables["deployment"]["query"]["query"]
        )

        panels = {panel.get("title"): panel for panel in dashboard["panels"]}
        for title in ("Running Managers", "Running Observers"):
            expression = panels[title]["targets"][0]["expr"]
            self.assertIn("canary_instance_role", expression)
            self.assertIn('job=~"$deployment"', expression)
            self.assertIn("up{", expression)
            self.assertIn("on(job, instance)", expression)

        total = panels["Running Canary Instances"]["targets"][0]["expr"]
        self.assertIn("canary_instance_role", total)
        self.assertIn("up{", total)
        unreachable = panels["Unreachable Canary Targets"]["targets"][0]["expr"]
        self.assertIn('up{job=~"$deployment"} == 0', unreachable)

    def test_manager_alert_covers_absent_zero_and_multiple_series(self):
        dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        panel = next(
            panel for panel in dashboard["panels"]
            if panel.get("title") == "Manager Count Alert"
        )
        expression = panel["targets"][0]["expr"]
        self.assertIn('role="manage"', expression)
        self.assertIn('job="cloud-canary"', expression)
        self.assertIn("up{", expression)
        self.assertIn("or vector(0)", expression)
        self.assertIn("!= bool 1", expression)
        self.assertNotIn("alert", panel)

        rule = ALERT_RULE.read_text(encoding="utf-8")
        for required in (
            "apiVersion: 1",
            "uid: cloud-canary-manager-count",
            "title: Cloud Canary manager count is not one",
            "condition: C",
            "datasourceUid: prometheus",
            'role="manage"',
            'job="cloud-canary"',
            "up{",
            "or vector(0)",
            "!= bool 1",
            "type: threshold",
            "noDataState: Alerting",
            "execErrState: Alerting",
            "for: 1m",
            "isPaused: false",
        ):
            self.assertIn(required, rule)

        compose = MONITORING_COMPOSE.read_text(encoding="utf-8")
        self.assertIn(
            "./grafana/provisioning:/etc/grafana/provisioning:ro", compose
        )

    def test_observer_scaling_has_no_expected_count_setting(self):
        configuration_sources = (
            ROOT / "src" / "config.py",
            ROOT / "config" / "config.ini.template",
        )
        for path in configuration_sources:
            normalized = path.read_text(encoding="utf-8").lower().replace("_", ".")
            self.assertNotIn("expected.observer", normalized, str(path))


if __name__ == "__main__":
    unittest.main()
