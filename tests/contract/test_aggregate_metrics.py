"""Contract tests for bounded aggregate Prometheus metrics."""

import unittest
from types import SimpleNamespace

from prometheus_client import generate_latest

from src import metrics


def _labels(collector):
    return set(collector._labelnames)


class AggregateMetricsContractTests(unittest.TestCase):
    def test_aggregate_metric_label_sets_omit_partition(self):
        expected = {
            metrics.CHECKS_TOTAL: {"host", "result"},
            metrics.FAILURES_TOTAL: {
                "host", "phase", "category", "recoverability"
            },
            metrics.E2E_LATENCY: {"host"},
            metrics.PARTITIONS_BY_STATE: {"host", "state"},
            metrics.WORST_PARTITION_STALENESS_SECONDS: {"host"},
            metrics.MAX_CONSECUTIVE_FAILURES: {"host"},
            metrics.OLDEST_PARTITION_OVERDUE_SECONDS: {"host"},
        }

        for collector, expected_labels in expected.items():
            self.assertEqual(expected_labels, _labels(collector))
            self.assertNotIn("partition", _labels(collector))

    def test_failure_labels_are_bounded_and_legacy_broker_is_normalized(self):
        self.assertEqual(
            "BROKER_SERVICE",
            metrics.bounded_label(
                "BROKER",
                metrics.FAILURE_CATEGORY_VALUES,
                aliases={"BROKER": "BROKER_SERVICE"},
            ),
        )
        self.assertEqual(
            "UNKNOWN",
            metrics.bounded_label(
                "arbitrary exception text", metrics.FAILURE_CATEGORY_VALUES
            ),
        )
        self.assertEqual(
            "UNKNOWN",
            metrics.bounded_label("arbitrary phase", metrics.FAILURE_PHASE_VALUES),
        )

    def test_health_snapshot_exports_only_bounded_state_values(self):
        snapshot = SimpleNamespace(
            partitions=(
                SimpleNamespace(status="healthy", staleness_seconds=2.0),
                SimpleNamespace(status="degraded", staleness_seconds=7.5),
                SimpleNamespace(status="unhealthy", staleness_seconds=None),
            )
        )

        metrics.update_health_metrics(snapshot, {0: 0, 1: 3, 2: 1})
        exposition = generate_latest().decode("utf-8")

        state_lines = [
            line for line in exposition.splitlines()
            if line.startswith("canary_partitions_by_state{")
        ]
        self.assertEqual(3, len(state_lines))
        for state in metrics.STATE_VALUES:
            self.assertTrue(
                any(
                    f'state="{state}"' in line and line.endswith(" 1.0")
                    for line in state_lines
                )
            )
        self.assertTrue(all("partition=" not in line for line in state_lines))
        with self.assertRaises(ValueError):
            metrics.bounded_label("unexpected", metrics.STATE_VALUES)


if __name__ == "__main__":
    unittest.main()
