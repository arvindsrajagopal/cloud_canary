"""Contract tests for stable, linearly scaling per-partition metrics."""

import unittest
from types import SimpleNamespace

from prometheus_client import REGISTRY, generate_latest

from src import metrics


def _samples(collector):
    return [sample for family in collector.collect() for sample in family.samples]


def _partition_families():
    """Discover every emitted family carrying a partition label."""
    return {
        family.name: family
        for family in REGISTRY.collect()
        if any("partition" in sample.labels for sample in family.samples)
    }


def _assert_no_generation_or_incarnation_labels(test_case):
    prohibited_fragments = ("generation", "incarnation")
    for collector, metric_names in REGISTRY._collector_to_names.items():
        if not any(name.startswith("canary_") for name in metric_names):
            continue
        for label in getattr(collector, "_labelnames", ()):
            test_case.assertFalse(
                any(fragment in label.lower() for fragment in prohibited_fragments),
                f"prohibited metric label {label!r}",
            )
    for family in REGISTRY.collect():
        if not family.name.startswith("canary_"):
            continue
        for sample in family.samples:
            for label in sample.labels:
                test_case.assertFalse(
                    any(fragment in label.lower() for fragment in prohibited_fragments),
                    f"prohibited label {label!r} on {sample.name}",
                )


class PartitionMetricsContractTests(unittest.TestCase):
    def tearDown(self):
        metrics.reconcile_partition_metrics((), reset=True)

    def test_exact_per_partition_label_sets(self):
        expected = {
            metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS: {"host", "partition"},
            metrics.PARTITION_CONSECUTIVE_FAILURES: {"host", "partition"},
            metrics.PARTITION_CHECKS_TOTAL: {"host", "partition", "result"},
            metrics.PARTITION_CURRENT_STATE: {"host", "partition", "state"},
        }
        for collector, labels in expected.items():
            self.assertEqual(labels, set(collector._labelnames))
            self.assertFalse(
                {"phase", "category", "recoverability", "generation"} & labels
            )

    def test_series_count_remains_linear_at_small_and_600_partition_sizes(self):
        expected_family_labels = {
            "canary_partition_last_success_timestamp_seconds": {
                "host", "partition"
            },
            "canary_partition_consecutive_failures": {"host", "partition"},
            "canary_partition_checks": {"host", "partition", "result"},
            "canary_partition_current_state": {"host", "partition", "state"},
        }
        counts = []
        for partition_count in (3, 600):
            metrics.reconcile_partition_metrics((), reset=True)
            partitions = tuple(
                SimpleNamespace(
                    component=f"kafka:{partition}",
                    status="healthy",
                    staleness_seconds=1.0,
                )
                for partition in range(partition_count)
            )
            for partition in range(partition_count):
                metrics.record_partition_check(partition, "success", 0, 100.0)
                metrics.record_partition_check(partition, "failure", 1)
            metrics.update_health_metrics(
                SimpleNamespace(partitions=partitions),
                {partition: 1 for partition in range(partition_count)},
            )

            partition_families = _partition_families()
            self.assertEqual(set(expected_family_labels), set(partition_families))
            for name, family in partition_families.items():
                expected_labels = expected_family_labels[name]
                for sample in family.samples:
                    self.assertEqual(expected_labels, set(sample.labels))
                    self.assertNotEqual("*", sample.labels["partition"])
                    self.assertFalse(
                        {"phase", "category", "recoverability"}
                        & set(sample.labels)
                    )

            samples = sum(
                len(family.samples) for family in partition_families.values()
            )
            counts.append(samples)
            self.assertLessEqual(samples, partition_count * 8)
            _assert_no_generation_or_incarnation_labels(self)

            exposition = generate_latest().decode("utf-8")
            self.assertNotIn('partition="*"', exposition)

        self.assertEqual(counts[1], counts[0] * 200)

    def test_state_changes_and_topology_changes_remove_obsolete_children(self):
        def snapshot(state0="healthy"):
            return SimpleNamespace(
                partitions=(
                    SimpleNamespace(
                        component="kafka:0", status=state0, staleness_seconds=1.0
                    ),
                    SimpleNamespace(
                        component="kafka:1", status="healthy", staleness_seconds=1.0
                    ),
                )
            )

        metrics.record_partition_check(0, "success", 0, 100.0)
        metrics.record_partition_check(1, "failure", 1)
        metrics.update_health_metrics(snapshot(), {0: 0, 1: 1})
        metrics.update_health_metrics(snapshot("degraded"), {0: 1, 1: 1})

        partition_zero_states = [
            sample.labels["state"]
            for sample in _samples(metrics.PARTITION_CURRENT_STATE)
            if sample.labels["partition"] == "0"
        ]
        self.assertEqual(["degraded"], partition_zero_states)

        metrics.reconcile_partition_metrics((0,))
        for collector in (
            metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
            metrics.PARTITION_CONSECUTIVE_FAILURES,
            metrics.PARTITION_CHECKS_TOTAL,
            metrics.PARTITION_CURRENT_STATE,
        ):
            self.assertFalse(
                any(sample.labels["partition"] == "1" for sample in _samples(collector))
            )

        metrics.reconcile_partition_metrics((0,), reset=True)
        for collector in (
            metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
            metrics.PARTITION_CONSECUTIVE_FAILURES,
            metrics.PARTITION_CHECKS_TOTAL,
            metrics.PARTITION_CURRENT_STATE,
        ):
            self.assertEqual([], _samples(collector))


if __name__ == "__main__":
    unittest.main()
