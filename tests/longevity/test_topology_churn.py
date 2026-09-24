"""Long-run topology replacement cardinality regressions."""

import unittest

from src import metrics
from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
)
from src.health_state import HEALTHY, HealthStateStore
from src.scheduler import PartitionScheduler
from tests.fakes.clock import FakeClock


def _partition_samples(collector):
    return [
        sample
        for family in collector.collect()
        for sample in family.samples
        if "partition" in sample.labels
    ]


class TopologyChurnTests(unittest.TestCase):
    def tearDown(self):
        metrics.reconcile_partition_metrics((), reset=True)

    def test_scale_and_recreation_return_all_state_to_latest_topology(self):
        clock = FakeClock()
        window = 5
        store = HealthStateStore(
            (), window_checks=window, minimum_checks=1, warmup_checks=0,
            monotonic_clock=clock.monotonic, wall_clock=clock.time,
        )
        store.record_schema_registry_result(success=True)
        scheduler = PartitionScheduler((), 15, monotonic_clock=clock.monotonic)

        # Repeat scale-up, scale-down, and recreated-topic generations.  The
        # final reduction to one partition catches retained high-water marks.
        for cycle in range(40):
            topologies = ((9, False), (3, False), (7, True), (1, True))
            for partition_count, recreated in topologies:
                expected = tuple(range(partition_count))
                store.replace_expected_partitions(
                    expected,
                    preserve_existing=not recreated,
                    scheduler_oldest_overdue=0,
                )
                if recreated:
                    scheduler.replace_partitions(expected)
                    metrics.reconcile_partition_metrics((), reset=True)
                    self.assertFalse(store.readiness_snapshot().ready)
                else:
                    scheduler.reconcile_partitions(expected)
                    metrics.reconcile_partition_metrics(expected)

                for partition in expected:
                    for attempt in range(window + 2):
                        store.record_partition_result(
                            partition, success=bool((cycle + attempt) % 2)
                        )
                    metrics.record_partition_check(
                        partition, "success", 0, last_success=clock.time()
                    )
                snapshot = store.snapshot()
                metrics.update_health_metrics(
                    snapshot, {partition: 0 for partition in expected}
                )

                self.assertEqual(partition_count, len(store._partitions))
                self.assertEqual(partition_count, len(scheduler._records))
                self.assertLessEqual(
                    sum(len(item.results) for item in store._partitions.values()),
                    partition_count * window,
                )
                self.assertEqual(HEALTHY, snapshot.scheduling_capacity.status)
                self.assertTrue(store.readiness_snapshot().ready)

                for collector in (
                    metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
                    metrics.PARTITION_CONSECUTIVE_FAILURES,
                    metrics.PARTITION_CHECKS_TOTAL,
                    metrics.PARTITION_CURRENT_STATE,
                ):
                    labels = {
                        sample.labels["partition"]
                        for sample in _partition_samples(collector)
                    }
                    self.assertLessEqual(len(labels), partition_count)
                    self.assertLessEqual(
                        len(_partition_samples(collector)), partition_count * 2
                    )

    def test_observer_mismatch_never_creates_a_synthetic_partition_metric(self):
        store = HealthStateStore((0, 1), minimum_checks=1, warmup_checks=0)
        store.publish_initialization_state(
            "RECONCILING_TOPIC",
            failure=FailureDescriptor(
                component=FailureComponent.TOPIC_ADMINISTRATION,
                phase=Phase.TOPIC_VERIFY,
                category=ErrorCategory.BROKER_SERVICE,
                recoverability=Recoverability.TRANSIENT,
                code="CANARY.TOPIC_MISMATCH",
                safe_summary=FailureSummary.OPERATION_FAILED,
            ),
            retry_attempts=1,
        )

        metrics.update_health_metrics(store.snapshot(), {0: 0, 1: 0})

        state_partitions = {
            sample.labels["partition"]
            for sample in _partition_samples(metrics.PARTITION_CURRENT_STATE)
        }
        self.assertEqual({"0", "1"}, state_partitions)
        for collector in (
            metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
            metrics.PARTITION_CONSECUTIVE_FAILURES,
            metrics.PARTITION_CHECKS_TOTAL,
            metrics.PARTITION_CURRENT_STATE,
        ):
            partitions = {
                sample.labels["partition"]
                for sample in _partition_samples(collector)
            }
            self.assertNotIn("kafka_startup_retry", partitions)
            self.assertLessEqual(len(partitions), 2)


if __name__ == "__main__":
    unittest.main()
