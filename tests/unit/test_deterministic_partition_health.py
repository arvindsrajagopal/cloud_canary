"""Partition-local deterministic health transition regressions."""

from dataclasses import FrozenInstanceError
import unittest

from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
)
from src.health_state import HealthStateStore
from tests.fakes.clock import FakeClock


def _failure() -> FailureDescriptor:
    return FailureDescriptor(
        component=FailureComponent.KAFKA_PARTITION,
        phase=Phase.CONSUME,
        category=ErrorCategory.AUTHORIZATION,
        recoverability=Recoverability.DETERMINISTIC,
        code="TOPIC_AUTHORIZATION_FAILED",
        safe_summary=FailureSummary.OPERATION_FAILED,
    )


def _store() -> HealthStateStore:
    clock = FakeClock()
    store = HealthStateStore(
        (0, 1),
        minimum_checks=1,
        warmup_checks=0,
        monotonic_clock=clock.monotonic,
        wall_clock=clock.time,
    )
    store.record_partition_result(0, success=True)
    store.record_partition_result(1, success=True)
    return store


class DeterministicPartitionHealthTests(unittest.TestCase):
    def test_failure_is_immediate_and_isolated_to_the_expected_partition(self):
        store = _store()

        accepted = store.mark_partition_deterministic_failure(
            1, failure=_failure(), generation=store.topic_generation
        )
        snapshot = store.snapshot()

        self.assertTrue(accepted)
        self.assertEqual(
            ["healthy", "unhealthy"],
            [component.status for component in snapshot.partitions],
        )
        self.assertIsNone(snapshot.partitions[0].latest_failure)
        self.assertIn(
            "deterministic:CONSUME:AUTHORIZATION",
            snapshot.partitions[1].latest_failure,
        )
        self.assertLessEqual(len(snapshot.partitions[1].latest_failure), 512)
        with self.assertRaises(ValueError):
            store.mark_partition_deterministic_failure(
                2, failure=_failure(), generation=store.topic_generation
            )
        with self.assertRaises(TypeError):
            store.mark_partition_deterministic_failure(
                0,
                failure=RuntimeError("raw-secret"),  # type: ignore[arg-type]
                generation=store.topic_generation,
            )

    def test_stale_topic_generation_is_rejected_without_mutation(self):
        store = _store()
        stale_generation = store.topic_generation
        store.replace_expected_partitions((0, 1), preserve_existing=False)
        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=True)

        accepted = store.mark_partition_deterministic_failure(
            0, failure=_failure(), generation=stale_generation
        )

        self.assertIsNone(accepted)
        self.assertEqual(
            ["healthy", "healthy"],
            [component.status for component in store.snapshot().partitions],
        )

    def test_successful_partition_result_clears_immediate_failure(self):
        store = _store()
        generation = store.topic_generation
        store.mark_partition_deterministic_failure(
            0, failure=_failure(), generation=generation
        )
        store.record_partition_result(
            0, success=False, failure="transient", generation=generation
        )
        self.assertEqual("unhealthy", store.snapshot().partitions[0].status)

        store.record_partition_result(0, success=True, generation=generation)

        recovered = store.snapshot().partitions[0]
        self.assertEqual("healthy", recovered.status)
        self.assertIsNone(recovered.latest_failure)

    def test_snapshot_remains_immutable_and_detached_after_recovery(self):
        store = _store()
        store.mark_partition_deterministic_failure(
            0, failure=_failure(), generation=store.topic_generation
        )
        failed_snapshot = store.snapshot()

        store.record_partition_result(0, success=True)

        self.assertEqual("unhealthy", failed_snapshot.partitions[0].status)
        self.assertIsNotNone(failed_snapshot.partitions[0].latest_failure)
        with self.assertRaises(FrozenInstanceError):
            failed_snapshot.partitions[0].status = "healthy"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
