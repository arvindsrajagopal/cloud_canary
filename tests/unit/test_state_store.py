"""Concurrency regressions for immutable, bounded health snapshots."""

from dataclasses import FrozenInstanceError
from threading import Event, Thread
import unittest

from src.health_state import HealthStateStore


def _store(partitions=(0,)) -> HealthStateStore:
    return HealthStateStore(
        partitions,
        window_checks=3,
        minimum_checks=1,
        warmup_checks=0,
        max_diagnostic_components=2,
        kafka_check_interval=10,
        kafka_degraded_after=60,
        kafka_unhealthy_after=300,
        sr_check_interval=60,
        sr_degraded_after=120,
        sr_unhealthy_after=300,
    )


class HealthStateSnapshotTests(unittest.TestCase):
    def test_snapshot_is_frozen_detached_and_omits_histories(self):
        store = _store()
        store.record_partition_result(0, success=True)
        store.record_schema_registry_result(success=True)
        snapshot = store.snapshot()

        store.record_partition_result(0, success=False, failure="broker")
        store.record_schema_registry_result(success=False, failure="network")

        self.assertEqual("healthy", snapshot.status)
        self.assertEqual(1, snapshot.partitions[0].observation_count)
        self.assertEqual(1, snapshot.schema_registry.observation_count)
        self.assertFalse(hasattr(snapshot.partitions[0], "results"))
        with self.assertRaises(FrozenInstanceError):
            snapshot.status = "unhealthy"  # type: ignore[misc]

    def test_topology_and_corresponding_capacity_are_published_atomically(self):
        store = _store()
        start = Event()
        failures = []

        def publish() -> None:
            start.wait()
            for _ in range(500):
                store.replace_expected_partitions(
                    (0, 1), scheduler_oldest_overdue=20.0
                )
                store.replace_expected_partitions(
                    (0,), scheduler_oldest_overdue=1.0
                )

        def read() -> None:
            start.wait()
            for _ in range(1000):
                snapshot = store.snapshot()
                observed = (
                    len(snapshot.partitions),
                    snapshot.scheduling_capacity.staleness_seconds,
                )
                if observed not in {(1, 1.0), (2, 20.0)}:
                    failures.append(observed)

        # Establish one of the two valid pairs before concurrent readers run.
        store.replace_expected_partitions(
            (0,), scheduler_oldest_overdue=1.0
        )
        threads = [Thread(target=publish)] + [Thread(target=read) for _ in range(4)]
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join()

        self.assertEqual([], failures)

    def test_concurrent_kafka_and_schema_registry_updates_are_coherent(self):
        store = _store()
        start = Event()
        failures = []

        def update() -> None:
            start.wait()
            for index in range(500):
                store.record_partition_result(0, success=index % 2 == 0)
                store.record_schema_registry_result(success=index % 2 == 0)

        def read() -> None:
            start.wait()
            for _ in range(1000):
                snapshot = store.snapshot()
                if snapshot.partitions[0].observation_count > 3:
                    failures.append("unbounded kafka history")
                if snapshot.schema_registry.observation_count > 3:
                    failures.append("unbounded SR history")

        threads = [Thread(target=update), Thread(target=read), Thread(target=read)]
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join()

        self.assertEqual([], failures)

    def test_readiness_diagnostics_are_bounded(self):
        store = _store(range(10))

        snapshot = store.readiness_snapshot()

        self.assertEqual((0, 1), snapshot.incomplete_partitions)
        self.assertEqual(8, snapshot.truncated_partition_count)
        self.assertLessEqual(len(snapshot.warmup_remaining), 2)


if __name__ == "__main__":
    unittest.main()
