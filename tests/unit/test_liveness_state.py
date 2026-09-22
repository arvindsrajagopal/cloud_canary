"""Regression tests for authoritative process liveness state."""

from dataclasses import FrozenInstanceError
from threading import Thread
import unittest

from src.health_state import HealthStateStore


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class LivenessStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.monotonic = MutableClock(100.0)
        self.wall = MutableClock(1_000.0)
        self.store = HealthStateStore(
            (0,),
            liveness_scheduler_max_staleness=10.0,
            monotonic_clock=self.monotonic,
            wall_clock=self.wall,
        )
        self.store.record_scheduler_heartbeat()

    def test_no_scheduler_heartbeat_fails_closed(self):
        store = HealthStateStore(
            liveness_scheduler_max_staleness=10.0,
            monotonic_clock=self.monotonic,
            wall_clock=self.wall,
        )

        snapshot = store.liveness_snapshot()
        self.assertFalse(snapshot.live)
        self.assertIsNone(snapshot.scheduler_heartbeat_age_seconds)

    def test_heartbeat_age_uses_monotonic_clock_and_inclusive_boundary(self):
        self.wall.value = 50_000.0
        self.monotonic.value = 110.0

        snapshot = self.store.liveness_snapshot()
        self.assertTrue(snapshot.live)
        self.assertEqual(10.0, snapshot.scheduler_heartbeat_age_seconds)
        self.assertEqual(50_000.0, snapshot.generated_at)

        self.monotonic.value = 110.001
        self.assertFalse(self.store.liveness_snapshot().live)

        self.store.record_scheduler_heartbeat()
        refreshed = self.store.liveness_snapshot()
        self.assertTrue(refreshed.live)
        self.assertEqual(0.0, refreshed.scheduler_heartbeat_age_seconds)

    def test_shutdown_and_fatal_internal_state_each_revoke_liveness(self):
        self.store.begin_shutdown()
        shutdown = self.store.liveness_snapshot()
        self.assertFalse(shutdown.live)
        self.assertTrue(shutdown.shutdown_started)

        other = HealthStateStore(
            liveness_scheduler_max_staleness=10.0,
            monotonic_clock=self.monotonic,
            wall_clock=self.wall,
        )
        other.record_fatal_internal("invariant:" + "x" * 600)
        fatal = other.liveness_snapshot()
        self.assertFalse(fatal.live)
        self.assertEqual(512, len(fatal.fatal_internal or ""))

        other.record_fatal_internal("replacement")
        self.assertEqual(fatal.fatal_internal, other.liveness_snapshot().fatal_internal)

    def test_dependency_observations_cannot_affect_liveness(self):
        baseline = self.store.liveness_snapshot()

        self.store.record_partition_result(0, success=False, failure="kafka")
        self.store.record_schema_registry_result(
            success=False,
            deterministic_failure=True,
            failure="schema_registry",
        )
        self.store.record_scheduler_capacity(999.0)

        after = self.store.liveness_snapshot()
        self.assertEqual(baseline.live, after.live)
        self.assertEqual(
            baseline.scheduler_heartbeat_age_seconds,
            after.scheduler_heartbeat_age_seconds,
        )
        self.assertFalse(after.shutdown_started)
        self.assertIsNone(after.fatal_internal)

    def test_snapshot_is_immutable_and_detached_from_later_updates(self):
        snapshot = self.store.liveness_snapshot()
        with self.assertRaises(FrozenInstanceError):
            snapshot.live = False  # type: ignore[misc]

        self.store.begin_shutdown()
        self.assertTrue(snapshot.live)
        self.assertFalse(snapshot.shutdown_started)
        self.assertFalse(self.store.liveness_snapshot().live)

    def test_concurrent_fatal_publication_is_first_write_wins(self):
        classifications = [f"fatal-{index}" for index in range(20)]
        writers = [
            Thread(target=self.store.record_fatal_internal, args=(classification,))
            for classification in classifications
        ]

        for writer in writers:
            writer.start()
        for writer in writers:
            writer.join()

        snapshot = self.store.liveness_snapshot()
        self.assertFalse(snapshot.live)
        self.assertIn(snapshot.fatal_internal, classifications)

    def test_rejects_non_positive_maximum_staleness(self):
        for value in (0.0, -1.0):
            with self.subTest(value=value), self.assertRaises(ValueError):
                HealthStateStore(liveness_scheduler_max_staleness=value)


if __name__ == "__main__":
    unittest.main()
