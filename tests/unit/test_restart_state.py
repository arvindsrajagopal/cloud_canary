"""Regression tests for process-local health state after restart."""

import unittest

from src.health_state import UNHEALTHY, HealthStateStore
from tests.fakes.clock import FakeClock


class RestartStateTests(unittest.TestCase):
    def test_fresh_store_does_not_retain_process_local_state(self):
        clock = FakeClock()
        previous = HealthStateStore(
            (0, 1),
            warmup_checks=2,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )
        previous.record_schema_registry_result(success=True)
        for partition in (0, 1):
            previous.record_partition_result(partition, success=True)
            previous.record_partition_result(partition, success=True)
            previous.record_partition_result(partition, success=True)
        self.assertTrue(previous.readiness_snapshot().ready)

        restarted = HealthStateStore(
            (0, 1),
            warmup_checks=2,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )

        health = restarted.snapshot()
        readiness = restarted.readiness_snapshot()
        self.assertNotEqual(previous.topic_generation, restarted.topic_generation)
        self.assertFalse(readiness.ready)
        self.assertEqual((0, 1), readiness.incomplete_partitions)
        self.assertEqual(((0, 2), (1, 2)), readiness.warmup_remaining)
        self.assertFalse(readiness.schema_registry_validated)

        for component in health.partitions:
            self.assertEqual(0, component.observation_count)
            self.assertIsNone(component.staleness_seconds)
            self.assertIsNone(component.failure_rate)
        self.assertEqual(UNHEALTHY, health.schema_registry.status)
        self.assertEqual(0, health.schema_registry.observation_count)
        self.assertIsNone(health.schema_registry.staleness_seconds)
        self.assertIsNone(health.schema_registry.failure_rate)
        self.assertIsNone(health.schema_registry.latest_failure)


if __name__ == "__main__":
    unittest.main()
