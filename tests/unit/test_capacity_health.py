"""Scheduling-capacity health boundary regression tests."""

import unittest

from src.health import _get_store_health_status
from src.health_state import DEGRADED, HEALTHY, HealthStateStore
from src.scheduler import PartitionScheduler
from tests.fakes.clock import FakeClock


def _healthy_store(clock: FakeClock) -> HealthStateStore:
    store = HealthStateStore(
        (0,),
        minimum_checks=1,
        warmup_checks=0,
        kafka_check_interval=10,
        kafka_degraded_after=60,
        kafka_unhealthy_after=300,
        sr_check_interval=60,
        sr_degraded_after=120,
        sr_unhealthy_after=300,
        monotonic_clock=clock.monotonic,
        wall_clock=clock.time,
    )
    store.record_partition_result(0, success=True)
    store.record_schema_registry_result(success=True)
    return store


class CapacityHealthTests(unittest.TestCase):
    def test_degrades_at_one_complete_interval_and_clears_below_it(self):
        store = _healthy_store(FakeClock())

        store.record_scheduler_capacity(9.999)
        self.assertEqual(HEALTHY, store.snapshot().scheduling_capacity.status)
        store.record_scheduler_capacity(10)
        self.assertEqual(DEGRADED, store.snapshot().scheduling_capacity.status)
        store.record_scheduler_capacity(0)
        self.assertEqual(HEALTHY, store.snapshot().scheduling_capacity.status)

    def test_capacity_backlog_affects_overall_health_without_unhealthy_boundary(self):
        store = _healthy_store(FakeClock())

        store.record_scheduler_capacity(10_000)
        snapshot = store.snapshot()
        response = _get_store_health_status(store)

        self.assertEqual(DEGRADED, snapshot.status)
        self.assertEqual(DEGRADED, snapshot.scheduling_capacity.status)
        self.assertFalse(response.checks["scheduling_capacity_healthy"])
        self.assertEqual(503, response.http_code)

    def test_published_capacity_age_degrades_while_all_workers_remain_occupied(self):
        clock = FakeClock()
        store = _healthy_store(clock)
        scheduler = PartitionScheduler((0,), 10, monotonic_clock=clock.monotonic)
        self.assertEqual((0,), scheduler.acquire_due(1))

        clock.advance(9.999)
        store.record_scheduler_capacity(
            scheduler.snapshot().oldest_overdue_seconds
        )
        self.assertEqual(HEALTHY, store.snapshot().scheduling_capacity.status)
        clock.advance(0.001)
        store.record_scheduler_capacity(
            scheduler.snapshot().oldest_overdue_seconds
        )

        snapshot = store.snapshot()
        self.assertEqual(DEGRADED, snapshot.scheduling_capacity.status)
        self.assertEqual(10, snapshot.scheduling_capacity.staleness_seconds)


if __name__ == "__main__":
    unittest.main()
