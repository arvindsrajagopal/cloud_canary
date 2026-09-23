"""Bounded health-diagnostic and independent probe contracts."""

import unittest

from src import health
from src.health_state import DEGRADED, HEALTHY, UNHEALTHY, HealthStateStore
from tests.fakes.clock import FakeClock


def _store(clock, partitions, *, limit=4):
    return HealthStateStore(
        partitions,
        minimum_checks=1,
        failure_threshold=0.25,
        warmup_checks=0,
        max_diagnostic_components=limit,
        kafka_check_interval=10,
        kafka_degraded_after=60,
        kafka_unhealthy_after=300,
        sr_check_interval=60,
        sr_degraded_after=120,
        sr_unhealthy_after=300,
        monotonic_clock=clock.monotonic,
        wall_clock=clock.time,
    )


class HealthDiagnosticsContractTests(unittest.TestCase):
    def test_capacity_boundary_is_inclusive_and_never_directly_unhealthy(self):
        clock = FakeClock()
        store = _store(clock, (0,))
        store.record_partition_result(0, success=True)
        store.record_schema_registry_result(success=True)

        store.record_scheduler_capacity(9.999)
        self.assertEqual(HEALTHY, store.snapshot().scheduling_capacity.status)
        store.record_scheduler_capacity(10)
        self.assertEqual(DEGRADED, store.snapshot().scheduling_capacity.status)
        store.record_scheduler_capacity(100_000)
        self.assertEqual(DEGRADED, store.snapshot().scheduling_capacity.status)
        store.record_scheduler_capacity(0)
        self.assertEqual(HEALTHY, store.snapshot().scheduling_capacity.status)

    def test_unobserved_dependencies_fail_health_but_not_liveness(self):
        clock = FakeClock()
        store = _store(clock, (0,))
        store.record_scheduler_heartbeat()
        previous_store = health._health_state_store
        previous_shutdown = health._shutdown_requested
        health.configure_health_state(store, shutdown_requested=lambda: False)
        try:
            snapshot = store.snapshot()
            self.assertEqual(UNHEALTHY, snapshot.partitions[0].status)
            self.assertEqual(UNHEALTHY, snapshot.schema_registry.status)
            self.assertEqual(503, health.get_health_status().http_code)
            self.assertEqual(200, health.get_liveness_status().http_code)
            self.assertEqual(503, health.get_readiness_status().http_code)
        finally:
            health._health_state_store = previous_store
            health._shutdown_requested = previous_shutdown

    def test_diagnostics_are_bounded_sorted_and_counted(self):
        clock = FakeClock()
        store = _store(clock, range(5), limit=4)
        store.record_partition_result(1, success=True)
        clock.advance(101)
        store.record_partition_result(3, success=True)
        clock.advance(200)
        store.record_partition_result(2, success=True)
        store.record_partition_result(4, success=True)
        store.record_partition_result(4, success=False, failure="transient")

        response = health._get_store_health_status(store)
        checks = response.checks

        self.assertEqual(UNHEALTHY, response.status.value)
        self.assertEqual(5, checks["affected_component_count"])
        self.assertEqual(4, checks["returned_component_count"])
        self.assertEqual(1, checks["truncated_component_count"])
        self.assertEqual(
            ["kafka:0", "schema_registry", "kafka:1", "kafka:3"],
            [component["component"] for component in checks["components"]],
        )
        self.assertEqual(
            [UNHEALTHY, UNHEALTHY, UNHEALTHY, DEGRADED],
            [component["status"] for component in checks["components"]],
        )


if __name__ == "__main__":
    unittest.main()
