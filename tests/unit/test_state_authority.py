"""Regression tests for the authoritative endpoint state source."""

import unittest
from unittest.mock import patch

from src import health
from src.health import HealthStatus, LivenessStatus, ReadinessStatus
from src.health_state import HealthStateStore
from tests.fakes.clock import FakeClock


class StateAuthorityTests(unittest.TestCase):
    def test_endpoints_use_store_without_reading_prometheus_history(self):
        clock = FakeClock()
        store = HealthStateStore(
            (0,),
            warmup_checks=0,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )
        store.record_scheduler_heartbeat()
        store.record_schema_registry_result(success=True)
        store.record_partition_result(0, success=True)

        with (
            patch.object(health, "_health_state_store", store),
            patch.object(health, "_shutdown_requested", return_value=False),
            patch.object(
                health.REGISTRY,
                "collect",
                side_effect=AssertionError("endpoint read Prometheus history"),
            ),
        ):
            self.assertEqual(HealthStatus.HEALTHY, health.get_health_status().status)
            self.assertEqual(
                ReadinessStatus.READY, health.get_readiness_status().status
            )
            self.assertEqual(
                LivenessStatus.LIVE, health.get_liveness_status().status
            )

    def test_metrics_values_cannot_supply_store_topology(self):
        store = HealthStateStore((), warmup_checks=0)

        with (
            patch.object(health, "_health_state_store", store),
            patch.object(health, "_get_metric_value", return_value=100),
            patch.object(health, "_get_max_staleness", return_value=0),
        ):
            result = health.get_health_status()

        self.assertEqual(HealthStatus.UNHEALTHY, result.status)
        self.assertNotIn(
            "kafka:0",
            [component["component"] for component in result.checks["components"]],
        )


if __name__ == "__main__":
    unittest.main()
