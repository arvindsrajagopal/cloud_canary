"""Readiness persistence and internal revocation regression tests."""

import unittest
from unittest.mock import patch

from src.health import (
    ReadinessStatus,
    _get_store_readiness_status,
    get_readiness_status,
)
from src.health_state import HealthStateStore
from tests.fakes.clock import FakeClock


def _ready_store(clock: FakeClock) -> HealthStateStore:
    store = HealthStateStore(
        (0,),
        warmup_checks=0,
        liveness_scheduler_max_staleness=10,
        monotonic_clock=clock.monotonic,
        wall_clock=clock.time,
    )
    store.record_scheduler_heartbeat()
    store.record_schema_registry_result(success=True)
    store.record_partition_result(0, success=True)
    return store


class ReadinessTransitionTests(unittest.TestCase):
    def test_external_dependency_outages_do_not_revoke_latch(self):
        clock = FakeClock()
        store = _ready_store(clock)

        store.record_partition_result(0, success=False, failure="broker_unavailable")
        store.record_schema_registry_result(
            success=False,
            deterministic_failure=True,
            failure="registry_unavailable",
        )

        status = _get_store_readiness_status(store)
        self.assertEqual(ReadinessStatus.READY, status.status)
        self.assertEqual(200, status.http_code)

    def test_recreation_and_worker_rebuild_revoke_while_active(self):
        for stage in ("topic_recreation", "worker_rebuild"):
            with self.subTest(stage=stage):
                clock = FakeClock()
                store = _ready_store(clock)
                store.begin_readiness_transition(stage)

                snapshot = store.readiness_snapshot()
                self.assertFalse(snapshot.ready)
                self.assertEqual((stage,), snapshot.blocking_reasons)
                self.assertEqual(
                    ReadinessStatus.NOT_READY,
                    _get_store_readiness_status(store).status,
                )

                store.end_readiness_transition()
                self.assertTrue(store.readiness_snapshot().ready)

    def test_recreated_topology_requires_fresh_partition_initialization(self):
        clock = FakeClock()
        store = _ready_store(clock)
        store.begin_readiness_transition("worker_rebuild")
        store.replace_expected_partitions((0, 1), preserve_existing=False)
        store.end_readiness_transition()

        snapshot = store.readiness_snapshot()
        self.assertFalse(snapshot.ready)
        self.assertEqual((0, 1), snapshot.incomplete_partitions)

        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=True)
        self.assertTrue(store.readiness_snapshot().ready)

    def test_new_partition_revokes_until_initialized(self):
        clock = FakeClock()
        store = _ready_store(clock)

        store.replace_expected_partitions((0, 1))
        self.assertFalse(store.readiness_snapshot().ready)
        self.assertEqual((1,), store.readiness_snapshot().incomplete_partitions)

        store.record_partition_result(1, success=True)
        self.assertTrue(store.readiness_snapshot().ready)

    def test_stale_scheduler_heartbeat_revokes_and_refresh_recovers(self):
        clock = FakeClock()
        store = _ready_store(clock)

        clock.advance(10.001)
        snapshot = store.readiness_snapshot()
        self.assertFalse(snapshot.ready)
        self.assertEqual(
            ("scheduler_heartbeat_stale",), snapshot.blocking_reasons
        )

        store.record_scheduler_heartbeat()
        self.assertTrue(store.readiness_snapshot().ready)

    def test_fatal_state_and_shutdown_revoke_readiness(self):
        clock = FakeClock()
        fatal_store = _ready_store(clock)
        fatal_store.record_fatal_internal("raw detail must not be exposed")
        fatal = _get_store_readiness_status(fatal_store)
        self.assertEqual(ReadinessStatus.NOT_READY, fatal.status)
        self.assertEqual(["fatal_internal"], fatal.checks["blocking_reasons"])
        self.assertNotIn("raw detail", fatal.message)

        shutdown_store = _ready_store(clock)
        shutdown_store.begin_shutdown()
        shutdown = _get_store_readiness_status(shutdown_store)
        self.assertEqual(ReadinessStatus.NOT_READY, shutdown.status)
        self.assertEqual(["shutdown"], shutdown.checks["blocking_reasons"])

    def test_runtime_shutdown_request_immediately_revokes_readiness(self):
        store = _ready_store(FakeClock())

        with (
            patch("src.health._health_state_store", store),
            patch("src.health._shutdown_requested", return_value=True),
        ):
            shutdown = get_readiness_status()

        self.assertTrue(store.readiness_snapshot().ready)
        self.assertEqual(ReadinessStatus.NOT_READY, shutdown.status)
        self.assertEqual(503, shutdown.http_code)
        self.assertEqual(["shutdown"], shutdown.checks["blocking_reasons"])

    def test_transition_diagnostics_are_fixed_and_bounded(self):
        store = _ready_store(FakeClock())
        with self.assertRaisesRegex(ValueError, "unknown readiness transition"):
            store.begin_readiness_transition("secret=" + "x" * 1000)

        snapshot = store.readiness_snapshot()
        self.assertTrue(snapshot.ready)
        self.assertEqual((), snapshot.blocking_reasons)

        bounded = HealthStateStore(range(5), max_diagnostic_components=2)
        bounded_snapshot = bounded.readiness_snapshot()
        self.assertEqual((0, 1), bounded_snapshot.incomplete_partitions)
        self.assertEqual(3, bounded_snapshot.truncated_partition_count)
        self.assertEqual(((0, 2), (1, 2)), bounded_snapshot.warmup_remaining)


if __name__ == "__main__":
    unittest.main()
