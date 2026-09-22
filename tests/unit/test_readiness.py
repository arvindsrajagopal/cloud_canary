"""Per-partition post-warmup readiness regression tests."""

import unittest
from unittest.mock import patch

from src.health import ReadinessStatus, _get_store_readiness_status
from src.health_state import HealthStateStore
from src.main import _record_successful_partition_attempt
from tests.fakes.clock import FakeClock


def _store(clock: FakeClock, warmup_checks: int) -> HealthStateStore:
    store = HealthStateStore(
        (0, 1),
        warmup_checks=warmup_checks,
        monotonic_clock=clock.monotonic,
        wall_clock=clock.time,
    )
    return store


class ReadinessTests(unittest.TestCase):
    def test_initial_schema_registry_validation_is_required(self):
        clock = FakeClock()
        store = _store(clock, warmup_checks=0)
        for partition in (0, 1):
            store.record_partition_result(partition, success=True)

        not_ready = _get_store_readiness_status(store)
        self.assertEqual(ReadinessStatus.NOT_READY, not_ready.status)
        self.assertEqual(503, not_ready.http_code)
        self.assertFalse(not_ready.checks["schema_registry_validated"])
        self.assertEqual([], not_ready.checks["incomplete_partitions"])

        store.record_schema_registry_result(success=True)

        ready = _get_store_readiness_status(store)
        self.assertEqual(ReadinessStatus.READY, ready.status)
        self.assertEqual(200, ready.http_code)

    def test_waits_for_each_partitions_successful_post_warmup_check(self):
        clock = FakeClock()
        store = _store(clock, warmup_checks=2)
        store.record_schema_registry_result(success=True)
        for partition in (0, 1):
            store.record_partition_result(partition, success=True)
            store.record_partition_result(partition, success=True)

        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=False)
        not_ready = _get_store_readiness_status(store)

        self.assertEqual(ReadinessStatus.NOT_READY, not_ready.status)
        self.assertEqual([1], not_ready.checks["incomplete_partitions"])

        store.record_partition_result(1, success=True)
        ready = _get_store_readiness_status(store)
        self.assertEqual(ReadinessStatus.READY, ready.status)
        self.assertEqual(200, ready.http_code)

        store.record_partition_result(0, success=False)
        self.assertEqual(ReadinessStatus.READY, _get_store_readiness_status(store).status)

    def test_zero_warmup_makes_first_check_eligible(self):
        clock = FakeClock()
        store = _store(clock, warmup_checks=0)
        store.record_schema_registry_result(success=True)

        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=False)
        self.assertEqual(
            ReadinessStatus.NOT_READY, _get_store_readiness_status(store).status
        )

        store.record_partition_result(1, success=True)
        self.assertEqual(ReadinessStatus.READY, _get_store_readiness_status(store).status)

    def test_new_partition_revokes_readiness_until_initialized(self):
        clock = FakeClock()
        store = HealthStateStore(
            (0,),
            warmup_checks=0,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )
        store.record_schema_registry_result(success=True)
        store.record_partition_result(0, success=True)
        self.assertTrue(store.readiness_snapshot().ready)

        store.replace_expected_partitions((0, 1))

        self.assertFalse(store.readiness_snapshot().ready)
        self.assertEqual((1,), store.readiness_snapshot().incomplete_partitions)

    @patch("src.main.log")
    @patch("src.main.metrics.E2E_LATENCY")
    def test_new_partition_runtime_latency_uses_its_own_warmup(
        self, latency_metric, runtime_log
    ):
        clock = FakeClock()
        store = HealthStateStore(
            (0,),
            warmup_checks=2,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )
        consecutive_failures = {0: 0}

        for check_sequence in (1, 2, 3):
            _record_successful_partition_attempt(
                store,
                0,
                check_sequence,
                check_sequence * 10,
                consecutive_failures,
                1,
                100,
            )
        self.assertEqual(1, latency_metric.labels.return_value.observe.call_count)

        store.replace_expected_partitions((0, 1))
        consecutive_failures[1] = 0
        for check_sequence in (4, 5):
            _record_successful_partition_attempt(
                store,
                1,
                check_sequence,
                check_sequence * 10,
                consecutive_failures,
                2,
                100,
            )
        self.assertEqual(1, latency_metric.labels.return_value.observe.call_count)
        self.assertEqual(
            ["Warmup check succeeded (latency not recorded)"] * 2,
            [call.args[0] for call in runtime_log.info.call_args_list[-2:]],
        )

        _record_successful_partition_attempt(
            store, 1, 6, 60, consecutive_failures, 2, 100
        )
        self.assertEqual(2, latency_metric.labels.return_value.observe.call_count)
        latency_metric.labels.return_value.observe.assert_called_with(60)
        self.assertEqual("Check succeeded", runtime_log.info.call_args.args[0])

    @patch("src.main.log")
    @patch("src.main.metrics.E2E_LATENCY")
    def test_zero_warmup_records_first_runtime_latency(
        self, latency_metric, runtime_log
    ):
        clock = FakeClock()
        store = HealthStateStore(
            (0,),
            warmup_checks=0,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )

        _record_successful_partition_attempt(store, 0, 1, 12, {0: 0}, 1, 100)

        latency_metric.labels.return_value.observe.assert_called_once_with(12)
        self.assertEqual("Check succeeded", runtime_log.info.call_args.args[0])

if __name__ == "__main__":
    unittest.main()
