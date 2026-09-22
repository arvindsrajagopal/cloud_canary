"""Regression tests for bounded, per-component health evaluation."""

import unittest

from src.config import validate_config
from src.health import _get_store_health_status
from src.health_state import DEGRADED, HEALTHY, UNHEALTHY, HealthStateStore
from tests.fakes.clock import FakeClock


def _store(clock: FakeClock, partitions=(0, 1), **overrides) -> HealthStateStore:
    settings = {
        "window_checks": 4,
        "minimum_checks": 4,
        "failure_threshold": 0.5,
        "monotonic_clock": clock.monotonic,
        "wall_clock": clock.time,
    }
    settings.update(overrides)
    return HealthStateStore(partitions, **settings)


def _partition(snapshot, partition: int):
    return next(
        component
        for component in snapshot.partitions
        if component.component == f"kafka:{partition}"
    )


def _valid_config(**app_overrides):
    app = {"topic": "canary"}
    app.update(app_overrides)
    return {
        "kafka": {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "test-user",
            "sasl.password": "test-password",
        },
        "schema_registry": {
            "url": "https://registry.invalid",
            "basic.auth.user.info": "test-user:test-password",
        },
        "app": app,
    }


class PartitionHealthTests(unittest.TestCase):
    def test_fresh_partition_cannot_conceal_stale_partition(self):
        clock = FakeClock()
        store = _store(clock)
        store.record_schema_registry_result(success=True)
        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=True)

        clock.advance(301)
        store.record_partition_result(0, success=True)
        snapshot = store.snapshot()

        self.assertEqual(HEALTHY, _partition(snapshot, 0).status)
        self.assertEqual(UNHEALTHY, _partition(snapshot, 1).status)
        self.assertEqual(UNHEALTHY, snapshot.status)

    def test_kafka_staleness_boundaries(self):
        for elapsed, expected in (
            (59.999, HEALTHY),
            (60, DEGRADED),
            (300, DEGRADED),
            (300.001, UNHEALTHY),
        ):
            with self.subTest(elapsed=elapsed):
                clock = FakeClock()
                store = _store(clock, partitions=(0,))
                store.record_partition_result(0, success=True)
                clock.advance(elapsed)
                self.assertEqual(expected, _partition(store.snapshot(), 0).status)

    def test_custom_kafka_staleness_thresholds_replace_defaults(self):
        clock = FakeClock()
        store = _store(
            clock,
            partitions=(0,),
            kafka_check_interval=5,
            kafka_degraded_after=20,
            kafka_unhealthy_after=40,
        )
        store.record_partition_result(0, success=True)

        clock.advance(20)
        self.assertEqual(DEGRADED, _partition(store.snapshot(), 0).status)
        clock.advance(20.001)
        self.assertEqual(UNHEALTHY, _partition(store.snapshot(), 0).status)

    def test_rolling_window_truncates_and_waits_for_minimum_samples(self):
        clock = FakeClock()
        store = _store(clock, partitions=(0,))
        store.record_partition_result(0, success=True)
        store.record_partition_result(0, success=False)
        store.record_partition_result(0, success=False)

        before_minimum = _partition(store.snapshot(), 0)
        self.assertEqual(3, before_minimum.observation_count)
        self.assertIsNone(before_minimum.failure_rate)
        self.assertEqual(HEALTHY, before_minimum.status)

        store.record_partition_result(0, success=True)
        at_threshold = _partition(store.snapshot(), 0)
        self.assertEqual(0.5, at_threshold.failure_rate)
        self.assertEqual(HEALTHY, at_threshold.status)

        store.record_partition_result(0, success=False)
        above_threshold = _partition(store.snapshot(), 0)
        self.assertEqual(0.75, above_threshold.failure_rate)
        self.assertEqual(DEGRADED, above_threshold.status)

        for _ in range(4):
            store.record_partition_result(0, success=True)
        truncated = _partition(store.snapshot(), 0)
        self.assertEqual(4, truncated.observation_count)
        self.assertEqual(0.0, truncated.failure_rate)
        self.assertEqual(HEALTHY, truncated.status)

    def test_failure_rate_is_evaluated_per_partition(self):
        clock = FakeClock()
        store = _store(clock)
        store.record_schema_registry_result(success=True)
        for result in (False, False, False, True):
            store.record_partition_result(0, success=result)
        for _ in range(20):
            store.record_partition_result(1, success=True)

        snapshot = store.snapshot()

        self.assertEqual(0.75, _partition(snapshot, 0).failure_rate)
        self.assertEqual(DEGRADED, _partition(snapshot, 0).status)
        self.assertEqual(0.0, _partition(snapshot, 1).failure_rate)
        self.assertEqual(DEGRADED, snapshot.status)

    def test_latest_transient_sr_failure_degrades_health_immediately(self):
        clock = FakeClock()
        store = _store(clock, partitions=())
        store.record_schema_registry_result(success=True)
        store.record_schema_registry_result(success=False, failure="network")

        snapshot = store.snapshot()

        self.assertEqual(DEGRADED, snapshot.schema_registry.status)
        self.assertEqual(2, snapshot.schema_registry.observation_count)
        self.assertIsNone(snapshot.schema_registry.failure_rate)
        self.assertEqual(DEGRADED, snapshot.status)

    def test_snapshot_is_immutable_and_does_not_expose_histories(self):
        clock = FakeClock()
        store = _store(clock, partitions=(0,))
        store.record_partition_result(0, success=True)
        snapshot = store.snapshot()

        with self.assertRaises(AttributeError):
            snapshot.status = UNHEALTHY
        self.assertFalse(hasattr(snapshot.partitions[0], "results"))

    def test_health_configuration_rejects_invalid_numeric_values(self):
        invalid_settings = (
            ("health.failure.window.checks", "not-an-integer"),
            ("health.failure.minimum.checks", "0"),
            ("health.failure.threshold", "1.01"),
            ("health.max.diagnostic.components", "0"),
            ("health.kafka.degraded.after.seconds", "nan"),
        )
        for key, value in invalid_settings:
            with self.subTest(key=key, value=value):
                with self.assertRaisesRegex(ValueError, key.replace(".", r"\.")):
                    validate_config(_valid_config(**{key: value}))

    def test_health_configuration_rejects_invalid_cross_field_values(self):
        invalid_configs = (
            {
                "health.failure.window.checks": "3",
                "health.failure.minimum.checks": "4",
            },
            {
                "check.interval.seconds": "15",
                "health.kafka.degraded.after.seconds": "15",
            },
            {
                "health.kafka.degraded.after.seconds": "60",
                "health.kafka.unhealthy.after.seconds": "60",
            },
            {
                "sr.check.interval.seconds": "60",
                "health.sr.degraded.after.seconds": "60",
            },
            {
                "health.sr.degraded.after.seconds": "120",
                "health.sr.unhealthy.after.seconds": "120",
            },
        )
        for overrides in invalid_configs:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    validate_config(_valid_config(**overrides))

    def test_health_diagnostics_are_ordered_and_bounded(self):
        clock = FakeClock()
        store = _store(
            clock,
            partitions=(0, 1, 2),
            max_diagnostic_components=2,
            kafka_check_interval=5,
            kafka_degraded_after=20,
            kafka_unhealthy_after=40,
        )
        store.record_schema_registry_result(success=True)
        store.record_partition_result(1, success=True)
        store.record_partition_result(2, success=True)
        clock.advance(20)

        result = _get_store_health_status(store)

        self.assertEqual(UNHEALTHY, result.status.value)
        self.assertEqual(
            ["kafka:0", "kafka:1"],
            [component["component"] for component in result.checks["components"]],
        )
        self.assertEqual(3, result.checks["affected_component_count"])
        self.assertEqual(2, result.checks["returned_component_count"])
        self.assertEqual(1, result.checks["truncated_component_count"])
        self.assertNotIn("kafka:2", result.message)
        self.assertIn("1 additional components omitted", result.message)


if __name__ == "__main__":
    unittest.main()
