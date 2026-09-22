"""Runtime wiring regressions for authoritative liveness state."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.config import validate_config
from src.health_state import HealthStateStore
from src.main import (
    _publish_scheduler_observability,
    _publish_shutdown_state,
    run,
)


def _config(**app_overrides):
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


class LivenessRuntimeTests(unittest.TestCase):
    def test_scheduler_staleness_must_be_finite_and_exceed_heartbeat(self):
        for value in ("-1", "0", "1", "nan", "inf"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "liveness.scheduler.max.staleness.seconds"
            ):
                validate_config(
                    _config(
                        **{"liveness.scheduler.max.staleness.seconds": value}
                    )
                )

        validate_config(
            _config(**{"liveness.scheduler.max.staleness.seconds": "1.001"})
        )

    @patch("src.main.metrics.update_scheduler_metrics")
    def test_scheduler_observability_publishes_heartbeat(self, update_metrics):
        snapshot = SimpleNamespace(oldest_overdue_seconds=2.5)
        scheduler = Mock()
        scheduler.snapshot.return_value = snapshot
        health_store = Mock()

        _publish_scheduler_observability(scheduler, health_store)

        health_store.record_scheduler_heartbeat.assert_called_once_with()
        health_store.record_scheduler_capacity.assert_called_once_with(2.5)
        update_metrics.assert_called_once_with(snapshot)

    def test_shutdown_state_is_published_outside_signal_handler(self):
        health_store = Mock()

        with patch("src.main._shutdown_requested", return_value=True):
            _publish_shutdown_state(health_store)

        health_store.begin_shutdown.assert_called_once_with()

    def test_initial_reconciliation_fatal_is_published_before_exit(self):
        config = _config(
            **{"liveness.scheduler.max.staleness.seconds": "3.5"}
        )
        configured_store = []

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch(
                "src.main.HealthStateStore", wraps=HealthStateStore
            ) as health_state_factory,
            patch("src.main.AdminClient", return_value=Mock()),
            patch("src.main.ensure_topic"),
            patch("src.main.SchemaRegistryClient", return_value=object()),
            patch("src.main.create_producer", return_value=(Mock(), object())),
            patch(
                "src.main.sync_topic_partitions",
                side_effect=RuntimeError("reconciliation failed"),
            ),
            patch(
                "src.main.configure_health_state",
                side_effect=configured_store.append,
            ),
            self.assertRaisesRegex(RuntimeError, "reconciliation failed"),
        ):
            run()

        snapshot = configured_store[0].liveness_snapshot()
        self.assertEqual(
            3.5,
            health_state_factory.call_args.kwargs[
                "liveness_scheduler_max_staleness"
            ],
        )
        self.assertFalse(snapshot.live)
        self.assertEqual("topic_reconciliation", snapshot.fatal_internal)

    def test_topic_creation_fatal_is_published_before_exit(self):
        configured_store = []

        with (
            patch("src.main.load_config", return_value=_config()),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=Mock()),
            patch(
                "src.main.ensure_topic",
                side_effect=RuntimeError("topic creation failed"),
            ),
            patch(
                "src.main.configure_health_state",
                side_effect=configured_store.append,
            ),
            self.assertRaisesRegex(RuntimeError, "topic creation failed"),
        ):
            run()

        snapshot = configured_store[0].liveness_snapshot()
        self.assertFalse(snapshot.live)
        self.assertEqual("topic_reconciliation", snapshot.fatal_internal)


if __name__ == "__main__":
    unittest.main()
