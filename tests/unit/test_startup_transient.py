"""Regressions for serial transient startup-stage retries."""

import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from src import main
from src.error_classifier import FailureComponent, Phase
from src.health_state import HealthStateStore


def _classify_kafka(error):
    return main._startup_failure(
        error,
        phase=Phase.METADATA_FETCH,
        component=FailureComponent.KAFKA_PARTITION,
    )


def _wrapped_transport_error():
    error = RuntimeError("dependency detail")
    error.__cause__ = KafkaException(KafkaError(KafkaError._TRANSPORT))
    return error


class StartupTransientRetryTests(unittest.TestCase):
    def test_retries_only_current_kafka_stage_before_downstream_work(self):
        store = HealthStateStore()
        operation = Mock(
            side_effect=(KafkaException(KafkaError(KafkaError._TRANSPORT)),
                         KafkaException(KafkaError(KafkaError._TIMED_OUT)),
                         "connected")
        )
        downstream = Mock()
        waiting_snapshots = []

        def wait_for_retry():
            waiting_snapshots.append((store.liveness_snapshot(),
                                      store.readiness_snapshot(),
                                      store.snapshot()))
            downstream.assert_not_called()

        with patch.object(main, "_shutdown_requested", return_value=False):
            result = main._retry_transient_startup_stage(
                operation,
                _classify_kafka,
                store,
                connecting_state="CONNECTING_KAFKA",
                waiting_state="WAITING_FOR_KAFKA",
                retry_wait=wait_for_retry,
            )
            downstream()

        self.assertEqual("connected", result)
        self.assertEqual(3, operation.call_count)
        downstream.assert_called_once_with()
        self.assertEqual(2, len(waiting_snapshots))
        components = tuple(item.component for item in store.snapshot().partitions)
        self.assertNotIn("kafka_startup_retry", components)
        for attempt, (live, ready, health) in enumerate(waiting_snapshots, 1):
            self.assertTrue(live.live)
            self.assertFalse(ready.ready)
            self.assertIn("waiting_for_kafka", ready.blocking_reasons)
            retry = next(item for item in health.partitions
                         if item.component == "kafka_startup_retry")
            self.assertEqual("unhealthy", retry.status)
            self.assertEqual(attempt, retry.observation_count)
            self.assertLessEqual(len(retry.latest_failure or ""), 512)

    def test_deterministic_failure_is_not_retried(self):
        operation = Mock(side_effect=ValueError("sensitive configuration"))
        retry_wait = Mock()

        with patch.object(main, "_request_fatal_shutdown"), \
                self.assertRaises(main.CanaryError):
            main._retry_transient_startup_stage(
                operation,
                _classify_kafka,
                HealthStateStore(),
                connecting_state="CONNECTING_KAFKA",
                waiting_state="WAITING_FOR_KAFKA",
                retry_wait=retry_wait,
            )

        operation.assert_called_once_with()
        retry_wait.assert_not_called()

    def test_reconciliation_retries_before_downstream_stage(self):
        store = HealthStateStore()
        operation = Mock(side_effect=(_wrapped_transport_error(), None, "verified"))
        downstream = Mock()

        def wait_for_retry():
            downstream.assert_not_called()
            self.assertFalse(store.readiness_snapshot().ready)
            self.assertIn(
                "reconciling_topic",
                store.readiness_snapshot().blocking_reasons,
            )

        with patch.object(main, "_shutdown_requested", return_value=False):
            result = main._retry_transient_reconciliation(
                operation, _classify_kafka, store, wait_for_retry
            )
            downstream()

        self.assertEqual("verified", result)
        self.assertEqual(3, operation.call_count)
        downstream.assert_called_once_with()

    def test_destructive_reconciliation_failure_is_never_retried(self):
        operation = Mock()

        def fail_after_delete(mark_destructive):
            mark_destructive()
            raise _wrapped_transport_error()

        operation.side_effect = fail_after_delete
        with (
            patch.object(main, "_shutdown_requested", return_value=False),
            self.assertRaises(main.CanaryError),
        ):
            main._retry_transient_reconciliation(
                operation,
                _classify_kafka,
                HealthStateStore(),
                Mock(),
            )

        operation.assert_called_once()
