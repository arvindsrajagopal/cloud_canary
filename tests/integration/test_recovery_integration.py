"""Kafka runtime completion-handler recovery integration tests."""

from concurrent.futures import Future
import unittest
from unittest.mock import MagicMock, patch

from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
)
from src.health_state import DEGRADED, HEALTHY, UNHEALTHY, HealthStateStore
from src.main import _record_kafka_completion
from src.recovery_policy import RecoveryContext, recovery_action
from src.worker_pool import ConsumerFailure, ConsumerInvalidError, WorkerPool
from tests.fakes.clock import FakeClock


def _store():
    clock = FakeClock()
    store = HealthStateStore(
        (0, 1), minimum_checks=1, warmup_checks=0,
        monotonic_clock=clock.monotonic, wall_clock=clock.time,
    )
    store.record_partition_result(0, success=True)
    store.record_partition_result(1, success=True)
    return store


def _failure(category, recoverability):
    return FailureDescriptor(
        component=FailureComponent.KAFKA_PARTITION,
        phase=Phase.CONSUME,
        category=category,
        recoverability=recoverability,
        code="CANARY.BOUNDED",
        safe_summary=(
            FailureSummary.INTERNAL_FAILURE
            if recoverability is Recoverability.INTERNAL_FATAL
            else FailureSummary.DEPENDENCY_UNAVAILABLE
        ),
    )


class _Consumer:
    def __init__(self):
        self.closed = False

    def assign(self, _partitions):
        pass

    def close(self):
        self.closed = True


class KafkaRecoveryIntegrationTests(unittest.TestCase):
    def _complete(self, store, error, partition=1):
        completed = Future()
        completed.set_exception(error)
        consecutive = {0: 0, 1: 0}
        with (
            patch("src.main.metrics.FAILURES_TOTAL", MagicMock()),
            patch("src.main.metrics.CHECKS_TOTAL", MagicMock()),
            patch("src.main.metrics.record_partition_check"),
            patch("src.main.metrics.CHECK_SEQUENCE", MagicMock()),
            patch("src.main.log") as completion_log,
            patch("src.main._request_fatal_shutdown") as fatal_shutdown,
        ):
            _record_kafka_completion(
                store, partition, 3, store.topic_generation,
                consecutive, completed,
            )
        return completion_log, fatal_shutdown

    def test_transient_completion_calls_runtime_policy_and_remains_failed(self):
        store = _store()
        error = CanaryError(
            _failure(ErrorCategory.NETWORK, Recoverability.TRANSIENT)
        )

        with patch("src.main.recovery_action", wraps=recovery_action) as policy:
            _, fatal_shutdown = self._complete(store, error)

        policy.assert_called_once_with(error.failure, RecoveryContext.RUNTIME)
        snapshot = store.snapshot()
        self.assertEqual(0.5, snapshot.partitions[1].failure_rate)
        self.assertIsNotNone(snapshot.partitions[1].latest_failure)
        self.assertEqual(HEALTHY, snapshot.partitions[0].status)
        fatal_shutdown.assert_not_called()

    def test_deterministic_failure_marks_only_partition_unhealthy(self):
        store = _store()
        error = CanaryError(
            _failure(
                ErrorCategory.AUTHENTICATION,
                Recoverability.DETERMINISTIC,
            )
        )

        _, fatal_shutdown = self._complete(store, error)

        snapshot = store.snapshot()
        self.assertEqual(HEALTHY, snapshot.partitions[0].status)
        self.assertEqual(UNHEALTHY, snapshot.partitions[1].status)
        self.assertIn("deterministic", snapshot.partitions[1].latest_failure)
        fatal_shutdown.assert_not_called()

    def test_capacity_failure_degrades_capacity_without_fatal_shutdown(self):
        store = _store()
        error = CanaryError(
            _failure(ErrorCategory.CAPACITY, Recoverability.TRANSIENT)
        )

        _, fatal_shutdown = self._complete(store, error)

        snapshot = store.snapshot()
        self.assertEqual(DEGRADED, snapshot.scheduling_capacity.status)
        self.assertEqual(0.5, snapshot.partitions[1].failure_rate)
        self.assertEqual(HEALTHY, snapshot.partitions[0].status)
        fatal_shutdown.assert_not_called()

    def test_serialization_and_unknown_completion_request_fatal_shutdown(self):
        cases = (
            CanaryError(_failure(
                ErrorCategory.SERIALIZATION,
                Recoverability.INTERNAL_FATAL,
            )),
            RuntimeError("raw internal detail"),
        )
        for error in cases:
            with self.subTest(error=type(error).__name__):
                store = _store()
                with patch(
                    "src.main.recovery_action", wraps=recovery_action
                ) as policy:
                    completion_log, fatal_shutdown = self._complete(store, error)

                policy.assert_called_once()
                self.assertIs(policy.call_args.args[1], RecoveryContext.RUNTIME)
                fatal_shutdown.assert_called_once_with()
                self.assertEqual(
                    0.5, store.snapshot().partitions[1].failure_rate
                )
                self.assertNotIn(
                    "raw internal detail", repr(completion_log.mock_calls)
                )

    def test_stale_fatal_completions_do_not_execute_recovery_actions(self):
        cases = (
            CanaryError(_failure(
                ErrorCategory.SERIALIZATION,
                Recoverability.INTERNAL_FATAL,
            )),
            RuntimeError("raw internal detail"),
        )
        for error in cases:
            with self.subTest(error=type(error).__name__):
                store = _store()
                stale_generation = store.topic_generation
                store.replace_expected_partitions((0, 1), preserve_existing=False)
                before = store.snapshot()
                completed = Future()
                completed.set_exception(error)
                metric_mocks = [MagicMock() for _ in range(4)]

                with (
                    patch("src.main.recovery_action", wraps=recovery_action) as policy,
                    patch("src.main.metrics.FAILURES_TOTAL", metric_mocks[0]),
                    patch("src.main.metrics.CHECKS_TOTAL", metric_mocks[1]),
                    patch("src.main.metrics.record_partition_check", metric_mocks[2]),
                    patch("src.main.metrics.CHECK_SEQUENCE", metric_mocks[3]),
                    patch("src.main.log") as completion_log,
                    patch("src.main._request_fatal_shutdown") as fatal_shutdown,
                ):
                    _record_kafka_completion(
                        store, 1, 3, stale_generation, {0: 0, 1: 0}, completed,
                    )

                policy.assert_called_once()
                self.assertEqual(before, store.snapshot())
                completion_log.error.assert_not_called()
                fatal_shutdown.assert_not_called()
                for metric_mock in metric_mocks:
                    metric_mock.assert_not_called()

    def test_consumer_state_replaces_only_borrowed_worker_before_completion(self):
        created = []

        def factory(_worker_id):
            consumer = _Consumer()
            created.append(consumer)
            return consumer, object()

        pool = WorkerPool(size=2, topic="canary", factory=factory)
        original_consumers = tuple(created)
        try:
            completed = Future()
            with self.assertRaises(ConsumerFailure) as caught:
                pool.run(
                    1,
                    lambda *_: (_ for _ in ()).throw(ConsumerInvalidError(
                        _failure(
                            ErrorCategory.CLIENT_STATE,
                            Recoverability.TRANSIENT,
                        )
                    )),
                )
            completed.set_exception(caught.exception)

            store = _store()
            with (
                patch("src.main.recovery_action", wraps=recovery_action) as policy,
                patch("src.main.metrics.FAILURES_TOTAL", MagicMock()),
                patch("src.main.metrics.CHECKS_TOTAL", MagicMock()),
                patch("src.main.metrics.record_partition_check"),
                patch("src.main.metrics.CHECK_SEQUENCE", MagicMock()),
                patch("src.main.log"),
            ):
                _record_kafka_completion(
                    store, 1, 3, store.topic_generation,
                    {0: 0, 1: 0}, completed,
                )

            self.assertEqual(3, len(created))
            self.assertEqual(1, sum(c.closed for c in original_consumers))
            policy.assert_called_once_with(
                completed.exception().failure, RecoveryContext.RUNTIME
            )
        finally:
            pool.close()


if __name__ == "__main__":
    unittest.main()
