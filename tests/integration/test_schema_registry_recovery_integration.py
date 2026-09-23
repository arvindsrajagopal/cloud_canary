"""Production Schema Registry completion-handler recovery integration tests."""

import unittest
from unittest.mock import ANY, MagicMock, patch

from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
)
from src.health_state import HEALTHY, UNHEALTHY, HealthStateStore
from src.main import _record_schema_registry_completion
from src.recovery_policy import RecoveryContext, recovery_action
from tests.fakes.clock import FakeClock


_RAW = "credential=must-not-survive"


def _store():
    clock = FakeClock()
    store = HealthStateStore(
        (0,), minimum_checks=1, warmup_checks=0,
        monotonic_clock=clock.monotonic, wall_clock=clock.time,
    )
    store.record_partition_result(0, success=True)
    store.record_schema_registry_result(success=True)
    return store


def _error(category, recoverability, code="CANARY.SR.BOUNDED"):
    error = CanaryError(FailureDescriptor(
        component=FailureComponent.SCHEMA_REGISTRY,
        phase=Phase.SCHEMA_REGISTRY,
        category=category,
        recoverability=recoverability,
        code=code,
        safe_summary=FailureSummary.DEPENDENCY_UNAVAILABLE,
    ))
    error.raw_exception_text = _RAW
    return error


class SchemaRegistryRecoveryIntegrationTests(unittest.TestCase):
    def _complete(self, store, error):
        with (
            patch("src.main.metrics.SR_CHECKS_TOTAL", MagicMock()),
            patch("src.main.metrics.SR_LATENCY", MagicMock()),
            patch("src.main.log") as completion_log,
            patch("src.main._request_fatal_shutdown") as fatal_shutdown,
        ):
            _record_schema_registry_completion(store, 8.5, error)
        return completion_log, fatal_shutdown

    def test_transient_failure_uses_runtime_policy_and_remains_unsuccessful(self):
        store = _store()
        error = _error(ErrorCategory.NETWORK, Recoverability.TRANSIENT)

        with patch("src.main.recovery_action", wraps=recovery_action) as policy:
            completion_log, fatal_shutdown = self._complete(store, error)

        policy.assert_called_once_with(error.failure, RecoveryContext.RUNTIME)
        health = store.snapshot().schema_registry
        self.assertEqual(2, health.observation_count)
        self.assertEqual(0.5, health.failure_rate)
        self.assertIn("transient:SCHEMA_REGISTRY:NETWORK", health.latest_failure)
        fatal_shutdown.assert_not_called()
        self.assertNotIn(_RAW, repr((health, completion_log.mock_calls)))

    def test_deterministic_failure_is_schema_registry_local_and_continues(self):
        store = _store()
        error = _error(
            ErrorCategory.AUTHENTICATION, Recoverability.DETERMINISTIC,
            code="401",
        )

        completion_log, fatal_shutdown = self._complete(store, error)

        snapshot = store.snapshot()
        self.assertEqual(UNHEALTHY, snapshot.schema_registry.status)
        self.assertEqual(HEALTHY, snapshot.partitions[0].status)
        self.assertIn("deterministic", snapshot.schema_registry.latest_failure)
        fatal_shutdown.assert_not_called()
        completion_log.error.assert_called_once_with(
            "Schema Registry check failed", extra=ANY,
        )
        self.assertNotIn(_RAW, repr((snapshot, completion_log.mock_calls)))

    def test_internal_fatal_requests_bounded_shutdown_without_raw_content(self):
        store = _store()
        error = _error(ErrorCategory.INTERNAL, Recoverability.INTERNAL_FATAL)

        completion_log, fatal_shutdown = self._complete(store, error)

        fatal_shutdown.assert_called_once_with()
        health = store.snapshot().schema_registry
        self.assertEqual(2, health.observation_count)
        self.assertEqual(0.5, health.failure_rate)
        self.assertNotIn(_RAW, repr((health, completion_log.mock_calls)))


if __name__ == "__main__":
    unittest.main()
