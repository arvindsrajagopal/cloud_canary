"""HTTP-model integration regressions for transient startup health."""

from concurrent.futures import Future
import unittest
from unittest.mock import Mock, patch

from src import main
from src.error_classifier import (CanaryError, ErrorCategory, FailureComponent,
                                  FailureDescriptor, FailureSummary, Phase,
                                  Recoverability)
from src.health import (configure_health_state, get_health_status,
                        get_liveness_status, get_readiness_status)
from src.health_state import HealthStateStore


class StartupHealthIntegrationTests(unittest.TestCase):
    def test_transient_schema_registry_retry_stays_live_and_unready(self):
        store = HealthStateStore(max_diagnostic_components=2)
        configure_health_state(store, shutdown_requested=lambda: False)
        operation = Mock(side_effect=(ConnectionError("private endpoint"), object()))
        observed = {}

        def classify(error):
            return main._startup_failure(
                error,
                phase=Phase.SCHEMA_REGISTRY,
                component=FailureComponent.SCHEMA_REGISTRY,
                schema_registry=True,
            )

        def inspect_retry_state():
            observed.update(live=get_liveness_status(),
                            ready=get_readiness_status(),
                            health=get_health_status())

        with patch.object(main, "_shutdown_requested", return_value=False):
            main._retry_transient_startup_stage(
                operation,
                classify,
                store,
                connecting_state="CONNECTING_SCHEMA_REGISTRY",
                waiting_state="WAITING_FOR_SCHEMA_REGISTRY",
                retry_wait=inspect_retry_state,
            )

        self.assertEqual(2, operation.call_count)
        self.assertEqual(200, observed["live"].http_code)
        self.assertEqual(503, observed["ready"].http_code)
        blockers = observed["ready"].checks["blocking_reasons"]
        self.assertIn("waiting_for_schema_registry", blockers)
        self.assertEqual(503, observed["health"].http_code)
        components = observed["health"].checks["components"]
        names = tuple(item["component"] for item in components)
        self.assertIn("schema_registry_startup_retry", names)
        self.assertLessEqual(len(components), 2)
        self.assertNotIn("private endpoint", repr(observed))

    def test_failed_warmup_capability_proof_stays_unready_and_sanitized(self):
        store = HealthStateStore((0,), warmup_checks=0)
        store.publish_initialization_state("WARMING_UP")
        store.record_schema_registry_result(success=True)
        failure = FailureDescriptor(
            component=FailureComponent.KAFKA_PARTITION,
            phase=Phase.CONSUME,
            category=ErrorCategory.BROKER_SERVICE,
            recoverability=Recoverability.TRANSIENT,
            code="CANARY.CONSUME_TIMEOUT",
            safe_summary=FailureSummary.OPERATION_TIMED_OUT,
        )
        completed = Future()
        completed.set_exception(CanaryError(failure))

        with patch.object(main, "_update_failure_metrics"), patch.object(
            main, "log"
        ):
            main._record_kafka_completion(
                store,
                0,
                1,
                store.topic_generation,
                {0: 0},
                completed,
            )

        readiness = store.readiness_snapshot()
        health = store.snapshot()
        self.assertFalse(readiness.ready)
        self.assertEqual((0,), readiness.incomplete_partitions)
        self.assertLessEqual(len(health.partitions), 1)
        self.assertNotIn("private", repr((readiness, health)))
