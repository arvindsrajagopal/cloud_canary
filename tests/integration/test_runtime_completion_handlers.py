from concurrent.futures import Future
import unittest
from unittest.mock import ANY, MagicMock, patch

from src.error_classifier import (CanaryError, ErrorCategory, FailureComponent,
                                  FailureDescriptor, FailureSummary, Phase,
                                  Recoverability)
from src.health_state import HealthStateStore
from src.main import _record_kafka_completion, _record_schema_registry_completion
from src.worker_pool import ConsumerFailure
from tests.fakes.clock import FakeClock


_RAW = "credential=must-not-reach-completion"


def _store():
    clock = FakeClock()
    return HealthStateStore((0, 1), minimum_checks=1, warmup_checks=0,
                            monotonic_clock=clock.monotonic,
                            wall_clock=clock.time)


def _failure(component, phase, category=ErrorCategory.NETWORK):
    return FailureDescriptor(component=component, phase=phase, category=category,
        recoverability=Recoverability.TRANSIENT,
        code="CANARY.BOUNDED",
        safe_summary=FailureSummary.DEPENDENCY_UNAVAILABLE)


class RuntimeCompletionHandlerTests(unittest.TestCase):
    def test_schema_registry_transient_completion_stays_failed_and_bounded(self):
        store = _store()
        error = CanaryError(_failure(FailureComponent.SCHEMA_REGISTRY,
                                    Phase.SCHEMA_REGISTRY))
        error.raw_exception_text = _RAW
        checks = MagicMock()
        latency = MagicMock()

        with (
            patch("src.main.metrics.SR_CHECKS_TOTAL", checks),
            patch("src.main.metrics.SR_LATENCY", latency),
            patch("src.main.log") as completion_log,
        ):
            _record_schema_registry_completion(store, 12.5, error)

        health = store.snapshot().schema_registry
        self.assertEqual("transient:SCHEMA_REGISTRY:NETWORK", health.latest_failure)
        self.assertEqual(1.0, health.failure_rate)
        checks.labels.assert_called_once_with(result="failure", host=ANY)
        latency.labels.assert_called_once_with(host=ANY)
        logged = repr(completion_log.error.call_args)
        self.assertIn("CANARY.BOUNDED", logged)
        self.assertNotIn(_RAW, repr((health, logged)))

    def test_consumer_completion_is_partition_local_and_generation_fenced(self):
        store = _store()
        store.record_partition_result(0, success=True)
        store.record_partition_result(1, success=True)
        generation = store.topic_generation
        error = ConsumerFailure(_failure(FailureComponent.KAFKA_PARTITION,
                                         Phase.CONSUMER_REPLACE,
                                         ErrorCategory.CLIENT_STATE))
        error.raw_exception_text = _RAW
        completed = Future()
        completed.set_exception(error)
        consecutive = {0: 0, 1: 0}
        failures = MagicMock()

        with (
            patch("src.main.metrics.FAILURES_TOTAL", failures),
            patch("src.main.metrics.CHECKS_TOTAL", MagicMock()),
            patch("src.main.metrics.record_partition_check"),
            patch("src.main.metrics.CHECK_SEQUENCE", MagicMock()),
            patch("src.main.log") as completion_log,
        ):
            _record_kafka_completion(store, 1, 7, generation, consecutive, completed)

        snapshot = store.snapshot()
        self.assertIsNone(snapshot.partitions[0].latest_failure)
        self.assertEqual("CONSUMER_REPLACE:CLIENT_STATE",
                         snapshot.partitions[1].latest_failure)
        failures.labels.assert_called_once_with(
            host=ANY, phase="CONSUMER_REPLACE",
            category="CLIENT_STATE", recoverability="TRANSIENT",
        )
        logged = repr(completion_log.error.call_args)
        self.assertIn("CANARY.BOUNDED", logged)
        self.assertNotIn(_RAW, repr((snapshot, logged)))

        store.replace_expected_partitions((0, 1), preserve_existing=False)
        with (
            patch("src.main._update_failure_metrics") as stale_metrics,
            patch("src.main.log") as stale_log,
        ):
            _record_kafka_completion(store, 1, 8, generation, consecutive, completed)
        stale_metrics.assert_not_called()
        stale_log.error.assert_not_called()
