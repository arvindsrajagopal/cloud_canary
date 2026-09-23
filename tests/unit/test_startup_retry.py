import threading
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from src import main
from src.config import validate_config
from src.error_classifier import FailureComponent, Phase
from src.health_state import HealthStateStore
class _Clock:
    def __init__(self):
        self.now = 100.0
        self.waits = []

    def monotonic(self):
        return self.now

    def wait(self, delay):
        self.waits.append(delay)
        self.now += delay
        return False
def _timing(clock, jitter_values):
    values = iter(jitter_values)
    return main._StartupRetryTiming(
        1.0, 3.0, 2.0, 0.25,
        monotonic_clock=clock.monotonic,
        wait_for_event=clock.wait,
        jitter_source=lambda lower, upper: next(values) * (upper - lower) + lower,
    )
def _classify(error):
    return main._startup_failure(
        error, phase=Phase.METADATA_FETCH,
        component=FailureComponent.KAFKA_PARTITION,
    )
def _transient():
    return KafkaException(KafkaError(KafkaError._TRANSPORT))
def _config(**overrides):
    app = {"topic": "canary", **overrides}
    return {
        "kafka": {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "user", "sasl.password": "password",
        },
        "schema_registry": {"url": "https://registry.invalid",
                            "basic.auth.user.info": "user:password"},
        "app": app,
    }
class StartupRetryTimingTests(unittest.TestCase):
    def test_native_attempt_finishes_before_retry_and_shutdown_stops_next(self):
        attempt_started = threading.Event()
        release_attempt = threading.Event()
        retry_waiting = threading.Event()
        release_retry = threading.Event()
        shutdown = [False]
        operation_calls = []
        outcome = []

        def operation():
            operation_calls.append("started")
            attempt_started.set()
            self.assertTrue(release_attempt.wait(1))
            raise _transient()

        def retry_wait():
            retry_waiting.set()
            self.assertTrue(release_retry.wait(1))

        def run_stage():
            try:
                main._retry_transient_startup_stage(
                    operation, _classify, HealthStateStore(),
                    connecting_state="CONNECTING_KAFKA",
                    waiting_state="WAITING_FOR_KAFKA",
                    retry_wait=retry_wait,
                )
            except BaseException as exc:
                outcome.append(exc)

        with patch.object(
            main, "_shutdown_requested", side_effect=lambda: shutdown[0]
        ):
            worker = threading.Thread(target=run_stage)
            worker.start()
            try:
                self.assertTrue(attempt_started.wait(2))
                self.assertEqual(["started"], operation_calls)
                self.assertFalse(retry_waiting.is_set())

                release_attempt.set()
                self.assertTrue(retry_waiting.wait(2))
                self.assertEqual(["started"], operation_calls)
                shutdown[0] = True
            finally:
                shutdown[0] = True
                release_attempt.set()
                release_retry.set()
                worker.join(2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(["started"], operation_calls)
        self.assertEqual(1, len(outcome))
        self.assertIsInstance(outcome[0], main._StartupDispatchRejected)

    def test_success_advances_stage_once_after_serial_retries(self):
        operation = Mock(side_effect=(_transient(), _transient(), "ready"))
        retry_wait = Mock()

        with patch.object(main, "_shutdown_requested", return_value=False):
            result = main._retry_transient_startup_stage(
                operation, _classify, HealthStateStore(),
                connecting_state="CONNECTING_KAFKA",
                waiting_state="WAITING_FOR_KAFKA",
                retry_wait=retry_wait,
            )

        self.assertEqual("ready", result)
        self.assertEqual(3, operation.call_count)
        self.assertEqual(2, retry_wait.call_count)
        retry_wait.complete_stage.assert_called_once_with()

    def test_progression_cap_independent_jitter_and_monotonic_deadlines(self):
        clock = _Clock()
        timing = _timing(clock, (0.0, 1.0, 0.5, 0.0))
        for attempt in range(1, 5):
            timing.wait(
                waiting_dependency="WAITING_FOR_KAFKA",
                retry_attempts=attempt,
            )
        self.assertEqual([0.75, 2.5, 3.0, 2.25], clock.waits)
    def test_success_resets_delay_for_next_initialization_stage(self):
        clock = _Clock()
        timing = _timing(clock, (0.5, 0.5, 0.5))
        operations = (
            (Mock(side_effect=(_transient(), _transient(), "kafka")),
             "CONNECTING_KAFKA", "WAITING_FOR_KAFKA"),
            (Mock(side_effect=(_transient(), "schema")),
             "CONNECTING_SCHEMA_REGISTRY", "WAITING_FOR_SCHEMA_REGISTRY"),
        )
        with patch.object(main, "_shutdown_requested", return_value=False):
            for operation, connecting, waiting in operations:
                main._retry_transient_startup_stage(
                    operation, _classify, HealthStateStore(),
                    connecting_state=connecting, waiting_state=waiting,
                    retry_wait=timing,
                )
        self.assertEqual([1.0, 2.0, 1.0], clock.waits)
    def test_stage_backoff_spans_construction_and_validation(self):
        clock = _Clock()
        timing = _timing(clock, (0.5, 0.5))
        operations = (
            (Mock(side_effect=(_transient(), "admin")), False),
            (Mock(side_effect=(_transient(), None)), True),
        )
        with patch.object(main, "_shutdown_requested", return_value=False):
            for operation, stage_complete in operations:
                main._retry_transient_startup_stage(
                    operation, _classify, HealthStateStore(),
                    connecting_state="CONNECTING_KAFKA",
                    waiting_state="WAITING_FOR_KAFKA",
                    retry_wait=timing, stage_complete=stage_complete,
                )
        self.assertEqual([1.0, 2.0], clock.waits)
    def test_bounded_diagnostics_and_metrics_include_monotonic_elapsed(self):
        clock = _Clock()
        timing = _timing(clock, (0.5,))
        attempts, delay, elapsed, dependency = (Mock() for _ in range(4))
        dependency_child = Mock()
        dependency.labels.return_value = dependency_child
        with (
            patch.object(main.log, "info") as logged,
            patch.object(main, "STARTUP_RETRY_ATTEMPTS", attempts),
            patch.object(main, "STARTUP_RETRY_DELAY_SECONDS", delay),
            patch.object(main, "STARTUP_STATE_ELAPSED_SECONDS", elapsed),
            patch.object(main, "STARTUP_WAITING_DEPENDENCY", dependency),
        ):
            timing.start_stage("CONNECTING_KAFKA")
            clock.now += 4.5
            timing.start_stage("WAITING_FOR_KAFKA")
            clock.now += 2.0
            timing.wait(
                waiting_dependency="WAITING_FOR_KAFKA", retry_attempts=3,
            )
        self.assertEqual(
            {
                "waiting_dependency": "WAITING_FOR_KAFKA",
                "retry_attempts": 3,
                "retry_delay_seconds": 1.0,
                "initialization_state_elapsed_seconds": 2.0,
            },
            logged.call_args.kwargs["extra"],
        )
        attempts.set.assert_called_once_with(3)
        delay.set.assert_called_once_with(1.0)
        elapsed_callback = elapsed.set_function.call_args.args[0]
        self.assertEqual(3.0, elapsed_callback())
        self.assertEqual(
            len(main._STARTUP_WAITING_DEPENDENCIES) + 1,
            dependency_child.set.call_count,
        )
        dependency_child.set.assert_any_call(True)
        dependency_child.set.assert_any_call(False)
    def test_validates_retry_configuration_bounds(self):
        validate_config(_config(**{
            "startup.retry.initial.seconds": "0.5",
            "startup.retry.max.seconds": "10",
            "startup.retry.multiplier": "1.1",
            "startup.retry.jitter.factor": "0.999",
        }))
        invalid = (
            {"startup.retry.initial.seconds": "0"},
            {"startup.retry.max.seconds": "nan"},
            {"startup.retry.initial.seconds": "2",
             "startup.retry.max.seconds": "1"},
            {"startup.retry.multiplier": "1"},
            {"startup.retry.multiplier": "inf"},
            {"startup.retry.jitter.factor": "-0.1"},
            {"startup.retry.jitter.factor": "1"},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                validate_config(_config(**overrides))
