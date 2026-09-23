"""Runtime checks transport only bounded failure descriptors to completion."""
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryError
from confluent_kafka.serialization import SerializationError
from src.error_classifier import (CanaryError, ErrorCategory, FailureComponent,
                                  FailureDescriptor, Phase, Recoverability)
from src.main import _bounded_completion_failure, _timed_sr_probe, check_kafka
_SECRET = "credential=must-not-cross-completion"
def _run_check():
    return check_kafka(*([object()] * 4), "canary", 1.0, 7, 0)
class _FailingSchemaRegistryClient:
    def get_subjects(self):
        raise SchemaRegistryError(503, 50001, _SECRET)
class FailureDescriptorPropagationTests(unittest.TestCase):
    def assert_bounded_error(self, error, expected):
        self.assertIsInstance(error.failure, FailureDescriptor)
        failure = error.failure
        self.assertEqual((failure.component, failure.phase, failure.category,
                          failure.recoverability, failure.code), expected)
        self.assertEqual(set(vars(error)), {"failure"})
        self.assertEqual((error.__cause__, error.__context__), (None, None))
        self.assertNotIn(_SECRET, repr((error, failure.safe_summary)))
        completed, label, fields = _bounded_completion_failure(error)
        self.assertIs(completed, failure)
        self.assertEqual(label, f"{expected[1].value}:{expected[2].value}")
        self.assertEqual(set(fields), {"phase", "category", "code", "detail"})
        self.assertNotIn(_SECRET, repr((label, fields)))
    def run_failed_check(self, produced=None, produce_error=None, consume_error=None):
        with patch("src.main.seek_to_end"), patch(
            "src.main.produce_canary", return_value=produced,
            side_effect=produce_error,
        ), patch("src.main.consume_canary", side_effect=consume_error), \
             ThreadPoolExecutor(max_workers=1) as executor:
            with self.assertRaises(CanaryError) as caught:
                executor.submit(_run_check).result(timeout=1)
        return caught.exception
    def test_kafka_check_future_retains_typed_descriptor_without_raw_error(self):
        raw = KafkaException(KafkaError(KafkaError._TRANSPORT, _SECRET))
        error = self.run_failed_check(produce_error=raw)
        self.assert_bounded_error(error, (FailureComponent.KAFKA_PARTITION,
            Phase.PRODUCE, ErrorCategory.NETWORK, Recoverability.TRANSIENT,
            "KAFKA._TRANSPORT"))
    def test_schema_registry_probe_completion_returns_only_descriptor(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            duration_ms, error = executor.submit(
                _timed_sr_probe, _FailingSchemaRegistryClient()
            ).result(timeout=1)

        self.assertGreaterEqual(duration_ms, 0)
        self.assert_bounded_error(error, (FailureComponent.SCHEMA_REGISTRY,
            Phase.SCHEMA_REGISTRY, ErrorCategory.BROKER_SERVICE,
            Recoverability.TRANSIENT, "SR.HTTP.503.50001"))
    def test_transient_consume_failure_is_not_returned_as_success(self):
        sent = SimpleNamespace(message_id="id", check_sequence=7, producer_host="host")
        error = self.run_failed_check(sent, consume_error=TimeoutError(_SECRET))
        self.assert_bounded_error(error, (FailureComponent.KAFKA_PARTITION,
            Phase.CONSUME, ErrorCategory.BROKER_SERVICE, Recoverability.TRANSIENT,
            "CANARY.CONSUME_TIMEOUT"))
    def test_typed_serialization_failures_remain_internal_fatal(self):
        sent = SimpleNamespace(message_id="id", check_sequence=7, producer_host="host")
        cases = (
            (Phase.PRODUCE, "CANARY.PRODUCE_SERIALIZATION", None, SerializationError),
            (Phase.CONSUME, "CANARY.CONSUME_DESERIALIZATION", sent, TypeError),
        )
        for phase, code, produced, exception_type in cases:
            with self.subTest(phase=phase), patch("src.main.seek_to_end"), patch(
                "src.main.produce_canary",
                side_effect=exception_type(_SECRET) if produced is None else None,
                return_value=produced,
            ), patch(
                "src.main.consume_canary", side_effect=exception_type(_SECRET)
            ):
                with self.assertRaises(CanaryError) as caught:
                    _run_check()
            self.assert_bounded_error(caught.exception, (
                FailureComponent.KAFKA_PARTITION, phase,
                ErrorCategory.SERIALIZATION, Recoverability.INTERNAL_FATAL, code,
            ))

    def test_success_result_remains_unchanged(self):
        sent = SimpleNamespace(message_id="id", check_sequence=7, producer_host="host")
        received = SimpleNamespace(send_timestamp_ms=125)
        with (
            patch("src.main.seek_to_end"),
            patch("src.main.produce_canary", return_value=sent),
            patch("src.main.consume_canary", return_value=(received, 150)),
        ):
            self.assertEqual(25, _run_check())


if __name__ == "__main__":
    unittest.main()
