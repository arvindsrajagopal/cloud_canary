import dataclasses
import sys
import types
import unittest

try:
    import confluent_kafka  # noqa: F401
except ModuleNotFoundError:
    confluent_kafka = types.ModuleType("confluent_kafka")

    class _KafkaError:
        _TRANSPORT = -1
        _ALL_BROKERS_DOWN = -2
        _RESOLVE = -3
        _SSL = -4

    confluent_kafka.KafkaError = _KafkaError
    confluent_kafka.KafkaException = Exception
    sys.modules["confluent_kafka"] = confluent_kafka

from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    MAX_SAFE_SUMMARY_LENGTH,
    MAX_STABLE_CODE_LENGTH,
    Phase,
    Recoverability,
    sanitize_failure_summary,
)


class FailureDescriptorTests(unittest.TestCase):
    def _descriptor(self, **overrides):
        values = {
            "component": FailureComponent.KAFKA_PARTITION,
            "phase": Phase.CONSUME,
            "category": ErrorCategory.NETWORK,
            "recoverability": Recoverability.TRANSIENT,
            "code": None,
            "safe_summary": FailureSummary.OPERATION_TIMED_OUT,
        }
        values.update(overrides)
        return FailureDescriptor(**values)

    def test_enums_cover_the_bounded_spec_values(self):
        self.assertEqual(
            {item.value for item in FailureComponent},
            {
                "KAFKA_PARTITION", "SCHEMA_REGISTRY", "TOPIC_ADMINISTRATION",
                "SCHEDULER", "HTTP_SERVER", "INTERNAL_STATE",
            },
        )
        self.assertEqual(
            {item.value for item in Phase},
            {
                "ASSIGNMENT", "SEEK", "PRODUCE", "CONSUME",
                "SCHEMA_REGISTRY", "METADATA_FETCH", "TOPIC_CREATE",
                "TOPIC_EXPAND", "TOPIC_DELETE", "TOPIC_VERIFY",
                "CONSUMER_CREATE", "CONSUMER_REPLACE", "SCHEDULER",
                "STATE_UPDATE", "HTTP_REQUEST", "STARTUP", "SHUTDOWN",
                "UNKNOWN",
            },
        )
        self.assertEqual(
            {item.value for item in ErrorCategory},
            {
                "NETWORK", "BROKER_SERVICE", "AUTHENTICATION",
                "AUTHORIZATION", "TLS_CERTIFICATE", "CONFIGURATION",
                "SERIALIZATION", "CLIENT_STATE", "CAPACITY", "INTERNAL",
                "UNKNOWN",
            },
        )
        self.assertEqual(
            {item.value for item in Recoverability},
            {"TRANSIENT", "DETERMINISTIC", "INTERNAL_FATAL", "UNKNOWN"},
        )

    def test_descriptor_is_frozen_and_has_only_the_spec_fields(self):
        descriptor = self._descriptor()
        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(descriptor)),
            (
                "component", "phase", "category", "recoverability", "code",
                "safe_summary",
            ),
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            descriptor.safe_summary = "changed"
        with self.assertRaises((AttributeError, TypeError)):
            descriptor.raw_exception = RuntimeError("retained")

    def test_summary_is_sanitized_and_bounded_during_construction(self):
        first = self._descriptor(
            safe_summary=FailureSummary.DEPENDENCY_UNAVAILABLE,
        ).safe_summary
        second = sanitize_failure_summary(FailureSummary.DEPENDENCY_UNAVAILABLE)
        self.assertEqual(first, second)
        self.assertLessEqual(len(first), MAX_SAFE_SUMMARY_LENGTH)
        self.assertEqual(first, "Dependency unavailable")

    def test_raw_sensitive_sources_are_rejected_instead_of_retained(self):
        try:
            raise RuntimeError("traceback-secret")
        except RuntimeError:
            traceback = sys.exc_info()[2]

        class KafkaMessageLike:
            def value(self):
                return b"customer-private-value"

        unsafe_sources = (
            RuntimeError("hunter2"),
            traceback,
            "hunter2",
            b"customer-private-value",
            KafkaMessageLike(),
            "https://alice:hunter2@example.test/path",
            {"value": "customer-private-value"},
        )
        for unsafe in unsafe_sources:
            with self.subTest(source_type=type(unsafe).__name__):
                with self.assertRaises(TypeError):
                    self._descriptor(safe_summary=unsafe)
                with self.assertRaises(TypeError):
                    sanitize_failure_summary(unsafe)

    def test_stable_code_is_optional_and_bounded(self):
        self.assertIsNone(self._descriptor().code)
        self.assertEqual(self._descriptor(code="KAFKA._TIMED_OUT").code,
                         "KAFKA._TIMED_OUT")
        with self.assertRaises(ValueError):
            self._descriptor(code="x" * (MAX_STABLE_CODE_LENGTH + 1))
        with self.assertRaises(ValueError):
            self._descriptor(code="https://user:password@example.test")

    def test_enum_fields_reject_unbounded_strings(self):
        with self.assertRaises(TypeError):
            self._descriptor(phase="CONSUME")


if __name__ == "__main__":
    unittest.main()
