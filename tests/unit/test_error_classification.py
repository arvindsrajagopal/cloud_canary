"""Bounded typed Kafka error classification tests."""

import unittest

from confluent_kafka import KafkaError

from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    Phase,
    Recoverability,
    classify_kafka_error,
)


class KafkaErrorClassificationTests(unittest.TestCase):
    def classify(self, code, message="untrusted dependency detail"):
        return classify_kafka_error(
            KafkaError(code, message),
            phase=Phase.CONSUME,
        )

    def test_classifier_returns_bounded_descriptor_for_operation(self):
        descriptor = classify_kafka_error(
            KafkaError(KafkaError._TRANSPORT, "broker=user:password@host"),
            phase=Phase.METADATA_FETCH,
            component=FailureComponent.TOPIC_ADMINISTRATION,
        )

        self.assertIsInstance(descriptor, FailureDescriptor)
        self.assertEqual(descriptor.component,
                         FailureComponent.TOPIC_ADMINISTRATION)
        self.assertEqual(descriptor.phase, Phase.METADATA_FETCH)
        self.assertEqual(descriptor.category, ErrorCategory.NETWORK)
        self.assertEqual(descriptor.recoverability, Recoverability.TRANSIENT)
        self.assertEqual(descriptor.code, "KAFKA._TRANSPORT")
        self.assertEqual(descriptor.safe_summary, "Dependency unavailable")
        self.assertNotIn("password", descriptor.safe_summary)

    def test_typed_code_takes_precedence_over_message_text(self):
        descriptor = self.classify(
            KafkaError._STATE,
            "DNS timeout SSL transport queue full serialization",
        )

        self.assertEqual(descriptor.category, ErrorCategory.CLIENT_STATE)
        self.assertEqual(descriptor.code, "KAFKA._STATE")

    def test_client_local_codes_are_classified_by_cause(self):
        cases = (
            (KafkaError._TRANSPORT, ErrorCategory.NETWORK,
             Recoverability.TRANSIENT),
            (KafkaError._STATE, ErrorCategory.CLIENT_STATE,
             Recoverability.TRANSIENT),
            (KafkaError._QUEUE_FULL, ErrorCategory.CAPACITY,
             Recoverability.TRANSIENT),
            (KafkaError._VALUE_SERIALIZATION, ErrorCategory.SERIALIZATION,
             Recoverability.INTERNAL_FATAL),
            (KafkaError._INVALID_ARG, ErrorCategory.CONFIGURATION,
             Recoverability.DETERMINISTIC),
            (KafkaError._FAIL, ErrorCategory.UNKNOWN,
             Recoverability.UNKNOWN),
        )
        for code, category, recoverability in cases:
            with self.subTest(code=code):
                descriptor = self.classify(code)
                self.assertEqual(descriptor.category, category)
                self.assertEqual(descriptor.recoverability, recoverability)

    def test_tls_and_security_codes_have_specific_categories(self):
        cases = (
            (KafkaError._SSL, ErrorCategory.TLS_CERTIFICATE),
            (KafkaError._AUTHENTICATION, ErrorCategory.AUTHENTICATION),
            (KafkaError.TOPIC_AUTHORIZATION_FAILED,
             ErrorCategory.AUTHORIZATION),
        )
        for code, category in cases:
            with self.subTest(code=code):
                descriptor = self.classify(code)
                self.assertEqual(descriptor.category, category)
                self.assertEqual(
                    descriptor.recoverability,
                    Recoverability.DETERMINISTIC,
                )

    def test_broker_protocol_response_is_broker_service_not_network(self):
        descriptor = self.classify(KafkaError.LEADER_NOT_AVAILABLE)

        self.assertEqual(descriptor.category, ErrorCategory.BROKER_SERVICE)
        self.assertEqual(descriptor.recoverability, Recoverability.TRANSIENT)
        self.assertEqual(descriptor.code, "KAFKA.LEADER_NOT_AVAILABLE")
        self.assertNotEqual(descriptor.phase, Phase.UNKNOWN)
