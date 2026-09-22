import ssl
import unittest

from confluent_kafka.schema_registry import SchemaRegistryError
from httpx import ConnectError, UnsupportedProtocol

from src.error_classifier import (
    ErrorCategory,
    Recoverability,
    classify_schema_registry_error,
)


class SchemaRegistryClassificationTests(unittest.TestCase):
    def classify(self, status, code=1, message="credential=user:secret"):
        return classify_schema_registry_error(SchemaRegistryError(status, code, message))

    def test_http_security_and_configuration_responses_are_distinct(self):
        cases = (
            (401, ErrorCategory.AUTHENTICATION),
            (403, ErrorCategory.AUTHORIZATION),
            (400, ErrorCategory.CONFIGURATION),
            (405, ErrorCategory.CONFIGURATION),
            (409, ErrorCategory.CONFIGURATION),
            (422, ErrorCategory.CONFIGURATION),
        )
        for status, category in cases:
            with self.subTest(status=status):
                descriptor = self.classify(status)
                self.assertEqual(descriptor.category, category)
                self.assertEqual(descriptor.recoverability, Recoverability.DETERMINISTIC)

    def test_other_4xx_and_5xx_remain_bounded_service_responses(self):
        not_found = self.classify(404)
        self.assertEqual(not_found.category, ErrorCategory.BROKER_SERVICE)
        self.assertEqual(not_found.recoverability, Recoverability.UNKNOWN)
        for status in (429, 503):
            descriptor = self.classify(status)
            self.assertEqual(descriptor.category, ErrorCategory.BROKER_SERVICE)
            self.assertEqual(descriptor.recoverability, Recoverability.TRANSIENT)

    def test_descriptor_is_bounded_and_does_not_retain_message(self):
        descriptor = self.classify(401, 40101, "https://user:secret@invalid/path")
        self.assertEqual(descriptor.code, "SR.HTTP.401.40101")
        self.assertEqual(descriptor.safe_summary, "Operation failed")
        self.assertNotIn("secret", descriptor.safe_summary)

    def test_typed_status_takes_precedence_over_exception_message(self):
        descriptor = self.classify(503, message="401 unauthorized certificate")
        self.assertEqual(descriptor.category, ErrorCategory.BROKER_SERVICE)
        self.assertEqual(descriptor.recoverability, Recoverability.TRANSIENT)
        message_only = classify_schema_registry_error(
            Exception("401 SSL ConnectionError configuration")
        )
        self.assertEqual(message_only.category, ErrorCategory.UNKNOWN)

    def test_unbounded_protocol_code_falls_back_to_bounded_http_code(self):
        descriptor = self.classify(400, int("9" * 200))
        self.assertEqual(descriptor.code, "SR.HTTP.400")

    def test_typed_transport_and_certificate_failures_are_distinct(self):
        cases = (
            (ssl.SSLCertVerificationError("secret"), ErrorCategory.TLS_CERTIFICATE,
             Recoverability.DETERMINISTIC),
            (ConnectError("certificate authentication"), ErrorCategory.NETWORK,
             Recoverability.TRANSIENT),
            (UnsupportedProtocol("secret"), ErrorCategory.CONFIGURATION,
             Recoverability.DETERMINISTIC),
        )
        for error, category, recoverability in cases:
            descriptor = classify_schema_registry_error(error)
            actual = descriptor.category, descriptor.recoverability
            self.assertEqual(actual, (category, recoverability))

    def test_wrapped_certificate_failure_beats_generic_transport_wrapper(self):
        wrapper = ConnectError("connection failed")
        wrapper.__cause__ = ssl.SSLCertVerificationError("verify failed")

        descriptor = classify_schema_registry_error(wrapper)
        self.assertEqual(descriptor.category, ErrorCategory.TLS_CERTIFICATE)
        self.assertEqual(descriptor.recoverability, Recoverability.DETERMINISTIC)
