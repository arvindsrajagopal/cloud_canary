"""Regression tests for deterministic startup failure execution."""

import ssl
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from src import main
from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    FailureComponent,
    Phase,
)


def _raise(error):
    raise error


def _classify_kafka(error):
    return main._startup_failure(
        error,
        phase=Phase.METADATA_FETCH,
        component=FailureComponent.KAFKA_PARTITION,
    )


class StartupFailureExecutionTests(unittest.TestCase):
    def test_deterministic_categories_use_fatal_startup_policy(self):
        cases = (
            (
                ErrorCategory.CONFIGURATION,
                ValueError("credential-shaped-value-must-not-escape"),
            ),
            (
                ErrorCategory.AUTHENTICATION,
                KafkaException(KafkaError(KafkaError._AUTHENTICATION)),
            ),
            (
                ErrorCategory.AUTHORIZATION,
                KafkaException(
                    KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)
                ),
            ),
            (
                ErrorCategory.TLS_CERTIFICATE,
                ssl.SSLCertVerificationError("private certificate detail"),
            ),
        )

        for expected_category, raw_error in cases:
            with self.subTest(category=expected_category), patch.object(
                main, "_request_fatal_shutdown"
            ) as request_fatal, patch.object(main.log, "error") as log_error:
                with self.assertRaises(CanaryError) as raised:
                    main._execute_startup_operation(
                        lambda error=raw_error: _raise(error),
                        _classify_kafka,
                    )

                self.assertIs(raised.exception.category, expected_category)
                self.assertIsNone(raised.exception.__context__)
                request_fatal.assert_called_once_with()
                logged = log_error.call_args.kwargs["extra"]
                self.assertIs(logged["category"], expected_category)
                self.assertNotIn(str(raw_error), repr(log_error.call_args))

    def test_schema_registry_typed_http_failure_uses_same_executor(self):
        error = RuntimeError("sensitive response body")
        error.http_status_code = 403
        classifier = lambda exc: main._startup_failure(
            exc,
            phase=Phase.SCHEMA_REGISTRY,
            component=FailureComponent.SCHEMA_REGISTRY,
            schema_registry=True,
        )

        with patch.object(main, "_request_fatal_shutdown") as request_fatal:
            with self.assertRaises(CanaryError) as raised:
                main._execute_startup_operation(
                    lambda: _raise(error), classifier
                )

        self.assertIs(raised.exception.category, ErrorCategory.AUTHORIZATION)
        request_fatal.assert_called_once_with()

    def test_schema_registry_configuration_failure_is_fatal(self):
        classifier = lambda exc: main._startup_failure(
            exc,
            phase=Phase.SCHEMA_REGISTRY,
            component=FailureComponent.SCHEMA_REGISTRY,
            schema_registry=True,
        )

        with patch.object(main, "_request_fatal_shutdown") as request_fatal:
            with self.assertRaises(CanaryError) as raised:
                main._execute_startup_operation(
                    lambda: _raise(ValueError("invalid configuration")),
                    classifier,
                )

        self.assertIs(raised.exception.category, ErrorCategory.CONFIGURATION)
        request_fatal.assert_called_once_with()

    def test_unknown_dependency_exception_crosses_boundary_only_as_descriptor(self):
        raw_error = RuntimeError("dependency secret")

        with patch.object(main, "_request_fatal_shutdown") as request_fatal:
            with self.assertRaises(CanaryError) as raised:
                main._execute_startup_operation(
                    lambda: _raise(raw_error), _classify_kafka
                )

        self.assertIs(raised.exception.category, ErrorCategory.UNKNOWN)
        self.assertIsNone(raised.exception.__context__)
        self.assertNotIn("dependency secret", str(raised.exception))
        request_fatal.assert_not_called()

    def test_fatal_startup_exits_nonzero_after_lifecycle_cleanup(self):
        cleanup = Mock()

        def lifecycle(_owner):
            main._execute_startup_operation(
                lambda: _raise(ValueError("invalid configuration")),
                _classify_kafka,
            )

        with (
            patch.object(main, "_run_lifecycle_owned", side_effect=lifecycle),
            patch.object(main, "_shutdown_http_service", cleanup),
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main, "_shutdown_request_claim", [main._shutdown_claim_token]
            ),
            patch.object(main, "_fatal_shutdown_request", None),
            patch.object(
                main,
                "_fatal_shutdown_request_claim",
                [main._shutdown_claim_token],
            ),
            self.assertRaises(SystemExit) as raised,
        ):
            main._run_behind_daemon_boundary(main._run_lifecycle)

        self.assertEqual(raised.exception.code, 1)
        cleanup.assert_called_once_with({})


if __name__ == "__main__":
    unittest.main()
