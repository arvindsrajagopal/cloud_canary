"""Schema Registry health regression tests using deterministic state fakes."""

import ssl
import unittest

from confluent_kafka.schema_registry import SchemaRegistryError
from httpx import ConnectError, UnsupportedProtocol
from requests.exceptions import ConnectionError, InvalidSchema, SSLError, Timeout

from src.error_classifier import CanaryError, is_deterministic_sr_error
from src.health_state import DEGRADED, HEALTHY, UNHEALTHY, HealthStateStore
from src.main import check_sr
from tests.fakes.clock import FakeClock


def _store(clock: FakeClock, **overrides) -> HealthStateStore:
    settings = {
        "window_checks": 4,
        "minimum_checks": 4,
        "failure_threshold": 0.5,
        "monotonic_clock": clock.monotonic,
        "wall_clock": clock.time,
    }
    settings.update(overrides)
    return HealthStateStore((0,), **settings)


class _FailingSchemaRegistryClient:
    def __init__(self, failure):
        self.failure = failure

    def get_subjects(self):
        raise self.failure


class SchemaRegistryHealthTests(unittest.TestCase):
    def test_typed_sr_errors_distinguish_deterministic_from_transient(self):
        unauthorized = SchemaRegistryError(401, 40101, "unauthorized")
        forbidden = SchemaRegistryError(403, 40301, "forbidden")
        bad_request = SchemaRegistryError(400, 40001, "bad request")
        not_found = SchemaRegistryError(404, 40401, "subject not found")
        conflict = SchemaRegistryError(409, 40901, "conflict")
        unavailable = SchemaRegistryError(503, 50001, "unavailable")

        self.assertTrue(is_deterministic_sr_error(unauthorized))
        self.assertTrue(is_deterministic_sr_error(forbidden))
        self.assertFalse(is_deterministic_sr_error(bad_request))
        self.assertFalse(is_deterministic_sr_error(not_found))
        self.assertFalse(is_deterministic_sr_error(conflict))
        self.assertFalse(is_deterministic_sr_error(unavailable))
        self.assertFalse(is_deterministic_sr_error(ConnectionError("refused")))

    def test_certificate_and_invalid_https_failures_reach_unhealthy_health(self):
        failures = (
            SchemaRegistryError(401, 40101, "unauthorized"),
            SchemaRegistryError(403, 40301, "forbidden"),
            ssl.SSLCertVerificationError("certificate verify failed"),
            InvalidSchema("No connection adapters were found for 'ftp://registry'"),
            UnsupportedProtocol("unsupported URL protocol"),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                clock = FakeClock()
                store = _store(clock)
                store.record_schema_registry_result(success=True)

                with self.assertRaises(CanaryError) as raised:
                    check_sr(_FailingSchemaRegistryClient(failure))
                store.record_schema_registry_result(
                    success=False,
                    deterministic_failure=raised.exception.deterministic,
                    failure="configuration",
                )

                self.assertTrue(raised.exception.deterministic)
                self.assertEqual(UNHEALTHY, store.snapshot().schema_registry.status)

    def test_generic_bad_request_is_not_immediately_unhealthy(self):
        clock = FakeClock()
        store = _store(clock)
        store.record_schema_registry_result(success=True)

        with self.assertRaises(CanaryError) as raised:
            check_sr(
                _FailingSchemaRegistryClient(
                    SchemaRegistryError(400, 40001, "bad request")
                )
            )
        store.record_schema_registry_result(
            success=False,
            deterministic_failure=raised.exception.deterministic,
            failure="unknown client response",
        )

        self.assertFalse(raised.exception.deterministic)
        self.assertEqual(DEGRADED, store.snapshot().schema_registry.status)

    def test_wrapped_certificate_failure_is_deterministic_but_timeout_is_transient(self):
        certificate_failure = ssl.SSLCertVerificationError("verify failed")
        wrapped = SSLError("TLS handshake failed")
        wrapped.__cause__ = certificate_failure

        self.assertTrue(is_deterministic_sr_error(wrapped))
        httpx_wrapped = ConnectError("TLS handshake failed")
        httpx_wrapped.__cause__ = certificate_failure
        self.assertTrue(is_deterministic_sr_error(httpx_wrapped))
        self.assertFalse(is_deterministic_sr_error(Timeout("timed out")))

    def test_deterministic_failure_is_immediately_unhealthy(self):
        clock = FakeClock()
        store = _store(clock)
        store.record_schema_registry_result(success=True)

        store.record_schema_registry_result(
            success=False, deterministic_failure=True, failure="authentication"
        )

        self.assertEqual(UNHEALTHY, store.snapshot().schema_registry.status)

    def test_historical_successes_do_not_conceal_latest_failure(self):
        clock = FakeClock()
        store = _store(clock, window_checks=20)
        for _ in range(19):
            store.record_schema_registry_result(success=True)

        store.record_schema_registry_result(success=False, failure="timeout")

        schema_registry = store.snapshot().schema_registry
        self.assertEqual(DEGRADED, schema_registry.status)
        self.assertEqual(0.05, schema_registry.failure_rate)

    def test_kafka_and_sr_histories_and_thresholds_are_independent(self):
        clock = FakeClock()
        store = _store(
            clock,
            minimum_checks=2,
            kafka_check_interval=5,
            kafka_degraded_after=20,
            kafka_unhealthy_after=40,
            sr_check_interval=10,
            sr_degraded_after=100,
            sr_unhealthy_after=200,
        )
        store.record_partition_result(0, success=True)
        store.record_partition_result(0, success=False)
        store.record_schema_registry_result(success=True)
        store.record_schema_registry_result(success=True)
        clock.advance(20)

        snapshot = store.snapshot()

        self.assertEqual(DEGRADED, snapshot.partitions[0].status)
        self.assertEqual(HEALTHY, snapshot.schema_registry.status)
        self.assertEqual(0.5, snapshot.partitions[0].failure_rate)
        self.assertEqual(0.0, snapshot.schema_registry.failure_rate)


if __name__ == "__main__":
    unittest.main()
