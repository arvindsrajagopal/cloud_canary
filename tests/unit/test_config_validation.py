"""Configuration validation regressions for health and transport security."""

import unittest
from unittest.mock import Mock, patch

from src.config import validate_config
from src.error_classifier import CanaryError
from src.main import run


def _config(**app_overrides):
    app = {"topic": "canary"}
    app.update(app_overrides)
    return {
        "kafka": {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "test-user",
            "sasl.password": "test-password",
        },
        "schema_registry": {
            "url": "https://registry.invalid",
            "basic.auth.user.info": "test-user:test-password",
        },
        "app": app,
    }


class ConfigValidationTests(unittest.TestCase):
    def test_accepts_each_http_concurrency_setting_independently(self):
        for setting in ("http.max.workers", "http.request.queue.size"):
            for value in ("1", "4", "1000000"):
                with self.subTest(setting=setting, value=value):
                    validate_config(_config(**{setting: value}))

    def test_rejects_invalid_http_worker_bounds_with_sanitized_error(self):
        self._assert_invalid_http_bound("http.max.workers")

    def test_rejects_invalid_http_queue_bounds_with_sanitized_error(self):
        self._assert_invalid_http_bound("http.request.queue.size")

    def _assert_invalid_http_bound(self, setting):
        for value in ("0", "-1", "1.5", "not-a-number", "nan", "inf", 1.5):
            with self.subTest(setting=setting, value=value):
                with self.assertRaises(ValueError) as raised:
                    validate_config(_config(**{setting: value}))

                self.assertEqual(
                    f"[app].{setting} must be a positive integer",
                    str(raised.exception),
                )

    def test_accepts_each_http_timeout_setting_independently(self):
        for setting in (
            "http.socket.timeout.seconds",
            "http.shutdown.timeout.seconds",
        ):
            for value in ("0.001", "5", "1000000.5"):
                with self.subTest(setting=setting, value=value):
                    validate_config(_config(**{setting: value}))

    def test_rejects_invalid_http_socket_timeout_with_sanitized_error(self):
        self._assert_invalid_http_timeout("http.socket.timeout.seconds")

    def test_rejects_invalid_http_shutdown_timeout_with_sanitized_error(self):
        self._assert_invalid_http_timeout("http.shutdown.timeout.seconds")

    def _assert_invalid_http_timeout(self, setting):
        for value in ("0", "-1", "not-a-number", "nan", "inf", "-inf"):
            with self.subTest(setting=setting, value=value):
                with self.assertRaises(ValueError) as raised:
                    validate_config(_config(**{setting: value}))

                self.assertEqual(
                    f"[app].{setting} must be a positive finite number",
                    str(raised.exception),
                )

    def test_accepts_valid_health_boundary_combinations(self):
        validate_config(
            _config(
                **{
                    "health.failure.window.checks": "4",
                    "health.failure.minimum.checks": "4",
                    "health.failure.threshold": "1",
                    "check.interval.seconds": "15",
                    "health.kafka.degraded.after.seconds": "15.1",
                    "health.kafka.unhealthy.after.seconds": "15.2",
                    "sr.check.interval.seconds": "60",
                    "sr.check.timeout.seconds": "59.9",
                    "health.sr.degraded.after.seconds": "60.1",
                    "health.sr.unhealthy.after.seconds": "60.2",
                }
            )
        )

    def test_rejects_invalid_health_combinations(self):
        cases = (
            {"health.failure.window.checks": "0"},
            {"health.failure.minimum.checks": "21"},
            {"health.failure.threshold": "0"},
            {"health.failure.threshold": "nan"},
            {"health.max.diagnostic.components": "0"},
            {"health.kafka.degraded.after.seconds": "15"},
            {
                "health.kafka.degraded.after.seconds": "60",
                "health.kafka.unhealthy.after.seconds": "60",
            },
            {"health.sr.degraded.after.seconds": "60"},
            {
                "health.sr.degraded.after.seconds": "120",
                "health.sr.unhealthy.after.seconds": "120",
            },
            {"sr.check.timeout.seconds": "0"},
            {"sr.check.timeout.seconds": "60"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                validate_config(_config(**overrides))

    def test_sr_timeout_must_be_positive_finite_and_below_interval(self):
        for timeout in ("0", "-1", "60", "61", "nan", "inf"):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(
                ValueError, "sr.check.timeout.seconds"
            ):
                validate_config(
                    _config(
                        **{
                            "sr.check.interval.seconds": "60",
                            "sr.check.timeout.seconds": timeout,
                        }
                    )
                )

    def test_sr_interval_must_be_finite_and_within_supported_bounds(self):
        for interval in ("0", "-1", "9.999", "3600.001", "nan", "inf"):
            with self.subTest(interval=interval), self.assertRaisesRegex(
                ValueError, "sr.check.interval.seconds"
            ):
                validate_config(
                    _config(
                        **{
                            "sr.check.interval.seconds": interval,
                            "sr.check.timeout.seconds": "1",
                        }
                    )
                )

    def test_accepts_sr_timeout_and_interval_supported_boundaries(self):
        for interval, timeout in (("10", "9.999"), ("3600", "3599.999")):
            with self.subTest(interval=interval, timeout=timeout):
                validate_config(
                    _config(
                        **{
                            "sr.check.interval.seconds": interval,
                            "sr.check.timeout.seconds": timeout,
                            "health.sr.degraded.after.seconds": "3601",
                            "health.sr.unhealthy.after.seconds": "3602",
                        }
                    )
                )

    def test_rejects_plain_http_schema_registry_without_echoing_credentials(self):
        config = _config()
        secret = "do-not-echo"
        config["schema_registry"]["url"] = f"http://user:{secret}@registry.invalid"

        with self.assertRaises(ValueError) as raised:
            validate_config(config)

        self.assertIn("HTTPS", str(raised.exception))
        self.assertNotIn(secret, str(raised.exception))

    def test_non_default_sr_timeout_reaches_startup_and_periodic_client(self):
        config = _config(
            **{
                "sr.check.interval.seconds": "60",
                "sr.check.timeout.seconds": "3.25",
            }
        )
        producer = Mock()

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity", create=True),
            patch("src.main.validate_kafka_startup_client", create=True),
            patch(
                "src.main.validate_schema_registry_startup_client", create=True
            ),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=Mock()),
            patch("src.main.ensure_topic"),
            patch("src.main.SchemaRegistryClient", return_value=object()) as client,
            patch("src.main.create_producer", return_value=(producer, object())),
            patch(
                "src.main.sync_topic_partitions",
                side_effect=RuntimeError("stop after client construction"),
            ),
            patch("src.main.configure_health_state"),
        ):
            with self.assertRaises(CanaryError):
                run()

        client.assert_called_once_with(
            {**config["schema_registry"], "timeout": 3.25}
        )


if __name__ == "__main__":
    unittest.main()
