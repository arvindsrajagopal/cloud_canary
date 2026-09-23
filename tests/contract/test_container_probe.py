"""Contract regressions for the configuration-aware container probe."""

import ssl
import urllib.error
from pathlib import Path
from unittest import TestCase
from unittest.mock import mock_open, patch

from src import container_probe


class _Response:
    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.closed = True

    def getcode(self) -> int:
        return self.status


class ContainerProbeContractTests(TestCase):
    def test_configured_http_port_and_live_path_are_used(self):
        config = "[app]\nmetrics.port=9123\nmetrics.ssl.enabled=false\n"

        with patch("builtins.open", mock_open(read_data=config)):
            url = container_probe.load_probe_url("config/config.ini")

        self.assertEqual("http://localhost:9123/live", url)

    def test_configured_https_mode_and_port_are_used(self):
        config = "[app]\nmetrics.port=9443\nmetrics.ssl.enabled=true\n"

        with patch("builtins.open", mock_open(read_data=config)):
            url = container_probe.load_probe_url("config/config.ini")

        self.assertEqual("https://localhost:9443/live", url)

    def test_https_uses_certificate_verifying_default_context(self):
        response = _Response()
        calls = []

        def opener(request, **kwargs):
            calls.append((request, kwargs))
            return response

        self.assertTrue(
            container_probe.request_liveness(
                "https://localhost:9443/live", opener=opener
            )
        )

        request, kwargs = calls[0]
        context = kwargs["context"]
        self.assertEqual("https://localhost:9443/live", request.full_url)
        self.assertEqual(container_probe.PROBE_TIMEOUT_SECONDS, kwargs["timeout"])
        self.assertTrue(context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, context.verify_mode)
        self.assertTrue(response.closed)

    def test_https_certificate_failure_has_no_insecure_retry(self):
        calls = []

        def opener(request, **kwargs):
            calls.append((request, kwargs))
            raise ssl.SSLCertVerificationError("certificate verify failed")

        with self.assertRaises(ssl.SSLCertVerificationError):
            container_probe.request_liveness(
                "https://localhost:9443/live", opener=opener
            )

        self.assertEqual(1, len(calls))
        context = calls[0][1]["context"]
        self.assertTrue(context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, context.verify_mode)

    def test_http_probe_does_not_add_tls_or_credentials(self):
        response = _Response()
        calls = []

        def opener(request, **kwargs):
            calls.append((request, kwargs))
            return response

        self.assertTrue(
            container_probe.request_liveness(
                "http://localhost:9123/live", opener=opener
            )
        )

        request, kwargs = calls[0]
        self.assertEqual("GET", request.get_method())
        self.assertEqual({}, dict(request.header_items()))
        self.assertNotIn("context", kwargs)

    def test_failures_return_bounded_nonzero_exit_code(self):
        with (
            patch.object(
                container_probe,
                "load_probe_url",
                return_value="http://localhost:8000/live",
            ),
            patch.object(
                container_probe,
                "request_liveness",
                side_effect=urllib.error.URLError("unavailable"),
            ),
        ):
            self.assertEqual(1, container_probe.main())

        with (
            patch.object(
                container_probe,
                "load_probe_url",
                return_value="http://localhost:8000/live",
            ),
            patch.object(container_probe, "request_liveness", return_value=False),
        ):
            self.assertEqual(1, container_probe.main())

    def test_probe_rejects_a_timeout_above_its_bound(self):
        with self.assertRaises(ValueError):
            container_probe.request_liveness(
                "http://localhost:8000/live",
                timeout=container_probe.PROBE_TIMEOUT_SECONDS + 0.001,
            )

    def test_success_returns_zero(self):
        with (
            patch.object(
                container_probe,
                "load_probe_url",
                return_value="http://localhost:8000/live",
            ),
            patch.object(container_probe, "request_liveness", return_value=True),
        ):
            self.assertEqual(0, container_probe.main())

    def test_docker_healthcheck_has_no_url_or_credentials_in_arguments(self):
        dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
        healthcheck = next(
            line.strip() for line in dockerfile.splitlines() if line.startswith("  CMD [")
        )

        self.assertEqual('CMD ["python", "-m", "src.container_probe"]', healthcheck)
        self.assertNotIn("/health", healthcheck)
        self.assertNotIn("http", healthcheck)
        self.assertNotIn("user", healthcheck.lower())
        self.assertNotIn("password", healthcheck.lower())


if __name__ == "__main__":
    import unittest

    unittest.main()
