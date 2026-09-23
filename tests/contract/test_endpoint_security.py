"""Contracts for read-only endpoint methods and response redaction."""

from io import BytesIO
import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from src import metrics


class _CapturingServer:
    handler = None

    def __init__(self, address, handler):
        type(self).handler = handler

    def serve_forever(self):
        raise AssertionError("contract test must not start a server")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target

    def start(self):
        pass


def _result(status, **values):
    fields = {
        "status": SimpleNamespace(value=status),
        "http_code": 200,
        "timestamp": "2026-01-01T00:00:00Z",
        "message": "safe",
    }
    fields.update(values)
    return SimpleNamespace(**fields)


class EndpointSecurityContractTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.generate = Mock(return_value=b"# HELP safe_metric safe\nsafe_metric 1\n")
        cls.live = Mock(
            return_value=_result(
                "live", scheduler_heartbeat_age_seconds=0.1, shutdown_started=False
            )
        )
        cls.health = Mock(
            return_value=_result("healthy", uptime_seconds=1.0, checks={})
        )
        cls.ready = Mock(return_value=_result("ready", checks={}))
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
            patch("prometheus_client.generate_latest", cls.generate),
            patch("src.health.get_liveness_status", cls.live),
            patch("src.health.get_health_status", cls.health),
            patch("src.health.get_readiness_status", cls.ready),
        ):
            cls.service = metrics.start_metrics_server(8000)

    @classmethod
    def tearDownClass(cls):
        cls.service._server._request_executor.shutdown(wait=True)

    def setUp(self):
        for endpoint in (self.generate, self.live, self.health, self.ready):
            endpoint.reset_mock()

    def _request(self, method, path):
        handler = _CapturingServer.handler.__new__(_CapturingServer.handler)
        handler.path = path
        handler.wfile = BytesIO()
        response = {"headers": {}}
        handler.send_response = lambda code: response.setdefault("code", code)
        handler.send_header = lambda name, value: response["headers"].__setitem__(
            name, value
        )
        handler.end_headers = lambda: None
        getattr(handler, f"do_{method}")()
        response["raw_body"] = handler.wfile.getvalue()
        return response

    def test_get_remains_supported_for_every_endpoint(self):
        for path in ("/metrics", "/live", "/health", "/ready"):
            with self.subTest(path=path):
                response = self._request("GET", path)
                self.assertEqual(200, response["code"])
                self.assertTrue(response["raw_body"])

    def test_mutation_methods_reject_without_endpoint_work(self):
        endpoint_mocks = (self.generate, self.live, self.health, self.ready)
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            for path in ("/metrics", "/live", "/health", "/ready"):
                with self.subTest(method=method, path=path):
                    response = self._request(method, path)
                    self.assertEqual(405, response["code"])
                    self.assertEqual("GET, HEAD", response["headers"]["Allow"])
                    self.assertLess(len(response["raw_body"]), 128)
        self.assertTrue(all(mock.call_count == 0 for mock in endpoint_mocks))

    def test_head_is_bodyless_read_only_get(self):
        for path, endpoint in (
            ("/metrics", self.generate),
            ("/live", self.live),
            ("/health", self.health),
            ("/ready", self.ready),
        ):
            with self.subTest(path=path):
                response = self._request("HEAD", path)
                self.assertEqual(200, response["code"])
                self.assertEqual(b"", response["raw_body"])
                endpoint.assert_called_once()
                endpoint.reset_mock()

    def test_probe_data_is_bounded_and_prohibited_fields_are_removed(self):
        self.health.return_value = _result(
            "healthy",
            uptime_seconds=1.0,
            message="password=hunter2 at /srv/private Traceback",
            checks={
                "kafka_record": "customer payload",
                "configuration": "bootstrap.servers=internal",
                "certificate_path": "/run/tls/private.pem",
                "detail": "/opt/cloud/config.ini",
                "other_detail": "client.id=private-client",
                "safe": "x" * 10_000,
            },
        )
        response = self._request("GET", "/health")
        rendered = response["raw_body"].decode()

        self.assertLess(len(rendered), 1024)
        for prohibited in (
            "hunter2",
            "/srv/private",
            "Traceback",
            "customer payload",
            "bootstrap.servers",
            "private.pem",
            "/opt/cloud/config.ini",
            "private-client",
        ):
            self.assertNotIn(prohibited, rendered)
        self.assertEqual("[redacted]", json.loads(rendered)["message"])

    def test_metrics_lines_with_sensitive_data_are_removed(self):
        self.generate.return_value = (
            b"safe_metric 1\n"
            b'leak{value="https://user:pass@example.test"} 1\n'
            b'path{value="/Users/person/private.key"} 1\n'
        )
        rendered = self._request("GET", "/metrics")["raw_body"]

        self.assertEqual(b"safe_metric 1\n", rendered)
        self.assertNotIn(b"user:pass", rendered)
        self.assertNotIn(b"private.key", rendered)
