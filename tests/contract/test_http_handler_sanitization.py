"""Contract regressions for sanitized HTTP handler failures."""

from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import patch

from src import metrics


_SECRET = "password=hunter2 https://user:pass@example.test /srv/private Traceback"


class _CapturingServer:
    instance = None
    handler = None

    def __init__(self, address, handler):
        type(self).instance = self
        type(self).handler = handler

    def serve_forever(self):
        raise AssertionError("contract test must not start a server")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target

    def start(self):
        return None


class _BrokenWriter:
    def write(self, body):
        raise BrokenPipeError(_SECRET)


class HTTPHandlerSanitizationTests(TestCase):
    @classmethod
    def setUpClass(cls):
        failure = RuntimeError(_SECRET)
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
            patch("prometheus_client.generate_latest", side_effect=failure),
            patch("src.health.get_liveness_status", side_effect=failure),
            patch("src.health.get_health_status", side_effect=failure),
            patch("src.health.get_readiness_status", side_effect=failure),
        ):
            metrics.start_metrics_server(8000)

    @classmethod
    def tearDownClass(cls):
        _CapturingServer.instance._request_executor.shutdown(wait=True)

    def _request(self, target, writer=None):
        handler = _CapturingServer.handler.__new__(_CapturingServer.handler)
        handler.path = target
        handler.wfile = writer or BytesIO()
        response = {"headers": {}}
        handler.send_response = lambda code: response.setdefault("code", code)
        handler.send_header = lambda name, value: response["headers"].setdefault(
            name, value
        )
        handler.end_headers = lambda: None

        with patch.object(metrics.log, "error") as error_log:
            handler.do_GET()

        response["log"] = str(error_log.call_args_list)
        if isinstance(handler.wfile, BytesIO):
            response["body"] = json.loads(handler.wfile.getvalue())
        return response

    def test_endpoint_failures_return_bounded_sanitized_json(self):
        expected = {
            "/metrics": (500, "error"),
            "/live": (503, "not_live"),
            "/health": (503, "unhealthy"),
            "/ready": (503, "not_ready"),
        }

        for target, (status, state) in expected.items():
            with self.subTest(target=target):
                response = self._request(target)
                rendered = json.dumps(response)
                self.assertEqual(status, response["code"])
                self.assertEqual(
                    "application/json", response["headers"]["Content-Type"]
                )
                self.assertEqual(state, response["body"]["status"])
                self.assertLess(len(rendered), 512)
                for sensitive in ("hunter2", "user:pass", "/srv/private", "Traceback"):
                    self.assertNotIn(sensitive, rendered)

    def test_unknown_target_is_not_reflected_in_response_or_log(self):
        response = self._request("/private/path?token=hunter2")

        self.assertEqual(404, response["code"])
        self.assertNotIn("private/path", json.dumps(response))
        self.assertNotIn("hunter2", json.dumps(response))

    def test_failure_response_write_error_is_contained(self):
        response = self._request("/metrics", writer=_BrokenWriter())

        self.assertEqual(500, response["code"])
        self.assertNotIn(_SECRET, response["log"])
