"""HTTP contract for initial post-warmup readiness."""

from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import patch

from src import health, metrics
from src.health_state import HealthStateStore


class _CapturingServer:
    handler = None

    def __init__(self, address, handler):
        type(self).handler = handler

    def serve_forever(self):
        raise AssertionError("contract test must not start a server")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target
        self.daemon = daemon

    def start(self):
        pass


class ReadinessHTTPContractTests(TestCase):
    def setUp(self) -> None:
        self.original_store = health._health_state_store
        self.original_shutdown_requested = health._shutdown_requested
        self.store = HealthStateStore((0, 1), warmup_checks=1)
        health.configure_health_state(self.store)

    def tearDown(self) -> None:
        health._health_state_store = self.original_store
        health._shutdown_requested = self.original_shutdown_requested

    def _request_ready(self):
        _CapturingServer.handler = None
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
        ):
            metrics.start_metrics_server(8000)

        handler = _CapturingServer.handler.__new__(_CapturingServer.handler)
        handler.path = "/ready"
        handler.wfile = BytesIO()
        response = {}
        handler.send_response = lambda code: response.setdefault("code", code)
        handler.send_header = lambda name, value: response.setdefault(name, value)
        handler.end_headers = lambda: None
        handler.send_error = lambda code, message: response.update(
            code=code, error=message
        )
        handler.do_GET()
        response["body"] = json.loads(handler.wfile.getvalue())
        return response

    def test_ready_requires_initial_sr_and_every_partitions_post_warmup_success(self):
        response = self._request_ready()
        self.assertEqual(503, response["code"])
        self.assertFalse(response["body"]["checks"]["schema_registry_validated"])

        self.store.record_schema_registry_result(success=True)
        for partition in (0, 1):
            self.store.record_partition_result(partition, success=True)
        self.store.record_partition_result(0, success=True)
        self.store.record_partition_result(1, success=False)

        response = self._request_ready()
        self.assertEqual(503, response["code"])
        self.assertEqual("not_ready", response["body"]["status"])
        self.assertEqual([1], response["body"]["checks"]["incomplete_partitions"])

        self.store.record_partition_result(1, success=True)

        response = self._request_ready()
        self.assertEqual(200, response["code"])
        self.assertEqual("ready", response["body"]["status"])
        self.assertEqual([], response["body"]["checks"]["incomplete_partitions"])


if __name__ == "__main__":
    import unittest

    unittest.main()
