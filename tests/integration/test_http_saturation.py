"""Integration regressions for saturated HTTP admission."""

import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch

from src import metrics


class _CapturingHTTPServer:
    instance = None

    def __init__(self, address, handler):
        _CapturingHTTPServer.instance = self
        self.handler = handler
        self.finished = []
        self.closed = []

    def serve_forever(self):
        return None

    def finish_request(self, request, client_address):
        self.finished.append(request)
        request()

    def shutdown_request(self, request):
        self.closed.append(request)

    def handle_error(self, request, client_address):
        raise AssertionError("request handler unexpectedly failed")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target
        self.daemon = daemon

    def start(self):
        return None


class _RejectedSocket:
    def __init__(self, *, fail_write=False):
        self.response = bytearray()
        self._fail_write = fail_write

    def sendall(self, data):
        if self._fail_write:
            raise OSError("request content and exception must not escape")
        self.response.extend(data)


class HTTPSaturationTests(TestCase):
    def _start_server(self):
        with (
            patch("http.server.HTTPServer", _CapturingHTTPServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
        ):
            metrics.start_metrics_server(
                8000, max_workers=1, request_queue_size=0
            )
        return _CapturingHTTPServer.instance

    def test_saturation_counts_and_rejects_without_handler_or_health_work(self):
        release = threading.Event()
        started = threading.Event()

        def occupy_worker():
            started.set()
            release.wait(2)

        overload_metric = MagicMock()
        kafka_health = MagicMock()
        sr_health = MagicMock()
        with (
            patch.object(metrics, "HTTP_OVERLOAD_TOTAL", overload_metric),
            patch("src.health.get_health_status", kafka_health),
            patch("src.health.get_readiness_status", sr_health),
        ):
            server = self._start_server()
            server.process_request(occupy_worker, ("127.0.0.1", 1))
            self.assertTrue(started.wait(1))

            rejected = _RejectedSocket()
            try:
                server.process_request(rejected, ("127.0.0.1", 2))
            finally:
                release.set()
                server._request_executor.shutdown(wait=True)

        overload_metric.labels.assert_called_once_with(host=metrics.HOST)
        overload_metric.labels.return_value.inc.assert_called_once_with()
        self.assertNotIn(rejected, server.finished)
        self.assertEqual(1, server.closed.count(rejected))
        self.assertEqual(0, kafka_health.call_count)
        self.assertEqual(0, sr_health.call_count)
        response = bytes(rejected.response)
        self.assertTrue(response.startswith(b"HTTP/1.1 503 Service Unavailable"))
        self.assertLessEqual(len(response), 256)
        self.assertIn(b"application/json", response)
        self.assertNotIn(b"127.0.0.1", response)

    def test_rejection_write_failure_is_sanitized_and_does_not_enqueue(self):
        server = self._start_server()
        self.assertTrue(server._request_slots.acquire(blocking=False))
        rejected = _RejectedSocket(fail_write=True)

        try:
            with patch.object(metrics, "HTTP_OVERLOAD_TOTAL", MagicMock()):
                server.process_request(rejected, ("127.0.0.1", 2))
        finally:
            server._request_slots.release()
            server._request_executor.shutdown(wait=True)

        self.assertEqual([], server.finished)
        self.assertEqual([rejected], server.closed)

    def test_overload_metric_has_only_the_fixed_host_label(self):
        self.assertEqual(("host",), metrics.HTTP_OVERLOAD_TOTAL._labelnames)
