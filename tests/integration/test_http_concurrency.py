"""Integration regressions for bounded HTTP request execution."""

import io
import threading
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from src import metrics
import src.main as main


class _CapturingHTTPServer:
    """Minimal server base used to inspect bounded admission without sockets."""

    instance = None

    def __init__(self, address, handler):
        _CapturingHTTPServer.instance = self
        self.handler = handler
        self.finished = []
        self.rejected = []

    def serve_forever(self):
        return None

    def finish_request(self, request, client_address):
        self.finished.append(request)
        if callable(request):
            request()
        else:
            self.handler(request, client_address, self)

    def shutdown_request(self, request):
        if request not in self.finished:
            self.rejected.append(request)

    def handle_error(self, request, client_address):
        raise AssertionError("request handler unexpectedly failed")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target
        self.daemon = daemon

    def start(self):
        return None


class _FakeSocket:
    def __init__(self, path):
        self._input = io.BytesIO(
            f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
        )
        self.output = io.BytesIO()

    def makefile(self, mode, buffering=None):
        return self._input if "r" in mode else self.output

    def sendall(self, data):
        self.output.write(data)

    def shutdown(self, how):
        return None

    def close(self):
        return None


class HTTPConcurrencyTests(TestCase):
    def test_active_handlers_and_waiting_work_stay_within_bounds(self):
        release = threading.Event()
        started = threading.Condition()
        active = 0
        peak_active = 0

        def blocking_request():
            nonlocal active, peak_active
            with started:
                active += 1
                peak_active = max(peak_active, active)
                started.notify_all()
            release.wait(2)
            with started:
                active -= 1

        with (
            patch("http.server.HTTPServer", _CapturingHTTPServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
        ):
            metrics.start_metrics_server(
                8000, max_workers=2, request_queue_size=2
            )

        server = _CapturingHTTPServer.instance
        requests = [lambda: blocking_request() for _ in range(5)]
        try:
            for request in requests:
                server.process_request(request, ("127.0.0.1", 1))

            with started:
                self.assertTrue(started.wait_for(lambda: active == 2, timeout=1))

            self.assertEqual(2, peak_active)
            self.assertEqual([requests[-1]], server.rejected)
            self.assertEqual(2, len(server.finished))
        finally:
            release.set()
            server._request_executor.shutdown(wait=True)

        self.assertEqual(4, len(server.finished))
        self.assertEqual(2, peak_active)

    def test_slow_metrics_does_not_block_live_with_worker_available(self):
        metrics_started = threading.Event()
        release_metrics = threading.Event()
        live_completed = threading.Event()

        def slow_metrics():
            metrics_started.set()
            release_metrics.wait(2)
            return b"metric 1\n"

        liveness = SimpleNamespace(
            http_code=200,
            status=SimpleNamespace(value="live"),
            timestamp=1.0,
            scheduler_heartbeat_age_seconds=0.0,
            shutdown_started=False,
            message="live",
        )
        metrics_request = _FakeSocket("/metrics")
        live_request = _FakeSocket("/live")

        with (
            patch("http.server.HTTPServer", _CapturingHTTPServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
            patch("prometheus_client.generate_latest", side_effect=slow_metrics),
            patch(
                "src.health.get_liveness_status",
                side_effect=lambda: (live_completed.set() or liveness),
            ),
        ):
            metrics.start_metrics_server(
                8000, max_workers=2, request_queue_size=1
            )

        server = _CapturingHTTPServer.instance
        try:
            server.process_request(metrics_request, ("127.0.0.1", 1))
            self.assertTrue(metrics_started.wait(1))
            server.process_request(live_request, ("127.0.0.1", 2))
            self.assertTrue(live_completed.wait(1))
        finally:
            release_metrics.set()
            server._request_executor.shutdown(wait=True)

        self.assertIn(b"200 OK", live_request.output.getvalue())

    def test_production_startup_passes_configured_http_bounds(self):
        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {
                "http.max.workers": "7",
                "http.request.queue.size": "11",
            },
        }

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.configure_health_state"),
            patch("src.main._run_startup_dependency"),
            patch(
                "src.main.start_metrics_server",
                side_effect=RuntimeError("stop after HTTP startup"),
            ) as start_server,
        ):
            with self.assertRaisesRegex(RuntimeError, "stop after HTTP startup"):
                main._run_lifecycle()

        self.assertEqual(7, start_server.call_args.kwargs["max_workers"])
        self.assertEqual(
            11, start_server.call_args.kwargs["request_queue_size"]
        )
