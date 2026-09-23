"""Contract regressions for accepted HTTP connection deadlines."""

import threading
from unittest import TestCase
from unittest.mock import patch

from src import metrics
import src.main as main


class _FakeSocket:
    def __init__(self):
        self.timeout = None

    def settimeout(self, timeout):
        self.timeout = timeout


class _CapturingServer:
    instance = None

    def __init__(self, address, handler):
        type(self).instance = self
        self.socket = _FakeSocket()
        self.handler_timeout = None
        self.handled = threading.Event()

    def get_request(self):
        return self.socket, ("127.0.0.1", 1234)

    def serve_forever(self):
        return None

    def finish_request(self, request, client_address):
        self.handler_timeout = request.timeout
        self.handled.set()

    def shutdown_request(self, request):
        return None

    def handle_error(self, request, client_address):
        raise AssertionError("request handler unexpectedly failed")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target

    def start(self):
        return None


class HTTPSocketDeadlineTests(TestCase):
    def test_accepted_socket_has_timeout_before_handler_execution(self):
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
        ):
            metrics.start_metrics_server(8000, socket_timeout=2.5)

        server = _CapturingServer.instance
        request, client_address = server.get_request()
        self.assertIs(request, server.socket)
        self.assertEqual(2.5, request.timeout)

        try:
            server.process_request(request, client_address)
            self.assertTrue(server.handled.wait(1))
        finally:
            server._request_executor.shutdown(wait=True)

        self.assertEqual(2.5, server.handler_timeout)

    def test_production_startup_passes_configured_socket_timeout(self):
        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {"http.socket.timeout.seconds": "3.25"},
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

        self.assertEqual(3.25, start_server.call_args.kwargs["socket_timeout"])


if __name__ == "__main__":
    import unittest

    unittest.main()
