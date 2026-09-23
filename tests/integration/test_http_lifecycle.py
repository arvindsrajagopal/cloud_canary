"""Integration regressions for explicit bounded HTTP lifecycle ownership."""

import io
import logging
import threading
import unittest
from unittest.mock import Mock, call, patch

from src import metrics
from src.bounded_executor import DaemonThreadPoolExecutor
import src.main as main


class _ListenerThread:
    def __init__(self):
        self.join_timeouts = []

    def join(self, timeout=None):
        self.join_timeouts.append(timeout)


class _CapturingServer:
    instance = None

    def __init__(self, address, handler):
        type(self).instance = self
        self.handler = handler

    def serve_forever(self):
        return None

    def shutdown(self):
        return None

    def server_close(self):
        return None

    def finish_request(self, request, _client_address):
        request()

    def shutdown_request(self, request):
        request.close()

    def handle_error(self, _request, _client_address):
        raise AssertionError("request handler unexpectedly failed")


class _NoopThread:
    def __init__(self, *, target, daemon):
        self.target = target

    def start(self):
        return None


class _BlockingServer:
    def __init__(self):
        self._accepting = threading.Event()
        self._accepting.set()
        self._request_executor = Mock()
        self.shutdown_started = threading.Event()
        self.release_shutdown = threading.Event()
        self.socket_closed = False
        self.active_requests_closed = False

    def shutdown(self):
        self.shutdown_started.set()
        self.release_shutdown.wait(1)

    def server_close(self):
        self.socket_closed = True

    def _close_active_requests(self):
        self.active_requests_closed = True


class _ExecutorServer:
    def __init__(self):
        self._accepting = threading.Event()
        self._accepting.set()
        self._request_executor = DaemonThreadPoolExecutor(
            max_workers=1, thread_name_prefix="canary-http-test"
        )
        self.socket_closed = False
        self.active_requests_closed = False

    def shutdown(self):
        return None

    def server_close(self):
        self.socket_closed = True

    def _close_active_requests(self):
        self.active_requests_closed = True


class _FakeSocket:
    def __init__(self, path):
        self._input = io.BytesIO(
            f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
        )
        self.output = io.BytesIO()

    def makefile(self, mode, buffering=None):
        return self._input if "r" in mode else self.output


class _BlockingRequest:
    def __init__(self):
        self.started = threading.Event()
        self.closed = threading.Event()
        self.shutdown_called = False

    def __call__(self):
        self.started.set()
        self.closed.wait(1)

    def shutdown(self, _how):
        self.shutdown_called = True
        self.closed.set()

    def close(self):
        self.closed.set()


class _DependencyHandler(logging.Handler):
    def __init__(self, producer):
        super().__init__()
        self._producer = producer

    def emit(self, record):
        self._producer.produce()
        self._producer.poll(0)


class HTTPLifecycleTests(unittest.TestCase):
    def test_shutdown_stops_acceptance_closes_socket_and_does_not_overrun(self):
        server = _BlockingServer()
        listener = _ListenerThread()
        service = metrics.HTTPService(server, listener)
        clock = Mock(side_effect=[10.0, 10.5, 11.0])

        service.shutdown(11.0, monotonic_clock=clock)

        self.assertFalse(server._accepting.is_set())
        self.assertTrue(server.shutdown_started.is_set())
        self.assertTrue(server.socket_closed)
        self.assertTrue(server.active_requests_closed)
        self.assertEqual(
            call(wait=False, cancel_futures=True),
            server._request_executor.shutdown.call_args_list[0],
        )
        self.assertIn(
            call(wait=True), server._request_executor.shutdown.call_args_list
        )
        self.assertLessEqual(sum(listener.join_timeouts), 1.0)
        server.release_shutdown.set()

    def test_blocked_request_closes_connections_then_abandons_worker(self):
        server = _ExecutorServer()
        release = threading.Event()
        started = threading.Event()

        def blocked_request():
            started.set()
            release.wait(2)

        server._request_executor.submit(blocked_request)
        self.assertTrue(started.wait(1))
        service = metrics.HTTPService(server, _ListenerThread())
        try:
            service.shutdown(0.0, monotonic_clock=lambda: 0.0)
            self.assertTrue(server.socket_closed)
            self.assertTrue(server.active_requests_closed)
            self.assertTrue(server._request_executor._shutdown)
            self.assertTrue(server._request_executor._threads[0].daemon)
            self.assertTrue(server._request_executor._threads[0].is_alive())
        finally:
            release.set()
            server._request_executor.shutdown(wait=True)

    def test_deadline_closes_tracked_active_connection(self):
        with patch("http.server.HTTPServer", _CapturingServer):
            service = metrics.start_metrics_server(8000, max_workers=1)
        request = _BlockingRequest()
        queued_request = _BlockingRequest()
        service._server.process_request(request, ("127.0.0.1", 1))
        self.assertTrue(request.started.wait(1))
        service._server.process_request(queued_request, ("127.0.0.1", 2))

        service.shutdown(0.0, monotonic_clock=lambda: 0.0)

        self.assertTrue(request.shutdown_called)
        self.assertTrue(request.closed.is_set())
        self.assertTrue(queued_request.shutdown_called)
        self.assertTrue(queued_request.closed.is_set())
        self.assertFalse(service._server._active_requests)
        service._server._request_executor.shutdown(wait=True)

    def test_main_marks_health_unavailable_before_http_cleanup(self):
        events = []
        health_store = Mock()
        health_store.begin_shutdown.side_effect = lambda: events.append("health")
        service = Mock()
        service.shutdown.side_effect = lambda timeout: events.append("http")
        owner = {"health_store": health_store, "service": service}

        with patch.object(main, "_shutdown_deadline", return_value=17.5):
            main._shutdown_http_service(owner)

        self.assertEqual(["health", "http"], events)
        service.shutdown.assert_called_once_with(17.5)
        self.assertNotIn("service", owner)

    def test_http_request_workers_are_independent_abandonable_daemon_workers(self):
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
        ):
            service = metrics.start_metrics_server(
                8000, max_workers=2, request_queue_size=1
            )

        executor = service._server._request_executor
        try:
            self.assertIsInstance(executor, DaemonThreadPoolExecutor)
            self.assertTrue(all(thread.daemon for thread in executor._threads))
            self.assertTrue(
                all(
                    thread.name.startswith("canary-http_")
                    for thread in executor._threads
                )
            )
        finally:
            executor.shutdown(wait=True)

    def test_http_failure_logging_cannot_reach_dependency_handler(self):
        producer = Mock()
        dependency_handler = _DependencyHandler(producer)
        root_logger = logging.getLogger()
        try:
            with (
                patch("http.server.HTTPServer", _CapturingServer),
                patch.object(metrics.threading, "Thread", _NoopThread),
            ):
                service = metrics.start_metrics_server(8000)

            root_logger.addHandler(dependency_handler)
            request = _FakeSocket("/missing")
            service._server.handler(request, ("127.0.0.1", 1), service._server)
        finally:
            root_logger.removeHandler(dependency_handler)
            service._server._request_executor.shutdown(wait=True)

        producer.produce.assert_not_called()
        producer.poll.assert_not_called()


if __name__ == "__main__":
    unittest.main()
