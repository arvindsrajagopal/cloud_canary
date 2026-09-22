"""HTTP contract for independent dependency, liveness, and readiness outcomes."""

from io import BytesIO
import json
from unittest import TestCase
from unittest.mock import patch

from src import health, metrics
from src.health_state import HealthStateStore


class MutableClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


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


class DependencyHealthHTTPContractTests(TestCase):
    def setUp(self) -> None:
        self.original_store = health._health_state_store
        self.original_shutdown_requested = health._shutdown_requested
        self.monotonic = MutableClock()
        self.wall = MutableClock(1_000.0)
        self.store = HealthStateStore(
            (0,),
            minimum_checks=1,
            warmup_checks=0,
            max_diagnostic_components=2,
            kafka_check_interval=10.0,
            kafka_degraded_after=60.0,
            kafka_unhealthy_after=300.0,
            liveness_scheduler_max_staleness=10.0,
            monotonic_clock=self.monotonic,
            wall_clock=self.wall,
        )
        self.store.record_scheduler_heartbeat()
        self.store.record_schema_registry_result(success=True)
        self.store.record_partition_result(0, success=True)
        health.configure_health_state(self.store, shutdown_requested=lambda: False)

    def tearDown(self) -> None:
        health._health_state_store = self.original_store
        health._shutdown_requested = self.original_shutdown_requested

    def _request(self, path, *, health_error=None, readiness_error=None):
        _CapturingServer.handler = None
        with (
            patch.object(
                health,
                "get_health_status",
                side_effect=health_error,
                wraps=health.get_health_status,
            ),
            patch.object(
                health,
                "get_readiness_status",
                side_effect=readiness_error,
                wraps=health.get_readiness_status,
            ),
        ):
            # Rebuild the handler so its function-local imports observe patches.
            with (
                patch("http.server.HTTPServer", _CapturingServer),
                patch.object(metrics.threading, "Thread", _NoopThread),
            ):
                metrics.start_metrics_server(8000)
            handler_class = _CapturingServer.handler
            handler = handler_class.__new__(handler_class)
            handler.path = path
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

    def _assert_independent_successes(self):
        health_response = self._request("/health")
        live_response = self._request("/live")
        ready_response = self._request("/ready")

        self.assertEqual(503, health_response["code"])
        self.assertEqual(200, live_response["code"])
        self.assertEqual("live", live_response["body"]["status"])
        self.assertEqual(200, ready_response["code"])
        self.assertEqual("ready", ready_response["body"]["status"])

    def test_dependency_outage_only_fails_health_after_readiness_latches(self):
        self.store.record_partition_result(
            0, success=False, failure="BROKER_SERVICE:password=must-not-leak"
        )
        self.store.record_schema_registry_result(
            success=False,
            deterministic_failure=True,
            failure="AUTHENTICATION:token=must-not-leak",
        )

        self._assert_independent_successes()
        body = self._request("/health")["body"]
        self.assertLessEqual(len(body["checks"]["components"]), 2)
        self.assertNotIn("must-not-leak", json.dumps(body))

    def test_capacity_degradation_only_fails_health(self):
        self.store.record_scheduler_capacity(10.0)

        self._assert_independent_successes()
        health_body = self._request("/health")["body"]
        self.assertFalse(health_body["checks"]["scheduling_capacity_healthy"])

    def test_endpoint_evaluation_errors_are_sanitized(self):
        secret = "password=highly-sensitive"
        health_response = self._request(
            "/health", health_error=RuntimeError(secret)
        )
        ready_response = self._request(
            "/ready", readiness_error=RuntimeError(secret)
        )

        self.assertEqual(503, health_response["code"])
        self.assertEqual("unhealthy", health_response["body"]["status"])
        self.assertEqual(503, ready_response["code"])
        self.assertEqual("not_ready", ready_response["body"]["status"])
        self.assertNotIn(secret, str(health_response))
        self.assertNotIn(secret, str(ready_response))


if __name__ == "__main__":
    import unittest

    unittest.main()
