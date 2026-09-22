"""HTTP contract regressions for the process-only liveness endpoint."""

from io import BytesIO
import json
import sys
from unittest import TestCase
from unittest.mock import patch

from src import health, metrics
from src.health_state import HealthStateStore
import src.main as main


class MutableClock:
    def __init__(self, value: float) -> None:
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


class LivenessHTTPContractTests(TestCase):
    def setUp(self) -> None:
        self.original_store = health._health_state_store
        self.original_shutdown_requested = health._shutdown_requested
        self.monotonic = MutableClock(100.0)
        self.wall = MutableClock(1_000.0)
        self.store = HealthStateStore(
            (0,),
            liveness_scheduler_max_staleness=10.0,
            monotonic_clock=self.monotonic,
            wall_clock=self.wall,
        )
        self.store.record_scheduler_heartbeat()
        health.configure_health_state(self.store)

    def tearDown(self) -> None:
        health._health_state_store = self.original_store
        health._shutdown_requested = self.original_shutdown_requested

    def _request_live(self, *, liveness_error=None):
        _CapturingServer.handler = None
        with (
            patch("http.server.HTTPServer", _CapturingServer),
            patch.object(metrics.threading, "Thread", _NoopThread),
            patch.object(
                health,
                "get_liveness_status",
                side_effect=liveness_error,
                wraps=health.get_liveness_status,
            ),
        ):
            metrics.start_metrics_server(8000)

        handler = _CapturingServer.handler.__new__(_CapturingServer.handler)
        handler.path = "/live"
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

    def test_dependency_failures_do_not_affect_live(self):
        self.store.record_partition_result(0, success=False, failure="kafka-secret")
        self.store.record_schema_registry_result(
            success=False,
            deterministic_failure=True,
            failure="schema-registry-secret",
        )

        response = self._request_live()

        self.assertEqual(200, response["code"])
        self.assertEqual("application/json", response["Content-Type"])
        self.assertEqual("live", response["body"]["status"])
        self.assertEqual(0.0, response["body"]["scheduler_heartbeat_age_seconds"])
        self.assertFalse(response["body"]["shutdown_started"])
        self.assertNotIn("kafka-secret", str(response["body"]))
        self.assertNotIn("schema-registry-secret", str(response["body"]))

    def test_stale_scheduler_heartbeat_returns_503(self):
        self.monotonic.value = 110.001

        response = self._request_live()

        self.assertEqual(503, response["code"])
        self.assertEqual("not_live", response["body"]["status"])
        self.assertAlmostEqual(
            10.001, response["body"]["scheduler_heartbeat_age_seconds"]
        )
        self.assertFalse(response["body"]["shutdown_started"])

    def test_fatal_internal_state_returns_503_without_details(self):
        self.store.record_fatal_internal("sensitive internal exception detail")

        response = self._request_live()

        self.assertEqual(503, response["code"])
        self.assertEqual("not_live", response["body"]["status"])
        self.assertNotIn("sensitive internal exception detail", str(response["body"]))

    def test_authoritative_shutdown_request_immediately_returns_503(self):
        with (
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main,
                "_shutdown_request_claim",
                [main._shutdown_claim_token],
            ),
            patch.object(main.time, "monotonic", return_value=100.0),
        ):
            main._handle_signal(main.signal.SIGTERM, None)

            response = self._request_live()

        self.assertEqual(503, response["code"])
        self.assertEqual("not_live", response["body"]["status"])
        self.assertTrue(response["body"]["shutdown_started"])

    def test_module_entrypoint_shutdown_request_immediately_returns_503(self):
        with (
            patch.dict(sys.modules, {"src.main": None, "__main__": main}),
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main,
                "_shutdown_request_claim",
                [main._shutdown_claim_token],
            ),
            patch.object(main.time, "monotonic", return_value=100.0),
        ):
            health.configure_health_state(self.store)
            main._handle_signal(main.signal.SIGTERM, None)

            response = self._request_live()

        self.assertEqual(503, response["code"])
        self.assertEqual("not_live", response["body"]["status"])
        self.assertTrue(response["body"]["shutdown_started"])

    def test_liveness_evaluation_error_is_sanitized(self):
        response = self._request_live(
            liveness_error=RuntimeError("password=highly-sensitive")
        )

        self.assertEqual(503, response["code"])
        self.assertEqual("application/json", response["Content-Type"])
        self.assertEqual("not_live", response["body"]["status"])
        self.assertNotIn("highly-sensitive", str(response))


if __name__ == "__main__":
    import unittest

    unittest.main()
