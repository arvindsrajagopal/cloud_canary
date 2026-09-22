"""Integration coverage for fatal invariant shutdown behavior."""

import threading
import time
import unittest
from unittest.mock import patch

from src import health, main
from src.health_state import HealthStateStore


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _DeadlineCompletion:
    def __init__(self, clock: _Clock, cleanup_started: threading.Event) -> None:
        self._clock = clock
        self._cleanup_started = cleanup_started
        self._completed = threading.Event()
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self._completed.is_set()

    def set(self) -> None:
        self._completed.set()

    def wait(self, timeout: float) -> bool:
        if not self._cleanup_started.wait(1):
            raise AssertionError("lifecycle did not enter cleanup")
        self.waits.append(timeout)
        self._clock.now += timeout
        time.sleep(0)
        return self._completed.is_set()


class _NeverCalledScheduler:
    def acquire_due(self, _limit: int) -> tuple[int, ...]:
        raise AssertionError("fatal shutdown must reject before scheduler dispatch")


class InvariantShutdownIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.patches = (
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main, "_shutdown_request_claim", [main._shutdown_claim_token]
            ),
            patch.object(main, "_fatal_shutdown_request", None),
            patch.object(
                main,
                "_fatal_shutdown_request_claim",
                [main._shutdown_claim_token],
            ),
            patch.object(main, "_shutdown_timeout_seconds", 0.2),
        )
        for active_patch in self.patches:
            active_patch.start()
        main._shutdown_event.clear()

    def tearDown(self) -> None:
        main._shutdown_event.clear()
        for active_patch in reversed(self.patches):
            active_patch.stop()

    def test_invariant_failure_rejects_dispatch_and_fatal_deadline_exits_nonzero(self):
        clock = _Clock(10.0)
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        completion = _DeadlineCompletion(clock, cleanup_started)
        store = HealthStateStore(
            (0,),
            minimum_checks=1,
            warmup_checks=0,
            invariant_failure_callback=main._request_fatal_shutdown,
        )
        store._scheduler_oldest_overdue = "corrupt"

        def lifecycle() -> None:
            try:
                store.snapshot()
            finally:
                cleanup_started.set()
                release_cleanup.wait()

        try:
            with (
                patch.object(main.time, "monotonic", side_effect=clock),
                self.assertRaises(SystemExit) as raised,
            ):
                main._run_behind_daemon_boundary(
                    lifecycle,
                    monotonic_clock=clock,
                    event_factory=lambda: completion,
                )

            self.assertEqual(1, raised.exception.code)
            self.assertEqual("state_invariant", store._fatal_internal)
            self.assertIs(main._fatal_shutdown_request, main._shutdown_request)
            self.assertAlmostEqual(10.2, main._shutdown_request.deadline)
            self.assertAlmostEqual(10.2, clock.now)
            self.assertLessEqual(sum(completion.waits), 0.2 + 1e-9)
            self.assertEqual(
                (),
                main._submit_due_checks(
                    _NeverCalledScheduler(), object(), {}, 1, 1, lambda _p: None
                ),
            )
        finally:
            release_cleanup.set()

    def test_fatal_publication_allows_cleanup_before_nonzero_exit(self):
        clock = _Clock(10.0)
        shared_claim_entered = threading.Event()
        release_publication = threading.Event()
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()

        class BlockedSharedClaim(list):
            def pop(self):
                shared_claim_entered.set()
                if not release_publication.wait(1):
                    raise AssertionError("fatal publication was not released")
                return super().pop()

        class PublicationRaceCompletion(_DeadlineCompletion):
            def wait(self, timeout: float) -> bool:
                self.assert_fatal_request_published()
                release_publication.set()
                return super().wait(timeout)

            @staticmethod
            def assert_fatal_request_published() -> None:
                if main._fatal_shutdown_request is None:
                    raise AssertionError("fatal deadline was not published")

        def lifecycle() -> None:
            try:
                main._request_fatal_shutdown()
            finally:
                cleanup_started.set()
                release_cleanup.wait()

        blocked_shared_claim = BlockedSharedClaim([main._shutdown_claim_token])
        try:
            with (
                patch.object(main.time, "monotonic", side_effect=clock),
                patch.object(
                    main,
                    "_shutdown_request_claim",
                    blocked_shared_claim,
                ),
                self.assertRaises(SystemExit) as raised,
            ):
                main._run_behind_daemon_boundary(
                    lifecycle,
                    monotonic_clock=clock,
                    event_factory=lambda: PublicationRaceCompletion(
                        clock, cleanup_started
                    ),
                )

            self.assertEqual(1, raised.exception.code)
            self.assertTrue(shared_claim_entered.is_set())
            self.assertTrue(cleanup_started.is_set())
            self.assertEqual([], blocked_shared_claim)
            self.assertIs(main._fatal_shutdown_request, main._shutdown_request)
            self.assertAlmostEqual(10.2, main._fatal_shutdown_request.deadline)
            self.assertAlmostEqual(10.2, clock.now)
        finally:
            release_publication.set()
            release_cleanup.set()

    def test_fatal_state_makes_public_liveness_and_readiness_return_503(self):
        store = HealthStateStore(
            (0,), minimum_checks=1, warmup_checks=0, monotonic_clock=lambda: 1.0
        )
        store.record_scheduler_heartbeat()
        store.record_schema_registry_result(success=True)
        store.record_partition_result(0, success=True)
        store.record_fatal_internal("state_invariant")

        with patch.object(health, "_health_state_store", store):
            self.assertEqual(503, health.get_liveness_status().http_code)
            self.assertEqual(503, health.get_readiness_status().http_code)
