"""Regression tests for the process-level lifecycle deadline boundary."""

import threading
import time
import unittest
from unittest.mock import patch

from src import main


class _Clock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


class _AdvancingEvent:
    def __init__(self, clock):
        self._clock = clock
        self._set = False
        self.waits = []

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def wait(self, timeout):
        self.waits.append(timeout)
        self._clock.now += timeout
        time.sleep(0)
        return self._set


class ShutdownLifecycleTests(unittest.TestCase):
    def test_complete_lifecycle_runs_on_daemon_boundary(self):
        observed = []

        main._run_behind_daemon_boundary(
            lambda: observed.append(
                (threading.current_thread().name, threading.current_thread().daemon)
            )
        )

        self.assertEqual([("cloud-canary-lifecycle", True)], observed)

    def test_blocked_startup_is_abandoned_at_original_deadline(self):
        clock = _Clock(10.0)
        completion = _AdvancingEvent(clock)
        release = threading.Event()
        request = main.ShutdownRequest(15, requested_at=10.0, deadline=11.25)

        try:
            with (
                patch.object(main, "_shutdown_request", request),
                patch.object(
                    main, "_shutdown_request_claim", [main._shutdown_claim_token]
                ),
            ):
                main._run_behind_daemon_boundary(
                    release.wait,
                    monotonic_clock=clock,
                    event_factory=lambda: completion,
                )
        finally:
            release.set()

        self.assertAlmostEqual(11.25, clock.now)
        self.assertLessEqual(sum(completion.waits), 1.25 + 1e-9)

    def test_blocked_runtime_is_abandoned_at_original_deadline(self):
        clock = _Clock(30.0)
        completion = _AdvancingEvent(clock)
        startup_complete = threading.Event()
        release_runtime = threading.Event()
        request = main.ShutdownRequest(15, requested_at=30.0, deadline=30.3)

        def lifecycle():
            startup_complete.set()
            release_runtime.wait()

        try:
            with (
                patch.object(main, "_shutdown_request", request),
                patch.object(
                    main, "_shutdown_request_claim", [main._shutdown_claim_token]
                ),
            ):
                main._run_behind_daemon_boundary(
                    lifecycle,
                    monotonic_clock=clock,
                    event_factory=lambda: completion,
                )
        finally:
            release_runtime.set()

        self.assertTrue(startup_complete.is_set())
        self.assertAlmostEqual(30.3, clock.now)
        self.assertLessEqual(sum(completion.waits), 0.3 + 1e-9)

    def test_blocked_partial_cleanup_cannot_open_a_new_deadline(self):
        clock = _Clock(20.0)
        completion = _AdvancingEvent(clock)
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        request = main.ShutdownRequest(2, requested_at=20.0, deadline=20.4)

        def lifecycle():
            try:
                return
            finally:
                cleanup_started.set()
                release_cleanup.wait()

        try:
            with (
                patch.object(main, "_shutdown_request", request),
                patch.object(
                    main, "_shutdown_request_claim", [main._shutdown_claim_token]
                ),
            ):
                main._run_behind_daemon_boundary(
                    lifecycle,
                    monotonic_clock=clock,
                    event_factory=lambda: completion,
                )
        finally:
            release_cleanup.set()

        self.assertTrue(cleanup_started.is_set())
        self.assertAlmostEqual(20.4, clock.now)
        self.assertLessEqual(max(completion.waits), 0.05)

    def test_lifecycle_exception_is_propagated_after_normal_completion(self):
        def fail():
            raise RuntimeError("lifecycle failed")

        with (
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main, "_shutdown_request_claim", [main._shutdown_claim_token]
            ),
            self.assertRaisesRegex(RuntimeError, "lifecycle failed"),
        ):
            main._run_behind_daemon_boundary(fail)


if __name__ == "__main__":
    unittest.main()
