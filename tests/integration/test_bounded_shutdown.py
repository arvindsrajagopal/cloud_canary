"""Integration coverage for shutdown dispatch and execution boundaries."""

from __future__ import annotations

import signal
import threading
import time
import unittest
from unittest.mock import patch

from src import main
from src.bounded_executor import DaemonThreadPoolExecutor
from src.worker_pool import WorkerPool


class _Consumer:
    def __init__(self) -> None:
        self.close_threads: list[str] = []

    def assign(self, _partitions) -> None:
        pass

    def close(self) -> None:
        self.close_threads.append(threading.current_thread().name)


class _SingleDueScheduler:
    def __init__(self) -> None:
        self.calls = 0

    def acquire_due(self, limit: int) -> tuple[int, ...]:
        self.calls += 1
        return (0,) if limit else ()


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _DeadlineCompletion:
    """Advance an injected clock only after lifecycle cleanup has begun."""

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

    def wait_for_real_completion(self, timeout: float) -> bool:
        return self._completed.wait(timeout)


class _ObservedExecutor:
    """Expose when production cleanup reaches its cooperative drain."""

    def __init__(self, executor: DaemonThreadPoolExecutor) -> None:
        self._executor = executor
        self.drain_started = threading.Event()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if wait:
            self.drain_started.set()
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)


class BoundedShutdownIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.request_patch = patch.object(main, "_shutdown_request", None)
        self.claim_patch = patch.object(
            main, "_shutdown_request_claim", [main._shutdown_claim_token]
        )
        self.timeout_patch = patch.object(main, "_shutdown_timeout_seconds", 0.2)
        self.request_patch.start()
        self.claim_patch.start()
        self.timeout_patch.start()
        main._shutdown_event.clear()

    def tearDown(self) -> None:
        main._shutdown_event.clear()
        self.timeout_patch.stop()
        self.claim_patch.stop()
        self.request_patch.stop()

    def _running_check(self):
        consumer = _Consumer()
        pool = WorkerPool(
            size=1,
            topic="canary",
            factory=lambda _worker_id: (consumer, object()),
        )
        executor = DaemonThreadPoolExecutor(
            max_workers=1, thread_name_prefix="bounded-shutdown"
        )
        started = threading.Event()
        release = threading.Event()

        def block(_consumer, _deserializer):
            started.set()
            release.wait()
            return 1

        scheduler = _SingleDueScheduler()
        active = {}
        selected = main._submit_due_checks(
            scheduler,
            executor,
            active,
            1,
            1,
            lambda partition: pool.run(partition, block),
        )
        self.assertEqual((0,), selected)
        self.assertTrue(started.wait(1))
        return consumer, pool, executor, scheduler, active, release

    def test_shutdown_stops_dispatch_and_cooperative_worker_closes_consumer(self):
        consumer, pool, executor, scheduler, active, release = self._running_check()
        observed_executor = _ObservedExecutor(executor)
        cleanup = threading.Thread(
            target=main._cleanup_runtime_resources,
            args=(observed_executor, pool, None),
        )
        try:
            with patch.object(main.time, "monotonic", return_value=10.0):
                main._handle_signal(signal.SIGTERM, None)

            rejected = main._submit_due_checks(
                scheduler, executor, active, 1, 2, lambda _partition: None
            )
            self.assertEqual((), rejected)
            self.assertEqual(1, scheduler.calls)

            cleanup.start()
            self.assertTrue(observed_executor.drain_started.wait(1))
            self.assertTrue(cleanup.is_alive())
            self.assertEqual([], consumer.close_threads)

            release.set()
            next(iter(active)).result(timeout=1)
            cleanup.join(1)

            self.assertFalse(cleanup.is_alive())
            self.assertEqual(1, len(consumer.close_threads))
            self.assertTrue(consumer.close_threads[0].startswith("bounded-shutdown_"))
        finally:
            release.set()
            pool.close()
            executor.shutdown(wait=True, cancel_futures=True)

    def test_wedged_worker_is_abandoned_at_original_deadline_without_cross_close(self):
        consumer, pool, executor, scheduler, active, release = self._running_check()
        clock = _Clock(20.0)
        cleanup_started = threading.Event()
        completion = _DeadlineCompletion(clock, cleanup_started)

        with patch.object(main.time, "monotonic", return_value=clock.now):
            main._handle_signal(signal.SIGINT, None)

        def blocking_cleanup() -> None:
            cleanup_started.set()
            main._cleanup_runtime_resources(executor, pool, None)

        try:
            main._run_behind_daemon_boundary(
                blocking_cleanup,
                monotonic_clock=clock,
                event_factory=lambda: completion,
            )

            self.assertAlmostEqual(20.2, clock.now)
            self.assertLessEqual(sum(completion.waits), 0.2 + 1e-9)
            self.assertEqual([], consumer.close_threads)
            self.assertEqual(
                (),
                main._submit_due_checks(
                    scheduler, executor, active, 1, 2, lambda _partition: None
                ),
            )
        finally:
            release.set()
            next(iter(active)).result(timeout=1)
            executor.shutdown(wait=True, cancel_futures=True)

        self.assertTrue(completion.wait_for_real_completion(1))
        self.assertEqual(1, len(consumer.close_threads))
        self.assertTrue(consumer.close_threads[0].startswith("bounded-shutdown_"))


if __name__ == "__main__":
    unittest.main()
