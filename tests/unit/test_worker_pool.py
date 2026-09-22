"""Regression tests for bounded, exclusively owned Kafka workers."""

import threading
import unittest
import unittest.mock
from concurrent.futures import ThreadPoolExecutor

from src.worker_pool import (
    ConsumerInvalidError,
    WorkerPool,
    WorkerPoolInvariantError,
)


class _Consumer:
    def __init__(self, tracker):
        self.tracker = tracker
        self.assignments = []
        self.closed = False
        with tracker["lock"]:
            tracker["live"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["live"])

    def assign(self, partitions):
        self.assignments.append(partitions[0].partition)

    def close(self):
        if not self.closed:
            self.closed = True
            with self.tracker["lock"]:
                self.tracker["live"] -= 1


class WorkerPoolTests(unittest.TestCase):
    def setUp(self):
        self.tracker = {"live": 0, "peak": 0, "lock": threading.Lock()}
        self.created = []

    def factory(self, _partition):
        consumer = _Consumer(self.tracker)
        self.created.append(consumer)
        return consumer, object()

    def test_count_is_minimum_of_partitions_and_worker_bound(self):
        pool = WorkerPool(size=min(7, 3), topic="canary", factory=self.factory)
        try:
            self.assertEqual(3, len(pool))
            self.assertEqual(3, self.tracker["live"])
        finally:
            pool.close()

    def test_each_concurrent_operation_has_an_exclusive_consumer(self):
        pool = WorkerPool(size=2, topic="canary", factory=self.factory)
        barrier = threading.Barrier(2)

        def operation(partition):
            return pool.run(
                partition,
                lambda consumer, _deserializer: (barrier.wait(), id(consumer))[1],
            )

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(operation, p) for p in (4, 5)]
                consumer_ids = {future.result() for future in futures}
            self.assertEqual(2, len(consumer_ids))
            self.assertEqual({4, 5}, {c.assignments[-1] for c in self.created})
        finally:
            pool.close()

    def test_growth_adds_capacity_without_waiting_for_existing_borrower(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)
        entered = threading.Event()
        release = threading.Event()

        def blocked_operation(_consumer, _deserializer):
            entered.set()
            release.wait(1)

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                blocked = executor.submit(pool.run, 0, blocked_operation)
                self.assertTrue(entered.wait(1))

                pool.grow(2)
                second_consumer = pool.run(1, lambda consumer, _: consumer)

                self.assertEqual(2, len(pool))
                self.assertEqual(2, self.tracker["live"])
                self.assertIs(self.created[1], second_consumer)
                release.set()
                blocked.result(timeout=1)
        finally:
            release.set()
            pool.close()

    def test_shutdown_during_growth_rejects_and_closes_late_consumer(self):
        factory_entered = threading.Event()
        release_factory = threading.Event()
        growth_errors = []

        def blocking_factory(worker_id):
            if worker_id == 1:
                factory_entered.set()
                release_factory.wait()
            return self.factory(worker_id)

        pool = WorkerPool(size=1, topic="canary", factory=blocking_factory)
        growth = threading.Thread(
            target=lambda: self._capture_error(
                growth_errors, lambda: pool.grow(2)
            ),
            daemon=True,
        )
        growth.start()
        self.assertTrue(factory_entered.wait(1))

        pool.begin_shutdown()
        release_factory.set()
        growth.join(1)

        self.assertFalse(growth.is_alive())
        self.assertEqual(1, len(pool))
        self.assertEqual(2, len(self.created))
        self.assertTrue(self.created[1].closed)
        self.assertEqual(1, len(growth_errors))
        self.assertIsInstance(growth_errors[0], WorkerPoolInvariantError)
        pool.close()

    @staticmethod
    def _capture_error(errors, operation):
        try:
            operation()
        except Exception as exc:
            errors.append(exc)

    def test_invalid_consumer_is_closed_before_replacement(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)
        original = self.created[0]

        failure = RuntimeError("invalid")
        with self.assertRaisesRegex(RuntimeError, "invalid"):
            pool.run(6, lambda _consumer, _deserializer: (_ for _ in ()).throw(
                ConsumerInvalidError(failure)
            ))

        self.assertTrue(original.closed)
        self.assertEqual(2, len(self.created))
        self.assertEqual(1, self.tracker["live"])
        self.assertEqual(1, self.tracker["peak"])
        pool.close()

    def test_ordinary_check_failure_keeps_consumer(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)
        original = self.created[0]

        with self.assertRaisesRegex(TimeoutError, "temporary"):
            pool.run(0, lambda *_: (_ for _ in ()).throw(TimeoutError("temporary")))

        self.assertFalse(original.closed)
        self.assertEqual(1, len(self.created))
        self.assertIs(original, pool.run(1, lambda consumer, _: consumer))
        pool.close()

    def test_failed_retirement_fails_pool_without_republishing_worker(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)
        original = self.created[0]

        def failed_close():
            raise RuntimeError("close failed")

        original.close = failed_close
        with self.assertRaisesRegex(WorkerPoolInvariantError, "could not retire"):
            pool.run(0, lambda _consumer, _deserializer: (_ for _ in ()).throw(
                ConsumerInvalidError(RuntimeError("invalid"))
            ))

        with self.assertRaisesRegex(WorkerPoolInvariantError, "could not retire"):
            pool.run(1, lambda _consumer, _deserializer: None)
        self.assertEqual(1, len(self.created))
        self.assertEqual(1, self.tracker["peak"])
        original.close = _Consumer.close.__get__(original, _Consumer)
        pool.close()

    def test_replacement_factory_failure_fails_pool(self):
        calls = 0

        def factory(_worker_id):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("factory failed")
            return _Consumer(self.tracker), object()

        pool = WorkerPool(size=1, topic="canary", factory=factory)
        with self.assertRaisesRegex(WorkerPoolInvariantError, "could not replace"):
            pool.run(0, lambda *_: (_ for _ in ()).throw(
                ConsumerInvalidError(RuntimeError("invalid"))
            ))

        self.assertEqual(0, self.tracker["live"])
        with self.assertRaisesRegex(WorkerPoolInvariantError, "could not replace"):
            pool.run(1, lambda *_: None)

    def test_close_attempts_every_consumer_and_can_retry_failure(self):
        pool = WorkerPool(size=2, topic="canary", factory=self.factory)
        first, second = self.created
        original_close = first.close
        first.close = unittest.mock.Mock(side_effect=RuntimeError("close failed"))

        with self.assertRaisesRegex(WorkerPoolInvariantError, "failed to close 1"):
            pool.close()

        self.assertTrue(second.closed)
        first.close = original_close
        pool.close()
        self.assertTrue(first.closed)
        self.assertEqual(0, self.tracker["live"])

    def test_running_worker_closes_its_consumer_on_its_owner_thread(self):
        close_threads = []

        class ThreadRecordingConsumer(_Consumer):
            def close(inner_self):
                close_threads.append(threading.get_ident())
                super().close()

        def factory(_worker_id):
            consumer = ThreadRecordingConsumer(self.tracker)
            self.created.append(consumer)
            return consumer, object()

        pool = WorkerPool(size=1, topic="canary", factory=factory)
        entered = threading.Event()
        release = threading.Event()
        worker_thread = []

        def operation(_consumer, _deserializer):
            worker_thread.append(threading.get_ident())
            entered.set()
            release.wait(1)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(pool.run, 0, operation)
            self.assertTrue(entered.wait(1))
            pool.begin_shutdown()
            self.assertEqual([], close_threads)
            release.set()
            future.result(timeout=1)

        self.assertEqual(worker_thread, close_threads)
        self.assertEqual(0, self.tracker["live"])

    def test_running_worker_reports_shutdown_close_failure_and_close_retries(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)
        consumer = self.created[0]
        original_close = consumer.close
        consumer.close = unittest.mock.Mock(side_effect=RuntimeError("close failed"))
        entered = threading.Event()
        release = threading.Event()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                pool.run,
                0,
                lambda *_: (entered.set(), release.wait(1)),
            )
            self.assertTrue(entered.wait(1))
            pool.begin_shutdown()
            release.set()
            with self.assertRaisesRegex(
                WorkerPoolInvariantError, "could not close consumer during shutdown"
            ):
                future.result(timeout=1)

        consumer.close = original_close
        pool.close()
        self.assertTrue(consumer.closed)
        self.assertEqual(0, self.tracker["live"])

    def test_begin_shutdown_rejects_dispatch_without_closing_idle_consumer(self):
        pool = WorkerPool(size=1, topic="canary", factory=self.factory)

        pool.begin_shutdown()

        self.assertFalse(self.created[0].closed)
        with self.assertRaisesRegex(WorkerPoolInvariantError, "pool is closed"):
            pool.run(0, lambda *_: None)
        pool.close()
        self.assertTrue(self.created[0].closed)


if __name__ == "__main__":
    unittest.main()
