"""Deterministic fair-scheduler regression tests."""

import unittest

from src.main import _submit_due_checks
from src.scheduler import PartitionScheduler
from tests.fakes.clock import FakeClock


class PartitionSchedulerTests(unittest.TestCase):
    def test_startup_work_is_distributed_across_interval(self):
        clock = FakeClock(monotonic=100)
        scheduler = PartitionScheduler((0, 1, 2, 3), 20, monotonic_clock=clock.monotonic)

        self.assertEqual((0,), scheduler.acquire_due(10))
        scheduler.complete(0)
        clock.advance(4.999)
        self.assertEqual((), scheduler.acquire_due(10))
        clock.advance(0.001)
        self.assertEqual((1,), scheduler.acquire_due(10))

    def test_oldest_due_wins_even_when_it_has_higher_partition_id(self):
        clock = FakeClock()
        scheduler = PartitionScheduler((), 10, monotonic_clock=clock.monotonic)
        scheduler.reconcile_partitions((9,))
        clock.advance(1)
        scheduler.reconcile_partitions((1, 9))

        self.assertEqual((9,), scheduler.acquire_due(1))

    def test_partition_id_breaks_equal_deadline_ties(self):
        clock = FakeClock()
        scheduler = PartitionScheduler((), 3, monotonic_clock=clock.monotonic)
        scheduler.reconcile_partitions((2,))
        scheduler.reconcile_partitions((2, 0))
        scheduler.reconcile_partitions((2, 0, 1))

        self.assertEqual((0, 1, 2), scheduler.acquire_due(3))

    def test_partition_cannot_be_acquired_twice_concurrently(self):
        clock = FakeClock()
        scheduler = PartitionScheduler((0,), 10, monotonic_clock=clock.monotonic)

        self.assertEqual((0,), scheduler.acquire_due(1))
        clock.advance(100)
        self.assertEqual((), scheduler.acquire_due(1))
        self.assertEqual(1, scheduler.snapshot().in_flight)

    def test_missed_occurrences_are_coalesced(self):
        clock = FakeClock()
        scheduler = PartitionScheduler((0,), 10, monotonic_clock=clock.monotonic)
        scheduler.acquire_due(1)
        clock.advance(35)

        scheduler.complete(0)

        self.assertEqual((), scheduler.acquire_due(1))
        clock.advance(5)
        self.assertEqual((0,), scheduler.acquire_due(1))

    def test_overdue_capacity_snapshot_includes_in_flight_work(self):
        clock = FakeClock()
        scheduler = PartitionScheduler((0, 1), 10, monotonic_clock=clock.monotonic)
        self.assertEqual((0,), scheduler.acquire_due(1))
        clock.advance(15)

        snapshot = scheduler.snapshot()

        self.assertEqual(1, snapshot.pending)
        self.assertEqual(1, snapshot.in_flight)
        self.assertEqual(15, snapshot.oldest_overdue_seconds)

    def test_coverage_duration_is_union_of_deadline_span_and_overdue_time(self):
        clock = FakeClock(monotonic=100)
        scheduler = PartitionScheduler((0, 1), 20, monotonic_clock=clock.monotonic)

        self.assertEqual(10, scheduler.snapshot(now=95).coverage_duration_seconds)
        self.assertEqual(10, scheduler.snapshot(now=105).coverage_duration_seconds)
        self.assertEqual(20, scheduler.snapshot(now=120).coverage_duration_seconds)

    def test_free_worker_is_refilled_while_sibling_check_remains_blocked(self):
        class FakeExecutor:
            def __init__(self):
                self.submissions = []

            def submit(self, function, partition):
                future = object()
                self.submissions.append((future, function, partition))
                return future

        clock = FakeClock()
        scheduler = PartitionScheduler((0, 1, 2), 9, monotonic_clock=clock.monotonic)
        clock.advance(9)
        executor = FakeExecutor()
        active = {}

        self.assertEqual(
            (0, 1),
            _submit_due_checks(scheduler, executor, active, 2, 1, lambda p: p),
        )
        completed = executor.submissions[0][0]
        active.pop(completed)
        scheduler.complete(0)

        self.assertEqual(
            (2,),
            _submit_due_checks(scheduler, executor, active, 2, 2, lambda p: p),
        )
        self.assertEqual([0, 1, 2], [item[2] for item in executor.submissions])
        self.assertEqual(2, len(active))


if __name__ == "__main__":
    unittest.main()
