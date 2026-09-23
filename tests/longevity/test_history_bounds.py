"""Long-run rolling-history and scheduler-cardinality regressions."""

import unittest

from src.health_state import HealthStateStore
from src.scheduler import PartitionScheduler
from tests.fakes.clock import FakeClock


class HistoryBoundTests(unittest.TestCase):
    def test_thousands_of_results_remain_within_partition_and_sr_windows(self):
        partitions = (0, 1, 2, 3)
        window = 7
        store = HealthStateStore(
            partitions,
            window_checks=window,
            minimum_checks=1,
            warmup_checks=0,
        )

        for attempt in range(5_000):
            partition = partitions[attempt % len(partitions)]
            store.record_partition_result(partition, success=attempt % 3 != 0)
            store.record_schema_registry_result(success=attempt % 5 != 0)

        kafka_entries = sum(
            len(component.results) for component in store._partitions.values()
        )
        self.assertLessEqual(kafka_entries, len(partitions) * window)
        self.assertEqual(window, len(store._schema_registry.results))
        self.assertTrue(all(
            component.results.maxlen == window
            for component in store._partitions.values()
        ))

    def test_missed_schedules_and_retries_keep_one_record_per_partition(self):
        clock = FakeClock()
        partitions = tuple(range(12))
        scheduler = PartitionScheduler(
            partitions, 1, monotonic_clock=clock.monotonic
        )

        for _ in range(2_000):
            clock.advance(10)
            selected = scheduler.acquire_due(len(partitions))
            for partition in selected:
                scheduler.complete(partition)

        self.assertEqual(set(partitions), set(scheduler._records))
        self.assertLessEqual(len(scheduler._due) + len(scheduler._future), len(partitions))
        self.assertLessEqual(len(scheduler._minimum_deadlines), len(partitions) * 2)
        self.assertLessEqual(len(scheduler._maximum_deadlines), len(partitions) * 2)


if __name__ == "__main__":
    unittest.main()
