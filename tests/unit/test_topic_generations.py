"""Topic-generation fencing regression tests."""

import unittest
from unittest.mock import patch

from src.health_state import HealthStateStore
from src.main import (
    _record_failed_partition_attempt,
    _record_successful_partition_attempt,
    _submit_due_checks,
)
from tests.fakes.clock import FakeClock


class TopicGenerationTests(unittest.TestCase):
    def _store(self):
        clock = FakeClock()
        return HealthStateStore(
            (0,),
            warmup_checks=0,
            monotonic_clock=clock.monotonic,
            wall_clock=clock.time,
        )

    def test_successful_recreation_advances_generation_and_resets_kafka_state(self):
        store = self._store()
        original = store.topic_generation
        store.record_schema_registry_result(success=True)
        store.record_partition_result(0, success=True, generation=original)
        self.assertTrue(store.readiness_snapshot().ready)

        store.replace_expected_partitions((0, 1), preserve_existing=False)

        self.assertNotEqual(original, store.topic_generation)
        self.assertFalse(store.readiness_snapshot().ready)
        self.assertEqual((0, 1), store.readiness_snapshot().incomplete_partitions)
        self.assertEqual(
            [0, 0],
            [component.observation_count for component in store.snapshot().partitions],
        )

    def test_old_generation_success_and_failure_are_rejected(self):
        store = self._store()
        old_generation = store.topic_generation
        store.replace_expected_partitions((0,), preserve_existing=False)

        self.assertIsNone(
            store.record_partition_result(
                0, success=True, generation=old_generation
            )
        )
        self.assertIsNone(
            store.record_partition_result(
                0,
                success=False,
                failure="broker_unavailable",
                generation=old_generation,
            )
        )
        component = store.snapshot().partitions[0]
        self.assertEqual(0, component.observation_count)
        self.assertIsNone(component.latest_failure)

    def test_dispatch_records_the_generation_with_each_future(self):
        generation = self._store().topic_generation

        class Scheduler:
            @staticmethod
            def acquire_due(_available):
                return (2,)

        class Executor:
            @staticmethod
            def submit(_operation, _partition):
                return "future"

        active = {}
        with patch("src.main._shutdown_requested", return_value=False):
            selected = _submit_due_checks(
                Scheduler(),
                Executor(),
                active,
                1,
                7,
                lambda partition: partition,
                generation,
            )

        self.assertEqual((2,), selected)
        self.assertEqual((2, 7, generation), active["future"])

    @patch("src.main._update_failure_metrics")
    @patch("src.main._update_success_metrics")
    def test_rejected_completions_do_not_update_metrics_facing_state(
        self, success_metrics, failure_metrics
    ):
        store = self._store()
        old_generation = store.topic_generation
        store.replace_expected_partitions((0,), preserve_existing=False)
        consecutive_failures = {0: 4}

        accepted_success = _record_successful_partition_attempt(
            store,
            0,
            1,
            12,
            consecutive_failures,
            generation=old_generation,
        )
        accepted_failure = _record_failed_partition_attempt(
            store,
            0,
            2,
            consecutive_failures,
            "CONSUME",
            "BROKER_SERVICE",
            failure="CONSUME:BROKER_SERVICE",
            generation=old_generation,
        )

        self.assertIsNone(accepted_success)
        self.assertFalse(accepted_failure)
        self.assertEqual({0: 4}, consecutive_failures)
        success_metrics.assert_not_called()
        failure_metrics.assert_not_called()


if __name__ == "__main__":
    unittest.main()
