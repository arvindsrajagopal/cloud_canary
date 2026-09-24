"""Long-run native-client, worker, and snapshot resource regressions."""

import unittest
from unittest.mock import Mock, patch

from src import constants as const
from src import consumer as consumer_module
from src import kafka_log_handler as log_module
from src import producer as producer_module
from src.health_state import HealthStateStore
from src.scheduler import PartitionScheduler
from src.worker_pool import WorkerPool
from tests.fakes.clock import FakeClock


class ResourceBoundTests(unittest.TestCase):
    def test_effective_native_client_queue_bounds_cannot_be_overridden(self):
        hostile = {
            "queue.buffering.max.messages": 999_999,
            "queue.buffering.max.kbytes": 999_999,
            "queued.min.messages": 999_999,
            "queued.max.messages.kbytes": 999_999,
        }
        native_check_producer = Mock()
        with (
            patch.object(producer_module, "Producer", return_value=native_check_producer) as producer_factory,
            patch.object(producer_module, "AvroSerializer", return_value=Mock()),
        ):
            producer_module.create_producer(hostile, Mock())
        check_config = producer_factory.call_args.args[0]
        self.assertEqual(
            const.CHECK_PRODUCER_QUEUE_MAX_MESSAGES,
            check_config["queue.buffering.max.messages"],
        )
        self.assertEqual(
            const.CHECK_PRODUCER_QUEUE_MAX_KBYTES,
            check_config["queue.buffering.max.kbytes"],
        )

        native_consumer = Mock()
        with (
            patch.object(consumer_module, "Consumer", return_value=native_consumer) as consumer_factory,
            patch.object(consumer_module, "AvroDeserializer", return_value=Mock()),
        ):
            consumer_module.create_partition_consumer(hostile, Mock(), "canary", 0)
        consumer_config = consumer_factory.call_args.args[0]
        self.assertEqual(
            const.CONSUMER_QUEUE_MIN_MESSAGES,
            consumer_config["queued.min.messages"],
        )
        self.assertEqual(
            const.CONSUMER_QUEUE_MAX_KBYTES,
            consumer_config["queued.max.messages.kbytes"],
        )
        self.assertLessEqual(const.CONSUMER_QUEUE_MAX_KBYTES * 200, 200 * 1024)

        native_producer = Mock()
        with patch.object(
            log_module, "Producer", return_value=native_producer
        ) as factory:
            handler = log_module.KafkaLogHandler(hostile, "logs")
        try:
            log_config = factory.call_args.args[0]
            self.assertEqual(
                const.LOG_PRODUCER_QUEUE_MAX_MESSAGES,
                log_config["queue.buffering.max.messages"],
            )
            self.assertEqual(
                const.LOG_PRODUCER_QUEUE_MAX_KBYTES,
                log_config["queue.buffering.max.kbytes"],
            )
        finally:
            handler.close()

    def test_check_producer_saturation_has_no_retry_or_shadow_queue(self):
        native_producer = Mock()
        native_producer.produce.side_effect = BufferError("full")
        serializer = Mock(return_value=b"canary")

        with self.assertRaises(BufferError):
            producer_module.produce_canary(
                native_producer, serializer, "canary", 1, 0
            )

        native_producer.produce.assert_called_once()
        native_producer.poll.assert_not_called()
        self.assertNotIn("queue", native_producer.__dict__)

    def test_queue_saturation_does_not_create_a_python_retry_queue(self):
        handler_producer = Mock()
        handler_producer.produce.side_effect = BufferError("full")
        with patch.object(log_module, "Producer", return_value=handler_producer):
            handler = log_module.KafkaLogHandler({}, "logs")
        record = log_module.logging.LogRecord(
            "test", log_module.logging.INFO, __file__, 1, "message", (), None
        )
        try:
            with patch.object(log_module, "record_log_failure") as failure:
                for _ in range(2_000):
                    handler.emit(record)
            self.assertEqual(2_000, handler_producer.produce.call_count)
            self.assertEqual(2_000, failure.call_count)
            self.assertFalse(hasattr(handler, "queue"))
        finally:
            handler.close()

    def test_workers_in_flight_checks_histories_and_snapshots_stay_bounded(self):
        partitions = tuple(range(17))
        maximum_workers = 4
        consumers = []

        def factory(_worker_id):
            consumer = Mock()
            consumers.append(consumer)
            return consumer, Mock()

        pool = WorkerPool(
            size=min(len(partitions), maximum_workers), topic="canary", factory=factory
        )
        clock = FakeClock()
        scheduler = PartitionScheduler(
            partitions, 1, monotonic_clock=clock.monotonic
        )
        store = HealthStateStore(partitions, window_checks=3, minimum_checks=1)
        try:
            self.assertEqual(maximum_workers, len(pool))
            self.assertEqual(maximum_workers, len(consumers))
            for _ in range(1_000):
                clock.advance(5)
                selected = scheduler.acquire_due(maximum_workers)
                self.assertLessEqual(len(selected), maximum_workers)
                self.assertLessEqual(scheduler.snapshot().in_flight, maximum_workers)
                for partition in selected:
                    store.record_partition_result(
                        partition, success=False, failure="x" * 900
                    )
                    scheduler.complete(partition)

            snapshots = [store.snapshot() for _ in range(maximum_workers)]
            self.assertEqual(maximum_workers, len(snapshots))
            for snapshot in snapshots:
                self.assertFalse(hasattr(snapshot, "results"))
                for component in snapshot.partitions:
                    self.assertFalse(hasattr(component, "results"))
                    if component.latest_failure is not None:
                        self.assertLessEqual(len(component.latest_failure), 512)
            self.assertLessEqual(
                sum(len(item.results) for item in store._partitions.values()),
                len(partitions) * 3,
            )
        finally:
            pool.close()


if __name__ == "__main__":
    unittest.main()
