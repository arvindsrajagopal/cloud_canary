"""Behavior tests for deterministic test doubles."""

from __future__ import annotations

import unittest

from tests.fakes import (
    FakeAdminClient,
    FakeClock,
    FakeConsumer,
    FakeProducer,
    FakeSchemaRegistryClient,
)


class FakeClockTests(unittest.TestCase):
    def test_monotonic_time_isolated_from_wall_clock_jump(self):
        clock = FakeClock(monotonic=10, wall=100)
        clock.jump_wall(-50)
        self.assertEqual(10, clock.monotonic())
        self.assertEqual(50, clock.time())
        clock.advance(5)
        self.assertEqual(15, clock.monotonic())
        self.assertEqual(55, clock.time())

    def test_clock_rejects_backwards_advance(self):
        with self.assertRaises(ValueError):
            FakeClock().advance(-1)


class KafkaFakeTests(unittest.TestCase):
    def test_admin_mutations_update_metadata_and_are_recorded(self):
        admin = FakeAdminClient(broker_count=3, topics={"canary": 2})

        class NewPartitions:
            topic = "canary"
            new_total_count = 3

        result = admin.create_partitions([NewPartitions()])["canary"]
        result.result()
        metadata = admin.list_topics()
        self.assertEqual((0, 1, 2), metadata.topics["canary"].partitions)
        self.assertIn(("create_partitions", ("canary", 3)), admin.calls)

    def test_producer_has_a_hard_queue_bound(self):
        producer = FakeProducer(max_messages=1)
        producer.produce(topic="canary", value=b"one")
        with self.assertRaises(BufferError):
            producer.produce(topic="canary", value=b"two")
        self.assertEqual(1, producer.poll())
        self.assertEqual(1, len(producer.delivered))

    def test_consumer_closes_and_records_calls(self):
        consumer = FakeConsumer(records=["record"])
        consumer.assign([0])
        self.assertEqual("record", consumer.poll(timeout=1))
        consumer.close()
        self.assertTrue(consumer.closed)


class SchemaRegistryFakeTests(unittest.TestCase):
    def test_scripted_success_and_failure(self):
        client = FakeSchemaRegistryClient([['subject'], RuntimeError("offline")])
        self.assertEqual(["subject"], client.get_subjects())
        with self.assertRaisesRegex(RuntimeError, "offline"):
            client.get_subjects()
        self.assertEqual(2, client.calls)


if __name__ == "__main__":
    unittest.main()
