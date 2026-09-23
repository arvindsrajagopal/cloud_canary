"""Broker-derived canary-topic policy regressions."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src import constants
from src.topic import ensure_topic, sync_topic_partitions
from tests.fakes.kafka import FakeAdminClient, FakeClusterMetadata, FakeTopicMetadata


class _ChangingBrokerCountAdmin(FakeAdminClient):
    def __init__(self, broker_counts, topics):
        super().__init__(broker_count=broker_counts[0], topics=topics)
        self._broker_counts = iter(broker_counts)

    def list_topics(self, timeout=None):
        self.calls.append(("list_topics", timeout))
        broker_count = next(self._broker_counts)
        return FakeClusterMetadata(
            brokers=tuple(range(broker_count)),
            topics={
                name: FakeTopicMetadata(tuple(range(count)))
                for name, count in self.topics.items()
            },
        )


class TopologyPolicyTests(unittest.TestCase):
    def _capture_creations(self):
        created = []

        def new_topic(**settings):
            created.append(settings)
            return SimpleNamespace(**settings)

        return created, patch("src.topic.NewTopic", side_effect=new_topic)

    def test_creation_uses_verified_broker_count_replication_and_retention(self):
        admin = FakeAdminClient(broker_count=5)
        created, factory = self._capture_creations()

        with factory:
            result = ensure_topic({}, "canary", admin=admin)

        self.assertEqual((5, 5), (result.broker_count, result.partition_count))
        self.assertEqual(
            [{
                "topic": "canary",
                "num_partitions": 5,
                "replication_factor": 3,
                "config": {
                    "retention.ms": str(constants.CANARY_TOPIC_RETENTION_MS)
                },
            }],
            created,
        )

    def test_matching_existing_topic_is_not_silently_reconfigured(self):
        admin = FakeAdminClient(broker_count=3, topics={"canary": 3})

        result = ensure_topic({}, "canary", admin=admin)

        self.assertEqual((3, 3), (result.broker_count, result.partition_count))
        operations = [operation for operation, _ in admin.calls]
        self.assertNotIn("create_topic", operations)
        self.assertNotIn("create_partitions", operations)
        self.assertNotIn("delete_topic", operations)

    def test_scale_up_expands_existing_topic_in_place(self):
        admin = FakeAdminClient(broker_count=4, topics={"canary": 2})

        result = sync_topic_partitions({}, "canary", admin=admin)

        self.assertEqual((4, 4), (result.broker_count, result.partition_count))
        self.assertFalse(result.recreated)
        self.assertIn(("create_partitions", ("canary", 4)), admin.calls)
        self.assertNotIn("delete_topic", [operation for operation, _ in admin.calls])

    def test_scale_down_recreates_and_reapplies_creation_policy(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 4})
        created, factory = self._capture_creations()

        with factory:
            result = sync_topic_partitions({}, "canary", admin=admin)

        self.assertTrue(result.recreated)
        self.assertEqual(2, admin.topics["canary"])
        self.assertIn(("delete_topic", "canary"), admin.calls)
        self.assertEqual(2, created[0]["num_partitions"])
        self.assertEqual(2, created[0]["replication_factor"])
        self.assertEqual(
            str(constants.CANARY_TOPIC_RETENTION_MS),
            created[0]["config"]["retention.ms"],
        )

    def test_scale_up_rejects_broker_count_change_during_final_verification(self):
        admin = _ChangingBrokerCountAdmin([4, 5], {"canary": 2})

        with self.assertRaisesRegex(RuntimeError, "changed from 4 to 5"):
            sync_topic_partitions({}, "canary", admin=admin)

    def test_recreation_rejects_broker_count_change_during_final_verification(self):
        admin = _ChangingBrokerCountAdmin([2, 2, 3], {"canary": 4})

        with self.assertRaisesRegex(RuntimeError, "changed from 2 to 3"):
            sync_topic_partitions({}, "canary", admin=admin)

    def test_zero_broker_metadata_is_rejected_without_mutation(self):
        admin = FakeAdminClient(broker_count=0, topics={"canary": 2})

        with self.assertRaisesRegex(RuntimeError, "contains no brokers"):
            sync_topic_partitions({}, "canary", admin=admin)

        self.assertEqual(
            ["list_topics"], [operation for operation, _ in admin.calls]
        )


if __name__ == "__main__":
    unittest.main()
