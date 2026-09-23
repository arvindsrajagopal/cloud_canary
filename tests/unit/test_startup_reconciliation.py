"""Startup recovery from interrupted canary-topic recreation."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src import main
from src.error_classifier import CanaryError
from src.main import run
from src.topic import ensure_topic
from tests.fakes.kafka import FakeAdminClient, FakeClusterMetadata, FakeTopicMetadata


class _TransientMetadataAdmin(FakeAdminClient):
    def list_topics(self, timeout=None):
        del timeout
        raise RuntimeError("metadata temporarily unavailable")


class _AmbiguousMetadataAdmin(FakeAdminClient):
    def __init__(self):
        super().__init__(broker_count=2, topics={"canary": 2})
        self._reads = 0

    def list_topics(self, timeout=None):
        del timeout
        self._reads += 1
        broker_count = 3 if self._reads == 3 else 2
        return FakeClusterMetadata(
            brokers=tuple(range(broker_count)),
            topics={"canary": FakeTopicMetadata((0, 1))},
        )


class _TopicErrorMetadataAdmin(FakeAdminClient):
    def list_topics(self, timeout=None):
        del timeout
        return SimpleNamespace(
            brokers=(0,),
            topics={
                "canary": SimpleNamespace(
                    partitions={0: SimpleNamespace(error=None)},
                    error=RuntimeError("leader unavailable"),
                )
            },
        )


class StartupReconciliationTests(unittest.TestCase):
    def test_transient_topic_reconciliation_retries_before_schema_registry(self):
        config = {
            "kafka": {},
            "schema_registry": {"url": "https://registry.invalid"},
            "app": {"topic": "canary", "startup.retry.initial.seconds": "0"},
        }
        rejected = main._StartupDispatchRejected("stop after retry proof")

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.start_metrics_server"),
            patch("src.main.AdminClient", return_value=Mock()),
            patch("src.main.validate_kafka_startup_client"),
            patch("src.main._shutdown_requested", return_value=False),
            patch("src.main.ensure_topic", side_effect=(None, rejected))
            as ensure_topic,
            patch("src.main.SchemaRegistryClient") as schema_registry,
            patch("src.main.create_producer") as create_producer,
            self.assertRaises(main._StartupDispatchRejected),
        ):
            main._run_lifecycle_owned({})

        self.assertEqual(2, ensure_topic.call_count)
        schema_registry.assert_not_called()
        create_producer.assert_not_called()

    def test_internal_transient_sync_result_is_preserved_for_outer_retry(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 2})

        with patch("src.topic.sync_topic_partitions", return_value=None):
            result = ensure_topic({}, "canary", admin=admin)

        self.assertIsNone(result)

    def test_failed_destructive_reconciliation_is_terminal_at_lifecycle_boundary(self):
        config = {
            "kafka": {},
            "schema_registry": {"url": "https://registry.invalid"},
            "app": {"topic": "canary"},
        }

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=Mock()),
            patch(
                "src.main.ensure_topic",
                side_effect=RuntimeError("replacement verification failed"),
            ),
            patch("src.main.SchemaRegistryClient") as schema_registry,
            patch("src.main.create_producer") as create_producer,
            self.assertRaises(CanaryError),
        ):
            run()

        schema_registry.assert_not_called()
        create_producer.assert_not_called()

    def test_missing_topic_from_delete_complete_interruption_is_recreated_and_verified(self):
        admin = FakeAdminClient(broker_count=2)

        result = ensure_topic({}, "canary", admin=admin)

        self.assertEqual((2, 2), (result.broker_count, result.partition_count))
        self.assertEqual(2, admin.topics["canary"])
        self.assertIn(("create_topic", ("canary", 2)), admin.calls)
        self.assertGreaterEqual(
            sum(operation == "list_topics" for operation, _ in admin.calls), 3
        )

    def test_existing_verified_topology_from_create_complete_interruption_is_accepted(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 2})

        result = ensure_topic({}, "canary", admin=admin)

        self.assertEqual((2, 2), (result.broker_count, result.partition_count))
        self.assertNotIn("create_topic", [operation for operation, _ in admin.calls])
        self.assertNotIn("delete_topic", [operation for operation, _ in admin.calls])

    def test_unexpected_existing_topology_is_reconciled_from_metadata(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 3})

        result = ensure_topic({}, "canary", admin=admin)

        self.assertTrue(result.recreated)
        self.assertEqual(2, admin.topics["canary"])
        operations = [operation for operation, _ in admin.calls]
        self.assertIn("delete_topic", operations)
        self.assertIn("create_topic", operations)

    def test_transient_metadata_failure_never_returns_verified_state(self):
        with self.assertRaisesRegex(RuntimeError, "temporarily unavailable"):
            ensure_topic({}, "canary", admin=_TransientMetadataAdmin())

    def test_metadata_that_changes_during_verification_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "Ambiguous startup metadata"):
            ensure_topic({}, "canary", admin=_AmbiguousMetadataAdmin())

    def test_logically_inconsistent_topic_metadata_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "contains an error"):
            ensure_topic({}, "canary", admin=_TopicErrorMetadataAdmin())


if __name__ == "__main__":
    unittest.main()
