"""Static topic-manager and observer role regressions."""

import logging
import unittest
from unittest.mock import Mock, patch

from src.config import validate_config
from src.main import _shutdown_event, run
from src.scheduler import ScheduledOperationLane
from src.topic import ReconciliationResult, ensure_topic, sync_topic_partitions
from tests.fakes.kafka import FakeAdminClient


def _config(mode=None):
    app = {"topic": "canary", "partition.sync.interval.seconds": "60"}
    if mode is not None:
        app["topic.management.mode"] = mode
    return {
        "kafka": {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "password",
        },
        "schema_registry": {
            "url": "https://registry.invalid",
            "basic.auth.user.info": "user:password",
        },
        "app": app,
    }


def _immediately_due_lane(interval, **kwargs):
    del interval
    kwargs["initial_delay"] = 0
    return ScheduledOperationLane(0.001, **kwargs)


class TopicRoleTests(unittest.TestCase):
    def test_management_mode_defaults_to_manage_and_accepts_only_static_roles(self):
        validate_config(_config())
        validate_config(_config("manage"))
        validate_config(_config("observe"))

        for value in ("", "manager", "observer", "MANAGE", "auto"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "topic.management.mode"
            ):
                validate_config(_config(value))

    def test_observer_verifies_stable_topic_without_admin_mutation(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 2})

        result = sync_topic_partitions(
            {}, "canary", admin=admin, management_mode="observe"
        )

        self.assertTrue(result.topology_matches)
        self.assertEqual((2, 2), tuple(result))
        self.assertEqual(["list_topics"], [name for name, _ in admin.calls])

    def test_observer_reports_missing_and_mismatched_topics_without_mutation(self):
        for topics, expected_partitions in (({}, 0), ({"canary": 1}, 1)):
            with self.subTest(topics=topics):
                admin = FakeAdminClient(broker_count=2, topics=topics)

                result = sync_topic_partitions(
                    {}, "canary", admin=admin, management_mode="observe"
                )

                self.assertFalse(result.topology_matches)
                self.assertEqual(expected_partitions, result.partition_count)
                self.assertEqual(
                    ["list_topics"], [name for name, _ in admin.calls]
                )

    def test_observer_startup_waits_for_verified_topology_without_creating(self):
        admin = FakeAdminClient(broker_count=2)

        result = ensure_topic(
            {}, "canary", admin=admin, management_mode="observe"
        )

        self.assertIsNone(result)
        self.assertEqual(
            ["list_topics"],
            [name for name, _ in admin.calls],
        )

    def test_observer_runtime_pauses_then_rebuilds_after_verified_topology(self):
        config = _config("observe")
        config["app"]["log.topic.enabled"] = "true"
        lifecycle_events = []

        class StrictConsumerPool:
            def __init__(self, size):
                self.size = size
                self.grow_calls = []

            def __len__(self):
                return self.size

            def grow(self, size):
                self.grow_calls.append(size)
                if size < self.size:
                    raise ValueError("worker pool cannot shrink in place")
                self.size = size

            def close(self):
                pass

        consumers = StrictConsumerPool(2)
        replacement_consumers = StrictConsumerPool(1)
        old_executor = Mock()
        old_executor.shutdown.side_effect = lambda **kwargs: lifecycle_events.append(
            ("old_executor_shutdown", kwargs)
        )
        replacement_executor = Mock()
        producer = Mock()
        replacement = (
            replacement_consumers,
            replacement_executor,
            [0],
            {0: 0},
        )
        mismatch_health = []
        dispatch_counts = {}
        sync_calls = 0

        def reconcile(*_args, **_kwargs):
            nonlocal sync_calls
            sync_calls += 1
            if sync_calls == 1:
                return ReconciliationResult(2, 2)
            if sync_calls == 2:
                dispatch_counts["mismatch_return"] = submit.call_count
                return ReconciliationResult(2, 0, topology_matches=False)
            dispatch_counts["stable_start"] = submit.call_count
            return ReconciliationResult(2, 1)

        def rebuild(*args, **_kwargs):
            lifecycle_events.append(("rebuild", {}))
            dispatch_counts["rebuild"] = submit.call_count
            store = args[7]
            mismatch_health.append(
                (store.snapshot().status, store.readiness_snapshot().ready)
            )
            _shutdown_event.set()
            return replacement

        _shutdown_event.clear()
        try:
            with (
                patch("src.main.load_config", return_value=config),
                patch("src.main.setup_logging"),
                patch("src.main.start_metrics_server"),
                patch("src.main.signal.signal"),
                patch("src.main.AdminClient", return_value=Mock()),
                patch("src.main.validate_kafka_startup_client"),
                patch("src.main.validate_schema_registry_startup_client"),
                patch("src.main.ensure_topic") as ensure,
                patch("src.main.ensure_log_topic") as ensure_log,
                patch(
                    "src.main.KafkaLogHandler",
                    return_value=logging.NullHandler(),
                ),
                patch("src.main.SchemaRegistryClient", return_value=object()),
                patch("src.main.create_producer", return_value=(producer, object())),
                patch(
                    "src.main.sync_topic_partitions",
                    side_effect=reconcile,
                ) as sync,
                patch("src.main._build_consumer_pool", return_value=consumers),
                patch(
                    "src.main.DaemonThreadPoolExecutor",
                    return_value=old_executor,
                ),
                patch(
                    "src.main._submit_due_checks", return_value=()
                ) as submit,
                patch(
                    "src.main.ScheduledOperationLane",
                    side_effect=_immediately_due_lane,
                ),
                patch("src.main._run_replacement_runtime_transaction", side_effect=lambda op: op()),
                patch("src.main._replace_recreated_runtime", side_effect=rebuild) as replace,
                patch("src.main.configure_health_state"),
            ):
                run()
        finally:
            _shutdown_event.clear()

        self.assertEqual("observe", ensure.call_args.kwargs["management_mode"])
        self.assertTrue(all(
            call.kwargs["management_mode"] == "observe"
            for call in sync.call_args_list
        ))
        ensure_log.assert_not_called()
        replace.assert_called_once()
        self.assertEqual(1, replace.call_args.args[5])
        self.assertEqual([], consumers.grow_calls)
        self.assertEqual(
            [
                ("old_executor_shutdown", {"wait": True}),
                ("rebuild", {}),
            ],
            lifecycle_events,
        )
        self.assertEqual([("unhealthy", False)], mismatch_health)
        # The reconciliation callback records mismatch_return before the owner
        # thread consumes that future, so one pre-pause dispatch can race that
        # marker. Once the next verified observation begins, dispatch remains
        # paused until the replacement is published.
        self.assertLessEqual(
            dispatch_counts["mismatch_return"],
            dispatch_counts["stable_start"],
        )
        self.assertEqual(
            dispatch_counts["stable_start"], dispatch_counts["rebuild"]
        )


if __name__ == "__main__":
    unittest.main()
