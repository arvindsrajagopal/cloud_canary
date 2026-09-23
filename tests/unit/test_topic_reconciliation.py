"""Verified topic reconciliation and replacement-pool regressions."""

import unittest
from unittest.mock import call, patch

from src import metrics
from src.error_classifier import CanaryError
from src.health_state import HealthStateStore
from src.main import (
    _shutdown_event,
    _build_consumer_pool,
    _replace_recreated_runtime,
    run,
)
from src.scheduler import ScheduledOperationLane
from src.topic import (
    ReconciliationResult,
    _await_admin_future,
    sync_topic_partitions,
)
from tests.fakes.clock import FakeClock
from tests.fakes.kafka import FakeAdminClient, FakeFuture


class _StickyDeleteAdmin(FakeAdminClient):
    def delete_topics(self, names):
        return {name: FakeFuture() for name in names}


class _UnverifiedCreateAdmin(FakeAdminClient):
    def create_topics(self, topics):
        return {getattr(topic, "topic", str(topic)): FakeFuture() for topic in topics}


def _immediately_due_operation_lane(interval, **kwargs):
    """Keep production intervals valid while making control-loop tests deterministic."""
    kwargs["initial_delay"] = 0
    return ScheduledOperationLane(interval, **kwargs)


class TopicReconciliationTests(unittest.TestCase):
    @patch("src.topic.const.ADMIN_FUTURE_TIMEOUT_SECONDS", 7)
    def test_admin_future_wait_uses_shared_bound_and_propagates_timeout(self):
        future = unittest.mock.Mock()
        future.result.side_effect = TimeoutError("admin timed out")

        with self.assertRaisesRegex(TimeoutError, "admin timed out"):
            _await_admin_future(future)

        future.result.assert_called_once_with(timeout=7)

    def test_delete_failure_is_fatal_and_never_claims_requested_count(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 3})
        admin.fail_next("delete_topic", RuntimeError("denied"))
        consumer = unittest.mock.Mock()
        consumers = {partition: consumer for partition in range(3)}
        streaks = {0: 1, 1: 2, 2: 3}
        store = HealthStateStore(
            range(3), kafka_check_interval=15, kafka_degraded_after=60,
            kafka_unhealthy_after=300, sr_check_interval=60,
            sr_degraded_after=120, sr_unhealthy_after=300,
        )

        with self.assertRaises(RuntimeError):
            sync_topic_partitions(
                {}, "canary", admin=admin,
                before_recreate=unittest.mock.Mock(),
            )

        self.assertEqual(3, admin.topics["canary"])
        consumer.close.assert_not_called()
        self.assertEqual({0, 1, 2}, set(consumers))
        self.assertEqual({0: 1, 1: 2, 2: 3}, streaks)
        self.assertEqual(
            ("kafka:0", "kafka:1", "kafka:2"),
            tuple(partition.component for partition in store.snapshot().partitions),
        )

    @patch("src.topic.const.DELETE_PROPAGATION_TIMEOUT_SECONDS", 0)
    def test_deletion_propagation_timeout_is_fatal(self):
        admin = _StickyDeleteAdmin(broker_count=2, topics={"canary": 3})

        with self.assertRaisesRegex(RuntimeError, "verification timed out"):
            sync_topic_partitions({}, "canary", admin=admin)

    @patch("src.topic.const.DELETE_PROPAGATION_TIMEOUT_SECONDS", 0.3)
    def test_deletion_propagation_timeout_uses_monotonic_clock(self):
        admin = _StickyDeleteAdmin(broker_count=2, topics={"canary": 3})
        clock = FakeClock(monotonic=10, wall=1_700_000_000)
        sleeps = []

        def advance_with_wall_jump(delay):
            sleeps.append(delay)
            clock.advance(delay)
            clock.jump_wall(-1_000_000)

        with self.assertRaisesRegex(RuntimeError, "verification timed out"):
            sync_topic_partitions(
                {},
                "canary",
                admin=admin,
                monotonic_clock=clock.monotonic,
                sleep=advance_with_wall_jump,
            )

        self.assertEqual([0.1, 0.2], sleeps[:2])
        self.assertLessEqual(len(sleeps), 3)
        self.assertGreaterEqual(clock.monotonic(), 10.3)

    def test_recreation_failure_is_fatal(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 3})
        admin.fail_next("create_topic", RuntimeError("create failed"))

        with self.assertRaises(RuntimeError):
            sync_topic_partitions({}, "canary", admin=admin)

        self.assertNotIn("canary", admin.topics)

    def test_unverified_replacement_is_fatal(self):
        admin = _UnverifiedCreateAdmin(broker_count=2, topics={"canary": 3})

        with self.assertRaisesRegex(RuntimeError, "topic is absent"):
            sync_topic_partitions({}, "canary", admin=admin)

    def test_success_is_verified_and_pauses_before_delete(self):
        admin = FakeAdminClient(broker_count=2, topics={"canary": 3})
        stages = []

        result = sync_topic_partitions(
            {}, "canary", admin=admin, before_recreate=lambda: stages.append("paused")
        )

        self.assertEqual(["paused"], stages)
        self.assertTrue(result.recreated)
        self.assertEqual(2, result.partition_count)
        self.assertEqual(2, admin.topics["canary"])

    @patch("src.main.create_partition_consumer")
    def test_replacement_pool_is_bounded_by_max_workers(self, create_consumer):
        created = []

        def create(_config, _sr, _topic, partition):
            consumer = unittest.mock.Mock()
            created.append((partition, consumer))
            return consumer, object()

        create_consumer.side_effect = create
        pool = _build_consumer_pool({}, object(), "canary", [0, 1, 2], 2)
        try:
            self.assertEqual(2, len(pool))
            self.assertEqual({0, 1}, {partition for partition, _ in created})
        finally:
            pool.close()

    @patch("src.main.create_partition_consumer")
    def test_partial_replacement_pool_is_closed_on_failure(self, create_consumer):
        first = unittest.mock.Mock()
        create_consumer.side_effect = [(first, object()), RuntimeError("boom")]

        with self.assertRaises(RuntimeError):
            _build_consumer_pool({}, object(), "canary", [0, 1], 2)

        first.close.assert_called_once_with()

    @patch("src.main.create_partition_consumer")
    def test_successful_runtime_replacement_retires_every_old_consumer_and_resets_readiness(
        self, create_consumer
    ):
        old_consumers = unittest.mock.MagicMock()
        created_consumers = {}

        def create(_config, _sr, _topic, partition):
            consumer = unittest.mock.Mock()
            created_consumers[partition] = consumer
            return consumer, f"deserializer-{partition}"

        create_consumer.side_effect = create
        store = HealthStateStore(
            range(3), warmup_checks=0, kafka_check_interval=15,
            kafka_degraded_after=60, kafka_unhealthy_after=300,
            sr_check_interval=60, sr_degraded_after=120,
            sr_unhealthy_after=300,
        )
        store.record_schema_registry_result(success=True)
        for partition in range(3):
            store.record_partition_result(partition, success=True)
            metrics.record_partition_check(
                partition, "success", partition, last_success=100.0 + partition
            )
            metrics.record_partition_check(partition, "failure", partition + 1)
        metrics.update_health_metrics(store.snapshot(), {0: 0, 1: 1, 2: 2})
        self.assertTrue(store.readiness_snapshot().ready)

        consumers, executor, partitions, streaks = (
            _replace_recreated_runtime(
                {}, object(), "canary", [0, 1, 2], old_consumers,
                replacement_partition_count=2, max_workers=10,
                health_store=store,
            )
        )
        try:
            self.assertEqual(2, len(consumers))
            self.assertEqual({0, 1}, set(created_consumers))
            self.assertEqual([0, 1], partitions)
            self.assertEqual({0: 0, 1: 0}, streaks)
            # Executor capacity remains at the configured ceiling so ordinary
            # scale-up can reuse this generation.  The worker pool above is
            # the active-concurrency bound for the two replacement partitions.
            self.assertEqual(10, executor.max_workers)
            old_consumers.close.assert_called_once_with()
            self.assertFalse(store.readiness_snapshot().ready)
            self.assertEqual(
                ("kafka:0", "kafka:1"),
                tuple(item.component for item in store.snapshot().partitions),
            )

            for collector in (
                metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
                metrics.PARTITION_CONSECUTIVE_FAILURES,
                metrics.PARTITION_CHECKS_TOTAL,
                metrics.PARTITION_CURRENT_STATE,
            ):
                samples = [
                    sample
                    for family in collector.collect()
                    for sample in family.samples
                ]
                self.assertEqual([], samples)

            for partition in partitions:
                metrics.record_partition_check(
                    partition, "success", 0, last_success=200.0 + partition
                )
            metrics.update_health_metrics(store.snapshot(), streaks)
            for collector in (
                metrics.PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS,
                metrics.PARTITION_CONSECUTIVE_FAILURES,
                metrics.PARTITION_CHECKS_TOTAL,
                metrics.PARTITION_CURRENT_STATE,
            ):
                emitted_partitions = {
                    sample.labels["partition"]
                    for family in collector.collect()
                    for sample in family.samples
                }
                self.assertEqual({"0", "1"}, emitted_partitions)
        finally:
            metrics.reconcile_partition_metrics((), reset=True)
            executor.shutdown(wait=True)
            consumers.close()

    @patch("src.main._prepare_replacement_runtime")
    def test_old_consumer_retirement_failure_does_not_build_replacement(
        self, prepare
    ):
        old_consumers = unittest.mock.Mock()
        old_consumers.close.side_effect = RuntimeError("close failed")
        store = HealthStateStore(
            range(2), warmup_checks=0, kafka_check_interval=15,
            kafka_degraded_after=60, kafka_unhealthy_after=300,
            sr_check_interval=60, sr_degraded_after=120,
            sr_unhealthy_after=300,
        )
        store.record_schema_registry_result(success=True)
        for partition in range(2):
            store.record_partition_result(partition, success=True)

        with self.assertRaisesRegex(RuntimeError, "Could not retire every consumer"):
            _replace_recreated_runtime(
                {}, object(), "canary", [0, 1], old_consumers,
                replacement_partition_count=2, max_workers=2,
                health_store=store,
            )

        old_consumers.close.assert_called_once_with()
        prepare.assert_not_called()
        self.assertTrue(store.readiness_snapshot().ready)

    @patch("src.main.DaemonThreadPoolExecutor")
    @patch("src.main._build_consumer_pool")
    def test_scheduler_rebuild_failure_does_not_exceed_consumer_bound(
        self, build_pool, executor_type
    ):
        old_consumers = unittest.mock.Mock()
        old_streaks = {0: 4}
        store = HealthStateStore(
            (0,), kafka_check_interval=15, kafka_degraded_after=60,
            kafka_unhealthy_after=300, sr_check_interval=60,
            sr_degraded_after=120, sr_unhealthy_after=300,
        )
        store.record_partition_result(0, success=True)
        replacement = unittest.mock.Mock()
        build_pool.return_value = replacement
        executor_type.side_effect = RuntimeError("scheduler failed")

        with self.assertRaisesRegex(RuntimeError, "scheduler failed"):
            _replace_recreated_runtime(
                {}, object(), "canary", [0], old_consumers,
                replacement_partition_count=1, max_workers=1,
                health_store=store,
            )

        replacement.close.assert_called_once_with()
        old_consumers.close.assert_called_once_with()
        self.assertEqual({0: 4}, old_streaks)
        self.assertEqual(("kafka:0",), tuple(p.component for p in store.snapshot().partitions))

    @patch("src.main._prepare_replacement_runtime")
    def test_replacement_build_failure_propagates_after_bounded_retirement(self, prepare):
        prepare.side_effect = RuntimeError("pool failed")
        old_consumers = unittest.mock.Mock()
        store = HealthStateStore(
            (0,), kafka_check_interval=15, kafka_degraded_after=60,
            kafka_unhealthy_after=300, sr_check_interval=60,
            sr_degraded_after=120, sr_unhealthy_after=300,
        )

        with self.assertRaisesRegex(RuntimeError, "pool failed"):
            _replace_recreated_runtime(
                {}, object(), "canary", [0], old_consumers,
                replacement_partition_count=1, max_workers=1,
                health_store=store,
            )

        old_consumers.close.assert_called_once_with()
        self.assertEqual(("kafka:0",), tuple(p.component for p in store.snapshot().partitions))

    def test_initial_reconciliation_failure_is_logged_cleaned_up_and_propagated(self):
        config = {"kafka": {}, "schema_registry": {}, "app": {}}
        producer = unittest.mock.Mock()
        logger = unittest.mock.Mock()

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=unittest.mock.Mock()),
            patch("src.main.ensure_topic"),
            patch("src.main.SchemaRegistryClient", return_value=object()),
            patch("src.main.create_producer", return_value=(producer, object())),
            patch(
                "src.main.sync_topic_partitions",
                side_effect=RuntimeError("password=must-not-be-logged"),
            ),
            patch("src.main.configure_health_state"),
            patch("src.main.log", logger),
        ):
            with self.assertRaises(CanaryError):
                run()

        producer.flush.assert_called_once_with(timeout=5)
        logger.error.assert_called_with(
            "Fatal topic reconciliation failure",
            extra={"stage": "initial_reconciliation", "error": "CanaryError"},
        )
        self.assertNotIn("must-not-be-logged", str(logger.method_calls))

    def test_initial_pool_failure_is_logged_cleaned_up_and_propagated(self):
        config = {"kafka": {}, "schema_registry": {}, "app": {}}
        producer = unittest.mock.Mock()
        logger = unittest.mock.Mock()

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=unittest.mock.Mock()),
            patch("src.main.ensure_topic"),
            patch("src.main.SchemaRegistryClient", return_value=object()),
            patch("src.main.create_producer", return_value=(producer, object())),
            patch(
                "src.main.sync_topic_partitions",
                return_value=ReconciliationResult(1, 1),
            ),
            patch(
                "src.main._build_consumer_pool",
                side_effect=RuntimeError("pool failed"),
            ),
            patch("src.main.configure_health_state"),
            patch("src.main.log", logger),
        ):
            with self.assertRaises(CanaryError) as raised:
                run()

        self.assertNotIn("pool failed", str(raised.exception))
        producer.flush.assert_called_once_with(timeout=5)
        logger.error.assert_called_with(
            "Fatal topic reconciliation failure",
            extra={"stage": "initial_runtime_build", "error": "CanaryError"},
        )

    def test_control_loop_rebuild_failure_runs_final_cleanup_and_propagates(self):
        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {"partition.sync.interval.seconds": "60"},
        }
        old_consumers = unittest.mock.MagicMock()
        producer = unittest.mock.Mock()
        executor = unittest.mock.Mock()
        logger = unittest.mock.Mock()
        sync_calls = 0

        def reconcile(*_args, **kwargs):
            nonlocal sync_calls
            sync_calls += 1
            if sync_calls == 1:
                return ReconciliationResult(1, 1)
            kwargs["before_recreate"]()
            return ReconciliationResult(1, 1, recreated=True)

        _shutdown_event.clear()
        try:
            with (
                patch("src.main.load_config", return_value=config),
                patch("src.main.setup_logging"),
                patch("src.main.validate_ssl_connectivity"),
                patch("src.main.start_metrics_server"),
                patch("src.main.signal.signal"),
                patch("src.main.AdminClient", return_value=unittest.mock.Mock()),
                patch("src.main.ensure_topic"),
                patch("src.main.SchemaRegistryClient", return_value=object()),
                patch("src.main.create_producer", return_value=(producer, object())),
                patch("src.main.sync_topic_partitions", side_effect=reconcile),
                patch(
                    "src.main._build_consumer_pool",
                    return_value=old_consumers,
                ),
                patch("src.main.DaemonThreadPoolExecutor", return_value=executor),
                patch("src.main._submit_due_checks", return_value=()),
                patch(
                    "src.main.ScheduledOperationLane",
                    side_effect=_immediately_due_operation_lane,
                ),
                patch("src.main.configure_health_state"),
                patch("src.main.log", logger),
                patch(
                    "src.main._replace_recreated_runtime",
                    side_effect=RuntimeError("replacement rebuild failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "replacement rebuild failed"):
                    run()
        finally:
            _shutdown_event.clear()

        self.assertEqual(
            [
                call(wait=True),
                call(wait=False, cancel_futures=True),
                call(wait=True),
            ],
            executor.shutdown.call_args_list,
        )
        old_consumers.close.assert_called_once_with()
        producer.flush.assert_called_once_with(timeout=5)
        logger.error.assert_called_with(
            "Fatal topic reconciliation failure",
            extra={"stage": "replacement_runtime_build", "error": "RuntimeError"},
        )

    def test_control_loop_reconciliation_failure_is_logged_cleaned_up_and_propagated(self):
        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {"partition.sync.interval.seconds": "60"},
        }
        consumer_pool = unittest.mock.MagicMock()
        producer = unittest.mock.Mock()
        executor = unittest.mock.Mock()
        logger = unittest.mock.Mock()

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.validate_ssl_connectivity"),
            patch("src.main.start_metrics_server"),
            patch("src.main.signal.signal"),
            patch("src.main.AdminClient", return_value=unittest.mock.Mock()),
            patch("src.main.ensure_topic"),
            patch("src.main.SchemaRegistryClient", return_value=object()),
            patch("src.main.create_producer", return_value=(producer, object())),
            patch(
                "src.main.sync_topic_partitions",
                side_effect=[
                    ReconciliationResult(1, 1),
                    RuntimeError("delete failed"),
                ],
            ),
            patch(
                "src.main._build_consumer_pool",
                return_value=consumer_pool,
            ),
            patch("src.main.DaemonThreadPoolExecutor", return_value=executor),
            patch("src.main._submit_due_checks", return_value=()),
            patch(
                "src.main.ScheduledOperationLane",
                side_effect=_immediately_due_operation_lane,
            ),
            patch("src.main.configure_health_state"),
            patch("src.main.log", logger),
        ):
            with self.assertRaisesRegex(RuntimeError, "delete failed"):
                run()

        self.assertEqual(
            [
                call(wait=False, cancel_futures=True),
                call(wait=True),
            ],
            executor.shutdown.call_args_list,
        )
        consumer_pool.close.assert_called_once_with()
        producer.flush.assert_called_once_with(timeout=5)
        logger.error.assert_called_with(
            "Fatal topic reconciliation failure",
            extra={"stage": "periodic_reconciliation", "error": "RuntimeError"},
        )


if __name__ == "__main__":
    unittest.main()
