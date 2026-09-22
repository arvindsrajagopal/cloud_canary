"""Regression tests for independent, single-flight operation lanes."""

import unittest
from concurrent.futures import Future
import threading
from unittest.mock import MagicMock, Mock, patch

from src import main
from src.consumer import consume_canary
from src.main import _shutdown_event, _submit_due_checks, run
from src.producer import produce_canary
from src.scheduler import (
    OperationDeadlineExceeded,
    PartitionScheduler,
    ScheduledOperationLane,
)
from src.topic import ReconciliationResult
from tests.fakes.clock import FakeClock


class RecordingExecutor:
    def __init__(self, *, complete_immediately=False):
        self.submissions = []
        self.shutdown_calls = []
        self.is_shutdown = False
        self.complete_immediately = complete_immediately

    def submit(self, function, *args):
        if self.is_shutdown:
            raise AssertionError("submission attempted after executor shutdown")
        future = Future()
        self.submissions.append((future, function, args))
        if self.complete_immediately:
            try:
                future.set_result(function(*args))
            except Exception as exc:
                future.set_exception(exc)
        return future

    def shutdown(self, **kwargs):
        self.shutdown_calls.append(kwargs)
        self.is_shutdown = True


class DeferredCallbackFuture(Future):
    """Expose done state before callbacks to reproduce Future's race window."""

    def __init__(self):
        super().__init__()
        self.deferred_callbacks = []

    def add_done_callback(self, callback, *, context=None):
        self.deferred_callbacks.append(callback)

    def publish_callbacks(self):
        for callback in self.deferred_callbacks:
            callback(self)


class ExecutionLaneTests(unittest.TestCase):
    def setUp(self):
        self.request_patch = patch.object(main, "_shutdown_request", None)
        self.claim_patch = patch.object(
            main, "_shutdown_request_claim", [main._shutdown_claim_token]
        )
        self.request_patch.start()
        self.claim_patch.start()
        _shutdown_event.clear()

    def tearDown(self):
        _shutdown_event.clear()
        self.claim_patch.stop()
        self.request_patch.stop()

    def test_replacement_construction_uses_guarded_abandonable_lane(self):
        observed = {}

        class RecordingExecutor(main.DaemonThreadPoolExecutor):
            def submit(self, function, /, *args, **kwargs):
                observed["submit_guard_locked"] = main._dispatch_guard.locked()
                return super().submit(function, *args, **kwargs)

        def construct_replacement():
            observed["native_guard_locked"] = main._dispatch_guard.locked()
            observed["daemon"] = threading.current_thread().daemon
            return "replacement"

        with patch.object(
            main.startup_executor_module,
            "DaemonThreadPoolExecutor",
            RecordingExecutor,
        ):
            result = main._run_replacement_runtime_transaction(
                construct_replacement
            )

        self.assertEqual("replacement", result)
        self.assertTrue(observed["submit_guard_locked"])
        self.assertFalse(observed["native_guard_locked"])
        self.assertTrue(observed["daemon"])

    def test_shutdown_before_replacement_authorization_skips_construction(self):
        construction = Mock()
        executor = Mock()
        _shutdown_event.set()

        with patch.object(
            main.startup_executor_module,
            "DaemonThreadPoolExecutor",
            return_value=executor,
        ):
            with self.assertRaises(main._ReplacementDispatchRejected):
                main._run_replacement_runtime_transaction(construction)

        executor.submit.assert_not_called()
        construction.assert_not_called()
        executor.shutdown.assert_called_once_with(
            wait=False, cancel_futures=True
        )

    @patch("src.producer.const.PRODUCER_FLUSH_TIMEOUT_SECONDS", 1)
    def test_producer_timeout_uses_monotonic_clock(self):
        clock = FakeClock(monotonic=20, wall=1_700_000_000)
        producer = Mock()

        def poll(*, timeout):
            clock.advance(0.6)
            clock.jump_wall(-1_000_000)

        producer.poll.side_effect = poll

        with self.assertRaisesRegex(RuntimeError, "Produce timed out after 1s"):
            produce_canary(
                producer,
                lambda _message, _context: b"payload",
                "canary",
                1,
                0,
                monotonic_clock=clock.monotonic,
            )

        self.assertEqual(2, producer.poll.call_count)

    def test_consumer_timeout_uses_monotonic_clock(self):
        clock = FakeClock(monotonic=30, wall=1_700_000_000)
        consumer = Mock()
        poll_timeouts = []

        def consume(*, num_messages, timeout):
            self.assertEqual(10, num_messages)
            poll_timeouts.append(timeout)
            clock.advance(timeout)
            clock.jump_wall(1_000_000)
            return []

        consumer.consume.side_effect = consume

        with self.assertRaisesRegex(TimeoutError, "not received within 1.5s"):
            consume_canary(
                consumer,
                Mock(),
                "target",
                timeout=1.5,
                monotonic_clock=clock.monotonic,
            )

        self.assertEqual([1.0, 0.5], poll_timeouts)

    def test_kafka_backlog_wakes_when_idle_lanes_become_due(self):
        clock = FakeClock()
        kafka_executor = RecordingExecutor()
        sr_executor = RecordingExecutor(complete_immediately=True)
        reconciliation_executor = RecordingExecutor(complete_immediately=True)
        lanes = []

        def lane_factory(interval, **kwargs):
            executor = (
                sr_executor
                if kwargs["thread_name_prefix"] == "cloud-canary-sr"
                else reconciliation_executor
            )
            lane = ScheduledOperationLane(
                interval,
                initial_delay=kwargs.get("initial_delay", 0),
                timeout=kwargs.get("timeout"),
                monotonic_clock=clock.monotonic,
                executor=executor,
            )
            lanes.append(lane)
            return lane

        wait_calls = 0

        def advance_control_loop(futures, **kwargs):
            nonlocal wait_calls
            wait_calls += 1
            if wait_calls == 1:
                self.assertAlmostEqual(0.25, kwargs["timeout"])
                self.assertEqual(1, len(futures))
                self.assertFalse(next(iter(futures)).done())
                self.assertEqual(1, len(sr_executor.submissions))
                self.assertEqual(0, len(reconciliation_executor.submissions))
                clock.advance(kwargs["timeout"])
            else:
                # Both non-Kafka lanes became due during the blocked wait and
                # dispatched at their target cadence before Kafka completed.
                self.assertEqual(2, len(futures))
                self.assertEqual(2, len(sr_executor.submissions))
                self.assertEqual(1, len(reconciliation_executor.submissions))
                self.assertTrue(consumers.grow.called)
                _shutdown_event.set()
            return set(), set(futures)

        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {
                "max.workers": "2",
                "partition.sync.interval.seconds": "0.25",
                "sr.check.interval.seconds": "0.25",
                "sr.check.timeout.seconds": "1",
            },
        }
        consumers = MagicMock()
        consumers.__len__.return_value = 1
        producer = Mock()
        _shutdown_event.clear()
        try:
            with (
                patch("src.main.load_config", return_value=config),
                patch("src.main.setup_logging"),
                patch("src.main.validate_ssl_connectivity"),
                patch("src.main.start_metrics_server"),
                patch("src.main.signal.signal"),
                patch("src.main.AdminClient", return_value=Mock()),
                patch("src.main.ensure_topic"),
                patch("src.main.SchemaRegistryClient", return_value=Mock()),
                patch("src.main.create_producer", return_value=(producer, object())),
                patch(
                    "src.main.sync_topic_partitions",
                    side_effect=(
                        ReconciliationResult(1, 1),
                        ReconciliationResult(2, 2),
                    ),
                ),
                patch("src.main._build_consumer_pool", return_value=consumers),
                patch(
                    "src.main.DaemonThreadPoolExecutor", return_value=kafka_executor
                ) as executor_factory,
                patch("src.main.ScheduledOperationLane", side_effect=lane_factory),
                patch("src.main.wait", side_effect=advance_control_loop),
                patch(
                    "src.main.concurrent.futures.wait",
                    return_value=(set(), set()),
                ),
                patch("src.main.configure_health_state"),
            ):
                run()
        finally:
            _shutdown_event.clear()

        self.assertEqual(2, len(kafka_executor.submissions))
        self.assertEqual(2, len(sr_executor.submissions))
        self.assertEqual(1, len(reconciliation_executor.submissions))
        self.assertEqual(2, len(lanes))
        consumers.grow.assert_called_once_with(2)
        executor_factory.assert_called_once_with(max_workers=2)
        self.assertEqual(
            [
                {"wait": False, "cancel_futures": True},
                {"wait": True},
            ],
            kafka_executor.shutdown_calls,
        )

    def test_destructive_reconciliation_drains_active_kafka_before_authorizing(self):
        old_executor = RecordingExecutor()
        replacement_executor = RecordingExecutor()
        old_consumers = MagicMock()
        old_consumers.__len__.return_value = 1
        replacement_consumers = MagicMock()
        replacement_consumers.__len__.return_value = 1
        kafka_submitted = threading.Event()
        recreation_requested = threading.Event()
        recreation_started = threading.Event()
        reconciliation_holding = threading.Event()
        release_reconciliation = threading.Event()
        idle_wait_delays = []
        original_submit = old_executor.submit

        def submit_active_check(function, *args):
            future = original_submit(function, *args)
            kafka_submitted.set()
            return future

        old_executor.submit = submit_active_check
        sync_calls = 0

        def reconcile(*_args, **kwargs):
            nonlocal sync_calls
            sync_calls += 1
            if sync_calls == 1:
                return ReconciliationResult(1, 1)
            self.assertTrue(kafka_submitted.wait(2))
            recreation_requested.set()
            kwargs["before_recreate"]()
            self.assertEqual([{"wait": True}], old_executor.shutdown_calls)
            recreation_started.set()
            reconciliation_holding.set()
            try:
                release_reconciliation.wait(0.5)
            finally:
                reconciliation_holding.clear()
            return ReconciliationResult(1, 1, recreated=True)

        def bounded_idle_wait(delay):
            if reconciliation_holding.is_set():
                idle_wait_delays.append(delay)
                release_reconciliation.set()
            return False

        wait_calls = 0

        def finish_kafka_work(futures, **_kwargs):
            nonlocal wait_calls
            wait_calls += 1
            future = next(iter(futures))
            if wait_calls == 1:
                self.assertTrue(recreation_requested.wait(2))
                self.assertFalse(recreation_started.is_set())
                self.assertFalse(old_executor.is_shutdown)
                future.set_result(1)
            else:
                future.set_result(1)
                _shutdown_event.set()
            return {future}, set()

        def lane_factory(interval, **kwargs):
            kwargs["initial_delay"] = 0
            return ScheduledOperationLane(interval, **kwargs)

        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {
                "max.workers": "1",
                "partition.sync.interval.seconds": "60",
                "sr.check.interval.seconds": "10",
                "sr.check.timeout.seconds": "1",
            },
        }
        producer = Mock()
        _shutdown_event.clear()
        try:
            with (
                patch("src.main.load_config", return_value=config),
                patch("src.main.setup_logging"),
                patch("src.main.validate_ssl_connectivity"),
                patch("src.main.start_metrics_server"),
                patch("src.main.signal.signal"),
                patch("src.main.AdminClient", return_value=Mock()),
                patch("src.main.ensure_topic"),
                patch("src.main.SchemaRegistryClient", return_value=Mock()),
                patch("src.main.create_producer", return_value=(producer, object())),
                patch("src.main.sync_topic_partitions", side_effect=reconcile),
                patch(
                    "src.main._build_consumer_pool",
                    side_effect=(old_consumers, replacement_consumers),
                ),
                patch(
                    "src.main.DaemonThreadPoolExecutor",
                    side_effect=(old_executor, replacement_executor),
                ),
                patch("src.main.ScheduledOperationLane", side_effect=lane_factory),
                patch("src.main.wait", side_effect=finish_kafka_work),
                patch.object(_shutdown_event, "wait", side_effect=bounded_idle_wait),
                patch("src.main.configure_health_state"),
            ):
                run()
        finally:
            _shutdown_event.clear()

        self.assertTrue(recreation_started.is_set())
        self.assertEqual(1, len(old_executor.submissions))
        self.assertEqual([{"wait": True}], old_executor.shutdown_calls)
        self.assertEqual(1, len(replacement_executor.submissions))
        self.assertEqual(
            [
                {"wait": False, "cancel_futures": True},
                {"wait": True},
            ],
            replacement_executor.shutdown_calls,
        )
        old_consumers.close.assert_called_once_with()
        self.assertTrue(idle_wait_delays)
        self.assertTrue(all(0 < delay <= 1 for delay in idle_wait_delays))

    def test_lane_serializes_invocations_and_coalesces_missed_occurrences(self):
        clock = FakeClock()
        executor = RecordingExecutor()
        lane = ScheduledOperationLane(
            10, monotonic_clock=clock.monotonic, executor=executor
        )

        self.assertTrue(lane.dispatch_if_due(lambda: "first"))
        clock.advance(35)
        self.assertFalse(lane.dispatch_if_due(lambda: "overlap"))
        executor.submissions[0][0].set_result("first")
        self.assertEqual("first", lane.take_completed().result())

        self.assertTrue(lane.dispatch_if_due(lambda: "coalesced"))
        self.assertFalse(lane.dispatch_if_due(lambda: "queued duplicate"))
        self.assertEqual(2, len(executor.submissions))
        executor.submissions[1][0].set_result("coalesced")
        lane.take_completed()
        clock.advance(4.999)
        self.assertFalse(lane.dispatch_if_due(lambda: "too early"))
        clock.advance(0.001)
        self.assertTrue(lane.dispatch_if_due(lambda: "next cadence"))

    def test_closed_lane_rejects_later_dispatch(self):
        executor = RecordingExecutor()
        lane = ScheduledOperationLane(10, executor=executor)

        lane.close(wait=False)

        self.assertFalse(lane.dispatch_if_due(lambda: "too late"))
        self.assertEqual([], executor.submissions)

    @patch("src.scheduler.DaemonThreadPoolExecutor")
    def test_owned_lane_close_cancels_queued_work_without_waiting(self, executor_type):
        executor = executor_type.return_value
        lane = ScheduledOperationLane(10)

        lane.close(wait=False)

        executor.shutdown.assert_called_once_with(
            wait=False, cancel_futures=True
        )

    def test_deadline_is_reported_without_allowing_a_second_probe(self):
        clock = FakeClock()
        executor = RecordingExecutor()
        lane = ScheduledOperationLane(
            10, timeout=2, monotonic_clock=clock.monotonic, executor=executor
        )

        self.assertTrue(lane.dispatch_if_due(lambda: "slow"))
        self.assertEqual(2, lane.seconds_until_deadline())
        clock.advance(1.999)
        self.assertAlmostEqual(0.001, lane.seconds_until_deadline(), places=6)
        self.assertIsNone(lane.take_completed())
        clock.advance(0.001)
        deadline = lane.take_completed()
        with self.assertRaises(OperationDeadlineExceeded) as raised:
            deadline.result()
        self.assertEqual(2, raised.exception.timeout)

        clock.advance(20)
        self.assertFalse(lane.dispatch_if_due(lambda: "overlap"))
        self.assertIsNone(lane.take_completed())
        executor.submissions[0][0].set_result("late")
        self.assertIsNone(lane.take_completed())
        self.assertTrue(lane.dispatch_if_due(lambda: "next"))

    def test_completion_after_deadline_is_never_reported_as_success(self):
        clock = FakeClock()
        executor = RecordingExecutor()
        lane = ScheduledOperationLane(
            10, timeout=2, monotonic_clock=clock.monotonic, executor=executor
        )

        lane.dispatch_if_due(lambda: "late")
        clock.advance(2.001)
        executor.submissions[0][0].set_result("late")

        with self.assertRaises(OperationDeadlineExceeded):
            lane.take_completed().result()
        self.assertIsNone(lane.take_completed())

    def test_completed_future_waits_for_timestamp_publication(self):
        clock = FakeClock()
        executor = RecordingExecutor()
        future = DeferredCallbackFuture()
        executor.submit = Mock(return_value=future)
        lane = ScheduledOperationLane(
            10, timeout=2, monotonic_clock=clock.monotonic, executor=executor
        )

        lane.dispatch_if_due(lambda: "late")
        clock.advance(2.001)
        future.set_result("late")

        self.assertIsNone(lane.take_completed())
        future.publish_callbacks()
        with self.assertRaises(OperationDeadlineExceeded):
            lane.take_completed().result()


if __name__ == "__main__":
    unittest.main()
