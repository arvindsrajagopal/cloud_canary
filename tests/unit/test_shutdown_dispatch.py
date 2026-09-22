"""Regression tests for shutdown-linearized dependency dispatch."""

import signal
import threading
import unittest
from unittest.mock import Mock, patch

from src import main


class ShutdownDispatchTests(unittest.TestCase):
    def setUp(self):
        self.request_patch = patch.object(main, "_shutdown_request", None)
        self.claim_patch = patch.object(
            main, "_shutdown_request_claim", [main._shutdown_claim_token]
        )
        self.request_patch.start()
        self.claim_patch.start()
        main._shutdown_event.clear()

    def tearDown(self):
        main._shutdown_event.clear()
        self.claim_patch.stop()
        self.request_patch.stop()

    def test_pre_request_kafka_transaction_may_finish_bounded_submission(self):
        scheduler = Mock()
        executor = Mock()
        future = object()
        executor.submit.return_value = future

        def publish_during_transaction(_available):
            main._handle_signal(signal.SIGTERM, None)
            return (3,)

        scheduler.acquire_due.side_effect = publish_during_transaction
        active = {}

        selected = main._submit_due_checks(
            scheduler, executor, active, 1, 7, Mock()
        )

        self.assertEqual((3,), selected)
        executor.submit.assert_called_once()
        self.assertEqual((3, 7), active[future])

    def test_kafka_transaction_entering_after_request_cannot_submit(self):
        main._handle_signal(signal.SIGTERM, None)
        scheduler = Mock()
        executor = Mock()

        selected = main._submit_due_checks(
            scheduler, executor, {}, 1, 1, Mock()
        )

        self.assertEqual((), selected)
        scheduler.acquire_due.assert_not_called()
        executor.submit.assert_not_called()

    def test_sr_and_reconciliation_entering_after_request_cannot_submit(self):
        main._handle_signal(signal.SIGTERM, None)

        for operation_name in ("schema registry", "reconciliation"):
            with self.subTest(operation=operation_name):
                lane = Mock()
                dispatched = main._dispatch_scheduled_operation(lane, Mock())
                self.assertFalse(dispatched)
                lane.dispatch_if_due.assert_not_called()

    def test_waiting_transaction_is_rejected_if_request_wins_guard(self):
        lane = Mock()
        entered = threading.Event()
        finished = threading.Event()

        def attempt_dispatch():
            entered.set()
            main._dispatch_scheduled_operation(lane, Mock())
            finished.set()

        with main._dispatch_guard:
            thread = threading.Thread(target=attempt_dispatch)
            thread.start()
            self.assertTrue(entered.wait(1))
            main._handle_signal(signal.SIGTERM, None)

        self.assertTrue(finished.wait(1))
        thread.join(1)
        self.assertFalse(thread.is_alive())
        lane.dispatch_if_due.assert_not_called()

    def test_pre_request_scheduled_transaction_may_finish_submission(self):
        lane = Mock()

        def publish_during_transaction(_operation):
            main._handle_signal(signal.SIGTERM, None)
            return True

        lane.dispatch_if_due.side_effect = publish_during_transaction

        self.assertTrue(main._dispatch_scheduled_operation(lane, Mock()))
        lane.dispatch_if_due.assert_called_once()


if __name__ == "__main__":
    unittest.main()
