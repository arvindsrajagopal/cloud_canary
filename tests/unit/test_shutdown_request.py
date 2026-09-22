"""Regression tests for first-write-wins shutdown request publication."""

import dataclasses
import signal
import unittest
from unittest.mock import patch

import src.main as main


class ShutdownRequestTests(unittest.TestCase):
    def setUp(self):
        self.request_patch = patch.object(main, "_shutdown_request", None)
        self.claim_patch = patch.object(
            main, "_shutdown_request_claim", [main._shutdown_claim_token]
        )
        self.timeout_patch = patch.object(main, "_shutdown_timeout_seconds", 7.5)
        self.request_patch.start()
        self.claim_patch.start()
        self.timeout_patch.start()
        self.addCleanup(self.timeout_patch.stop)
        self.addCleanup(self.claim_patch.stop)
        self.addCleanup(self.request_patch.stop)

    def test_first_signal_publishes_immutable_absolute_deadline(self):
        with patch.object(main.time, "monotonic", return_value=123.25):
            main._handle_signal(signal.SIGTERM, None)

        request = main._shutdown_request
        self.assertEqual([], main._shutdown_request_claim)
        self.assertEqual(signal.SIGTERM, request.signal_number)
        self.assertEqual(123.25, request.requested_at)
        self.assertEqual(130.75, request.deadline)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            request.deadline = 999.0

    def test_later_signals_cannot_replace_request_or_deadline(self):
        with patch.object(main.time, "monotonic", side_effect=(10.0, 80.0)) as clock:
            main._handle_signal(signal.SIGINT, None)
            first_request = main._shutdown_request
            main._handle_signal(signal.SIGTERM, None)

        self.assertIs(first_request, main._shutdown_request)
        self.assertEqual(17.5, main._shutdown_request.deadline)
        clock.assert_called_once_with()

    def test_repeated_winning_signal_cannot_replace_request_or_deadline(self):
        with patch.object(main.time, "monotonic", side_effect=(10.0, 80.0)) as clock:
            main._handle_signal(signal.SIGINT, None)
            first_request = main._shutdown_request
            main._handle_signal(signal.SIGINT, None)

        self.assertIs(first_request, main._shutdown_request)
        self.assertEqual(17.5, main._shutdown_request.deadline)
        clock.assert_called_once_with()

    def test_reentrant_signal_wins_if_earlier_invocation_has_not_claimed(self):
        class ReentrantClaim(list):
            def __init__(self):
                super().__init__([main._shutdown_claim_token])
                self.reentered = False

            def pop(self):
                if not self.reentered:
                    self.reentered = True
                    main._handle_signal(signal.SIGTERM, None)
                return super().pop()

        claim = ReentrantClaim()
        with (
            patch.object(main, "_shutdown_request_claim", claim),
            patch.object(main.time, "monotonic", return_value=40.0) as clock,
        ):
            main._handle_signal(signal.SIGINT, None)

        self.assertTrue(claim.reentered)
        clock.assert_called_once_with()
        self.assertEqual(signal.SIGTERM, main._shutdown_request.signal_number)
        self.assertEqual(40.0, main._shutdown_request.requested_at)
        self.assertEqual(47.5, main._shutdown_request.deadline)

    def test_handler_does_not_access_event_or_log(self):
        with (
            patch.object(main.time, "monotonic", return_value=5.0),
            patch.object(
                main._shutdown_event,
                "is_set",
                side_effect=AssertionError("signal handler accessed Event"),
            ),
            patch.object(
                main._shutdown_event,
                "set",
                side_effect=AssertionError("signal handler accessed Event"),
            ),
            patch.object(
                main.log,
                "info",
                side_effect=AssertionError("signal handler logged"),
            ),
        ):
            main._handle_signal(signal.SIGTERM, None)

        self.assertIsNotNone(main._shutdown_request)


if __name__ == "__main__":
    unittest.main()
