"""Regression tests for shutdown-aware Kafka logging lifecycle."""

import logging
import unittest
from unittest.mock import Mock, patch

import src.main as main
from src.kafka_log_handler import KafkaLogHandler


class KafkaLogHandlerShutdownTests(unittest.TestCase):
    def _handler(self, *, requested=False, deadline=None, now=0.0):
        producer = Mock()
        with patch("src.kafka_log_handler.Producer", return_value=producer):
            handler = KafkaLogHandler(
                {},
                "logs",
                shutdown_requested=lambda: requested,
                shutdown_deadline=lambda: deadline,
                monotonic=lambda: now,
            )
        return handler, producer

    def test_emit_is_disarmed_after_shutdown(self):
        handler, producer = self._handler(requested=True)

        handler.emit(
            logging.LogRecord("test", logging.INFO, __file__, 1, "x", (), None)
        )

        producer.produce.assert_not_called()
        producer.poll.assert_not_called()

    def test_close_uses_only_time_remaining_and_interpreter_close_is_noop(self):
        handler, producer = self._handler(deadline=12.5, now=10.0)

        handler.close()
        handler.close()

        producer.flush.assert_called_once_with(timeout=2.5)

    def test_expired_deadline_never_starts_a_new_flush_window(self):
        handler, producer = self._handler(deadline=9.0, now=10.0)

        handler.close()

        producer.flush.assert_called_once_with(timeout=0.0)

    def test_registration_is_rejected_after_shutdown(self):
        handler = Mock()
        root_logger = Mock()
        with (
            patch.object(main, "_shutdown_requested", return_value=True),
            patch.object(main.logging, "getLogger", return_value=root_logger),
        ):
            registered = main._register_kafka_log_handler(handler)

        self.assertFalse(registered)
        root_logger.addHandler.assert_not_called()
        handler.close.assert_called_once_with()

    def test_claimed_shutdown_without_published_request_has_no_flush_budget(self):
        with (
            patch.object(main, "_shutdown_request", None),
            patch.object(main, "_shutdown_request_claim", []),
            patch.object(main.time, "monotonic", return_value=14.0),
        ):
            self.assertEqual(14.0, main._shutdown_deadline())

    def test_registration_is_disarmed_when_shutdown_wins_add_race(self):
        handler = Mock()
        root_logger = Mock()
        with (
            patch.object(main, "_shutdown_requested", side_effect=(False, True)),
            patch.object(main.logging, "getLogger", return_value=root_logger),
        ):
            registered = main._register_kafka_log_handler(handler)

        self.assertFalse(registered)
        root_logger.addHandler.assert_called_once_with(handler)
        root_logger.removeHandler.assert_called_once_with(handler)
        handler.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
