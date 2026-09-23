"""Optional Kafka logging failure-isolation regressions."""

import logging
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

import src.kafka_log_handler as kafka_logging
from src.config import validate_config
from src.error_classifier import ErrorCategory


def _config(**app_overrides):
    app = {"topic": "canary"}
    app.update(app_overrides)
    return {
        "kafka": {
            "bootstrap.servers": "broker.invalid:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "test-user",
            "sasl.password": "test-password",
        },
        "schema_registry": {
            "url": "https://registry.invalid",
            "basic.auth.user.info": "test-user:test-password",
        },
        "app": app,
    }


class KafkaLogHandlerTests(unittest.TestCase):
    def _handler(self):
        producer = Mock()
        with patch.object(kafka_logging, "Producer", return_value=producer):
            handler = kafka_logging.KafkaLogHandler({}, "logs")
        return handler, producer

    @staticmethod
    def _record():
        return logging.LogRecord("test", logging.INFO, __file__, 1, "safe", (), None)

    def test_queue_full_is_counted_without_recursive_logging(self):
        handler, producer = self._handler()
        producer.produce.side_effect = BufferError("sensitive queue detail")

        with (
            patch.object(kafka_logging, "record_log_failure") as count,
            patch.object(handler, "handleError") as recursive_fallback,
        ):
            handler.emit(self._record())

        count.assert_called_once_with(ErrorCategory.CAPACITY)
        recursive_fallback.assert_not_called()

    def test_delivery_callback_counts_typed_failure_without_logging(self):
        handler, producer = self._handler()
        handler.emit(self._record())
        callback = producer.produce.call_args.kwargs["on_delivery"]
        error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)

        with patch.object(kafka_logging, "record_log_failure") as count:
            callback(error, None)

        count.assert_called_once_with(ErrorCategory.AUTHORIZATION)

    def test_emit_failure_never_changes_or_raises_to_caller(self):
        handler, producer = self._handler()
        producer.produce.side_effect = KafkaException(
            KafkaError(KafkaError._TRANSPORT)
        )

        with patch.object(kafka_logging, "record_log_failure") as count:
            handler.emit(self._record())

        count.assert_called_once_with(ErrorCategory.NETWORK)

    def test_enabled_log_topic_must_differ_from_canary_topic(self):
        with self.assertRaisesRegex(ValueError, "must differ"):
            validate_config(_config(**{
                "log.topic.enabled": "true",
                "log.topic": "canary",
            }))

    def test_nonzero_flush_result_is_counted_without_raising(self):
        handler, producer = self._handler()
        producer.flush.return_value = 2

        with patch.object(kafka_logging, "record_log_failure") as count:
            handler.close()

        count.assert_called_once_with(ErrorCategory.CAPACITY)

    def test_flush_exception_is_counted_without_raising(self):
        handler, producer = self._handler()
        producer.flush.side_effect = KafkaException(
            KafkaError(KafkaError._TRANSPORT)
        )

        with patch.object(kafka_logging, "record_log_failure") as count:
            handler.close()

        count.assert_called_once_with(ErrorCategory.NETWORK)

if __name__ == "__main__":
    unittest.main()
