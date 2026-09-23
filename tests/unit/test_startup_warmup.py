"""Warmup capability-proof regressions using retained-client fakes."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src import main
from src.error_classifier import CanaryError, Phase


class _DeliveredMessage:
    def topic(self):
        return "canary"

    def partition(self):
        return 0

    def offset(self):
        return 1


class _Producer:
    def __init__(self):
        self.produced = []

    def produce(self, **kwargs):
        self.produced.append(kwargs)
        kwargs["on_delivery"](None, _DeliveredMessage())

    def poll(self, timeout):
        raise AssertionError("delivery callback completed synchronously")


class _SchemaCodec:
    def __init__(self, schema_registry, value=None):
        self.schema_registry = schema_registry
        self.value = value
        self.calls = 0

    def __call__(self, value, context):
        self.calls += 1
        self.schema_registry.get_subjects()
        return self.value if self.value is not None else b"encoded"


class StartupWarmupTests(unittest.TestCase):
    @patch("src.main.metrics.PRODUCE_DURATION")
    @patch("src.main.metrics.SEEK_DURATION")
    @patch("src.main.seek_to_end")
    def test_check_proves_capabilities_with_retained_operational_clients(
        self, seek_to_end, _seek_metric, _produce_metric
    ):
        schema_registry = Mock()
        producer = _Producer()
        consumer = object()
        serializer = _SchemaCodec(schema_registry)
        sent_holder = {}
        deserializer = _SchemaCodec(schema_registry)

        def consume(retained_consumer, retained_deserializer, message_id, timeout):
            self.assertIs(consumer, retained_consumer)
            self.assertIs(deserializer, retained_deserializer)
            retained_deserializer(b"encoded", object())
            sent = sent_holder["message"]
            self.assertEqual(sent.message_id, message_id)
            self.assertEqual(3.0, timeout)
            return sent, sent.send_timestamp_ms + 7

        original_produce = main.produce_canary

        def produce(retained_producer, retained_serializer, *args):
            self.assertIs(producer, retained_producer)
            self.assertIs(serializer, retained_serializer)
            sent_holder["message"] = original_produce(
                retained_producer, retained_serializer, *args
            )
            return sent_holder["message"]

        with patch("src.main.produce_canary", side_effect=produce), patch(
            "src.main.consume_canary", side_effect=consume
        ):
            latency = main.check_kafka(
                producer,
                serializer,
                consumer,
                deserializer,
                "canary",
                3.0,
                11,
                0,
            )

        self.assertEqual(7, latency)
        seek_to_end.assert_called_once_with(consumer)
        self.assertEqual(1, serializer.calls)
        self.assertEqual(1, deserializer.calls)
        self.assertEqual(2, schema_registry.get_subjects.call_count)
        self.assertEqual(1, len(producer.produced))

    @patch("src.main.metrics.PRODUCE_DURATION")
    @patch("src.main.metrics.SEEK_DURATION")
    @patch("src.main.seek_to_end")
    def test_schema_failure_prevents_produce_and_consume_proof(
        self, _seek_to_end, _seek_metric, _produce_metric
    ):
        producer = Mock()
        serializer = Mock(side_effect=ValueError("credential=do-not-report"))

        with patch("src.main.consume_canary") as consume, self.assertRaises(
            CanaryError
        ) as raised:
            main.check_kafka(
                producer,
                serializer,
                object(),
                object(),
                "canary",
                3.0,
                1,
                0,
            )

        producer.produce.assert_not_called()
        consume.assert_not_called()
        self.assertIs(Phase.PRODUCE, raised.exception.failure.phase)
        self.assertNotIn("do-not-report", str(raised.exception))

    @patch("src.main.metrics.PRODUCE_DURATION")
    @patch("src.main.metrics.SEEK_DURATION")
    @patch("src.main.seek_to_end")
    @patch("src.main.produce_canary")
    def test_consume_failure_prevents_complete_capability_proof(
        self, produce, _seek_to_end, _seek_metric, _produce_metric
    ):
        produce.return_value = SimpleNamespace(
            message_id="message", producer_host="host", check_sequence=1
        )

        with patch(
            "src.main.consume_canary", side_effect=TimeoutError("private detail")
        ), self.assertRaises(CanaryError) as raised:
            main.check_kafka(
                object(), object(), object(), object(), "canary", 3.0, 1, 0
            )

        self.assertIs(Phase.CONSUME, raised.exception.failure.phase)
        self.assertNotIn("private detail", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
