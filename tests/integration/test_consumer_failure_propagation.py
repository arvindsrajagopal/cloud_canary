"""Consumer invalidation remains bounded across worker completion."""

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from confluent_kafka import KafkaError, KafkaException

from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    Phase,
    Recoverability,
    classify_consumer_error,
)
from src.main import check_kafka
from src.worker_pool import ConsumerFailure, WorkerPool


class _Consumer:
    def __init__(self, assignment_error=None):
        self.assignment_error = assignment_error
        self.closed = False

    def assign(self, _partitions):
        if self.assignment_error is not None:
            raise self.assignment_error

    def close(self):
        self.closed = True


def _fatal_error(code):
    return KafkaException(
        KafkaError(code, "credential=do-not-propagate", fatal=True)
    )


class ConsumerFailurePropagationTests(unittest.TestCase):
    def test_typed_deterministic_category_precedes_fatal_flag(self):
        cases = (
            (KafkaError._AUTHENTICATION, ErrorCategory.AUTHENTICATION),
            (KafkaError.TOPIC_AUTHORIZATION_FAILED, ErrorCategory.AUTHORIZATION),
            (KafkaError._SSL, ErrorCategory.TLS_CERTIFICATE),
            (KafkaError._INVALID_ARG, ErrorCategory.CONFIGURATION),
        )
        for code, category in cases:
            with self.subTest(code=code):
                failure = classify_consumer_error(
                    _fatal_error(code), phase=Phase.CONSUME
                )
                self.assertIs(failure.category, category)
                self.assertIs(
                    failure.recoverability, Recoverability.DETERMINISTIC
                )

    def test_assignment_replaces_only_borrowed_with_bounded_failure(self):
        created = []
        lock = threading.Lock()

        def factory(worker_id):
            with lock:
                assignment_error = (
                    _fatal_error(KafkaError._STATE) if not created else None
                )
                consumer = _Consumer(assignment_error)
                created.append((worker_id, consumer))
            return consumer, object()

        pool = WorkerPool(size=2, topic="canary", factory=factory)
        original, unaffected = [item[1] for item in created]
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(pool.run, 3, lambda *_: None)
                with self.assertRaises(ConsumerFailure) as caught:
                    future.result(timeout=1)

            failure = caught.exception.failure
            self.assertIs(failure.phase, Phase.ASSIGNMENT)
            self.assertIs(failure.category, ErrorCategory.CLIENT_STATE)
            self.assertIsNone(caught.exception.__cause__)
            self.assertIsNone(caught.exception.__context__)
            self.assertNotIn("do-not-propagate", str(caught.exception))
            self.assertTrue(original.closed)
            self.assertFalse(unaffected.closed)
            self.assertEqual(3, len(created))
            self.assertIn(
                pool.run(4, lambda consumer, _: consumer), pool.consumers
            )
        finally:
            pool.close()

    def test_consumer_state_replacement_propagates_descriptor(self):
        created = []

        def factory(_worker_id):
            consumer = _Consumer()
            created.append(consumer)
            return consumer, object()

        pool = WorkerPool(size=1, topic="canary", factory=factory)
        try:
            with patch(
                "src.main.seek_to_end",
                side_effect=_fatal_error(KafkaError._STATE),
            ):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        pool.run,
                        0,
                        lambda consumer, deserializer: check_kafka(
                            object(), object(), consumer, deserializer,
                            "canary", 1.0, 1, 0,
                        ),
                    )
                    with self.assertRaises(ConsumerFailure) as caught:
                        future.result(timeout=1)

            self.assertIs(caught.exception.failure.phase, Phase.SEEK)
            self.assertIs(
                caught.exception.failure.category, ErrorCategory.CLIENT_STATE
            )
            self.assertNotIsInstance(caught.exception, KafkaException)
            self.assertNotIn("do-not-propagate", str(caught.exception))
            self.assertTrue(created[0].closed)
            self.assertEqual(2, len(created))
        finally:
            pool.close()

    def test_fatal_authentication_does_not_replace_consumer(self):
        created = []

        def factory(_worker_id):
            consumer = _Consumer()
            created.append(consumer)
            return consumer, object()

        pool = WorkerPool(size=1, topic="canary", factory=factory)
        try:
            with patch(
                "src.main.seek_to_end",
                side_effect=_fatal_error(KafkaError._AUTHENTICATION),
            ):
                with self.assertRaises(CanaryError) as caught:
                    pool.run(
                        0,
                        lambda consumer, deserializer: check_kafka(
                            object(), object(), consumer, deserializer,
                            "canary", 1.0, 1, 0,
                        ),
                    )

            self.assertIs(
                caught.exception.category, ErrorCategory.AUTHENTICATION
            )
            self.assertFalse(created[0].closed)
            self.assertEqual(1, len(created))
        finally:
            pool.close()


if __name__ == "__main__":
    unittest.main()
