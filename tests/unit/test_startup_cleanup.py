"""Regression tests for explicit partial-startup resource ownership."""

import unittest
from unittest.mock import Mock, patch

from src import consumer as consumer_module
from src import main
from src import producer as producer_module


class StartupCleanupTests(unittest.TestCase):
    def setUp(self):
        self.state_patches = (
            patch.object(main, "_shutdown_request", None),
            patch.object(
                main, "_shutdown_request_claim", [main._shutdown_claim_token]
            ),
            patch.object(main, "_fatal_shutdown_request", None),
            patch.object(
                main,
                "_fatal_shutdown_request_claim",
                [main._shutdown_claim_token],
            ),
        )
        for state_patch in self.state_patches:
            state_patch.start()
        main._shutdown_event.clear()

    def tearDown(self):
        main._shutdown_event.clear()
        for state_patch in reversed(self.state_patches):
            state_patch.stop()

    def test_cleanup_releases_every_owned_client_once_with_existing_deadline(self):
        executor = Mock()
        consumers = Mock()
        producer = Mock()
        schema_registry = Mock()
        admin = Mock()
        log_handler = Mock()
        owner = {
            "executor": executor,
            "consumers": consumers,
            "producer": producer,
            "schema_registry": schema_registry,
            "admin": admin,
            "kafka_log_handler": log_handler,
        }

        with (
            patch("src.main._shutdown_deadline", return_value=14.0),
            patch("src.main.time.monotonic", return_value=12.5),
            patch("src.main.logging.getLogger") as get_logger,
        ):
            main._cleanup_startup_resources(owner)
            main._cleanup_startup_resources(owner)

        self.assertEqual({}, owner)
        self.assertEqual(
            [
                unittest.mock.call(wait=False, cancel_futures=True),
                unittest.mock.call(wait=True),
            ],
            executor.shutdown.call_args_list,
        )
        consumers.close.assert_called_once_with()
        producer.flush.assert_called_once_with(timeout=1.5)
        schema_registry.close.assert_called_once_with()
        admin.close.assert_called_once_with()
        get_logger.return_value.removeHandler.assert_called_once_with(log_handler)
        log_handler.close.assert_called_once_with()

    def test_cleanup_only_touches_resources_already_created(self):
        admin = Mock()
        owner = {"admin": admin}

        main._cleanup_startup_resources(owner)

        admin.close.assert_called_once_with()
        self.assertEqual({}, owner)

    def test_lifecycle_preserves_single_argument_seam_and_cleans_ledger(self):
        admin = Mock()

        def lifecycle(http_owner):
            http_owner["startup_resources"] = {"admin": admin}

        with (
            patch("src.main._run_lifecycle_owned", side_effect=lifecycle) as owned,
            patch("src.main._shutdown_http_service") as shutdown_http,
        ):
            main._run_lifecycle()

        owned.assert_called_once_with({})
        shutdown_http.assert_called_once_with({})
        admin.close.assert_called_once_with()

    def test_cleanup_errors_are_sanitized_and_do_not_stop_later_cleanup(self):
        secret = "password=must-not-be-logged"
        producer = Mock()
        producer.flush.side_effect = RuntimeError(secret)
        schema_registry = Mock()
        schema_registry.close.side_effect = RuntimeError(secret)
        admin = Mock()
        admin.close.side_effect = RuntimeError(secret)
        log_handler = Mock()
        log_handler.close.side_effect = RuntimeError(secret)
        logger = Mock()

        with (
            patch("src.main.log", logger),
            patch("src.main.logging.getLogger"),
        ):
            main._cleanup_startup_resources(
                {
                    "producer": producer,
                    "schema_registry": schema_registry,
                    "admin": admin,
                    "kafka_log_handler": log_handler,
                }
            )

        producer.flush.assert_called_once_with(timeout=5.0)
        schema_registry.close.assert_called_once_with()
        admin.close.assert_called_once_with()
        log_handler.close.assert_called_once_with()
        self.assertNotIn(secret, str(logger.method_calls))
        self.assertEqual(4, logger.error.call_count)
        logger.exception.assert_not_called()

    def test_schema_validation_failure_cleans_prior_startup_clients(self):
        config = {
            "kafka": {},
            "schema_registry": {},
            "app": {"log.topic.enabled": "true"},
        }
        admin = Mock()
        schema_registry = Mock()
        log_handler = Mock()
        http_service = Mock()

        with (
            patch("src.main.load_config", return_value=config),
            patch("src.main.setup_logging"),
            patch("src.main.start_metrics_server", return_value=http_service),
            patch("src.main.configure_health_state"),
            patch("src.main.AdminClient", return_value=admin),
            patch("src.main.validate_kafka_startup_client"),
            patch("src.main.ensure_topic"),
            patch("src.main.ensure_log_topic"),
            patch("src.main.KafkaLogHandler", return_value=log_handler),
            patch("src.main.SchemaRegistryClient", return_value=schema_registry),
            patch(
                "src.main.validate_schema_registry_startup_client",
                side_effect=ValueError("credential=must-not-be-logged"),
            ),
            patch("src.main.create_producer") as create_producer,
            self.assertRaises(main.CanaryError),
        ):
            main._run_lifecycle()

        create_producer.assert_not_called()
        schema_registry.close.assert_called_once_with()
        admin.close.assert_called_once_with()
        log_handler.close.assert_called_once_with()
        http_service.shutdown.assert_called_once()

    def test_producer_factory_flushes_unpublished_client_on_serializer_failure(self):
        producer = Mock()
        failure = RuntimeError("serializer construction failed")

        with (
            patch.object(producer_module, "Producer", return_value=producer),
            patch.object(
                producer_module, "AvroSerializer", side_effect=failure
            ),
            self.assertRaisesRegex(RuntimeError, "serializer construction failed"),
        ):
            producer_module.create_producer({}, Mock())

        producer.flush.assert_called_once_with(
            timeout=producer_module.const.PRODUCER_FLUSH_TIMEOUT_SECONDS
        )

    def test_consumer_factory_closes_each_unpublished_client_before_retry(self):
        consumers = [Mock(), Mock()]

        with (
            patch.object(
                consumer_module, "Consumer", side_effect=consumers
            ),
            patch.object(
                consumer_module,
                "AvroDeserializer",
                side_effect=[
                    RuntimeError("first construction failed"),
                    RuntimeError("second construction failed"),
                ],
            ),
        ):
            for expected in ("first", "second"):
                with self.assertRaisesRegex(RuntimeError, expected):
                    consumer_module.create_partition_consumer(
                        {}, Mock(), "canary", 0
                    )

        for consumer in consumers:
            consumer.close.assert_called_once_with()

    def test_consumer_factory_closes_client_when_assignment_fails(self):
        consumer = Mock()
        consumer.assign.side_effect = RuntimeError("assignment failed")

        with (
            patch.object(consumer_module, "Consumer", return_value=consumer),
            patch.object(consumer_module, "AvroDeserializer", return_value=Mock()),
            self.assertRaisesRegex(RuntimeError, "assignment failed"),
        ):
            consumer_module.create_partition_consumer({}, Mock(), "canary", 7)

        consumer.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
