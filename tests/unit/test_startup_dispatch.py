"""Regression tests for shutdown-authorized startup dependency dispatch."""

import logging
import signal
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch

from src import main
from src.topic import ReconciliationResult


class StartupDispatchTests(unittest.TestCase):
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

    def test_native_work_runs_on_daemon_lane_outside_dispatch_guard(self):
        observed = {}

        class RecordingExecutor(main.DaemonThreadPoolExecutor):
            def submit(self, fn, /, *args, **kwargs):
                observed["submit_guard_locked"] = main._dispatch_guard.locked()
                return super().submit(fn, *args, **kwargs)

        def dependency_operation():
            observed["daemon"] = threading.current_thread().daemon
            observed["guard_locked"] = main._dispatch_guard.locked()
            return "complete"

        with patch.object(
            main.startup_executor_module,
            "DaemonThreadPoolExecutor",
            RecordingExecutor,
        ):
            result = main._run_startup_dependency(dependency_operation)

        self.assertEqual("complete", result)
        self.assertTrue(observed["submit_guard_locked"])
        self.assertTrue(observed["daemon"])
        self.assertFalse(observed["guard_locked"])

    def test_shutdown_rejection_does_not_run_or_submit_dependency_work(self):
        main._handle_signal(signal.SIGTERM, None)
        operation = Mock()
        executor = Mock()

        with patch.object(
            main.startup_executor_module,
            "DaemonThreadPoolExecutor",
            return_value=executor,
        ):
            with self.assertRaises(main._StartupDispatchRejected):
                main._run_startup_dependency(operation)

        executor.submit.assert_not_called()
        executor.shutdown.assert_called_once_with(
            wait=False, cancel_futures=True
        )
        operation.assert_not_called()

    def test_shutdown_after_authorization_does_not_hold_guard_for_native_work(self):
        operation_finished = threading.Event()

        def dependency_operation():
            main._handle_signal(signal.SIGTERM, None)
            operation_finished.set()

        main._run_startup_dependency(dependency_operation)

        self.assertTrue(operation_finished.is_set())
        self.assertIsNotNone(main._shutdown_request)

    def test_rejection_aborts_remaining_startup_stages(self):
        completed_stages = []

        def request_shutdown():
            completed_stages.append("kafka")
            main._handle_signal(signal.SIGTERM, None)

        def startup_sequence():
            main._run_startup_dependency(request_shutdown)
            main._run_startup_dependency(
                lambda: completed_stages.append("schema_registry")
            )
            main._run_startup_dependency(
                lambda: completed_stages.append("reconciliation")
            )

        with self.assertRaises(main._StartupDispatchRejected):
            startup_sequence()

        self.assertEqual(["kafka"], completed_stages)

    def test_kafka_logging_attaches_only_after_real_startup_path_completes(self):
        completed_stages = []
        expected_stages = [
            "admin",
            "kafka_validation",
            "topic",
            "log_topic",
            "log_handler",
            "schema_registry",
            "schema_registry_validation",
            "producer",
            "reconciliation",
            "consumers",
            "workers",
        ]

        class RecordingHandler(logging.Handler):
            def emit(self, _record):
                self.assert_startup_complete()
                main._shutdown_event.set()

            def assert_startup_complete(self):
                self_test.assertEqual(expected_stages, completed_stages)

        self_test = self
        handler = RecordingHandler()
        admin_client = Mock()
        schema_registry_client = object()
        consumers = MagicMock()
        consumers.__len__.return_value = 1
        runtime_executor = MagicMock()

        def stage(name, result=None):
            def invoke(*_args, **_kwargs):
                completed_stages.append(name)
                return result

            return invoke

        def validate_stage(name, expected_client):
            def invoke(client):
                self.assertIs(expected_client, client)
                completed_stages.append(name)

            return invoke

        config = {
            "kafka": {"bootstrap.servers": "broker", "custom.kafka": "kept"},
            "schema_registry": {
                "url": "https://registry.invalid",
                "custom.schema": "kept",
            },
            "app": {
                "log.topic.enabled": "true",
                "max.workers": "1",
            },
        }

        try:
            with (
                patch("src.main.load_config", return_value=config),
                patch("src.main.setup_logging"),
                patch("src.main.start_metrics_server"),
                patch(
                    "src.main.AdminClient",
                    side_effect=stage("admin", admin_client),
                ) as admin_constructor,
                patch(
                    "src.main.validate_kafka_startup_client",
                    side_effect=validate_stage("kafka_validation", admin_client),
                ) as validate_kafka,
                patch(
                    "src.main.ensure_topic", side_effect=stage("topic")
                ) as ensure_topic,
                patch("src.main.ensure_log_topic", side_effect=stage("log_topic")),
                patch(
                    "src.main.KafkaLogHandler",
                    side_effect=stage("log_handler", handler),
                ),
                patch(
                    "src.main.SchemaRegistryClient",
                    side_effect=stage("schema_registry", schema_registry_client),
                ) as schema_registry_constructor,
                patch(
                    "src.main.validate_schema_registry_startup_client",
                    side_effect=validate_stage(
                        "schema_registry_validation", schema_registry_client
                    ),
                ) as validate_schema_registry,
                patch(
                    "src.main.create_producer",
                    side_effect=stage("producer", (Mock(), object())),
                ) as create_producer,
                patch(
                    "src.main.sync_topic_partitions",
                    side_effect=stage(
                        "reconciliation", ReconciliationResult(1, 1)
                    ),
                ) as sync_topic_partitions,
                patch(
                    "src.main._build_consumer_pool",
                    side_effect=stage("consumers", consumers),
                ),
                patch(
                    "src.main.DaemonThreadPoolExecutor",
                    side_effect=stage("workers", runtime_executor),
                ),
                patch("src.main.configure_health_state"),
            ):
                main._run_lifecycle()
        finally:
            main._shutdown_event.clear()
            logging.getLogger().removeHandler(handler)

        self.assertEqual(expected_stages, completed_stages)
        admin_constructor.assert_called_once_with(config["kafka"])
        validate_kafka.assert_called_once_with(admin_client)
        ensure_topic.assert_called_once_with(
            config["kafka"], "cloud-canary", admin=admin_client
        )
        schema_registry_constructor.assert_called_once_with(
            {
                **config["schema_registry"],
                "timeout": 10.0,
            }
        )
        validate_schema_registry.assert_called_once_with(schema_registry_client)
        create_producer.assert_called_once_with(
            config["kafka"], schema_registry_client
        )
        sync_topic_partitions.assert_called_once_with(
            config["kafka"], "cloud-canary", admin=admin_client
        )


if __name__ == "__main__":
    unittest.main()
