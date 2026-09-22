"""Fail-closed and cost-bounded state invariant regressions."""
import unittest
from unittest.mock import Mock, patch

from src.health_state import HealthStateStore, StateInvariantError

class StateInvariantTests(unittest.TestCase):
    def _store(self, partitions=(0, 1), callback=None):
        return HealthStateStore(
            partitions, window_checks=3, minimum_checks=1, warmup_checks=0,
            invariant_failure_callback=callback,
        )

    def test_malformed_scalar_fails_closed_with_bounded_classification(self):
        callback = Mock()
        store = self._store(callback=callback)
        store._failure_threshold = "not-a-number"
        with self.assertRaises(StateInvariantError):
            store.snapshot()
        with self.assertRaises(StateInvariantError):
            store.snapshot()
        self.assertEqual("state_invariant", store._fatal_internal)
        callback.assert_called_once_with()

    def test_malformed_truthy_notification_flag_still_notifies_once(self):
        callback = Mock()
        store = self._store(callback=callback)
        store._invariant_failure_notified = "already-notified"
        with self.assertRaises(StateInvariantError):
            store.snapshot()
        self.assertEqual("state_invariant", store._fatal_internal)
        store._failure_threshold = "not-a-number"
        with self.assertRaises(StateInvariantError):
            store.snapshot()
        callback.assert_called_once_with()

    def test_malformed_component_type_fails_closed(self):
        callback = Mock(side_effect=RuntimeError("shutdown callback failed"))
        store = self._store(callback=callback)
        store._partitions[0] = object()
        with self.assertRaises(StateInvariantError):
            store.readiness_snapshot()
        self.assertEqual("state_invariant", store._fatal_internal)
        callback.assert_called_once_with()

    def test_hot_partition_update_checks_only_affected_component(self):
        store = self._store()
        store._partitions[1].results = []
        with patch.object(
            store, "_validate_component", wraps=store._validate_component
        ) as validate:
            self.assertFalse(store.record_partition_result(0, success=True))
        self.assertEqual(2, validate.call_count)
        validated = [call.args[0] for call in validate.call_args_list]
        self.assertEqual([store._partitions[0]] * 2, validated)
        self.assertIsNone(store._fatal_internal)
        with self.assertRaises(StateInvariantError):
            store.snapshot()
        self.assertEqual("state_invariant", store._fatal_internal)

    def test_snapshot_full_validation_is_linear_in_component_count(self):
        store = self._store(range(50))

        with patch.object(
            store, "_validate_component", wraps=store._validate_component
        ) as validate:
            store.snapshot()
        self.assertEqual(51, validate.call_count)

    def test_topology_boundary_detects_corruption_without_rebuilding(self):
        store = self._store()
        original_partitions = store._partitions
        original_generation = store.topic_generation
        store._partitions[0].post_warmup_success = "yes"
        with self.assertRaises(StateInvariantError):
            store.replace_expected_partitions((2,), preserve_existing=False)
        self.assertIs(original_partitions, store._partitions)
        self.assertEqual(original_generation, store._topic_generation)
        self.assertEqual("state_invariant", store._fatal_internal)

    def test_shutdown_after_readiness_latches_is_valid_blocking_state(self):
        callback = Mock()
        store = self._ready_store(callback)
        store.begin_shutdown()
        readiness = store.readiness_snapshot()
        liveness = store.liveness_snapshot()
        self.assertFalse(readiness.ready)
        self.assertIn("shutdown", readiness.blocking_reasons)
        self.assertFalse(liveness.live)
        self.assertTrue(liveness.shutdown_started)
        self.assertIsNone(store._fatal_internal)
        callback.assert_not_called()

    def test_fatal_after_readiness_latches_preserves_classification(self):
        callback = Mock()
        store = self._ready_store(callback)
        store.record_fatal_internal("scheduler_stalled")
        readiness = store.readiness_snapshot()
        liveness = store.liveness_snapshot()
        self.assertFalse(readiness.ready)
        self.assertIn("fatal_internal", readiness.blocking_reasons)
        self.assertFalse(liveness.live)
        self.assertEqual("scheduler_stalled", liveness.fatal_internal)
        self.assertEqual("scheduler_stalled", store._fatal_internal)
        callback.assert_not_called()

    def _ready_store(self, callback):
        store = self._store(callback=callback)
        for partition in (0, 1):
            store.record_partition_result(partition, success=True)
        store.record_schema_registry_result(success=True)
        self.assertTrue(store.readiness_snapshot().ready)
        return store
