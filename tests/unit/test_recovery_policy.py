"""Independent tests for the centralized recovery-policy mapping."""

import unittest

from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
)
from src.recovery_policy import (
    RecoveryAction,
    RecoveryContext,
    recovery_action,
)


class RecoveryPolicyTests(unittest.TestCase):
    def descriptor(self, **overrides):
        values = {
            "component": FailureComponent.KAFKA_PARTITION,
            "phase": Phase.CONSUME,
            "category": ErrorCategory.NETWORK,
            "recoverability": Recoverability.TRANSIENT,
            "code": None,
            "safe_summary": FailureSummary.OPERATION_FAILED,
        }
        values.update(overrides)
        return FailureDescriptor(**values)

    def test_action_and_context_enums_are_closed(self):
        self.assertEqual(
            {action.value for action in RecoveryAction},
            {
                "RECORD_AND_CONTINUE",
                "REPLACE_CONSUMER",
                "MARK_COMPONENT_UNHEALTHY",
                "FAIL_STARTUP",
                "TERMINATE_PROCESS",
            },
        )
        self.assertEqual(
            {context.value for context in RecoveryContext},
            {"STARTUP", "RUNTIME"},
        )

    def test_deterministic_security_and_configuration_depend_on_context(self):
        for category in (
            ErrorCategory.AUTHENTICATION,
            ErrorCategory.AUTHORIZATION,
            ErrorCategory.TLS_CERTIFICATE,
            ErrorCategory.CONFIGURATION,
        ):
            with self.subTest(category=category):
                failure = self.descriptor(
                    category=category,
                    recoverability=Recoverability.DETERMINISTIC,
                )
                self.assertIs(
                    recovery_action(failure, RecoveryContext.STARTUP),
                    RecoveryAction.FAIL_STARTUP,
                )
                self.assertIs(
                    recovery_action(failure, RecoveryContext.RUNTIME),
                    RecoveryAction.MARK_COMPONENT_UNHEALTHY,
                )

    def test_deterministic_context_mapping_precedes_reconciliation_policy(self):
        for phase in (
            Phase.TOPIC_CREATE,
            Phase.TOPIC_EXPAND,
            Phase.TOPIC_DELETE,
            Phase.TOPIC_VERIFY,
        ):
            for category in (
                ErrorCategory.AUTHENTICATION,
                ErrorCategory.AUTHORIZATION,
                ErrorCategory.TLS_CERTIFICATE,
                ErrorCategory.CONFIGURATION,
            ):
                with self.subTest(phase=phase, category=category):
                    failure = self.descriptor(
                        component=FailureComponent.TOPIC_ADMINISTRATION,
                        phase=phase,
                        category=category,
                        recoverability=Recoverability.DETERMINISTIC,
                    )
                    self.assertIs(
                        recovery_action(failure, RecoveryContext.STARTUP),
                        RecoveryAction.FAIL_STARTUP,
                    )
                    self.assertIs(
                        recovery_action(failure, RecoveryContext.RUNTIME),
                        RecoveryAction.MARK_COMPONENT_UNHEALTHY,
                    )

    def test_transient_external_failures_record_and_continue(self):
        for category in (
            ErrorCategory.NETWORK,
            ErrorCategory.BROKER_SERVICE,
        ):
            with self.subTest(category=category):
                failure = self.descriptor(category=category)
                self.assertIs(
                    recovery_action(failure, RecoveryContext.RUNTIME),
                    RecoveryAction.RECORD_AND_CONTINUE,
                )

    def test_schema_registry_http_5xx_records_until_next_probe_cadence(self):
        failure = self.descriptor(
            component=FailureComponent.SCHEMA_REGISTRY,
            phase=Phase.SCHEMA_REGISTRY,
            category=ErrorCategory.BROKER_SERVICE,
            code="503",
        )
        for context in (RecoveryContext.STARTUP, RecoveryContext.RUNTIME):
            with self.subTest(context=context):
                self.assertIs(
                    recovery_action(failure, context),
                    RecoveryAction.RECORD_AND_CONTINUE,
                )

    def test_consumer_state_and_capacity_have_bounded_recovery(self):
        self.assertIs(
            recovery_action(
                self.descriptor(category=ErrorCategory.CLIENT_STATE),
                RecoveryContext.RUNTIME,
            ),
            RecoveryAction.REPLACE_CONSUMER,
        )
        self.assertIs(
            recovery_action(
                self.descriptor(
                    component=FailureComponent.SCHEDULER,
                    phase=Phase.SCHEDULER,
                    category=ErrorCategory.CAPACITY,
                ),
                RecoveryContext.RUNTIME,
            ),
            RecoveryAction.MARK_COMPONENT_UNHEALTHY,
        )

    def test_fatal_internal_and_reconciliation_failures_terminate(self):
        failures = (
            self.descriptor(
                category=ErrorCategory.SERIALIZATION,
                recoverability=Recoverability.INTERNAL_FATAL,
            ),
            self.descriptor(
                component=FailureComponent.INTERNAL_STATE,
                phase=Phase.STATE_UPDATE,
                category=ErrorCategory.UNKNOWN,
                recoverability=Recoverability.UNKNOWN,
            ),
            self.descriptor(
                component=FailureComponent.SCHEDULER,
                phase=Phase.SCHEDULER,
                category=ErrorCategory.INTERNAL,
                recoverability=Recoverability.INTERNAL_FATAL,
            ),
            self.descriptor(
                component=FailureComponent.TOPIC_ADMINISTRATION,
                phase=Phase.TOPIC_VERIFY,
                category=ErrorCategory.BROKER_SERVICE,
            ),
        )
        for failure in failures:
            with self.subTest(failure=failure):
                self.assertIs(
                    recovery_action(failure, RecoveryContext.RUNTIME),
                    RecoveryAction.TERMINATE_PROCESS,
                )

    def test_unknown_external_failure_records_without_claiming_health(self):
        failure = self.descriptor(
            category=ErrorCategory.UNKNOWN,
            recoverability=Recoverability.UNKNOWN,
        )
        self.assertIs(
            recovery_action(failure, RecoveryContext.RUNTIME),
            RecoveryAction.RECORD_AND_CONTINUE,
        )

    def test_policy_rejects_unstructured_inputs(self):
        with self.assertRaises(TypeError):
            recovery_action(RuntimeError("raw"), RecoveryContext.RUNTIME)
        with self.assertRaises(TypeError):
            recovery_action(self.descriptor(), "RUNTIME")


if __name__ == "__main__":
    unittest.main()
