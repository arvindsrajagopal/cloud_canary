"""Pure recovery decisions for bounded operational failure descriptors."""

from enum import Enum

from src.error_classifier import (
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    Phase,
    Recoverability,
)


class RecoveryContext(str, Enum):
    """Lifecycle context in which a failure was observed."""

    STARTUP = "STARTUP"
    RUNTIME = "RUNTIME"


class RecoveryAction(str, Enum):
    """Closed set of actions that callers may execute after classification."""

    RECORD_AND_CONTINUE = "RECORD_AND_CONTINUE"
    REPLACE_CONSUMER = "REPLACE_CONSUMER"
    MARK_COMPONENT_UNHEALTHY = "MARK_COMPONENT_UNHEALTHY"
    FAIL_STARTUP = "FAIL_STARTUP"
    TERMINATE_PROCESS = "TERMINATE_PROCESS"


_STARTUP_FATAL_CATEGORIES = frozenset(
    {
        ErrorCategory.AUTHENTICATION,
        ErrorCategory.AUTHORIZATION,
        ErrorCategory.TLS_CERTIFICATE,
        ErrorCategory.CONFIGURATION,
    }
)
_INTERNAL_FATAL_COMPONENTS = frozenset(
    {FailureComponent.SCHEDULER, FailureComponent.INTERNAL_STATE}
)
_INTERNAL_FATAL_PHASES = frozenset({Phase.SCHEDULER, Phase.STATE_UPDATE})
_RECONCILIATION_PHASES = frozenset(
    {
        Phase.TOPIC_CREATE,
        Phase.TOPIC_EXPAND,
        Phase.TOPIC_DELETE,
        Phase.TOPIC_VERIFY,
    }
)


def recovery_action(
    failure: FailureDescriptor,
    context: RecoveryContext,
) -> RecoveryAction:
    """Map a classified failure and lifecycle context to one bounded action."""
    if not isinstance(failure, FailureDescriptor):
        raise TypeError("failure must be a FailureDescriptor")
    if not isinstance(context, RecoveryContext):
        raise TypeError("context must be a RecoveryContext")

    if (
        failure.recoverability is Recoverability.DETERMINISTIC
        and failure.category in _STARTUP_FATAL_CATEGORIES
    ):
        if context is RecoveryContext.STARTUP:
            return RecoveryAction.FAIL_STARTUP
        return RecoveryAction.MARK_COMPONENT_UNHEALTHY

    if (
        failure.recoverability is Recoverability.INTERNAL_FATAL
        or failure.category
        in {ErrorCategory.SERIALIZATION, ErrorCategory.INTERNAL}
        or failure.component is FailureComponent.TOPIC_ADMINISTRATION
        or failure.phase in _RECONCILIATION_PHASES
    ):
        return RecoveryAction.TERMINATE_PROCESS

    if (
        context is RecoveryContext.STARTUP
        and failure.category in _STARTUP_FATAL_CATEGORIES
    ):
        return RecoveryAction.FAIL_STARTUP

    if failure.category is ErrorCategory.CLIENT_STATE:
        return RecoveryAction.REPLACE_CONSUMER

    if failure.category in _STARTUP_FATAL_CATEGORIES:
        return RecoveryAction.MARK_COMPONENT_UNHEALTHY

    if failure.category is ErrorCategory.CAPACITY:
        return RecoveryAction.MARK_COMPONENT_UNHEALTHY

    if (
        failure.component in _INTERNAL_FATAL_COMPONENTS
        or failure.phase in _INTERNAL_FATAL_PHASES
    ):
        return RecoveryAction.TERMINATE_PROCESS

    return RecoveryAction.RECORD_AND_CONTINUE
