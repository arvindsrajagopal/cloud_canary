# ---------------------------------------------------------------------------
# error_classifier.py — Error taxonomy and classification helpers
#
# The canary distinguishes failures along two dimensions:
#
#   Phase         — *where* in the check pipeline the error occurred
#   ErrorCategory — *why* the error occurred (network vs. broker vs. unknown)
#
# This distinction drives both Prometheus label cardinality and operational
# triage:
#   • NETWORK errors (DNS, TCP, TLS) usually point to client-side connectivity
#     or firewall/VPN problems and often self-resolve or require network team.
#   • BROKER errors (4xx/5xx from the broker) indicate cluster-side conditions
#     such as ISR degradation, quota violations, or broker overload.
#
# Classification starts with librdkafka's typed error code.  Negative codes
# are client-local, but only a subset are transport failures; positive codes
# are broker protocol responses unless they carry stronger security semantics.
# ---------------------------------------------------------------------------

import re
import ssl
from dataclasses import dataclass, fields
from enum import Enum
from typing import Optional

from confluent_kafka import KafkaError, KafkaException

_INVALID_SR_CONFIG_EXCEPTIONS: tuple[type[BaseException], ...] = ()
_SR_TRANSPORT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError, TimeoutError,
)
try:
    from requests.exceptions import InvalidSchema, InvalidURL, MissingSchema
    _INVALID_SR_CONFIG_EXCEPTIONS += (InvalidSchema, InvalidURL, MissingSchema)
except ImportError:  # pragma: no cover - requests is an SR client dependency
    pass

try:
    from httpx import TransportError
    from httpx import InvalidURL as HttpxInvalidURL
    from httpx import UnsupportedProtocol
    _INVALID_SR_CONFIG_EXCEPTIONS += (HttpxInvalidURL, UnsupportedProtocol)
    _SR_TRANSPORT_EXCEPTIONS += (TransportError,)
except ImportError:  # pragma: no cover - supported for older SR client releases
    pass


class Phase(str, Enum):
    """
    The pipeline stage at which a canary check error was detected.

    SCHEMA_REGISTRY
        Health check against the Schema Registry (independent of Kafka checks).
        Failure here means the SR is unreachable or returning errors, which
        would also prevent new schema registrations by producers.

    ASSIGNMENT
        Retained as a taxonomy value for compatibility. The current consumer
        implementation uses manual assign() and does not emit this phase.

    SEEK
        Fetching high-watermark offsets from the broker and seeking the
        consumer to the end of each partition.  This is a broker round-trip
        performed before every produce, so failure here is a strong signal
        of broker-side or network issues.

    PRODUCE
        Sending the canary message and waiting for the broker acknowledgement
        (acks=all, so all in-sync replicas must acknowledge).  Failure here
        means the broker received the produce request but could not satisfy it.

    CONSUME
        Polling for the canary message after a successful produce.  Because
        the broker already acked the message, a timeout here suggests the
        broker is not serving it back — possible replication lag or follower
        issue rather than a write failure.
    """
    SCHEMA_REGISTRY = "SCHEMA_REGISTRY"
    ASSIGNMENT      = "ASSIGNMENT"
    SEEK            = "SEEK"
    PRODUCE         = "PRODUCE"
    CONSUME         = "CONSUME"
    METADATA_FETCH  = "METADATA_FETCH"
    TOPIC_CREATE    = "TOPIC_CREATE"
    TOPIC_EXPAND    = "TOPIC_EXPAND"
    TOPIC_DELETE    = "TOPIC_DELETE"
    TOPIC_VERIFY    = "TOPIC_VERIFY"
    CONSUMER_CREATE = "CONSUMER_CREATE"
    CONSUMER_REPLACE = "CONSUMER_REPLACE"
    SCHEDULER       = "SCHEDULER"
    STATE_UPDATE    = "STATE_UPDATE"
    HTTP_REQUEST    = "HTTP_REQUEST"
    STARTUP         = "STARTUP"
    SHUTDOWN        = "SHUTDOWN"
    UNKNOWN         = "UNKNOWN"


class ErrorCategory(str, Enum):
    """
    High-level cause category for a failed phase.

    NETWORK
        The client could not reach the endpoint at the TCP/DNS/TLS layer.
        Examples: broker unreachable, DNS failure, expired TLS certificate.

    BROKER_SERVICE
        The broker received the request but returned an error response.
        Examples: leader not available, message too large, quota exceeded.

    UNKNOWN
        Could not be mapped to a known category.  Used as a catch-all to
        avoid silently dropping classification data.
    """
    NETWORK = "NETWORK"
    BROKER_SERVICE = "BROKER_SERVICE"
    AUTHENTICATION = "AUTHENTICATION"
    AUTHORIZATION = "AUTHORIZATION"
    TLS_CERTIFICATE = "TLS_CERTIFICATE"
    CONFIGURATION = "CONFIGURATION"
    SERIALIZATION = "SERIALIZATION"
    CLIENT_STATE = "CLIENT_STATE"
    CAPACITY = "CAPACITY"
    INTERNAL = "INTERNAL"
    UNKNOWN = "UNKNOWN"

    # Compatibility for pre-descriptor call sites; aliases do not add a value
    # when the enum is iterated or serialized.
    BROKER = BROKER_SERVICE


class FailureComponent(str, Enum):
    KAFKA_PARTITION = "KAFKA_PARTITION"
    SCHEMA_REGISTRY = "SCHEMA_REGISTRY"
    TOPIC_ADMINISTRATION = "TOPIC_ADMINISTRATION"
    SCHEDULER = "SCHEDULER"
    HTTP_SERVER = "HTTP_SERVER"
    INTERNAL_STATE = "INTERNAL_STATE"


class Recoverability(str, Enum):
    TRANSIENT = "TRANSIENT"
    DETERMINISTIC = "DETERMINISTIC"
    INTERNAL_FATAL = "INTERNAL_FATAL"
    UNKNOWN = "UNKNOWN"


MAX_SAFE_SUMMARY_LENGTH = 512
MAX_STABLE_CODE_LENGTH = 128

_STABLE_CODE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*")


class FailureSummary(str, Enum):
    """Provenance-safe summaries that never incorporate dependency data."""

    OPERATION_FAILED = "Operation failed"
    OPERATION_TIMED_OUT = "Operation timed out"
    DEPENDENCY_UNAVAILABLE = "Dependency unavailable"
    INVALID_CONFIGURATION = "Invalid configuration"
    CAPACITY_EXHAUSTED = "Capacity exhausted"
    INTERNAL_FAILURE = "Internal failure"


def sanitize_failure_summary(summary: FailureSummary) -> str:
    """Render a deterministic bounded summary from an allowlisted template."""
    if not isinstance(summary, FailureSummary):
        raise TypeError("summary must be a FailureSummary template")
    return " ".join(summary.value.split())[:MAX_SAFE_SUMMARY_LENGTH]


@dataclass(frozen=True, slots=True)
class FailureDescriptor:
    component: FailureComponent
    phase: Phase
    category: ErrorCategory
    recoverability: Recoverability
    code: Optional[str]
    safe_summary: FailureSummary

    def __post_init__(self) -> None:
        for field_name, enum_type in (
            ("component", FailureComponent),
            ("phase", Phase),
            ("category", ErrorCategory),
            ("recoverability", Recoverability),
        ):
            if not isinstance(getattr(self, field_name), enum_type):
                raise TypeError(f"{field_name} must be a {enum_type.__name__}")

        if self.code is not None:
            if not isinstance(self.code, str):
                raise TypeError("code must be a string or None")
            if (
                len(self.code) > MAX_STABLE_CODE_LENGTH
                or _STABLE_CODE.fullmatch(self.code) is None
            ):
                raise ValueError("code must be a bounded stable identifier")

        object.__setattr__(
            self,
            "safe_summary",
            sanitize_failure_summary(self.safe_summary),
        )

        # Keep the retained descriptor schema explicit and closed.
        if tuple(field.name for field in fields(self)) != (
            "component",
            "phase",
            "category",
            "recoverability",
            "code",
            "safe_summary",
        ):  # pragma: no cover - guards future edits
            raise TypeError("unexpected FailureDescriptor field")


def _kafka_codes(*names: str) -> frozenset[int]:
    """Resolve codes supported by the installed librdkafka binding."""
    return frozenset(
        value
        for name in names
        if isinstance((value := getattr(KafkaError, name, None)), int)
    )


_NETWORK_CODES = _kafka_codes(
    "_TRANSPORT", "_ALL_BROKERS_DOWN", "_RESOLVE", "_TIMED_OUT",
    "_TIMED_OUT_QUEUE", "_MSG_TIMED_OUT", "NETWORK_EXCEPTION",
)
_TLS_CODES = _kafka_codes("_SSL")
_AUTHENTICATION_CODES = _kafka_codes(
    "_AUTHENTICATION", "SASL_AUTHENTICATION_FAILED",
    "UNACCEPTABLE_CREDENTIAL",
)
_AUTHORIZATION_CODES = _kafka_codes(
    "CLUSTER_AUTHORIZATION_FAILED", "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
    "GROUP_AUTHORIZATION_FAILED", "TOPIC_AUTHORIZATION_FAILED",
    "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
)
_SERIALIZATION_CODES = _kafka_codes(
    "_KEY_SERIALIZATION", "_VALUE_SERIALIZATION", "_KEY_DESERIALIZATION",
    "_VALUE_DESERIALIZATION", "_BAD_MSG", "_BAD_COMPRESSION",
)
_CONFIGURATION_CODES = _kafka_codes(
    "_INVALID_ARG", "_INVALID_TYPE", "_NOT_CONFIGURED",
    "_UNKNOWN_PROTOCOL", "_UNSUPPORTED_FEATURE", "INVALID_CONFIG",
)
_CAPACITY_CODES = _kafka_codes("_QUEUE_FULL", "_CRIT_SYS_RESOURCE")
_CLIENT_STATE_CODES = _kafka_codes(
    "_STATE", "_ASSIGNMENT_LOST", "_EXISTING_SUBSCRIPTION", "_FENCED",
    "_IN_PROGRESS", "_MAX_POLL_EXCEEDED", "_NO_OFFSET",
    "_PREV_IN_PROGRESS", "_PURGE_INFLIGHT", "_PURGE_QUEUE",
    "_READ_ONLY", "_REVOKE_PARTITIONS", "_UNKNOWN_GROUP",
    "_UNKNOWN_PARTITION", "_UNKNOWN_TOPIC",
)


def _kafka_category(code: int) -> ErrorCategory:
    """Map a typed librdkafka code without consulting exception text."""
    for codes, category in (
        (_NETWORK_CODES, ErrorCategory.NETWORK),
        (_TLS_CODES, ErrorCategory.TLS_CERTIFICATE),
        (_AUTHENTICATION_CODES, ErrorCategory.AUTHENTICATION),
        (_AUTHORIZATION_CODES, ErrorCategory.AUTHORIZATION),
        (_SERIALIZATION_CODES, ErrorCategory.SERIALIZATION),
        (_CONFIGURATION_CODES, ErrorCategory.CONFIGURATION),
        (_CAPACITY_CODES, ErrorCategory.CAPACITY),
        (_CLIENT_STATE_CODES, ErrorCategory.CLIENT_STATE),
    ):
        if code in codes:
            return category
    if code >= 0:
        return ErrorCategory.BROKER_SERVICE
    return ErrorCategory.UNKNOWN


def classify_kafka_error(
    err: KafkaError,
    *,
    phase: Phase,
    component: FailureComponent = FailureComponent.KAFKA_PARTITION,
) -> FailureDescriptor:
    """Build a bounded descriptor from a typed Kafka error at its boundary."""
    code = err.code()
    category = _kafka_category(code)
    recoverability = {
        ErrorCategory.AUTHENTICATION: Recoverability.DETERMINISTIC,
        ErrorCategory.AUTHORIZATION: Recoverability.DETERMINISTIC,
        ErrorCategory.TLS_CERTIFICATE: Recoverability.DETERMINISTIC,
        ErrorCategory.CONFIGURATION: Recoverability.DETERMINISTIC,
        ErrorCategory.SERIALIZATION: Recoverability.INTERNAL_FATAL,
        ErrorCategory.UNKNOWN: Recoverability.UNKNOWN,
    }.get(category, Recoverability.TRANSIENT)
    summary = {
        ErrorCategory.NETWORK: FailureSummary.DEPENDENCY_UNAVAILABLE,
        ErrorCategory.BROKER_SERVICE: FailureSummary.DEPENDENCY_UNAVAILABLE,
        ErrorCategory.CONFIGURATION: FailureSummary.INVALID_CONFIGURATION,
        ErrorCategory.CAPACITY: FailureSummary.CAPACITY_EXHAUSTED,
        ErrorCategory.SERIALIZATION: FailureSummary.INTERNAL_FAILURE,
    }.get(category, FailureSummary.OPERATION_FAILED)
    try:
        code_name = err.name()
    except (AttributeError, TypeError):
        code_name = str(code)
    stable_code = f"KAFKA.{code_name}"

    return FailureDescriptor(
        component=component,
        phase=phase,
        category=category,
        recoverability=recoverability,
        code=stable_code,
        safe_summary=summary,
    )


def classify_consumer_error(exc: Exception, *, phase: Phase) -> FailureDescriptor:
    """Classify a consumer-boundary failure without retaining the exception."""
    if (
        isinstance(exc, KafkaException)
        and exc.args
        and isinstance(exc.args[0], KafkaError)
    ):
        descriptor = classify_kafka_error(exc.args[0], phase=phase)
        # A fatal client flag is only a fallback indication of invalid state;
        # stronger typed causes must retain their deterministic semantics.
        if (
            exc.args[0].fatal()
            and descriptor.category
            not in {
                ErrorCategory.AUTHENTICATION,
                ErrorCategory.AUTHORIZATION,
                ErrorCategory.TLS_CERTIFICATE,
                ErrorCategory.CONFIGURATION,
                ErrorCategory.SERIALIZATION,
            }
        ):
            return FailureDescriptor(
                component=descriptor.component,
                phase=descriptor.phase,
                category=ErrorCategory.CLIENT_STATE,
                recoverability=Recoverability.TRANSIENT,
                code=descriptor.code,
                safe_summary=FailureSummary.OPERATION_FAILED,
            )
        return descriptor

    return FailureDescriptor(
        component=FailureComponent.KAFKA_PARTITION,
        phase=phase,
        category=ErrorCategory.CLIENT_STATE,
        recoverability=Recoverability.TRANSIENT,
        code=None,
        safe_summary=FailureSummary.OPERATION_FAILED,
    )


_SR_CONFIGURATION_HTTP_STATUSES = frozenset((400, 405, 409, 422))
_SR_TRANSIENT_HTTP_STATUSES = frozenset((408, 425, 429))


def _sr_exception_chain(exc: BaseException) -> tuple[BaseException, ...]:
    """Return a bounded exception chain without inspecting exception text."""
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    chain: list[BaseException] = []
    while pending and len(chain) < 16:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        chain.append(current)
        related = (current.__cause__, current.__context__,
                   getattr(current, "reason", None), *current.args)
        pending.extend(item for item in related if isinstance(item, BaseException))
    return tuple(chain)


def _sr_http_data(
    chain: tuple[BaseException, ...],
) -> tuple[Optional[int], Optional[int]]:
    """Extract bounded typed HTTP and SR codes, preferring the nearest error."""
    for current in chain:
        status = getattr(current, "http_status_code", None)
        if not isinstance(status, int):
            status = getattr(current, "status_code", None)
        if not isinstance(status, int):
            response = getattr(current, "response", None)
            status = getattr(response, "status_code", None)
        if isinstance(status, int) and 100 <= status <= 599:
            error_code = getattr(current, "error_code", None)
            return status, error_code if isinstance(error_code, int) else None
    return None, None


def classify_schema_registry_error(exc: Exception) -> FailureDescriptor:
    """Build a bounded descriptor from typed Schema Registry failure data."""
    chain = _sr_exception_chain(exc)
    status, sr_code = _sr_http_data(chain)

    if status is not None:
        if status in (401, 403):
            category = (
                ErrorCategory.AUTHENTICATION if status == 401
                else ErrorCategory.AUTHORIZATION
            )
            recoverability = Recoverability.DETERMINISTIC
        elif status in _SR_CONFIGURATION_HTTP_STATUSES:
            category = ErrorCategory.CONFIGURATION
            recoverability = Recoverability.DETERMINISTIC
        else:
            category = ErrorCategory.BROKER_SERVICE
            recoverability = (
                Recoverability.TRANSIENT
                if status >= 500 or status in _SR_TRANSIENT_HTTP_STATUSES
                else Recoverability.UNKNOWN
            )
    else:
        typed_categories = (
            (ssl.SSLCertVerificationError, ErrorCategory.TLS_CERTIFICATE,
             Recoverability.DETERMINISTIC),
            (_INVALID_SR_CONFIG_EXCEPTIONS, ErrorCategory.CONFIGURATION,
             Recoverability.DETERMINISTIC),
            (_SR_TRANSPORT_EXCEPTIONS, ErrorCategory.NETWORK,
             Recoverability.TRANSIENT),
            (ssl.SSLError, ErrorCategory.NETWORK, Recoverability.TRANSIENT),
        )
        category, recoverability = ErrorCategory.UNKNOWN, Recoverability.UNKNOWN
        for exception_types, typed_category, typed_recoverability in typed_categories:
            if any(isinstance(item, exception_types) for item in chain):
                category, recoverability = typed_category, typed_recoverability
                break

    code = None if status is None else f"SR.HTTP.{status}"
    if code is not None and sr_code is not None:
        detailed_code = f"SR.HTTP.{status}.{sr_code}"
        if len(detailed_code) <= MAX_STABLE_CODE_LENGTH:
            code = detailed_code

    summary = {
        ErrorCategory.NETWORK: FailureSummary.DEPENDENCY_UNAVAILABLE,
        ErrorCategory.BROKER_SERVICE: FailureSummary.DEPENDENCY_UNAVAILABLE,
        ErrorCategory.CONFIGURATION: FailureSummary.INVALID_CONFIGURATION,
    }.get(category, FailureSummary.OPERATION_FAILED)
    return FailureDescriptor(
        component=FailureComponent.SCHEMA_REGISTRY,
        phase=Phase.SCHEMA_REGISTRY,
        category=category,
        recoverability=recoverability,
        code=code,
        safe_summary=summary,
    )


def classify_sr_error(exc: Exception) -> ErrorCategory:
    """Compatibility wrapper returning the descriptor's bounded category."""
    return classify_schema_registry_error(exc).category


def is_deterministic_sr_error(exc: Exception) -> bool:
    """Retain the legacy health signal for explicit security/transport setup."""
    chain = _sr_exception_chain(exc)
    status, _ = _sr_http_data(chain)
    if status is not None:
        return status in (401, 403)
    return (
        classify_schema_registry_error(exc).recoverability
        is Recoverability.DETERMINISTIC
    )


class CanaryError(Exception):
    """Transport one bounded failure through a runtime completion boundary."""

    def __init__(self, failure: FailureDescriptor) -> None:
        if not isinstance(failure, FailureDescriptor):
            raise TypeError("failure must be a FailureDescriptor")
        self.failure = failure
        super().__init__(str(self))

    @property
    def phase(self) -> Phase:
        return self.failure.phase

    @property
    def category(self) -> ErrorCategory:
        return self.failure.category

    @property
    def detail(self) -> str:
        """Compatibility name for the descriptor's provenance-safe summary."""
        return self.failure.safe_summary

    @property
    def deterministic(self) -> bool:
        """Preserve the established SR health signal from bounded fields."""
        if (
            self.failure.component is FailureComponent.SCHEMA_REGISTRY
            and self.failure.category is ErrorCategory.CONFIGURATION
            and self.failure.code is not None
            and self.failure.code.startswith("SR.HTTP.")
        ):
            return False
        return self.failure.recoverability is Recoverability.DETERMINISTIC

    def __str__(self) -> str:
        return (
            f"phase={self.phase.value} category={self.category.value} "
            f"detail={self.detail}"
        )
