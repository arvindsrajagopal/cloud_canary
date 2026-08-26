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
# The classification relies on librdkafka's error code sign convention:
#   Negative codes  →  client-local / transport-level errors  →  NETWORK
#   Positive codes  →  broker-returned protocol errors         →  BROKER
# ---------------------------------------------------------------------------

from enum import Enum

from confluent_kafka import KafkaError, KafkaException


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


class ErrorCategory(str, Enum):
    """
    High-level cause category for a failed phase.

    NETWORK
        The client could not reach the endpoint at the TCP/DNS/TLS layer.
        Examples: broker unreachable, DNS failure, expired TLS certificate.

    BROKER
        The broker received the request but returned an error response.
        Examples: leader not available, message too large, quota exceeded.

    UNKNOWN
        Could not be mapped to a known category.  Used as a catch-all to
        avoid silently dropping classification data.
    """
    NETWORK = "NETWORK"
    BROKER  = "BROKER"
    UNKNOWN = "UNKNOWN"


# Explicit set of librdkafka error codes that unambiguously indicate a
# client-side / transport failure.  All of these have negative values.
# Other negative codes (e.g. _TIMED_OUT) are also treated as NETWORK because
# they originate on the client before any broker response is received.
_NETWORK_CODES = frozenset({
    KafkaError._TRANSPORT,        # TCP connection failure or loss
    KafkaError._ALL_BROKERS_DOWN, # No bootstrap broker is reachable
    KafkaError._RESOLVE,          # DNS resolution failed for the bootstrap address
    KafkaError._SSL,              # TLS/SSL handshake failed
})


def classify_kafka_error(err: KafkaError) -> ErrorCategory:
    """
    Map a librdkafka KafkaError to an ErrorCategory.

    librdkafka uses a signed integer error code space:
      Negative values  →  client-generated (local/transport)
      Positive values  →  broker protocol error codes (Kafka protocol spec)

    Parameters
    ----------
    err : KafkaError
        The error object extracted from a KafkaException or a message's
        error() field.

    Returns
    -------
    ErrorCategory
        NETWORK for transport-level failures, BROKER for protocol errors.
    """
    code = err.code()

    # Named network codes take priority for readability in logs/metrics.
    if code in _NETWORK_CODES:
        return ErrorCategory.NETWORK

    # Any other negative code is still a client-side (local) error.
    # For example, _TIMED_OUT (-185) means the client gave up waiting
    # for a broker response — this is still transport-level behaviour.
    if code < 0:
        return ErrorCategory.NETWORK

    # Positive codes are sent by the broker in its response, meaning the
    # message reached the broker but the broker rejected or failed to
    # process it (e.g. UNKNOWN_TOPIC_OR_PART, NOT_LEADER_OR_FOLLOWER).
    return ErrorCategory.BROKER


def classify_sr_error(exc: Exception) -> ErrorCategory:
    """
    Map a Schema Registry exception to an ErrorCategory.

    The confluent_kafka Schema Registry client surfaces errors as either
    SchemaRegistryError (HTTP-level, has an http_status_code) or as
    underlying requests-library exceptions when the SR is completely
    unreachable.

    Parameters
    ----------
    exc : Exception
        The raw exception caught when calling sr_client.get_subjects() or
        any other Schema Registry operation.

    Returns
    -------
    ErrorCategory
        BROKER  if the SR returned an HTTP 5xx response (server-side fault).
        NETWORK if the SR was unreachable at the connection level.
        UNKNOWN if the exception type doesn't fit either category.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryError
        if isinstance(exc, SchemaRegistryError):
            # HTTP 5xx → the SR process is up but erroring server-side (BROKER).
            # HTTP 4xx → client misconfiguration or auth failure (treat as NETWORK
            #             because it's a config/connectivity issue, not a server fault).
            return ErrorCategory.BROKER if exc.http_status_code >= 500 else ErrorCategory.NETWORK
    except ImportError:
        pass

    # When the SR host is unreachable, the requests library raises connection-
    # level exceptions whose class names contain "Connection", "Timeout", or "SSL".
    name = type(exc).__name__
    if any(kw in name for kw in ("Connection", "Timeout", "SSL")):
        return ErrorCategory.NETWORK

    return ErrorCategory.UNKNOWN


class CanaryError(Exception):
    """
    Raised when any phase of a canary check fails.

    Carries structured metadata (phase + category) alongside the raw error
    detail string so that callers can update Prometheus labels and log a
    consistent message format without re-parsing the exception text.

    Attributes
    ----------
    phase : Phase
        The pipeline stage that failed (used as a Prometheus label).
    category : ErrorCategory
        The classified cause (used as a Prometheus label).
    detail : str
        Human-readable description of the underlying error, suitable for
        log output.
    """

    def __init__(self, phase: Phase, category: ErrorCategory, detail: str) -> None:
        self.phase    = phase
        self.category = category
        self.detail   = detail
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"phase={self.phase} category={self.category} detail={self.detail}"
