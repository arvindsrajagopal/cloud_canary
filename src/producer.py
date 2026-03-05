# ---------------------------------------------------------------------------
# producer.py — Avro-serializing Kafka producer for canary messages
#
# Design decisions
# ----------------
# Long-lived client
#     The producer is created once at startup and reused for every check.
#     Reuse eliminates the per-check cost of TCP/TLS handshake + broker
#     metadata fetch, which would otherwise inflate the latency measurement.
#
# No idempotence
#     enable.idempotence is intentionally omitted.  Idempotence adds a
#     broker-side sequence check and requires max.in.flight = 1 in some
#     older librdkafka versions.  The canary already deduplicates by UUID
#     (message_id), so broker-side dedup buys nothing here.
#
# No compression
#     compression.type=none avoids CPU overhead for a single small message
#     per check.  The goal is to measure cluster latency, not throughput.
#
# acks=all
#     Requires acknowledgement from all in-sync replicas before flush()
#     returns.  This tests the full replication pipeline on every check,
#     making ISR degradation visible as increased produce latency.
#
# linger.ms=0
#     Send immediately; do not wait to accumulate a batch.  Latency over
#     throughput.
# ---------------------------------------------------------------------------

import logging
import socket
import time
import uuid

from confluent_kafka import KafkaException, SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from src.schema import (
    CANARY_SCHEMA_STR,
    CanaryMessage,
    canary_to_dict,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default librdkafka producer configuration.
# These are merged with the user-supplied connection/auth settings from
# config.ini (which take precedence via the dict merge order in create_producer).
# ---------------------------------------------------------------------------
_PRODUCER_DEFAULTS = {
    # ---- Identification ----
    # Used in broker logs to identify which client produced a message.
    "client.id": socket.gethostname(),

    # ---- Durability ----
    # Require all in-sync replicas to ack before flush() returns.
    # This exercises the full replication pipeline on every canary check.
    "acks": "all",

    # ---- Retry / timeout ----
    # retries=MAX_INT defers all retry control to delivery.timeout.ms so
    # the producer keeps trying until the 2-minute budget is exhausted.
    "retries": 2147483647,
    "delivery.timeout.ms": 120000,          # 2-minute total per-message budget

    # Keep up to 5 in-flight requests per broker connection for throughput;
    # safe without idempotence because the canary doesn't require ordering.
    "max.in.flight.requests.per.connection": 5,

    # ---- Batching (optimised for latency, not throughput) ----
    "linger.ms": 0,                         # send immediately, no batching delay
    "batch.size": 65536,                    # 64 KB max batch size
    "compression.type": "none",             # no compression: single tiny message
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 1048576,  # 1 GB producer queue limit

    # ---- Connection / network reliability ----
    # Required by Confluent Cloud: resolve all IPs behind the DNS name and
    # try each one so that a single unhealthy broker doesn't block connections.
    "client.dns.lookup": "use_all_dns_ips",

    # Increase the API version negotiation timeout from the 10 s default;
    # Confluent Cloud can occasionally take longer on cold start.
    "api.version.request.timeout.ms": 30000,

    # Keep TCP connections alive to detect silent network drops quickly.
    "socket.keepalive.enable": True,

    # Disable Nagle's algorithm so small packets (like a single canary message)
    # are sent immediately rather than waiting to fill a TCP segment.
    "socket.nagle.disable": True,

    "socket.connection.setup.timeout.ms": 30000,

    # Exponential backoff on reconnect attempts (1 s base, 10 s cap).
    "reconnect.backoff.ms": 1000,
    "reconnect.backoff.max.ms": 10000,

    # Refresh broker metadata every 5 minutes so partition leadership changes
    # (e.g. after a broker restart) are picked up without manual intervention.
    "metadata.max.age.ms": 300000,
}


def create_producer(kafka_config: dict, sr_client) -> SerializingProducer:
    """
    Build and return a long-lived SerializingProducer with Avro value serialization.

    Parameters
    ----------
    kafka_config : dict
        Connection and authentication settings from config.ini [kafka].
        Merged on top of _PRODUCER_DEFAULTS, so any overlapping key in
        kafka_config overrides the default.

    sr_client : SchemaRegistryClient
        Shared Schema Registry client.  The AvroSerializer uses it to
        register (or look up) the CanaryCheck schema subject on first use.

    Returns
    -------
    SerializingProducer
        Ready-to-use producer.  Call produce() then flush() to send a message.
    """
    avro_serializer = AvroSerializer(
        sr_client,
        CANARY_SCHEMA_STR,
        canary_to_dict,   # converts CanaryMessage dataclass → dict for Avro encoding
    )
    return SerializingProducer({
        **_PRODUCER_DEFAULTS,
        **kafka_config,                              # auth/connection settings override defaults
        "key.serializer":   StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    })


def produce_canary(
    producer: SerializingProducer,
    topic: str,
    check_sequence: int,
    partition: int,
) -> CanaryMessage:
    """
    Produce a single canary message to a specific partition and block until
    the broker acknowledges it.

    The send_timestamp_ms is captured just before produce() is called so that
    network + broker write time is included in the end-to-end latency.

    Parameters
    ----------
    producer : SerializingProducer
        The long-lived producer created by create_producer().

    topic : str
        Kafka topic to produce to (the canary topic, e.g. "cloud-canary").

    check_sequence : int
        Monotonically increasing check counter, embedded in the message so
        that receivers can detect gaps caused by restarts or skipped checks.

    partition : int
        Target partition index.  Pinning the message to a specific partition
        ensures each check exercises a distinct broker leader, giving
        per-broker latency attribution.

    Returns
    -------
    CanaryMessage
        The message that was sent, including its generated message_id and
        send_timestamp_ms.  The consumer uses message_id to find this exact
        message and send_timestamp_ms to compute end-to-end latency.

    Raises
    ------
    KafkaException
        If the broker does not acknowledge the message within
        delivery.timeout.ms (120 s), or if any other produce error occurs.
    """
    message = CanaryMessage(
        message_id=        str(uuid.uuid4()),         # unique ID for consumer to match this message
        send_timestamp_ms= int(time.time() * 1000),   # capture as late as possible before produce
        producer_host=     socket.gethostname(),
        check_sequence=    check_sequence,
    )

    # Track delivery outcome via callback.  Two separate lists — one for
    # errors, one for confirmed delivery — let the caller distinguish between
    # three states after flush() returns:
    #   delivered non-empty → ack received, success
    #   errors non-empty    → broker rejected the message
    #   both empty          → flush() timed out before the callback fired
    #                         (message still in flight; not a confirmed failure
    #                          but also not a confirmed success)
    delivered = []
    errors = []

    def on_delivery(err, msg):
        if err:
            errors.append(err)
            log.error(f"Delivery failed: {err}")
        else:
            delivered.append(True)
            log.debug(
                f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
            )

    # produce() is asynchronous — it enqueues the message in librdkafka's
    # internal buffer and returns immediately.
    producer.produce(topic=topic, partition=partition, key=message.message_id, value=message, on_delivery=on_delivery)

    # flush() blocks until the delivery report callback fires (ack received
    # or delivery.timeout.ms exceeded).  The 15 s wall-clock guard prevents
    # flush() from hanging indefinitely if librdkafka itself gets stuck.
    # Note: in the multi-threaded per-partition check pattern, any thread's
    # flush() may process delivery callbacks for other threads' messages.
    # The per-call delivered/errors lists correctly capture only this message's
    # outcome regardless of which thread's flush() invokes the callback.
    producer.flush(timeout=15)

    if errors:
        raise KafkaException(errors[0])

    if not delivered:
        # flush() returned without the callback firing — the 15 s wall-clock
        # guard expired before the broker acked.  delivery.timeout.ms (120 s)
        # has not yet elapsed so this is not a definitive delivery failure, but
        # we cannot claim success either.  Raise so the caller records a PRODUCE
        # failure rather than silently returning a message that was never acked.
        raise RuntimeError(
            f"Produce flush timed out after 15s on partition {partition} — "
            "broker did not ack within the wall-clock guard "
            "(message may still be in flight; check broker connectivity)"
        )

    return message
