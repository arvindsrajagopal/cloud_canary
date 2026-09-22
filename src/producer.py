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
#
# poll() instead of flush() for multi-threaded concurrency
#     produce_canary() uses producer.poll() in a loop instead of flush() to
#     wait for delivery callbacks. flush() is a global blocking operation that
#     waits for ALL in-flight messages across ALL threads to complete, causing
#     effective serialization in ThreadPoolExecutor environments. On a 100-partition
#     cluster with 20 concurrent workers, flush() would serialize checks into a
#     sequential 25-minute cycle instead of the desired 15-second parallel cycle.
#     poll() processes callbacks without blocking on other threads' messages,
#     enabling true parallelization (20x performance improvement).
# ---------------------------------------------------------------------------

import logging
import socket
import threading
import time
import uuid
from typing import Callable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src import constants as const
from src.schema import (
    CANARY_SCHEMA_STR,
    CanaryMessage,
    canary_to_dict,
)

log = logging.getLogger(__name__)

# Import metrics.HOST to use the configurable instance ID.
# This respects the CANARY_INSTANCE_ID environment variable.
from src import metrics

# ---------------------------------------------------------------------------
# Default librdkafka producer configuration.
# Implemented as a function to pick up the current metrics.HOST value, which
# is established when the metrics module is imported.
# These are merged with the user-supplied connection/auth settings from
# config.ini (which take precedence via the dict merge order in create_producer).
# ---------------------------------------------------------------------------
def _get_producer_defaults() -> dict:
    """
    Return producer default configuration with current instance ID.

    Called when creating the producer (after config is loaded), ensuring
    client.id respects the configurable instance ID from config.ini or env var.
    """
    return {
        # ---- Identification ----
        # Used in broker logs to identify which client produced a message.
        # Uses metrics.HOST, which respects the CANARY_INSTANCE_ID environment variable.
        "client.id": metrics.HOST,

        # ---- Durability ----
        # Require all in-sync replicas to ack before the delivery callback succeeds.
        # This exercises the full replication pipeline on every canary check.
        "acks": "all",

        # ---- Retry / timeout ----
        # retries=MAX_INT defers all retry control to delivery.timeout.ms so
        # the producer keeps trying until the 2-minute budget is exhausted.
        "retries": const.MAX_PRODUCER_RETRIES,
        "delivery.timeout.ms": const.PRODUCER_DELIVERY_TIMEOUT_MS,

        # Keep up to 5 in-flight requests per broker connection for throughput;
        # safe without idempotence because the canary doesn't require ordering.
        "max.in.flight.requests.per.connection": const.MAX_IN_FLIGHT_REQUESTS,

        # ---- Batching (optimised for latency, not throughput) ----
        "linger.ms": 0,                         # send immediately, no batching delay
        "batch.size": const.PRODUCER_BATCH_SIZE_BYTES,
        "compression.type": "none",             # no compression: single tiny message
        "queue.buffering.max.messages": const.PRODUCER_QUEUE_MAX_MESSAGES,
        "queue.buffering.max.kbytes": const.PRODUCER_QUEUE_MAX_KBYTES,

        # ---- Connection / network reliability ----
        # Required by Confluent Cloud: resolve all IPs behind the DNS name and
        # try each one so that a single unhealthy broker doesn't block connections.
        "client.dns.lookup": "use_all_dns_ips",

        # Increase the API version negotiation timeout from the 10 s default;
        # Confluent Cloud can occasionally take longer on cold start.
        "api.version.request.timeout.ms": const.API_VERSION_REQUEST_TIMEOUT_MS,

        # Keep TCP connections alive to detect silent network drops quickly.
        "socket.keepalive.enable": True,

        # Disable Nagle's algorithm so small packets (like a single canary message)
        # are sent immediately rather than waiting to fill a TCP segment.
        "socket.nagle.disable": True,

        "socket.connection.setup.timeout.ms": const.SOCKET_CONNECTION_SETUP_TIMEOUT_MS,

        # Exponential backoff on reconnect attempts (1 s base, 10 s cap).
        "reconnect.backoff.ms": const.RECONNECT_BACKOFF_MIN_MS,
        "reconnect.backoff.max.ms": const.RECONNECT_BACKOFF_MAX_MS,

        # Refresh broker metadata every 5 minutes so partition leadership changes
        # (e.g. after a broker restart) are picked up without manual intervention.
        "metadata.max.age.ms": const.METADATA_MAX_AGE_MS,
    }


def create_producer(kafka_config: dict, sr_client) -> tuple[Producer, AvroSerializer]:
    """
    Build and return a long-lived Producer with Avro value serialization.

    This uses the stable pattern recommended by Confluent: standard Producer
    with manual serialization, rather than the experimental SerializingProducer.

    Parameters
    ----------
    kafka_config : dict
        Connection and authentication settings from config.ini [kafka].
        Merged with producer defaults from _get_producer_defaults(), so any
        overlapping key in kafka_config overrides the default.

    sr_client : SchemaRegistryClient
        Shared Schema Registry client.  The AvroSerializer uses it to
        register (or look up) the CanaryCheck schema subject on first use.

    Returns
    -------
    tuple[Producer, AvroSerializer]
        Producer instance and AvroSerializer. Call produce() with manually
        serialized values and poll the producer to serve delivery callbacks.
    """
    producer = Producer({
        **_get_producer_defaults(),  # get defaults with current instance ID
        **kafka_config,               # auth/connection settings override defaults
    })

    avro_serializer = AvroSerializer(
        sr_client,
        CANARY_SCHEMA_STR,
        canary_to_dict,  # converts CanaryMessage dataclass → dict for Avro encoding
    )

    return producer, avro_serializer


def produce_canary(
    producer: Producer,
    avro_serializer: AvroSerializer,
    topic: str,
    check_sequence: int,
    partition: int,
    *,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> CanaryMessage:
    """
    Produce a single canary message to a specific partition and block until
    the broker acknowledges it.

    Thread-safe implementation using threading.Event to eliminate race conditions
    in the delivery callback mechanism. Safe to call concurrently from multiple
    threads using the same producer instance.

    The send_timestamp_ms is captured just before produce() is called so that
    network + broker write time is included in the end-to-end latency.

    Parameters
    ----------
    producer : Producer
        The long-lived producer created by create_producer().

    avro_serializer : AvroSerializer
        The Avro serializer for encoding CanaryMessage objects.

    topic : str
        Kafka topic to produce to (the canary topic, e.g. "cloud-canary").

    check_sequence : int
        Monotonically increasing check counter, embedded in the message so
        that receivers can detect gaps caused by restarts or skipped checks.

    partition : int
        Target partition index.  Pinning the message to a specific partition
        provides per-partition latency attribution. Distinct broker coverage
        depends on Kafka's current partition-leader placement and is not
        guaranteed.

    Returns
    -------
    CanaryMessage
        The message that was sent, including its generated message_id and
        send_timestamp_ms.  The consumer uses message_id to find this exact
        message and send_timestamp_ms to compute end-to-end latency.

    Raises
    ------
    KafkaException
        If librdkafka reports a delivery failure before the local callback wait
        expires, or if any other produce error occurs.

    RuntimeError
        If the delivery callback does not fire within the timeout period
        (PRODUCER_FLUSH_TIMEOUT_SECONDS).
    """
    message = CanaryMessage(
        message_id=        str(uuid.uuid4()),         # unique ID for consumer to match this message
        send_timestamp_ms= int(time.time() * 1000),   # capture as late as possible before produce
        producer_host=     metrics.HOST,              # use configurable instance ID
        check_sequence=    check_sequence,
    )

    # Thread-safe callback state using Event and optional error holder.
    # Event.set() is atomic and thread-safe, providing happens-before
    # relationship that guarantees delivery_error is visible after wait()
    # returns True.
    callback_complete = threading.Event()
    delivery_error = None

    def on_delivery(err, msg):
        """
        Delivery callback - called by librdkafka thread when broker acks.

        Thread-safe: Uses Event.set() which is atomic and thread-safe.
        The error is captured before setting the event, ensuring atomicity.
        """
        nonlocal delivery_error

        if err:
            delivery_error = err
            # Use % formatting for lazy evaluation (not evaluated if ERROR level disabled)
            log.error("Delivery failed: %s", err)
        else:
            # Use % formatting for lazy evaluation (not evaluated if DEBUG level disabled)
            log.debug(
                "Delivered to %s[%s] @ offset %s",
                msg.topic(), msg.partition(), msg.offset()
            )

        # Signal completion atomically.
        # Once set, the main thread will wake and see delivery_error state.
        callback_complete.set()

    # Manually serialize the value using AvroSerializer.
    # SerializationContext provides topic and field information for schema resolution.
    serialized_value = avro_serializer(
        message,
        SerializationContext(topic, MessageField.VALUE)
    )

    # produce() is asynchronous — it enqueues the message in librdkafka's
    # internal buffer and returns immediately.
    # Key is sent as plain string (UTF-8 encoded message_id).
    producer.produce(
        topic=topic,
        partition=partition,
        key=message.message_id.encode('utf-8'),
        value=serialized_value,
        on_delivery=on_delivery
    )

    # Use poll() instead of flush() to avoid global blocking.
    # flush() blocks until ALL in-flight messages complete across ALL threads,
    # causing serialization in multi-threaded environments (20 workers → 20x slowdown).
    # poll() processes delivery callbacks without waiting for other threads' messages,
    # enabling true parallelization of concurrent partition checks.
    deadline = monotonic_clock() + const.PRODUCER_FLUSH_TIMEOUT_SECONDS

    while not callback_complete.is_set():
        # poll() drives librdkafka's event loop to process delivery callbacks.
        # Returns immediately if no events are pending (non-blocking).
        # Short timeout (0.1s) ensures responsive callback processing while
        # allowing other threads to make progress.
        producer.poll(timeout=0.1)

        if monotonic_clock() >= deadline:
            # Timeout waiting for this message's callback.
            raise RuntimeError(
                f"Produce timed out after {const.PRODUCER_FLUSH_TIMEOUT_SECONDS}s "
                f"on partition {partition} — broker did not ack within timeout "
                "(message may still be in flight; check broker connectivity)"
            )

    # Callback fired - check result atomically.
    # At this point, delivery_error is finalized (callback won't fire again).
    if delivery_error:
        raise KafkaException(delivery_error)

    # Success - callback fired and no error.
    return message
