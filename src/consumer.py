# ---------------------------------------------------------------------------
# consumer.py — Avro-deserializing Kafka consumer for canary messages
#
# Design decisions
# ----------------
# Long-lived client
#     Each consumer is assigned to a single partition at startup (via assign())
#     and reused across every check cycle.  This avoids the cost of group
#     coordinator round-trips, partition assignment, and offset fetch on every
#     check iteration.
#
# seek_to_end() instead of unique group IDs
#     An earlier design used a fresh group ID (auto.offset.reset=latest) per
#     check so the consumer would start from the end of the partition.  This
#     works but forces a new group registration, join, sync, and offset fetch
#     on every check — significant overhead.
#
#     The current approach keeps one long-lived consumer and calls seek_to_end()
#     before each produce.  seek_to_end() fetches the current high-watermark
#     offset via get_watermark_offsets() and manually seeks to it, so the next
#     poll will only return messages produced *after* that point.
#
# fetch.wait.max.ms=100
#     Reduced from the 500 ms default.  The broker normally waits up to
#     fetch.wait.max.ms before responding to a fetch request if fewer bytes
#     than fetch.min.bytes are available.  Lowering it reduces the best-case
#     consume latency at the cost of slightly more fetch requests.
#
# enable.auto.commit=false
#     Auto-commit is disabled for manually-assigned consumers.  Offset position
#     is controlled entirely by seek_to_end() before each check, so committed
#     offsets are never used for positioning.  Disabling auto-commit avoids
#     unnecessary coordinator traffic on manually-assigned partitions.
# ---------------------------------------------------------------------------

import socket
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src import constants as const
from src.schema import (
    CANARY_SCHEMA_STR,
    CanaryMessage,
    dict_to_canary,
)

# Import metrics.HOST to use the configurable instance ID.
# This respects CANARY_INSTANCE_ID env var and instance.id config setting.
from src import metrics

# ---------------------------------------------------------------------------
# Default librdkafka consumer configuration.
# Implemented as a function to pick up the current metrics.HOST value, which
# may be updated after config loading (via instance.id setting).
# Merged with connection/auth settings from config.ini in create_partition_consumer().
# ---------------------------------------------------------------------------
def _get_consumer_defaults() -> dict:
    """
    Return consumer default configuration with current instance ID.

    Called when creating consumers (after config is loaded), ensuring
    client.id respects the configurable instance ID from config.ini or env var.
    """
    return {
        # ---- Identification ----
        # Uses metrics.HOST which respects CANARY_INSTANCE_ID env var and instance.id config.
        "client.id": metrics.HOST,

        # ---- Group membership & rebalance ----
        # session.timeout.ms: how long the broker waits for a heartbeat before
        # considering the consumer dead and triggering a rebalance.  45 s gives
        # headroom for transient network hiccups without over-triggering rebalances.
        "session.timeout.ms": const.CONSUMER_SESSION_TIMEOUT_MS,

        # heartbeat.interval.ms must be < session.timeout.ms / 3 per the Kafka spec.
        # 3 s is aggressive enough to detect failures promptly.
        "heartbeat.interval.ms": const.CONSUMER_HEARTBEAT_INTERVAL_MS,

        # max.poll.interval.ms: maximum time between poll() calls before the broker
        # evicts this consumer from the group.  300 s accommodates the canary's
        # consumer.timeout.seconds (default 5 s) with ample margin.
        "max.poll.interval.ms": const.CONSUMER_MAX_POLL_INTERVAL_MS,

        # ---- Fetch behaviour (optimised for low latency) ----
        # Return a fetch response as soon as any data is available (1 byte minimum).
        "fetch.min.bytes": const.CONSUMER_FETCH_MIN_BYTES,

        # Reduce the broker's max wait time before responding to a fetch request.
        # The default 500 ms adds unnecessary latency for a canary that polls for
        # a single known message.
        "fetch.wait.max.ms": const.CONSUMER_FETCH_WAIT_MAX_MS,

        # ---- Connection / network reliability ----
        "client.dns.lookup": "use_all_dns_ips",         # required for Confluent Cloud
        "api.version.request.timeout.ms": const.API_VERSION_REQUEST_TIMEOUT_MS,
        "socket.keepalive.enable": True,
        "socket.nagle.disable": True,                   # flush small packets immediately
        "socket.connection.setup.timeout.ms": const.SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
        "reconnect.backoff.ms": const.RECONNECT_BACKOFF_MIN_MS,
        "reconnect.backoff.max.ms": const.RECONNECT_BACKOFF_MAX_MS,
        "metadata.max.age.ms": const.METADATA_MAX_AGE_MS,
    }


def create_partition_consumer(
    kafka_config: dict,
    sr_client,
    topic: str,
    partition: int,
) -> tuple[Consumer, AvroDeserializer]:
    """
    Build a consumer manually assigned to a single partition.

    This uses the stable pattern recommended by Confluent: standard Consumer
    with manual deserialization, rather than the experimental DeserializingConsumer.

    Uses consumer.assign() instead of subscribe() so no group coordinator
    round-trip is needed — the partition is available immediately without
    calling wait_for_assignment().  This is the correct pattern when the
    caller controls partition assignment explicitly (one consumer per partition).

    Parameters
    ----------
    kafka_config : dict
        Connection and authentication settings from config.ini [kafka].

    sr_client : SchemaRegistryClient
        Shared Schema Registry client used by AvroDeserializer.

    topic : str
        Kafka topic to assign (the canary topic, e.g. "cloud-canary").

    partition : int
        Zero-based partition index to assign exclusively to this consumer.

    Returns
    -------
    tuple[Consumer, AvroDeserializer]
        Consumer with partition immediately assigned, and AvroDeserializer
        for manual deserialization. Ready for seek_to_end() and consume_canary()
        without any further setup.
    """
    consumer = Consumer({
        **_get_consumer_defaults(),  # get defaults with current instance ID
        **kafka_config,
        # A unique group.id is required by librdkafka even for manually-assigned
        # consumers (it is used internally for offset commits and heartbeats).
        # The UUID ensures no two consumers share a group, avoiding committed-offset
        # inheritance across restarts.
        "group.id":           f"cloud-canary-{uuid.uuid4()}",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": False,
    })

    avro_deserializer = AvroDeserializer(
        sr_client,
        CANARY_SCHEMA_STR,
        dict_to_canary,
    )

    consumer.assign([TopicPartition(topic, partition)])
    return consumer, avro_deserializer


def seek_to_end(consumer: Consumer) -> None:
    """
    Seek all assigned partitions to their current high-watermark offset.

    This is called before every produce so that the subsequent poll() only
    returns messages produced *after* this seek point.  Without this, the
    consumer might replay stale canary messages from previous checks,
    causing the consume phase to return immediately with the wrong message.

    How it works
    ------------
    get_watermark_offsets() issues a synchronous OffsetFetch request to the
    broker to get the current (low, high) watermark pair for each partition.
    consumer.seek() then repositions the fetch offset to `high`, so the next
    poll will wait for the *next* message appended to that partition.

    Parameters
    ----------
    consumer : Consumer
        Consumer with an active partition assignment.

    Raises
    ------
    RuntimeError
        If the consumer has no assignment (e.g. a rebalance is in progress).
        The caller should treat this as a SEEK phase failure.
    """
    assignment = consumer.assignment()
    if not assignment:
        raise RuntimeError("No partition assignment — rebalance may be in progress.")

    # Drive librdkafka's internal event loop so assigned partitions transition
    # from START to ACTIVE state.  With assign() (not subscribe()), no poll
    # is ever called by the framework.  poll(0) is non-blocking; on the very
    # first call right after assign() the state machine may not have transitioned
    # yet, so retry a few times with a short sleep on _STATE errors.
    for attempt in range(const.CONSUMER_STATE_TRANSITION_MAX_ATTEMPTS):
        consumer.poll(0)
        try:
            for partition in assignment:
                # get_watermark_offsets returns (low, high).
                # low  = earliest available offset (oldest retained message)
                # high = next offset to be written (one past the last message)
                # Seeking to `high` means: "give me the next message produced after now."
                _, high = consumer.get_watermark_offsets(
                    partition,
                    timeout=const.CONSUMER_WATERMARK_TIMEOUT_SECONDS
                )
                consumer.seek(TopicPartition(partition.topic, partition.partition, high))
            return  # all seeks succeeded
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError._STATE and attempt < const.CONSUMER_STATE_TRANSITION_MAX_ATTEMPTS - 1:
                time.sleep(const.CONSUMER_STATE_TRANSITION_RETRY_SLEEP_SECONDS)
                continue
            raise

    # If we exhausted all retry attempts without success, raise an error
    raise RuntimeError(
        f"Consumer failed to transition to ACTIVE state after "
        f"{const.CONSUMER_STATE_TRANSITION_MAX_ATTEMPTS} attempts. "
        "This may indicate broker connectivity issues or consumer group problems."
    )


def consume_canary(
    consumer: Consumer,
    avro_deserializer: AvroDeserializer,
    target_id: str,
    timeout: float = 30.0,
) -> tuple[CanaryMessage, int]:
    """
    Poll until the canary message with the given message_id is received.

    The consumer may encounter messages from other concurrent canary instances
    or from the same instance's previous checks (if seek_to_end failed to
    advance past them).  It skips all messages whose message_id does not match
    `target_id`.

    Parameters
    ----------
    consumer : Consumer
        Consumer already seeked to the end (via seek_to_end) so that polling
        starts from after the produce.

    avro_deserializer : AvroDeserializer
        The Avro deserializer for decoding CanaryMessage objects.

    target_id : str
        UUID of the specific canary message to wait for.  Generated by
        produce_canary() and passed through by check_kafka().

    timeout : float
        Maximum seconds to wait before raising TimeoutError.  Should match
        consumer.timeout.seconds from config.ini (default 5 s).

    Returns
    -------
    tuple[CanaryMessage, int]
        The received CanaryMessage and the receive timestamp in milliseconds
        (captured immediately when the matching message is found).
        Latency = receive_timestamp_ms − message.send_timestamp_ms.

    Raises
    ------
    TimeoutError
        If `target_id` is not received within `timeout` seconds.  Because
        the produce already succeeded (broker acked), this indicates the
        broker is not serving the message back — possible replication lag,
        ISR issues, or consume-side connectivity loss.

    KafkaException
        Propagated from consumer.poll() for non-EOF broker errors.
    """
    deadline = time.time() + timeout

    while True:
        # Calculate remaining time dynamically to avoid unnecessary busy-waiting
        remaining = deadline - time.time()
        if remaining <= 0:
            # Timeout exceeded
            break

        # poll() with a timeout drives the librdkafka event loop and
        # returns as soon as a message (or error) is available, or after the timeout.
        # Use dynamic timeout based on remaining time (capped at CONSUMER_POLL_TIMEOUT_SECONDS).
        poll_timeout = min(remaining, const.CONSUMER_POLL_TIMEOUT_SECONDS)
        msg = consumer.poll(timeout=poll_timeout)

        if msg is None:
            # No message in this poll window; keep waiting.
            continue

        if msg.error():
            # _PARTITION_EOF is informational — the consumer has caught up
            # to the end of a partition.  Not an error; just keep polling.
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            # Any other error is unexpected; propagate to the caller.
            raise KafkaException(msg.error())

        # Manually deserialize the value using AvroDeserializer.
        # SerializationContext provides topic and field information for schema resolution.
        value: CanaryMessage = avro_deserializer(
            msg.value(),
            SerializationContext(msg.topic(), MessageField.VALUE)
        )

        # Record the receive timestamp before any further processing so it
        # is as close as possible to the actual arrival of the message.
        receive_ts = int(time.time() * 1000)

        if value and value.message_id == target_id:
            return value, receive_ts
        # Otherwise skip — this is a message from a different check cycle
        # (unlikely with seek_to_end but handled for correctness).

    raise TimeoutError(f"Message '{target_id}' not received within {timeout}s.")
