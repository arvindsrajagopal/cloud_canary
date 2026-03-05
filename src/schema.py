# ---------------------------------------------------------------------------
# schema.py — Avro schema definition and Python data model for canary messages
#
# Every canary check produces exactly one message conforming to this schema.
# The schema is registered with Confluent Schema Registry on first use by
# AvroSerializer; subsequent runs retrieve it by subject name, ensuring
# producer and consumer always agree on the wire format.
#
# Schema design rationale
# -----------------------
# • All fields are required primitives (no nullable unions).  Avro unions add
#   encoding overhead and complicate deserialization; for a fixed diagnostic
#   payload, required primitives are simpler and faster.
# • "long" is used for both timestamp and sequence to avoid overflow for
#   long-running deployments (int32 would overflow after ~49 days of ms values).
# • No payload/data field — the canary's job is to measure the cluster, not
#   transmit application data.  A smaller message also reduces the chance that
#   latency is dominated by serialization rather than broker round-trips.
# ---------------------------------------------------------------------------

from dataclasses import dataclass

# Avro schema JSON string passed to AvroSerializer / AvroDeserializer.
# The namespace "com.cloud.canary" scopes the subject in Schema Registry,
# preventing collisions with other Avro schemas on the same cluster.
CANARY_SCHEMA_STR = """
{
  "type": "record",
  "name": "CanaryCheck",
  "namespace": "com.cloud.canary",
  "fields": [
    {"name": "message_id",        "type": "string"},
    {"name": "send_timestamp_ms", "type": "long"},
    {"name": "producer_host",     "type": "string"},
    {"name": "check_sequence",    "type": "long"}
  ]
}
"""


@dataclass
class CanaryMessage:
    """
    In-memory representation of a single canary check message.

    Fields
    ------
    message_id : str
        UUID v4 generated at produce time.  Used by the consumer to identify
        its own message among any stale messages already on the topic, and to
        deduplicate retries without relying on broker-side idempotence.

    send_timestamp_ms : int
        Unix epoch milliseconds captured immediately before producer.produce()
        is called.  The end-to-end latency is computed as:
            receive_timestamp_ms − send_timestamp_ms
        Both timestamps are taken on the same host (producer = consumer for
        the canary), so clock skew between machines is not a concern.

    producer_host : str
        Hostname of the machine running the canary.  Useful when multiple
        canary instances run across environments; the field lets you filter
        dashboards and logs by host without needing separate topics.

    check_sequence : int
        Monotonically increasing counter incremented on every check attempt
        (including failures).  Gaps in the sequence number visible in
        Grafana/logs indicate that the process restarted or that an earlier
        check crashed before producing.
    """
    message_id: str
    send_timestamp_ms: int
    producer_host: str
    check_sequence: int


def canary_to_dict(msg: CanaryMessage, ctx) -> dict:
    """
    Serialize a CanaryMessage to a plain dict for AvroSerializer.

    The `ctx` argument is passed by the Confluent serialization framework
    (it carries the serialization context such as topic name and field name)
    but is not needed for this simple flat mapping.
    """
    return {
        "message_id":        msg.message_id,
        "send_timestamp_ms": msg.send_timestamp_ms,
        "producer_host":     msg.producer_host,
        "check_sequence":    msg.check_sequence,
    }


def dict_to_canary(data: dict, ctx) -> CanaryMessage:
    """
    Deserialize a plain dict from AvroDeserializer back into a CanaryMessage.

    The `ctx` argument is passed by the Confluent deserialization framework
    but is not needed here.
    """
    return CanaryMessage(
        message_id=        data["message_id"],
        send_timestamp_ms= data["send_timestamp_ms"],
        producer_host=     data["producer_host"],
        check_sequence=    data["check_sequence"],
    )
