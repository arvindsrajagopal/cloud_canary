# ---------------------------------------------------------------------------
# topic.py — Kafka topic lifecycle management
#
# Responsibilities
# ----------------
# ensure_topic()          Create the canary topic at startup if it doesn't exist,
#                         with partitions = broker count and replication factor
#                         = min(brokers, 3), retention.ms = 86400000 (1 day).
#
# ensure_log_topic()      Create the optional log-capture topic with 1 partition
#                         and a configurable retention period.
#
# sync_topic_partitions() Periodically reconcile the canary topic's partition
#                         count with the current broker count.  Handles both
#                         scale-up (add partitions) and scale-down (delete and
#                         recreate, since Kafka cannot reduce partition count
#                         in place).
#
# Why partition count = broker count?
# ------------------------------------
# Each partition is served by a distinct broker leader.  Matching partitions to
# brokers ensures that every broker participates in canary traffic, so a single
# unhealthy broker will surface as a failure rather than being silently avoided
# by the round-robin partition assignment.
#
# Multi-instance safety
# ---------------------
# All three functions are safe to call concurrently from multiple instances
# running against the same cluster.  Races are handled at the error level:
#
#   ensure_topic() / ensure_log_topic()
#       If two instances race to create the same topic, the second call
#       receives TOPIC_ALREADY_EXISTS and treats it as success.
#
#   sync_topic_partitions() — scale-up
#       If two instances both call create_partitions(), the failing instance
#       re-fetches metadata; if the count already matches the target it skips.
#
#   sync_topic_partitions() — scale-down
#       If two instances race to delete the topic, UNKNOWN_TOPIC_OR_PART is
#       treated as success and the instance falls through to the propagation
#       wait.  If the topic reappears during the wait with the correct
#       partition count (recreated by another instance), recreation is skipped.
# ---------------------------------------------------------------------------

from __future__ import annotations

import logging
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

log = logging.getLogger(__name__)

# How long to wait for a topic deletion to propagate across all brokers before
# attempting to recreate it.  Kafka deletes topics asynchronously; re-creating
# before deletion completes can result in a "topic already exists" error.
_DELETE_PROPAGATION_TIMEOUT = 30  # seconds


def _get_metadata(admin: AdminClient):
    """
    Fetch current cluster metadata (brokers + topics) from the Kafka cluster.

    Parameters
    ----------
    admin : AdminClient
        An active AdminClient connected to the cluster.

    Returns
    -------
    ClusterMetadata
        Contains .brokers (dict of broker_id → BrokerMetadata) and
        .topics (dict of topic_name → TopicMetadata).

    Raises
    ------
    RuntimeError
        Wraps KafkaException with a human-readable message if the cluster
        is unreachable.
    """
    try:
        return admin.list_topics(timeout=10)
    except KafkaException as exc:
        raise RuntimeError(f"Could not reach cluster to inspect topics: {exc}")


def _create_topic(admin: AdminClient, topic: str, num_brokers: int) -> None:
    """
    Create a new Kafka topic with partitions = num_brokers and
    replication_factor = min(num_brokers, 3).

    Capping replication at 3 avoids unnecessary storage overhead on large
    clusters while still providing triple-replication durability on clusters
    with 3+ brokers.

    Parameters
    ----------
    admin : AdminClient
        Active AdminClient.

    topic : str
        Name of the topic to create.

    num_brokers : int
        Current broker count; used to size partitions and replication factor.

    Raises
    ------
    RuntimeError
        If the AdminClient returns a KafkaException during topic creation
        (e.g. authorisation failure or cluster in an unready state).
        TOPIC_ALREADY_EXISTS is not raised — it is treated as success so that
        concurrent instances racing to create the same topic at startup do not
        cause one of them to exit.
    """
    replication_factor = min(num_brokers, 3)
    log.info(
        f"Creating topic '{topic}' with "
        f"num_partitions={num_brokers}, replication_factor={replication_factor}."
    )
    futures = admin.create_topics([
        NewTopic(
            topic=topic,
            num_partitions=num_brokers,
            replication_factor=replication_factor,
            config={"retention.ms": "86400000"},  # 1 day — canary messages have no value after the check completes
        )
    ])
    for topic_name, future in futures.items():
        try:
            future.result()   # blocks until the broker confirms or rejects creation
            log.info(f"Topic '{topic_name}' created successfully.")
        except KafkaException as exc:
            # Another instance won the creation race — the topic exists and is
            # ready to use, so treat this as a success rather than an error.
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info(f"Topic '{topic_name}' already exists (created by another instance) — skipping.")
            else:
                raise RuntimeError(f"Failed to create topic '{topic_name}': {exc}")


def _delete_and_recreate(admin: AdminClient, topic: str, num_brokers: int) -> None:
    """
    Delete `topic`, wait for deletion to propagate, then recreate it.

    Kafka does not support reducing a topic's partition count in place.
    When the cluster scales down, the only way to right-size the canary topic
    is to drop it and recreate it with the new lower partition count.

    The function polls cluster metadata after deletion to confirm the topic is
    gone before recreating.  If the topic is still visible after
    _DELETE_PROPAGATION_TIMEOUT seconds, recreation is skipped and logged as
    an error; the sync will be retried on the next sync cadence.

    Multi-instance races are handled as follows:
    - Delete collision: if another instance already deleted the topic, the
      delete call returns UNKNOWN_TOPIC_OR_PART.  This is treated as success
      and the function falls through to the propagation wait.
    - Propagation wait: if the topic reappears with the correct partition count
      while waiting (meaning another instance completed delete-and-recreate),
      the function skips its own recreation and returns immediately.

    Parameters
    ----------
    admin : AdminClient
        Active AdminClient.

    topic : str
        Name of the topic to delete and recreate.

    num_brokers : int
        New (reduced) broker count; the recreated topic will have this many
        partitions.
    """
    log.warning(
        f"Cluster scaled DOWN to {num_brokers} broker(s). Topic '{topic}' cannot have its "
        f"partition count reduced in place — dropping and recreating with {num_brokers} partition(s)."
    )

    futures = admin.delete_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info(f"Topic '{topic_name}' deleted.")
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                # Another instance already deleted the topic — that is the desired
                # state.  Fall through to the propagation wait; if the other instance
                # also already recreated it with the correct partition count, the
                # wait loop will detect that and skip recreation.
                log.info(
                    f"Topic '{topic_name}' already deleted by another instance "
                    "— verifying recreation."
                )
            else:
                log.error(f"Failed to delete topic '{topic_name}': {exc}")
                return   # genuine delete failure; abort

    # Poll until the topic disappears from cluster metadata.  Deletion is
    # propagated asynchronously — the controller marks it deleted, then each
    # broker cleans up its local log segments.  Re-creating before this
    # completes can trigger a "topic already exists" error from the broker.
    #
    # Multi-instance case: if another instance deleted and already recreated
    # the topic with the correct partition count, the topic will reappear in
    # metadata before we see it disappear.  Detect this and skip recreation.
    deadline = time.time() + _DELETE_PROPAGATION_TIMEOUT
    while time.time() < deadline:
        metadata = _get_metadata(admin)
        if topic not in metadata.topics:
            break            # deletion has propagated — safe to recreate
        if len(metadata.topics[topic].partitions) == num_brokers:
            # Topic is back with the correct partition count — another instance
            # completed the delete-and-recreate cycle ahead of us.
            log.info(
                f"Topic '{topic}' already recreated with {num_brokers} partition(s) "
                "by another instance — skipping recreation."
            )
            return
        time.sleep(1)
    else:
        # Loop exhausted without breaking — topic still visible after timeout.
        log.error(
            f"Timed out waiting for topic '{topic}' deletion to propagate "
            f"({_DELETE_PROPAGATION_TIMEOUT}s). Skipping recreation — will retry on next sync."
        )
        return

    _create_topic(admin, topic, num_brokers)


def ensure_log_topic(kafka_config: dict, topic: str, retention_ms: int) -> None:
    """
    Ensure the Kafka log-capture topic exists with the requested retention period.

    The log topic uses a single partition.  Log records from one canary are
    sequential, so a single ordered partition is both sufficient and preferable
    to multiple partitions (which would interleave records from concurrent
    writes and complicate time-ordered consumption by log readers).

    Unlike the canary topic, the log topic is not resized on broker count
    changes — one partition is always correct regardless of cluster size.

    Parameters
    ----------
    kafka_config : dict
        Connection/auth settings passed to AdminClient.

    topic : str
        Name of the log topic (e.g. "cloud-canary-logs").

    retention_ms : int
        How long (milliseconds) messages are retained before being deleted.
        Default in config is 604800000 (7 days).

    Raises
    ------
    RuntimeError
        If topic creation fails (propagated from the AdminClient future).
    """
    admin = AdminClient(kafka_config)
    metadata = _get_metadata(admin)

    if topic in metadata.topics:
        num_partitions = len(metadata.topics[topic].partitions)
        log.info(
            f"Log topic '{topic}' already exists ({num_partitions} partition(s)) — skipping creation."
        )
        return

    num_brokers = len(metadata.brokers)
    replication_factor = min(num_brokers, 3)

    log.info(
        f"Creating log topic '{topic}' with "
        f"num_partitions=1, replication_factor={replication_factor}, "
        f"retention.ms={retention_ms}."
    )

    futures = admin.create_topics([
        NewTopic(
            topic=topic,
            num_partitions=1,
            replication_factor=replication_factor,
            config={"retention.ms": str(retention_ms)},   # topic-level retention override
        )
    ])

    for topic_name, future in futures.items():
        try:
            future.result()
            log.info(f"Log topic '{topic_name}' created successfully.")
        except KafkaException as exc:
            # Another instance won the creation race — treat as success.
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info(f"Log topic '{topic_name}' already exists (created by another instance) — skipping.")
            else:
                raise RuntimeError(f"Failed to create log topic '{topic_name}': {exc}")


def ensure_topic(kafka_config: dict, topic: str) -> None:
    """
    Ensure the canary topic exists on the cluster, creating it if necessary.

    Called once at startup before the producer and consumer are created.
    If the topic already exists, this is a no-op (the function logs the
    existing partition count and returns immediately).

    Partition count is set to the number of brokers so that every broker
    holds a leader partition and is exercised by the canary.  Replication
    factor is capped at 3 to avoid redundant replicas on large clusters.

    Parameters
    ----------
    kafka_config : dict
        Connection/auth settings passed to AdminClient.

    topic : str
        Name of the canary topic (e.g. "cloud-canary").

    Raises
    ------
    RuntimeError
        If the cluster is unreachable or topic creation fails.
    """
    admin = AdminClient(kafka_config)
    metadata = _get_metadata(admin)

    num_brokers = len(metadata.brokers)
    log.info(f"Cluster has {num_brokers} broker(s).")

    if topic in metadata.topics:
        num_partitions = len(metadata.topics[topic].partitions)
        log.info(
            f"Topic '{topic}' already exists ({num_partitions} partition(s)) — skipping creation."
        )
        return

    _create_topic(admin, topic, num_brokers)


def sync_topic_partitions(
    kafka_config: dict,
    topic: str,
) -> tuple[int, int] | None:
    """
    Detect broker count changes and reconcile the canary topic's partition count.

    This function runs on a separate cadence from the canary health checks
    (controlled by partition.sync.interval.seconds in config.ini, default 86400 s).
    It is designed to handle Confluent Cloud's cluster scaling events:

    Scale-up (brokers added)
        Increase the topic's partition count to match by calling
        create_partitions().  Kafka supports adding partitions in place without
        data loss, but note that existing messages in already-assigned partitions
        are not redistributed.
        Multi-instance: if create_partitions() fails because another instance
        already increased the count, the metadata is re-fetched; if the count
        matches the target the call is silently skipped.

    Scale-down (brokers removed)
        Kafka cannot reduce partition count in place.  The topic is deleted and
        recreated with the new lower partition count via _delete_and_recreate().
        In-flight canary messages are lost, but this is acceptable — the canary
        is diagnostic infrastructure, not an application data stream.
        Multi-instance races in delete-and-recreate are handled inside
        _delete_and_recreate() — see that function's docstring.

    No change
        Returns (num_brokers, num_partitions) immediately with no cluster calls
        beyond the initial metadata fetch.

    Parameters
    ----------
    kafka_config : dict
        Connection/auth settings passed to AdminClient.

    topic : str
        Name of the canary topic to sync.

    Returns
    -------
    tuple[int, int] | None
        (num_brokers, final_partition_count) reflecting the cluster state after
        any reconciliation.  Returns None if cluster metadata could not be
        fetched (errors are logged; the next sync will retry).
    """
    admin = AdminClient(kafka_config)

    try:
        metadata = _get_metadata(admin)
    except RuntimeError as exc:
        log.error(f"Partition sync skipped — could not fetch cluster metadata: {exc}")
        return None

    if topic not in metadata.topics:
        log.warning(f"Partition sync skipped — topic '{topic}' not found in metadata.")
        return None

    num_brokers = len(metadata.brokers)
    current_partitions = len(metadata.topics[topic].partitions)

    # No change — broker count matches partition count.
    if num_brokers == current_partitions:
        return num_brokers, current_partitions

    # Scale-down: partition count exceeds broker count.
    # Kafka does not support reducing partitions, so drop and recreate.
    if num_brokers < current_partitions:
        _delete_and_recreate(admin, topic, num_brokers)
        return num_brokers, num_brokers

    # Scale-up: more brokers than partitions.
    # create_partitions() increases the count to num_brokers.
    log.info(
        f"Broker count change detected: {current_partitions} → {num_brokers}. "
        f"Increasing partitions for '{topic}' to {num_brokers}."
    )
    final_partitions = current_partitions
    futures = admin.create_partitions([NewPartitions(topic, num_brokers)])
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info(f"Topic '{topic_name}' now has {num_brokers} partition(s).")
            final_partitions = num_brokers
        except KafkaException as exc:
            # Re-fetch metadata to check whether another instance already
            # increased the partition count while this call was in flight.
            try:
                refreshed = _get_metadata(admin)
                actual = len(refreshed.topics[topic_name].partitions) if topic_name in refreshed.topics else current_partitions
                if actual == num_brokers:
                    log.info(
                        f"Topic '{topic_name}' already has {num_brokers} partition(s) "
                        "(updated by another instance) — skipping."
                    )
                    final_partitions = num_brokers
                else:
                    log.error(f"Failed to update partitions for '{topic_name}': {exc}")
                    final_partitions = actual
            except RuntimeError:
                log.error(f"Failed to update partitions for '{topic_name}': {exc}")
                final_partitions = current_partitions

    return num_brokers, final_partitions
