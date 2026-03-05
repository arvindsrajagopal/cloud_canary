# ---------------------------------------------------------------------------
# main.py — Cloud Canary entry point and main run loop
#
# Overview
# --------
# The cloud canary continuously measures end-to-end health of a Confluent Cloud
# Kafka cluster by producing a small Avro-encoded message and consuming it back,
# recording the round-trip latency and any errors that occur.
#
# Three independent check cadences run in a single-threaded loop:
#
#   Canary check          (check.interval.seconds, default 15 s)
#       The core measurement: seek → produce → consume.  One check per
#       partition runs concurrently via a ThreadPoolExecutor so that every
#       broker leader is exercised in every cycle.  Per-partition latency
#       and errors are recorded to Prometheus metrics after each attempt.
#
#   Partition sync        (partition.sync.interval.seconds, default 86400 s)
#       Detects broker count changes (cluster scale-up/down) and reconciles
#       the canary topic's partition count accordingly.  If the partition
#       count changes, the per-partition consumer pool is rebuilt automatically.
#
#   Schema Registry check (sr.check.interval.seconds, default 60 s)
#       Independent health check against the Schema Registry, surfacing SR
#       availability as a separate Prometheus counter, distinct from the
#       Kafka broker health signal.
#
# Long-lived clients
# ------------------
# The producer, per-partition consumers, and Schema Registry client are created
# once at startup and reused across all check iterations.  Reuse eliminates the
# TCP/TLS handshake, SASL authentication, and consumer group coordinator
# round-trip that would otherwise inflate each check's latency measurement.
#
# check_kafka() phases
# --------------------
#   SEEK    — Advance the consumer's fetch offset to the current partition
#             high watermark so the next poll only returns the newly produced
#             message.  Timed separately to isolate broker fetch latency.
#
#   PRODUCE — Send the canary message to the target partition and wait for
#             broker ack (acks=all).  Timed to isolate write latency and
#             replication lag.  The explicit partition pin ensures the message
#             lands on the broker leader for that partition.
#
#   CONSUME — Poll for the specific message by UUID.  A timeout here means
#             the broker accepted the write but is not serving it back —
#             possible ISR or replication issue.
#
# Per-partition concurrency
# -------------------------
# check_kafka() is called once per partition on every cycle via a
# ThreadPoolExecutor.  Each partition has a dedicated DeserializingConsumer
# (created with assign() rather than subscribe() — no group coordinator needed).
# The shared SerializingProducer is thread-safe in librdkafka; all metrics
# writes are thread-safe in prometheus_client.
#
# Warmup period
# -------------
# The first N checks (warmup.checks, default 2) are executed normally but
# E2E_LATENCY is not recorded.  This suppresses the artificially high first-
# check latency caused by cold-start overhead (TCP/TLS handshake, broker
# metadata fetch, Schema Registry schema registration).  Set warmup.checks=0
# to disable and record all observations from the very first check.
#
# SEEK_DURATION and PRODUCE_DURATION are not suppressed during warmup — they
# are recorded inside check_kafka()'s finally blocks.  Their impact on long-
# running percentiles is negligible with the default of 2 warmup checks.
#
# Graceful shutdown
# -----------------
# SIGINT and SIGTERM set _shutdown=True, which causes the main loop to exit
# after the current check completes.  All per-partition consumers are closed
# in the finally block to cleanly leave the consumer group and release resources.
#
# Multi-instance support
# ----------------------
# Multiple instances can run concurrently against the same Confluent Cloud
# cluster without interfering:
#
#   Consumer groups — each per-partition consumer generates a UUID-based
#       group_id at startup, so consumer groups are always distinct.  Canary
#       messages are matched by the UUID embedded in each message, so a message
#       produced by one instance is never accidentally consumed as a result by
#       another.
#
#   Prometheus metrics — every metric carries a `host` label set to
#       socket.gethostname() (metrics.HOST) and a `partition` label.  This
#       makes every time series unambiguously tied to the producing instance
#       and broker partition.  When multiple instances run on the same machine,
#       set a distinct HOSTNAME environment variable before starting each one.
#       Each instance must also use a unique metrics.port in config.ini to
#       avoid port conflicts.
#
#   Topic management — ensure_topic(), ensure_log_topic(), and
#       sync_topic_partitions() are all collision-safe.  See topic.py for
#       details of how each race condition is handled.
# ---------------------------------------------------------------------------

import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient

from src import metrics
from src.config import load_config
from src.consumer import (
    consume_canary,
    create_partition_consumer,
    seek_to_end,
)
from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    Phase,
    classify_kafka_error,
    classify_sr_error,
)
from src.kafka_log_handler import KafkaLogHandler
from src.metrics import start_metrics_server
from src.producer import create_producer, produce_canary
from src.topic import ensure_log_topic, ensure_topic, sync_topic_partitions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# Path to the INI configuration file, relative to the working directory
# from which the application is launched (typically the project root).
CONFIG_FILE = "config/config.ini"

# Shutdown flag — set to True by the SIGINT/SIGTERM handler.
# The main loop checks this at the top of each iteration and exits cleanly
# after the current check completes.  Using a module-level bool is safe here
# because Python's GIL ensures the flag write is atomic.
_shutdown = False


def _handle_signal(sig, frame) -> None:
    """
    Signal handler for SIGINT (Ctrl-C) and SIGTERM (container stop).

    Sets _shutdown=True so the main loop exits after the current check
    rather than terminating mid-flight, which could leave a stale consumer
    group registration on the broker.
    """
    global _shutdown
    log.info("Shutdown signal received — finishing current check then exiting.")
    _shutdown = True


def check_sr(sr_client) -> None:
    """
    Verify that the Schema Registry is reachable and responding.

    Calls get_subjects() as a lightweight liveness probe.  Any exception
    is classified and re-raised as a CanaryError so the caller can record
    it to Prometheus with the appropriate labels.

    Parameters
    ----------
    sr_client : SchemaRegistryClient
        The shared SR client created at startup.

    Raises
    ------
    CanaryError
        With phase=SCHEMA_REGISTRY and the appropriate ErrorCategory.
    """
    try:
        sr_client.get_subjects()
    except Exception as exc:
        raise CanaryError(Phase.SCHEMA_REGISTRY, classify_sr_error(exc), str(exc))


def check_kafka(
    producer,
    consumer,
    topic: str,
    timeout: float,
    check_sequence: int,
    partition: int,
) -> int:
    """
    Execute a single end-to-end canary check for a specific partition.

    The check proceeds through three timed phases.  Each phase's duration is
    recorded to a Prometheus histogram via a finally block so the metric is
    always updated even if the phase raises an exception.

    Parameters
    ----------
    producer : SerializingProducer
        Long-lived Avro producer created at startup.  Thread-safe in librdkafka.

    consumer : DeserializingConsumer
        Per-partition Avro consumer, manually assigned to `partition` via
        assign() at startup.

    topic : str
        Kafka topic used for the canary check (e.g. "cloud-canary").

    timeout : float
        Maximum seconds to wait for the canary message to be consumed before
        declaring the consume phase a failure.

    check_sequence : int
        Monotonically increasing counter embedded in the canary message for
        gap detection.

    partition : int
        Partition index this check targets.  The producer pins the message to
        this partition; the consumer is already assigned to it.

    Returns
    -------
    int
        End-to-end latency in milliseconds:
            consumer receive timestamp − producer send timestamp

    Raises
    ------
    CanaryError
        If any phase fails, with the phase name and error category set so the
        caller can apply the correct Prometheus labels.
    """

    # ------------------------------------------------------------------
    # Phase: SEEK
    # Advance the consumer's fetch offset to the current high watermark on
    # the assigned partition.  This ensures the subsequent poll() will
    # only return the message we are about to produce, not stale messages
    # from previous checks.
    #
    # The seek involves a synchronous OffsetFetch request to the broker
    # (one per partition), so its duration reflects broker fetch latency
    # independent of the write/read path.
    # ------------------------------------------------------------------
    t_seek = time.time()
    try:
        seek_to_end(consumer)
    except (RuntimeError, KafkaException) as exc:
        category = (
            classify_kafka_error(exc.args[0])
            if isinstance(exc, KafkaException) and exc.args
            else ErrorCategory.BROKER
        )
        raise CanaryError(Phase.SEEK, category, str(exc))
    finally:
        # Record seek duration regardless of success or failure so the
        # histogram captures partial attempts (e.g. successful on some
        # partitions before failing on another).
        metrics.SEEK_DURATION.labels(host=metrics.HOST, partition=str(partition)).observe(
            (time.time() - t_seek) * 1000
        )

    # ------------------------------------------------------------------
    # Phase: PRODUCE
    # Send the canary message to the target partition and wait for the
    # broker to acknowledge receipt by all in-sync replicas (acks=all).
    # A failure here means the broker could not commit the write —
    # possible ISR degradation, quota exceeded, or network issue between
    # the client and the leader for this partition.
    # ------------------------------------------------------------------
    t_produce = time.time()
    try:
        sent = produce_canary(producer, topic, check_sequence, partition)
        log.info(
            f"Produced  | seq={sent.check_sequence} partition={partition} "
            f"id={sent.message_id} host={sent.producer_host}"
        )
    except KafkaException as exc:
        category = classify_kafka_error(exc.args[0]) if exc.args else ErrorCategory.UNKNOWN
        raise CanaryError(Phase.PRODUCE, category, str(exc))
    except RuntimeError as exc:
        # flush() wall-clock timeout — broker did not ack within 15 s.
        # delivery.timeout.ms has not expired so the cause is unknown;
        # UNKNOWN is more honest than NETWORK or BROKER here.
        raise CanaryError(Phase.PRODUCE, ErrorCategory.UNKNOWN, str(exc))
    finally:
        metrics.PRODUCE_DURATION.labels(host=metrics.HOST, partition=str(partition)).observe(
            (time.time() - t_produce) * 1000
        )

    # ------------------------------------------------------------------
    # Phase: CONSUME
    # Poll until the message we just produced is received back.  Because the
    # produce already succeeded (broker acked), a timeout here means the
    # broker is not serving the message — possible replication lag or ISR
    # issue causing the follower serving this consumer to be behind.
    #
    # Note: CONSUME phase has no duration histogram because the consume
    # latency is already captured by E2E_LATENCY (which measures the full
    # round-trip from send_timestamp_ms to receive_timestamp_ms).
    # ------------------------------------------------------------------
    try:
        received, receive_ts = consume_canary(consumer, sent.message_id, timeout)
    except TimeoutError:
        raise CanaryError(
            Phase.CONSUME,
            ErrorCategory.BROKER,
            f"Message not returned within {timeout}s — produce succeeded so broker received it "
            "(possible replication lag or ISR issue)",
        )
    except KafkaException as exc:
        category = classify_kafka_error(exc.args[0]) if exc.args else ErrorCategory.UNKNOWN
        raise CanaryError(Phase.CONSUME, category, str(exc))

    # End-to-end latency: consumer receive time minus the timestamp captured
    # by the producer just before calling produce().  Both timestamps are
    # taken on the same host so clock skew is not a factor.
    return receive_ts - received.send_timestamp_ms


def _build_consumer_pool(
    kafka_config: dict,
    sr_client,
    topic: str,
    partitions: list[int],
) -> dict:
    """
    Create one DeserializingConsumer per partition, each manually assigned.

    Returns a dict mapping partition index → consumer.
    """
    return {
        p: create_partition_consumer(kafka_config, sr_client, topic, p)
        for p in partitions
    }


def run() -> None:
    """
    Main entry point: load configuration, initialise all clients, then run the
    check loop until a shutdown signal is received.

    Startup sequence
    ----------------
    1. Load config.ini and extract per-section settings.
    2. Start the Prometheus HTTP metrics server.
    3. Ensure the canary topic exists (create if missing).
    4. Optionally attach the Kafka log handler to the root logger.
    5. Create the Schema Registry client and producer.
    6. Run an initial partition sync to get the current partition count.
    7. Create one DeserializingConsumer per partition (manual assignment).
    8. Enter the main loop (canary checks, partition sync, SR checks).

    Shutdown sequence (finally block)
    -----------------------------------
    • All per-partition consumers are closed so each broker removes them from
      the group immediately rather than waiting for the session timeout.
    • ThreadPoolExecutor is shut down.
    • kafka_log_handler.close() flushes any buffered log records to Kafka.
    """
    config = load_config(CONFIG_FILE)
    kafka_config  = config["kafka"]
    sr_config     = config["schema_registry"]
    app           = config["app"]

    # -- Application settings (with defaults) --
    topic               = app.get("topic",                          "cloud-canary")
    timeout             = float(app.get("consumer.timeout.seconds",              "5"))
    interval            = float(app.get("check.interval.seconds",               "15"))
    sync_interval       = float(app.get("partition.sync.interval.seconds",   "86400"))
    sr_check_interval   = float(app.get("sr.check.interval.seconds",            "60"))
    metrics_port        = int(app.get("metrics.port",                          "8000"))
    log_topic_enabled   = app.get("log.topic.enabled", "false").lower() == "true"
    log_topic           = app.get("log.topic",                    "cloud-canary-logs")
    log_topic_retention_ms = int(app.get("log.topic.retention.ms",        "604800000"))
    warmup_checks          = int(app.get("warmup.checks",                         "2"))

    # Register signal handlers so SIGINT (Ctrl-C) and SIGTERM (container stop)
    # cause a clean exit after the current check rather than a hard kill.
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info(
        f"cloud_canary started | topic={topic} "
        f"check.interval={interval}s "
        f"partition.sync.interval={sync_interval}s "
        f"sr.check.interval={sr_check_interval}s "
        f"timeout={timeout}s "
        f"metrics.port={metrics_port} "
        f"warmup.checks={warmup_checks}"
    )

    # Start the Prometheus metrics HTTP server (background daemon thread).
    # The /metrics endpoint is available immediately, returning zeros for
    # counters/histograms that haven't been updated yet.
    start_metrics_server(metrics_port)
    log.info(f"Metrics available at http://0.0.0.0:{metrics_port}/metrics")

    # ------------------------------------------------------------------
    # Startup: ensure the canary topic exists before creating clients.
    # AdminClient is used only here and in sync_topic_partitions(); it is
    # not kept alive because topic management calls are infrequent.
    # ------------------------------------------------------------------
    try:
        ensure_topic(kafka_config, topic)
    except RuntimeError as exc:
        log.error(f"Startup failed: {exc}")
        return

    # Optionally attach the Kafka log handler so all subsequent log output
    # is also published to the log topic.  This is done before creating the
    # producer/consumer so those startup messages are captured too.
    kafka_log_handler = None
    if log_topic_enabled:
        try:
            ensure_log_topic(kafka_config, log_topic, log_topic_retention_ms)
            kafka_log_handler = KafkaLogHandler(kafka_config, log_topic)
            kafka_log_handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            ))
            logging.getLogger().addHandler(kafka_log_handler)
            log.info(f"Log topic enabled — publishing logs to '{log_topic}'.")
        except RuntimeError as exc:
            log.error(f"Failed to set up log topic — continuing without it: {exc}")

    # Create the shared Schema Registry client.  The same instance is used by
    # both the producer's AvroSerializer and the consumer's AvroDeserializer,
    # and by check_sr() for independent SR health checks.
    sr_client = SchemaRegistryClient(sr_config)

    # Create the long-lived producer.  Thread-safe in librdkafka — shared
    # across all per-partition check threads.
    producer = create_producer(kafka_config, sr_client)

    # ------------------------------------------------------------------
    # Initial partition sync — determines how many per-partition consumers
    # to create.  Setting last_sync_time=now prevents a duplicate sync on
    # the first main loop iteration.
    # ------------------------------------------------------------------
    log.info("Running initial partition sync...")
    result = sync_topic_partitions(kafka_config, topic)
    if not result:
        log.error("Startup failed — could not determine partition count.")
        return
    num_brokers, num_partitions = result
    metrics.BROKER_COUNT.set(num_brokers)
    metrics.TOPIC_PARTITION_COUNT.set(num_partitions)
    partitions = list(range(num_partitions))
    last_sync_time = time.time()

    # Create one consumer per partition using manual assignment (assign()).
    # No subscribe() or wait_for_assignment() needed — partitions are
    # immediately available for seek_to_end() and poll().
    consumers = _build_consumer_pool(kafka_config, sr_client, topic, partitions)
    log.info(f"Per-partition consumers ready: {num_partitions} partition(s) → {partitions}")

    consecutive_failures = {p: 0 for p in partitions}
    check_sequence       = 0
    start_time           = time.time()
    last_sr_check_time   = 0.0   # set to 0 so the first iteration triggers an SR check immediately

    executor = ThreadPoolExecutor(max_workers=num_partitions)

    try:
        while not _shutdown:

            # Update uptime gauge at the top of every iteration so it reflects
            # elapsed time even if a check takes a long time.
            metrics.UPTIME_SECONDS.set(time.time() - start_time)

            # ------------------------------------------------------------------
            # Partition sync — detect broker count changes (scale-up/down)
            # Runs on its own cadence, independently of canary checks.
            # If the partition count changes, the consumer pool is rebuilt.
            # ------------------------------------------------------------------
            if time.time() - last_sync_time >= sync_interval:
                result = sync_topic_partitions(kafka_config, topic)
                if result:
                    metrics.BROKER_COUNT.set(result[0])
                    metrics.TOPIC_PARTITION_COUNT.set(result[1])
                    if result[1] != len(partitions):
                        log.info(
                            f"Partition count changed {len(partitions)} → {result[1]} "
                            "— rebuilding consumer pool."
                        )
                        executor.shutdown(wait=False)
                        for c in consumers.values():
                            c.close()
                        partitions = list(range(result[1]))
                        consumers = _build_consumer_pool(kafka_config, sr_client, topic, partitions)
                        consecutive_failures = {p: 0 for p in partitions}
                        executor = ThreadPoolExecutor(max_workers=result[1])
                        log.info(f"Consumer pool rebuilt: {result[1]} partition(s) → {partitions}")
                last_sync_time = time.time()

            # ------------------------------------------------------------------
            # Schema Registry health check — independent cadence and signal
            # SR availability is not directly exercised by the canary check
            # (the schema is cached after first use), so it must be probed
            # separately to surface SR outages as a distinct metric.
            # ------------------------------------------------------------------
            if time.time() - last_sr_check_time >= sr_check_interval:
                try:
                    check_sr(sr_client)
                    metrics.SR_CHECKS_TOTAL.labels(result="success", host=metrics.HOST).inc()
                    log.info("SR OK")
                except CanaryError as exc:
                    metrics.SR_CHECKS_TOTAL.labels(result="failure", host=metrics.HOST).inc()
                    log.error(f"SR FAIL | {exc}")
                last_sr_check_time = time.time()

            # ------------------------------------------------------------------
            # Canary check — the core measurement
            # One check per partition runs concurrently via ThreadPoolExecutor.
            # check_sequence is incremented once per cycle (not per partition)
            # so it represents loop iterations; gaps indicate restarts.
            # ------------------------------------------------------------------
            check_sequence += 1
            is_warming_up = check_sequence <= warmup_checks

            futures = {
                executor.submit(
                    check_kafka, producer, consumers[p], topic, timeout, check_sequence, p
                ): p
                for p in partitions
            }

            for future in as_completed(futures):
                p = futures[future]
                try:
                    latency_ms = future.result()

                    # Success path — counters are always updated (even during warmup)
                    # so failure detection is never suppressed.
                    consecutive_failures[p] = 0
                    metrics.CHECKS_TOTAL.labels(result="success", host=metrics.HOST, partition=str(p)).inc()
                    metrics.CONSECUTIVE_FAILURES.labels(host=metrics.HOST, partition=str(p)).set(0)
                    metrics.CHECK_SEQUENCE.set(check_sequence)

                    if is_warming_up:
                        log.info(
                            f"WARMUP    | seq={check_sequence} partition={p} "
                            f"latency={latency_ms}ms (not recorded)"
                        )
                    else:
                        metrics.E2E_LATENCY.labels(host=metrics.HOST, partition=str(p)).observe(latency_ms)
                        log.info(f"Consumed  | seq={check_sequence} partition={p} latency={latency_ms}ms")

                except CanaryError as exc:
                    # Classified failure — phase and category are known
                    consecutive_failures[p] += 1
                    metrics.CHECKS_TOTAL.labels(result="failure", host=metrics.HOST, partition=str(p)).inc()
                    metrics.FAILURES_TOTAL.labels(
                        phase=exc.phase, category=exc.category,
                        host=metrics.HOST, partition=str(p),
                    ).inc()
                    metrics.CONSECUTIVE_FAILURES.labels(host=metrics.HOST, partition=str(p)).set(
                        consecutive_failures[p]
                    )
                    metrics.CHECK_SEQUENCE.set(check_sequence)
                    log.error(
                        f"FAIL [{consecutive_failures[p]}] | seq={check_sequence} partition={p} {exc}"
                    )

                except Exception as exc:
                    # Unexpected failure — not a CanaryError, so phase/category unknown.
                    # Still recorded in metrics so the failure is visible.
                    consecutive_failures[p] += 1
                    metrics.CHECKS_TOTAL.labels(result="failure", host=metrics.HOST, partition=str(p)).inc()
                    metrics.FAILURES_TOTAL.labels(
                        phase="UNKNOWN", category="UNKNOWN",
                        host=metrics.HOST, partition=str(p),
                    ).inc()
                    metrics.CONSECUTIVE_FAILURES.labels(host=metrics.HOST, partition=str(p)).set(
                        consecutive_failures[p]
                    )
                    metrics.CHECK_SEQUENCE.set(check_sequence)
                    log.error(
                        f"FAIL [{consecutive_failures[p]}] | seq={check_sequence} partition={p} "
                        f"phase=UNKNOWN category=UNKNOWN detail={exc}"
                    )

            if is_warming_up and check_sequence == warmup_checks:
                log.info("Warmup complete — latency metrics enabled from the next check.")

            # Wait for the configured interval before the next check.
            # Skip the sleep if a shutdown signal arrived during this check.
            if not _shutdown:
                log.info(f"Next check in {interval}s")
                time.sleep(interval)

    finally:
        # Close all per-partition consumers so brokers remove them from their
        # groups immediately rather than waiting for the session timeout.
        for c in consumers.values():
            c.close()
        executor.shutdown(wait=False)
        log.info("cloud_canary stopped.")

        # Flush and detach the Kafka log handler so the last few log records
        # (including "cloud_canary stopped") reach the log topic before exit.
        if kafka_log_handler:
            logging.getLogger().removeHandler(kafka_log_handler)
            kafka_log_handler.close()


if __name__ == "__main__":
    run()
