# ---------------------------------------------------------------------------
# main.py — Cloud Canary entry point and main run loop
#
# Overview
# --------
# The cloud canary continuously measures end-to-end health of a Confluent Cloud
# Kafka cluster by producing a small Avro-encoded message and consuming it back,
# recording the round-trip latency and any errors that occur.
#
# Three check cadences are coordinated by one control loop:
#
#   Canary check          (check.interval.seconds, default 15 s)
#       The core measurement: seek → produce → consume.  One check per
#       partition is submitted to a ThreadPoolExecutor. At most max.workers
#       checks run concurrently, and Kafka leader placement means distinct
#       coverage of every broker is not guaranteed. Results are recorded to
#       Prometheus metrics after each attempt.
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
#   CONSUME — Poll for the specific message by UUID. A timeout occurs after
#             a successful produce acknowledgement, but can still reflect a
#             broker/read-path problem or a client connectivity problem.
#
# Per-partition concurrency
# -------------------------
# check_kafka() is called once per partition on every cycle via a
# ThreadPoolExecutor.  Each partition has a dedicated Consumer
# (created with assign() rather than subscribe() — no group coordinator needed).
# The shared Producer is thread-safe in librdkafka; all metrics
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
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from confluent_kafka import KafkaException, Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from src import constants as const
from src import metrics
from src.__version__ import __version__
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
from src.structured_logging import CanaryLoggerAdapter, setup_logging
from src.topic import ensure_log_topic, ensure_topic, sync_topic_partitions

# Logging will be configured in run() after loading config.
# Initialize adapter at module level with placeholder context.
# The context will be updated in run() after loading config,
# avoiding global variable reassignment.
_base_logger = logging.getLogger(__name__)
log = CanaryLoggerAdapter(_base_logger, {
    "host": "unknown",     # Updated in run() after loading config
    "version": "unknown",  # Updated in run() after loading config
})

# Path to the INI configuration file, relative to the working directory
# from which the application is launched (typically the project root).
# Can be overridden via CANARY_CONFIG_FILE environment variable.
CONFIG_FILE = os.getenv("CANARY_CONFIG_FILE", "config/config.ini")

# Shutdown event — set by the SIGINT/SIGTERM handler.
# The main loop checks this at the top of each iteration and exits cleanly
# after the current check completes.  Using threading.Event() provides
# thread-safe signaling and prevents race conditions when multiple signals
# arrive concurrently.
_shutdown_event = threading.Event()


def _handle_signal(sig, frame) -> None:
    """
    Signal handler for SIGINT (Ctrl-C) and SIGTERM (container stop).

    Sets the shutdown event so the main loop exits after the current check
    rather than terminating mid-flight, which could leave a stale consumer
    group registration on the broker.

    Multiple signals are handled safely — only the first signal triggers
    the log message; subsequent signals are silently ignored.
    """
    if not _shutdown_event.is_set():
        log.info("Shutdown signal received — finishing current check then exiting.")
        _shutdown_event.set()


def check_sr(sr_client: SchemaRegistryClient) -> None:
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
    producer: Producer,
    avro_serializer: AvroSerializer,
    consumer: Consumer,
    avro_deserializer: AvroDeserializer,
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
    producer : Producer
        Long-lived Kafka producer created at startup.  Thread-safe in librdkafka.

    avro_serializer : AvroSerializer
        Avro serializer for encoding CanaryMessage objects.

    consumer : Consumer
        Per-partition consumer, manually assigned to `partition` via
        assign() at startup.

    avro_deserializer : AvroDeserializer
        Avro deserializer for decoding CanaryMessage objects.

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
    t_seek = time.perf_counter()
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
        # Using perf_counter() for monotonic, accurate timing measurement.
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.SEEK_DURATION.labels(host=metrics.HOST).observe(
            (time.perf_counter() - t_seek) * 1000
        )

    # ------------------------------------------------------------------
    # Phase: PRODUCE
    # Send the canary message to the target partition and wait for the
    # broker to acknowledge receipt by all in-sync replicas (acks=all).
    # A failure here means the broker could not commit the write —
    # possible ISR degradation, quota exceeded, or network issue between
    # the client and the leader for this partition.
    # ------------------------------------------------------------------
    t_produce = time.perf_counter()
    try:
        sent = produce_canary(producer, avro_serializer, topic, check_sequence, partition)
        log.debug(
            "Message produced",
            extra={
                "check_sequence": sent.check_sequence,
                "partition": partition,
                "message_id": sent.message_id,
                "producer_host": sent.producer_host,
            }
        )
    except KafkaException as exc:
        category = classify_kafka_error(exc.args[0]) if exc.args else ErrorCategory.UNKNOWN
        raise CanaryError(Phase.PRODUCE, category, str(exc))
    except RuntimeError as exc:
        # Local delivery-callback wait expired after 2s.
        # delivery.timeout.ms has not expired so the cause is unknown;
        # UNKNOWN is more honest than NETWORK or BROKER here.
        raise CanaryError(Phase.PRODUCE, ErrorCategory.UNKNOWN, str(exc))
    finally:
        # Using perf_counter() for monotonic, accurate timing measurement.
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.PRODUCE_DURATION.labels(host=metrics.HOST).observe(
            (time.perf_counter() - t_produce) * 1000
        )

    # ------------------------------------------------------------------
    # Phase: CONSUME
    # Poll until the message we just produced is received back.  Because the
    # produce already succeeded (broker acked), a timeout here means the
    # broker is not serving the message — possible replication lag or ISR
    # issue on the broker or consume path.
    #
    # Note: CONSUME phase has no duration histogram because the consume
    # latency is already captured by E2E_LATENCY (which measures the full
    # round-trip from send_timestamp_ms to receive_timestamp_ms).
    # ------------------------------------------------------------------
    try:
        received, receive_ts = consume_canary(consumer, avro_deserializer, sent.message_id, timeout)
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
    kafka_config: dict[str, str],
    sr_client: SchemaRegistryClient,
    topic: str,
    partitions: list[int],
) -> tuple[dict[int, Consumer], dict[int, AvroDeserializer]]:
    """
    Create one Consumer per partition, each manually assigned.

    Consumers are created in parallel (up to 10 concurrent connections) to reduce
    startup time. Sequential creation can take 1+ second per consumer due to
    TCP handshake, metadata fetch, and group join operations.

    Returns a tuple of two dicts:
        - consumers: partition index → Consumer
        - deserializers: partition index → AvroDeserializer
    """
    # Use a temporary executor to parallelize consumer creation during startup.
    # Cap at 10 workers to avoid overwhelming the broker with concurrent connections.
    with ThreadPoolExecutor(max_workers=min(len(partitions), 10)) as pool:
        futures = {
            p: pool.submit(create_partition_consumer, kafka_config, sr_client, topic, p)
            for p in partitions
        }
        # Wait for all consumers to be created, then return as dict
        # If any consumer fails, the entire pool creation fails (fail-fast behavior)
        consumers = {}
        deserializers = {}
        for p, future in futures.items():
            try:
                consumer, deserializer = future.result()
                consumers[p] = consumer
                deserializers[p] = deserializer
            except Exception as exc:
                log.error(f"Failed to create consumer for partition {p}: {exc}")
                raise
        return consumers, deserializers


def _update_success_metrics(
    partition: int,
    check_sequence: int,
    latency_ms: int,
    consecutive_failures: dict[int, int],
    is_warming_up: bool,
    num_partitions: int,
    partition_threshold: int,
) -> None:
    """
    Update Prometheus metrics after a successful check.

    Parameters
    ----------
    partition : int
        Partition index that succeeded.
    check_sequence : int
        Current check sequence number.
    latency_ms : int
        Measured end-to-end latency in milliseconds.
    consecutive_failures : dict[int, int]
        Mutable dict tracking consecutive failure counts per partition.
        This partition's count will be reset to 0.
    is_warming_up : bool
        Whether this check is part of the warmup period (latency not recorded).
    num_partitions : int
        Total number of partitions in the cluster.
    partition_threshold : int
        Threshold above which partition labels are aggregated to prevent
        metric cardinality explosion.
    """
    consecutive_failures[partition] = 0

    # Cardinality control: Use per-partition labels only if under threshold
    # When partition count exceeds threshold, use aggregated label to prevent
    # Prometheus memory explosion (100 partitions = 2,200+ series per host)
    if num_partitions <= partition_threshold:
        partition_label = str(partition)
    else:
        partition_label = "*"  # Aggregated - all partitions combined

    metrics.CHECKS_TOTAL.labels(result="success", host=metrics.HOST, partition=partition_label).inc()
    metrics.CONSECUTIVE_FAILURES.labels(host=metrics.HOST, partition=partition_label).set(0)
    metrics.LAST_SUCCESS_TIMESTAMP.labels(host=metrics.HOST, partition=partition_label).set(time.time())
    metrics.CHECK_SEQUENCE.set(check_sequence)

    if not is_warming_up:
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.E2E_LATENCY.labels(host=metrics.HOST).observe(latency_ms)


def _update_failure_metrics(
    partition: int,
    check_sequence: int,
    consecutive_failures: dict[int, int],
    phase: str,
    category: str,
    num_partitions: int,
    partition_threshold: int,
) -> None:
    """
    Update Prometheus metrics after a failed check.

    Parameters
    ----------
    partition : int
        Partition index that failed.
    check_sequence : int
        Current check sequence number.
    consecutive_failures : dict[int, int]
        Mutable dict tracking consecutive failure counts per partition.
        This partition's count will be incremented.
    phase : str
        Phase where the failure occurred (SEEK, PRODUCE, CONSUME, etc.).
    category : str
        Error category (NETWORK, BROKER, UNKNOWN).
    num_partitions : int
        Total number of partitions in the cluster.
    partition_threshold : int
        Threshold above which partition labels are aggregated to prevent
        metric cardinality explosion.
    """
    consecutive_failures[partition] += 1

    # Cardinality control: Use per-partition labels only if under threshold
    if num_partitions <= partition_threshold:
        partition_label = str(partition)
    else:
        partition_label = "*"  # Aggregated - all partitions combined

    metrics.CHECKS_TOTAL.labels(result="failure", host=metrics.HOST, partition=partition_label).inc()
    metrics.FAILURES_TOTAL.labels(
        phase=phase, category=category,
        host=metrics.HOST, partition=partition_label,
    ).inc()
    metrics.CONSECUTIVE_FAILURES.labels(host=metrics.HOST, partition=partition_label).set(
        consecutive_failures[partition]
    )
    metrics.CHECK_SEQUENCE.set(check_sequence)


def validate_ssl_connectivity(kafka_config: dict, sr_config: dict) -> None:
    """
    Verify SSL/TLS connectivity to Confluent Cloud before entering main loop.

    This startup validation ensures:
    1. CA certificates are properly configured and valid
    2. Server certificate chains are trusted
    3. Hostname verification works correctly
    4. No MITM attack is in progress

    Fails fast with a clear error message if SSL/TLS validation fails,
    preventing the application from running with compromised security.

    Parameters
    ----------
    kafka_config : dict
        Kafka connection settings from [kafka] section.
    sr_config : dict
        Schema Registry connection settings from [schema_registry] section.

    Raises
    ------
    SystemExit
        If SSL/TLS validation fails for either Kafka or Schema Registry.
    """
    from confluent_kafka import Producer

    log.info("Validating SSL/TLS connectivity to Confluent Cloud...")

    # ------------------------------------------------------------------
    # Test 1: Kafka broker SSL/TLS validation
    # ------------------------------------------------------------------
    try:
        # Create a test producer with SSL settings
        test_config = {
            'bootstrap.servers': kafka_config['bootstrap.servers'],
            'security.protocol': kafka_config['security.protocol'],
            'sasl.mechanisms': kafka_config['sasl.mechanisms'],
            'sasl.username': kafka_config['sasl.username'],
            'sasl.password': kafka_config['sasl.password'],
            'socket.timeout.ms': 10000,
            'api.version.request.timeout.ms': 10000,
        }

        # Include all SSL-related settings from config
        for key, value in kafka_config.items():
            if key.startswith('ssl.') or key.startswith('enable.ssl'):
                test_config[key] = value

        test_producer = Producer(test_config)

        # Trigger actual connection by fetching metadata
        # This performs TCP handshake, TLS handshake, and certificate validation
        metadata = test_producer.list_topics(timeout=15)

        broker_count = len(metadata.brokers)
        log.info(
            "SSL/TLS validation successful for Kafka",
            extra={
                "brokers": broker_count,
                "security_protocol": kafka_config['security.protocol'],
                "ssl_ca_location": kafka_config.get('ssl.ca.location', 'system default'),
            }
        )

        # Clean up test producer
        test_producer.flush(timeout=2)

    except Exception as exc:
        error_msg = str(exc).lower()

        # Provide specific guidance based on error type
        if "certificate verify failed" in error_msg or "ssl" in error_msg:
            log.error(
                "Kafka SSL/TLS certificate validation FAILED",
                extra={
                    "error": str(exc),
                    "bootstrap_servers": kafka_config['bootstrap.servers'],
                    "ssl_ca_location": kafka_config.get('ssl.ca.location', 'not set (using system default)'),
                }
            )
            log.error("")
            log.error("Possible causes:")
            log.error("  1. Expired or invalid CA certificate bundle")
            log.error("  2. MITM attack in progress")
            log.error("  3. Incorrect ssl.ca.location setting")
            log.error("  4. System CA certificates not up to date")
            log.error("")
            log.error("To fix:")
            log.error("  - Verify ssl.ca.location points to valid CA bundle")
            log.error("  - Run 'update-ca-certificates' to refresh system CAs")
            log.error("  - Check network for MITM proxies or intercepting firewalls")
            sys.exit(1)
        else:
            log.error(
                "Kafka connection failed (non-SSL error)",
                extra={"error": str(exc)}
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Test 2: Schema Registry SSL/TLS validation
    # ------------------------------------------------------------------
    try:
        import urllib.request
        import ssl as ssl_module

        sr_url = sr_config['url']
        auth_string = sr_config['basic.auth.user.info']
        username, password = auth_string.split(':', 1)

        # Create SSL context with strict verification
        ssl_context = ssl_module.create_default_context()
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl_module.CERT_REQUIRED

        # Create authenticated request
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, sr_url, username, password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        https_handler = urllib.request.HTTPSHandler(context=ssl_context)
        opener = urllib.request.build_opener(https_handler, auth_handler)

        # Test connection to Schema Registry
        request = urllib.request.Request(f"{sr_url}/subjects")
        response = opener.open(request, timeout=10)
        response.read()

        log.info(
            "SSL/TLS validation successful for Schema Registry",
            extra={"url": sr_url}
        )

    except urllib.error.URLError as exc:
        if hasattr(exc, 'reason') and 'CERTIFICATE_VERIFY_FAILED' in str(exc.reason):
            log.error(
                "Schema Registry SSL/TLS certificate validation FAILED",
                extra={
                    "error": str(exc),
                    "url": sr_config['url'],
                }
            )
            log.error("")
            log.error("Possible causes:")
            log.error("  1. Expired or invalid CA certificate")
            log.error("  2. MITM attack in progress")
            log.error("  3. Schema Registry using self-signed certificate")
            log.error("")
            log.error("To fix:")
            log.error("  - Verify Schema Registry URL is correct")
            log.error("  - Update system CA certificates")
            log.error("  - Check for intercepting proxies")
            sys.exit(1)
        else:
            log.error(
                "Schema Registry connection failed",
                extra={"error": str(exc)}
            )
            sys.exit(1)

    except Exception as exc:
        log.error(
            "Schema Registry validation failed",
            extra={"error": str(exc)}
        )
        sys.exit(1)

    log.info("SSL/TLS connectivity validation complete - all checks passed")


def run() -> None:
    """
    Main entry point: load configuration, initialise all clients, then run the
    check loop until a shutdown signal is received.

    Startup sequence
    ----------------
    1. Load config.ini and extract per-section settings.
    2. Configure structured logging based on environment.
    3. Start the Prometheus HTTP metrics server.
    4. Ensure the canary topic exists (create if missing).
    5. Optionally attach the Kafka log handler to the root logger.
    6. Create the Schema Registry client, producer, and serializers.
    7. Run an initial partition sync to get the current partition count.
    8. Create one Consumer per partition (manual assignment).
    9. Enter the main loop (canary checks, partition sync, SR checks).

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

    # -- Logging configuration --
    # Configure structured logging before any log output.
    # Format can be controlled via CANARY_LOG_FORMAT env var or config.ini.
    # "auto" = JSON for non-TTY (Docker/production), text for TTY (local dev)
    log_format = os.getenv("CANARY_LOG_FORMAT", app.get("log.format", "auto"))
    log_level = getattr(logging, app.get("log.level", "INFO").upper(), logging.INFO)
    setup_logging(log_format=log_format, level=log_level)

    # Update logger adapter context with actual values (host, version)
    # This automatically includes these fields in every log entry.
    # We update the existing adapter's context dict instead of reassigning
    # the global variable, avoiding the global reassignment anti-pattern.
    log.extra.update({
        "host": metrics.HOST,
        "version": __version__,
    })

    # -- Application settings (with defaults) --
    # Instance ID is set via CANARY_INSTANCE_ID env var (defaults to hostname in metrics.py).
    # Do NOT modify metrics.HOST after module load - pre-labeled metrics capture it at import time.

    topic               = app.get("topic",                          "cloud-canary")
    timeout             = float(app.get("consumer.timeout.seconds",              "5"))
    interval            = float(app.get("check.interval.seconds",               "15"))
    sync_interval       = float(app.get("partition.sync.interval.seconds",   "86400"))
    sr_check_interval   = float(app.get("sr.check.interval.seconds",            "60"))
    metrics_port        = int(app.get("metrics.port",                          "8000"))
    metrics_bind_addr   = app.get("metrics.bind.address",                  "0.0.0.0")
    metrics_ssl_enabled = app.get("metrics.ssl.enabled", "false").lower() == "true"
    metrics_ssl_cert    = app.get("metrics.ssl.cert")
    metrics_ssl_key     = app.get("metrics.ssl.key")
    log_topic_enabled   = app.get("log.topic.enabled", "false").lower() == "true"
    log_topic           = app.get("log.topic",                    "cloud-canary-logs")
    log_topic_retention_ms = int(app.get("log.topic.retention.ms",        "604800000"))
    warmup_checks          = int(app.get("warmup.checks",                         "2"))
    max_workers            = int(app.get("max.workers",                str(const.MAX_WORKERS)))
    partition_threshold    = int(app.get("metrics.partition.threshold",         "100"))

    # Register signal handlers so SIGINT (Ctrl-C) and SIGTERM (container stop)
    # cause a clean exit after the current check rather than a hard kill.
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Set version info metric
    metrics.VERSION_INFO.info({
        'version': __version__,
        'host': metrics.HOST,
    })

    log.info(
        "cloud_canary started",
        extra={
            "topic": topic,
            "check_interval_seconds": interval,
            "partition_sync_interval_seconds": sync_interval,
            "sr_check_interval_seconds": sr_check_interval,
            "consumer_timeout_seconds": timeout,
            "metrics_port": metrics_port,
            "warmup_checks": warmup_checks,
            "max_workers": max_workers,
            "partition_threshold": partition_threshold,
            "log_format": log_format,
        }
    )

    # ------------------------------------------------------------------
    # CRITICAL: Validate SSL/TLS connectivity before creating clients
    # ------------------------------------------------------------------
    # This fail-fast check ensures certificates are valid and prevents
    # the application from running with compromised security.
    # Validates both Kafka broker and Schema Registry SSL/TLS connections.
    validate_ssl_connectivity(kafka_config, sr_config)

    # Start the Prometheus metrics HTTP(S) server (background daemon thread).
    # The /metrics endpoint is available immediately, returning zeros for
    # counters/histograms that haven't been updated yet.
    try:
        start_metrics_server(
            port=metrics_port,
            addr=metrics_bind_addr,
            ssl_enabled=metrics_ssl_enabled,
            ssl_cert=metrics_ssl_cert,
            ssl_key=metrics_ssl_key,
        )
        protocol = "https" if metrics_ssl_enabled else "http"
        log.info(
            "Metrics server started",
            extra={
                "protocol": protocol,
                "bind_address": metrics_bind_addr,
                "port": metrics_port,
                "ssl_enabled": metrics_ssl_enabled,
            }
        )
    except (ValueError, FileNotFoundError) as exc:
        log.error(
            "Failed to start metrics server",
            extra={"error": str(exc)}
        )
        return

    # ------------------------------------------------------------------
    # Startup: Create a long-lived AdminClient for topic management.
    # Reusing the AdminClient across topic operations eliminates repeated
    # TCP handshakes and metadata fetches, improving startup performance.
    # ------------------------------------------------------------------
    admin = AdminClient(kafka_config)

    # ------------------------------------------------------------------
    # Startup: ensure the canary topic exists before creating clients.
    # ------------------------------------------------------------------
    try:
        ensure_topic(kafka_config, topic, admin=admin)
    except RuntimeError as exc:
        log.error("Startup failed during topic creation", extra={"error": str(exc)})
        return

    # Optionally attach the Kafka log handler so all subsequent log output
    # is also published to the log topic.  This is done before creating the
    # producer/consumer so those startup messages are captured too.
    kafka_log_handler = None
    if log_topic_enabled:
        try:
            ensure_log_topic(kafka_config, log_topic, log_topic_retention_ms, admin=admin)
            kafka_log_handler = KafkaLogHandler(kafka_config, log_topic)
            kafka_log_handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            ))
            logging.getLogger().addHandler(kafka_log_handler)
            log.info(
                "Log topic enabled",
                extra={
                    "log_topic": log_topic,
                    "retention_ms": log_topic_retention_ms,
                }
            )
        except RuntimeError as exc:
            log.error(
                "Failed to set up log topic, continuing without it",
                extra={"error": str(exc)}
            )

    # Create the shared Schema Registry client.  The same instance is used by
    # both the producer's AvroSerializer and the consumer's AvroDeserializer,
    # and by check_sr() for independent SR health checks.
    sr_client = SchemaRegistryClient(sr_config)

    # Create the long-lived producer and serializer.  Thread-safe in librdkafka —
    # shared across all per-partition check threads.
    producer, avro_serializer = create_producer(kafka_config, sr_client)

    # ------------------------------------------------------------------
    # Initial partition sync — determines how many per-partition consumers
    # to create.  Setting last_sync_time=now prevents a duplicate sync on
    # the first main loop iteration. Reuse the AdminClient created earlier.
    # ------------------------------------------------------------------
    log.info("Running initial partition sync")
    result = sync_topic_partitions(kafka_config, topic, admin=admin)
    if not result:
        log.error("Startup failed, could not determine partition count")
        return
    num_brokers, num_partitions = result
    metrics.BROKER_COUNT.set(num_brokers)
    metrics.TOPIC_PARTITION_COUNT.set(num_partitions)
    partitions = list(range(num_partitions))
    last_sync_time = time.time()

    # Create one consumer per partition using manual assignment (assign()).
    # No subscribe() or wait_for_assignment() needed — partitions are
    # immediately available for seek_to_end() and poll().
    consumers, deserializers = _build_consumer_pool(kafka_config, sr_client, topic, partitions)
    log.info(
        "Per-partition consumers ready",
        extra={
            "num_brokers": num_brokers,
            "num_partitions": num_partitions,
            "partitions": partitions,
        }
    )

    # Warn if partition count exceeds threshold (cardinality control)
    if num_partitions > partition_threshold:
        log.warning(
            "Partition count exceeds threshold - using aggregated partition labels",
            extra={
                "num_partitions": num_partitions,
                "partition_threshold": partition_threshold,
                "message": f"Metrics will use partition='*' instead of per-partition labels to prevent "
                          f"Prometheus cardinality explosion ({num_partitions} partitions would create "
                          f"~{num_partitions * 22} time series per host). Increase metrics.partition.threshold "
                          f"in config.ini if you need per-partition granularity and have sufficient Prometheus memory."
            }
        )

    consecutive_failures = {p: 0 for p in partitions}
    check_sequence       = 0
    start_time           = time.time()
    last_sr_check_time   = 0.0   # set to 0 so the first iteration triggers an SR check immediately

    # Cap max workers to prevent unbounded thread growth on large clusters.
    # Each thread uses ~8MB stack memory (100 threads = ~800MB).
    # max_workers is configurable via config.ini (default: 20).
    # Formula: actual_workers = min(partition_count, max_workers)
    executor = ThreadPoolExecutor(max_workers=min(num_partitions, max_workers))

    try:
        while not _shutdown_event.is_set():

            # Update uptime gauge at the top of every iteration so it reflects
            # elapsed time even if a check takes a long time.
            metrics.UPTIME_SECONDS.set(time.time() - start_time)

            # ------------------------------------------------------------------
            # Partition sync — detect broker count changes (scale-up/down)
            # Runs on its own cadence, independently of canary checks.
            # Uses incremental updates: only add/remove changed consumers instead
            # of rebuilding the entire pool, preventing monitoring gaps.
            # Reuses the long-lived AdminClient to avoid connection overhead.
            # ------------------------------------------------------------------
            if time.time() - last_sync_time >= sync_interval:
                result = sync_topic_partitions(kafka_config, topic, admin=admin)
                if result:
                    metrics.BROKER_COUNT.set(result[0])
                    metrics.TOPIC_PARTITION_COUNT.set(result[1])
                    if result[1] != len(partitions):
                        log.info(
                            "Partition count changed, updating consumer pool incrementally",
                            extra={
                                "old_partition_count": len(partitions),
                                "new_partition_count": result[1],
                            }
                        )
                        new_partitions = set(range(result[1]))
                        old_partitions = set(partitions)

                        # Add new partitions (scale-up case)
                        added = new_partitions - old_partitions
                        if added:
                            log.info(f"Adding {len(added)} new partition(s): {sorted(added)}")
                            # Create new consumers in parallel to minimize downtime
                            with ThreadPoolExecutor(max_workers=min(len(added), 10)) as pool:
                                futures = {p: pool.submit(create_partition_consumer, kafka_config, sr_client, topic, p) for p in added}
                                for p, future in futures.items():
                                    consumer, deserializer = future.result()
                                    consumers[p] = consumer
                                    deserializers[p] = deserializer
                                    consecutive_failures[p] = 0

                        # Remove deleted partitions (scale-down case)
                        removed = old_partitions - new_partitions
                        if removed:
                            log.info(f"Removing {len(removed)} partition(s): {sorted(removed)}")
                            for p in removed:
                                consumers[p].close()
                                del consumers[p]
                                del deserializers[p]
                                del consecutive_failures[p]

                        # Update partition list
                        partitions = list(range(result[1]))

                        # Resize executor only if needed (worker count changed significantly)
                        new_max_workers_count = min(result[1], max_workers)
                        old_max_workers_count = min(len(old_partitions), max_workers)
                        if new_max_workers_count != old_max_workers_count:
                            log.info(
                                f"Resizing executor: {old_max_workers_count} → {new_max_workers_count} workers"
                            )
                            # Shut down old executor gracefully after in-flight checks complete
                            executor.shutdown(wait=True)
                            executor = ThreadPoolExecutor(max_workers=new_max_workers_count)

                        log.info(
                            "Consumer pool updated",
                            extra={
                                "num_partitions": result[1],
                                "partitions": partitions,
                                "added": len(added),
                                "removed": len(removed),
                            }
                        )
                last_sync_time = time.time()

            # ------------------------------------------------------------------
            # Schema Registry health check — independent cadence and signal
            # SR availability is not directly exercised by the canary check
            # (the schema is cached after first use), so it must be probed
            # separately to surface SR outages as a distinct metric.
            # ------------------------------------------------------------------
            if time.time() - last_sr_check_time >= sr_check_interval:
                sr_start = time.time()
                try:
                    check_sr(sr_client)
                    sr_duration_ms = (time.time() - sr_start) * 1000
                    metrics.SR_LATENCY.labels(host=metrics.HOST).observe(sr_duration_ms)
                    metrics.SR_CHECKS_TOTAL.labels(result="success", host=metrics.HOST).inc()
                    log.info("Schema Registry check succeeded", extra={"latency_ms": sr_duration_ms})
                except CanaryError as exc:
                    sr_duration_ms = (time.time() - sr_start) * 1000
                    metrics.SR_LATENCY.labels(host=metrics.HOST).observe(sr_duration_ms)
                    metrics.SR_CHECKS_TOTAL.labels(result="failure", host=metrics.HOST).inc()
                    log.error(
                        "Schema Registry check failed",
                        extra={
                            "phase": exc.phase,
                            "category": exc.category,
                            "detail": exc.detail,
                            "latency_ms": sr_duration_ms,
                        }
                    )
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
                    check_kafka, producer, avro_serializer, consumers[p], deserializers[p], topic, timeout, check_sequence, p
                ): p
                for p in partitions
            }

            for future in as_completed(futures):
                p = futures[future]
                try:
                    latency_ms = future.result()

                    # Success path — counters are always updated (even during warmup)
                    # so failure detection is never suppressed.
                    _update_success_metrics(p, check_sequence, latency_ms, consecutive_failures, is_warming_up, num_partitions, partition_threshold)

                    if is_warming_up:
                        log.info(
                            "Warmup check succeeded (latency not recorded)",
                            extra={
                                "check_sequence": check_sequence,
                                "partition": p,
                                "latency_ms": latency_ms,
                                "warmup": True,
                            }
                        )
                    else:
                        log.info(
                            "Check succeeded",
                            extra={
                                "check_sequence": check_sequence,
                                "partition": p,
                                "latency_ms": latency_ms,
                            }
                        )

                except CanaryError as exc:
                    # Classified failure — phase and category are known
                    _update_failure_metrics(p, check_sequence, consecutive_failures, exc.phase, exc.category, num_partitions, partition_threshold)
                    log.error(
                        "Check failed",
                        extra={
                            "consecutive_failures": consecutive_failures[p],
                            "check_sequence": check_sequence,
                            "partition": p,
                            "phase": exc.phase,
                            "category": exc.category,
                            "detail": exc.detail,
                        }
                    )

                except Exception as exc:
                    # Unexpected failure — not a CanaryError, so phase/category unknown.
                    # Still recorded in metrics so the failure is visible.
                    _update_failure_metrics(p, check_sequence, consecutive_failures, "UNKNOWN", "UNKNOWN", num_partitions, partition_threshold)
                    log.error(
                        "Check failed (unexpected exception)",
                        extra={
                            "consecutive_failures": consecutive_failures[p],
                            "check_sequence": check_sequence,
                            "partition": p,
                            "phase": "UNKNOWN",
                            "category": "UNKNOWN",
                            "detail": str(exc),
                            "exception_type": type(exc).__name__,
                        }
                    )

            if is_warming_up and check_sequence == warmup_checks:
                log.info(
                    "Warmup complete, latency metrics enabled",
                    extra={"warmup_checks": warmup_checks}
                )

            # Wait for the configured interval before the next check.
            # Using Event.wait() instead of time.sleep() ensures that shutdown
            # signals are detected immediately rather than waiting for the full
            # interval. wait() returns True if shutdown was signaled, False on timeout.
            log.debug("Sleeping until next check", extra={"interval_seconds": interval})
            _shutdown_event.wait(interval)

    finally:
        # Shut down executor gracefully, waiting for any in-flight checks to complete.
        # This ensures all metrics are properly recorded before exit.
        executor.shutdown(wait=True)

        # Close all per-partition consumers so brokers remove them from their
        # groups immediately rather than waiting for the session timeout.
        for c in consumers.values():
            c.close()

        # Flush and release the producer to close connections immediately.
        # confluent_kafka.Producer doesn't have a close() method, but flush()
        # ensures all messages are delivered before the process exits and
        # resources are cleaned up by __del__.
        producer.flush(timeout=5)

        # Release the AdminClient to close connections immediately.
        del admin

        log.info("cloud_canary stopped")

        # Flush and detach the Kafka log handler so the last few log records
        # (including "cloud_canary stopped") reach the log topic before exit.
        if kafka_log_handler:
            logging.getLogger().removeHandler(kafka_log_handler)
            kafka_log_handler.close()


if __name__ == "__main__":
    run()
