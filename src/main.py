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
#       count changes, the bounded worker pool is resized automatically.
#
#   Schema Registry check (sr.check.interval.seconds, default 60 s)
#       Independent health check against the Schema Registry, surfacing SR
#       availability as a separate Prometheus counter, distinct from the
#       Kafka broker health signal.
#
# Long-lived clients
# ------------------
# The producer, worker-owned consumers, and Schema Registry client are created
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
# Worker concurrency
# -------------------------
# check_kafka() is called once per partition on every cycle via a bounded
# ThreadPoolExecutor. Each worker exclusively owns one Consumer and reassigns
# it to the selected partition before each check.
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
# after the current check completes.  All worker-owned consumers are closed
# in the finally block to cleanly leave the consumer group and release resources.
#
# Multi-instance support
# ----------------------
# Multiple instances can run concurrently against the same Confluent Cloud
# cluster without interfering:
#
#   Consumer groups — each worker-owned consumer generates a UUID-based
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

import concurrent.futures
import logging
import os
import signal
import ssl
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, wait
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Consumer, Producer
from confluent_kafka.serialization import SerializationError
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from src import constants as const
from src import bounded_executor as startup_executor_module
from src import metrics
from src.__version__ import __version__
from src.bounded_executor import DaemonThreadPoolExecutor
from src.config import SCHEDULER_HEARTBEAT_INTERVAL_SECONDS, load_config
from src.consumer import (
    consume_canary,
    create_partition_consumer,
    seek_to_end,
)
from src.worker_pool import (
    ConsumerFailure,
    ConsumerInvalidError,
    WorkerPool,
    WorkerPoolInvariantError,
)
from src.error_classifier import (
    CanaryError,
    ErrorCategory,
    FailureComponent,
    FailureDescriptor,
    FailureSummary,
    Phase,
    Recoverability,
    classify_consumer_error,
    classify_kafka_error,
    classify_schema_registry_error,
)
from src.health import configure_health_state
from src.health_state import HealthStateStore
from src.kafka_log_handler import KafkaLogHandler
from src.metrics import start_metrics_server
from src.producer import create_producer, produce_canary
from src.recovery_policy import RecoveryAction, RecoveryContext, recovery_action
from src.scheduler import (
    OperationDeadlineExceeded,
    PartitionScheduler,
    ScheduledOperationLane,
)
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

# Legacy control-loop wakeup used outside the signal handler. Signal delivery
# publishes _shutdown_request directly so its deadline cannot be delayed by an
# Event implementation or dispatch-owned synchronization.
_shutdown_event = threading.Event()
_shutdown_timeout_seconds = 10.0


@dataclass(frozen=True, slots=True)
class ShutdownRequest:
    """The first observed process-shutdown request and its fixed deadline."""

    signal_number: int
    requested_at: float
    deadline: float


_shutdown_request: ShutdownRequest | None = None
_shutdown_claim_token = object()
_shutdown_request_claim = [_shutdown_claim_token]
_fatal_shutdown_request: ShutdownRequest | None = None
_fatal_shutdown_request_claim = [_shutdown_claim_token]
_dispatch_guard = threading.Lock()


class _StartupDispatchRejected(RuntimeError):
    """Shutdown won before a startup dependency could be submitted."""


class _ReplacementDispatchRejected(RuntimeError):
    """Shutdown won before a replacement runtime could be submitted."""


def _shutdown_requested() -> bool:
    """Return whether signal publication or an internal stop was requested."""
    return (
        _fatal_shutdown_request is not None
        or not _shutdown_request_claim
        or _shutdown_request is not None
        or _shutdown_event.is_set()
    )


def _active_shutdown_request() -> ShutdownRequest | None:
    """Return the published request with the earliest immutable deadline."""
    requests = tuple(
        request
        for request in (_shutdown_request, _fatal_shutdown_request)
        if request is not None
    )
    if not requests:
        return None
    return min(requests, key=lambda request: request.deadline)


def _shutdown_deadline() -> float | None:
    """Return the fixed deadline, or no remaining time during publication."""
    request = _active_shutdown_request()
    if request is not None:
        return request.deadline
    if not _shutdown_request_claim or _shutdown_event.is_set():
        return time.monotonic()
    return None


def _handle_signal(sig, frame) -> None:
    """Publish the first shutdown request without locks or dependency work."""
    global _shutdown_request

    try:
        _shutdown_request_claim.pop()
    except IndexError:
        return

    # The atomic pop above reserves publication before calling even the
    # monotonic clock. The separate claim lets shutdown observers notice the
    # request without exposing a partially initialized ShutdownRequest.
    requested_at = time.monotonic()
    _shutdown_request = ShutdownRequest(
        signal_number=sig,
        requested_at=requested_at,
        deadline=requested_at + _shutdown_timeout_seconds,
    )


def _request_fatal_shutdown() -> None:
    """Request bounded non-zero termination for an internal invariant failure."""
    global _fatal_shutdown_request, _shutdown_request

    requested_at = time.monotonic()
    request = ShutdownRequest(
        signal_number=0,
        requested_at=requested_at,
        deadline=requested_at + _shutdown_timeout_seconds,
    )

    # Publish a complete immutable fatal request before competing for the
    # shared shutdown claim. This makes dispatch rejection and the original
    # fatal deadline observable even while shared publication is interrupted.
    try:
        _fatal_shutdown_request_claim.pop()
    except IndexError:
        return
    _fatal_shutdown_request = request

    # Keep the fatal outcome distinct even when an earlier ordinary signal
    # already owns the immutable first shutdown request and its deadline.
    try:
        _shutdown_request_claim.pop()
    except IndexError:
        return
    _shutdown_request = request


def _startup_failure(
    exc: BaseException,
    *,
    phase: Phase,
    component: FailureComponent,
    schema_registry: bool = False,
) -> FailureDescriptor:
    """Classify a startup exception without retaining dependency data."""
    if isinstance(exc, CanaryError):
        return exc.failure
    if (
        isinstance(exc, KafkaException)
        and exc.args
        and isinstance(exc.args[0], KafkaError)
    ):
        return classify_kafka_error(
            exc.args[0], phase=phase, component=component
        )
    if isinstance(exc, ssl.SSLCertVerificationError):
        return FailureDescriptor(
            component=component,
            phase=phase,
            category=ErrorCategory.TLS_CERTIFICATE,
            recoverability=Recoverability.DETERMINISTIC,
            code="TLS.CERTIFICATE_VERIFICATION_FAILED",
            safe_summary=FailureSummary.OPERATION_FAILED,
        )
    if isinstance(exc, (SystemExit, ValueError, TypeError, FileNotFoundError)):
        return FailureDescriptor(
            component=component,
            phase=phase,
            category=ErrorCategory.CONFIGURATION,
            recoverability=Recoverability.DETERMINISTIC,
            code="STARTUP.INVALID_CONFIGURATION",
            safe_summary=FailureSummary.INVALID_CONFIGURATION,
        )
    if schema_registry:
        return classify_schema_registry_error(exc)
    return FailureDescriptor(
        component=component,
        phase=phase,
        category=ErrorCategory.UNKNOWN,
        recoverability=Recoverability.UNKNOWN,
        code=None,
        safe_summary=FailureSummary.OPERATION_FAILED,
    )


def _execute_startup_operation(operation, classifier):
    """Execute startup work through centralized classification and policy."""
    try:
        return operation()
    except _StartupDispatchRejected:
        raise
    except (Exception, SystemExit) as exc:
        failure = classifier(exc)
        action = recovery_action(failure, RecoveryContext.STARTUP)

    log.error(
        "Startup operation failed",
        extra={
            "phase": failure.phase,
            "category": failure.category,
            "code": failure.code,
            "detail": failure.safe_summary,
            "recovery_action": action,
        },
    )
    if action is RecoveryAction.FAIL_STARTUP:
        _request_fatal_shutdown()
    # Raise after leaving the handler so the bounded error does not retain the
    # raw exception or its potentially sensitive text through __context__.
    raise CanaryError(failure)


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
    failure = None
    try:
        sr_client.get_subjects()
    except Exception as exc:
        failure = classify_schema_registry_error(exc)
    if failure is not None:
        # Raise outside the handler so the raw dependency exception is not
        # attached as __context__ to the object transported by a Future.
        raise CanaryError(failure)


def _local_kafka_failure(phase: Phase, category: ErrorCategory,
                         recoverability: Recoverability, code: str,
                         summary: FailureSummary) -> FailureDescriptor:
    """Describe a local check failure without retaining exception text."""
    return FailureDescriptor(
        component=FailureComponent.KAFKA_PARTITION,
        phase=phase,
        category=category,
        recoverability=recoverability,
        code=code,
        safe_summary=summary,
    )


def _bounded_completion_failure(error):
    """Return storage and logging inputs derived only from a descriptor."""
    failure = error.failure
    label = f"{failure.phase.value}:{failure.category.value}"
    fields = {"phase": failure.phase, "category": failure.category,
              "code": failure.code, "detail": failure.safe_summary}
    return failure, label, fields


def _timed_sr_probe(sr_client: SchemaRegistryClient):
    """Run one SR probe in its lane and retain its actual monotonic duration."""
    started = time.monotonic()
    try:
        check_sr(sr_client)
        return (time.monotonic() - started) * 1000, None
    except CanaryError as exc:
        return (time.monotonic() - started) * 1000, exc


def _record_schema_registry_completion(health_store, duration_ms, error) -> None:
    """Record one completed production SR probe from bounded result data."""
    metrics.SR_LATENCY.labels(host=metrics.HOST).observe(duration_ms)
    if error is None:
        health_store.record_schema_registry_result(success=True)
        metrics.SR_CHECKS_TOTAL.labels(result="success", host=metrics.HOST).inc()
        log.info(
            "Schema Registry check succeeded", extra={"latency_ms": duration_ms}
        )
        return

    failure, failure_label, failure_fields = _bounded_completion_failure(error)
    action = recovery_action(failure, RecoveryContext.RUNTIME)
    health_store.record_schema_registry_result(
        success=False,
        deterministic_failure=(
            action is RecoveryAction.MARK_COMPONENT_UNHEALTHY
        ),
        failure=failure_label,
    )
    metrics.SR_CHECKS_TOTAL.labels(result="failure", host=metrics.HOST).inc()
    log.error(
        "Schema Registry check failed",
        extra={**failure_fields, "latency_ms": duration_ms},
    )
    if action is RecoveryAction.TERMINATE_PROCESS:
        _request_fatal_shutdown()


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
        Worker-owned consumer, manually assigned to `partition` immediately
        before this check.

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
        this partition; the worker has already assigned its consumer to it.

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
    seek_failure = None
    try:
        seek_to_end(consumer)
    except (RuntimeError, KafkaException) as exc:
        descriptor = classify_consumer_error(exc, phase=Phase.SEEK)
        if descriptor.category is ErrorCategory.CLIENT_STATE:
            raise ConsumerInvalidError(descriptor) from None
        seek_failure = descriptor
    except Exception:
        seek_failure = _local_kafka_failure(
            Phase.SEEK, ErrorCategory.UNKNOWN, Recoverability.UNKNOWN,
            "CANARY.SEEK_UNKNOWN", FailureSummary.OPERATION_FAILED,
        )
    finally:
        # Record seek duration regardless of success or failure so the
        # histogram captures partial attempts (e.g. successful on some
        # partitions before failing on another).
        # Using perf_counter() for monotonic, accurate timing measurement.
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.SEEK_DURATION.labels(host=metrics.HOST).observe(
            (time.perf_counter() - t_seek) * 1000
        )
    if seek_failure is not None:
        raise CanaryError(seek_failure)

    # ------------------------------------------------------------------
    # Phase: PRODUCE
    # Send the canary message to the target partition and wait for the
    # broker to acknowledge receipt by all in-sync replicas (acks=all).
    # A failure here means the broker could not commit the write —
    # possible ISR degradation, quota exceeded, or network issue between
    # the client and the leader for this partition.
    # ------------------------------------------------------------------
    t_produce = time.perf_counter()
    produce_failure = None
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
    except (SerializationError, TypeError, ValueError):
        produce_failure = _local_kafka_failure(
            Phase.PRODUCE, ErrorCategory.SERIALIZATION,
            Recoverability.INTERNAL_FATAL, "CANARY.PRODUCE_SERIALIZATION",
            FailureSummary.INTERNAL_FAILURE,
        )
    except KafkaException as exc:
        produce_failure = (
            classify_kafka_error(exc.args[0], phase=Phase.PRODUCE)
            if exc.args and isinstance(exc.args[0], KafkaError)
            else _local_kafka_failure(
                Phase.PRODUCE, ErrorCategory.UNKNOWN,
                Recoverability.UNKNOWN, "CANARY.PRODUCE_UNKNOWN",
                FailureSummary.OPERATION_FAILED,
            )
        )
    except RuntimeError:
        # Local delivery-callback wait expired after 2s.
        # delivery.timeout.ms has not expired so the cause is unknown;
        # UNKNOWN is more honest than NETWORK or BROKER here.
        produce_failure = _local_kafka_failure(
            Phase.PRODUCE, ErrorCategory.UNKNOWN, Recoverability.UNKNOWN,
            "CANARY.PRODUCE_TIMEOUT", FailureSummary.OPERATION_TIMED_OUT,
        )
    except Exception:
        produce_failure = _local_kafka_failure(
            Phase.PRODUCE, ErrorCategory.UNKNOWN, Recoverability.UNKNOWN,
            "CANARY.PRODUCE_UNKNOWN", FailureSummary.OPERATION_FAILED,
        )
    finally:
        # Using perf_counter() for monotonic, accurate timing measurement.
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.PRODUCE_DURATION.labels(host=metrics.HOST).observe(
            (time.perf_counter() - t_produce) * 1000
        )
    if produce_failure is not None:
        raise CanaryError(produce_failure)

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
    consume_failure = None
    try:
        received, receive_ts = consume_canary(consumer, avro_deserializer, sent.message_id, timeout)
    except TimeoutError:
        consume_failure = _local_kafka_failure(
            Phase.CONSUME, ErrorCategory.BROKER_SERVICE,
            Recoverability.TRANSIENT, "CANARY.CONSUME_TIMEOUT",
            FailureSummary.OPERATION_TIMED_OUT,
        )
    except (SerializationError, TypeError, ValueError):
        consume_failure = _local_kafka_failure(
            Phase.CONSUME, ErrorCategory.SERIALIZATION,
            Recoverability.INTERNAL_FATAL, "CANARY.CONSUME_DESERIALIZATION",
            FailureSummary.INTERNAL_FAILURE,
        )
    except KafkaException as exc:
        descriptor = classify_consumer_error(exc, phase=Phase.CONSUME)
        if descriptor.category is ErrorCategory.CLIENT_STATE:
            raise ConsumerInvalidError(descriptor) from None
        consume_failure = descriptor
    except Exception:
        consume_failure = _local_kafka_failure(
            Phase.CONSUME, ErrorCategory.UNKNOWN, Recoverability.UNKNOWN,
            "CANARY.CONSUME_UNKNOWN", FailureSummary.OPERATION_FAILED,
        )
    if consume_failure is not None:
        raise CanaryError(consume_failure)

    # End-to-end latency: consumer receive time minus the timestamp captured
    # by the producer just before calling produce().  Both timestamps are
    # taken on the same host so clock skew is not a factor.
    return receive_ts - received.send_timestamp_ms


def _build_consumer_pool(
    kafka_config: dict[str, str],
    sr_client: SchemaRegistryClient,
    topic: str,
    partitions: list[int],
    max_workers: int,
) -> WorkerPool:
    """
    Create exactly one exclusively owned consumer per active worker.
    """
    worker_count = min(len(partitions), max_workers)
    return WorkerPool(
        size=worker_count,
        topic=topic,
        factory=lambda partition: create_partition_consumer(
            kafka_config, sr_client, topic, partition
        ),
    )


def _prepare_replacement_runtime(
    kafka_config: dict[str, str],
    sr_client: SchemaRegistryClient,
    topic: str,
    partitions: list[int],
    max_workers: int,
):
    """Build a complete unpublished consumer/scheduler generation or clean it up."""
    replacement_pool = _build_consumer_pool(
        kafka_config, sr_client, topic, partitions, max_workers
    )
    try:
        # Keep one executor for the lifetime of this runtime generation.  The
        # scheduler limits active work to min(partitions, max_workers), while
        # reserving max_workers here lets additive topology changes use new
        # capacity without overlapping executor generations.
        replacement_executor = DaemonThreadPoolExecutor(max_workers=max_workers)
    except Exception:
        replacement_pool.close()
        raise
    return replacement_pool, replacement_executor


def _cleanup_unpublished_replacement(consumer_pool, executor) -> None:
    """Best-effort bounded cleanup for a generation that was never published."""
    try:
        executor.shutdown(wait=True)
    except Exception:
        log.exception("Failed to stop unpublished replacement scheduler")
    try:
        consumer_pool.close()
    except Exception:
        log.exception("Failed to close unpublished replacement consumer pool")


def _replace_recreated_runtime(
    kafka_config: dict[str, str],
    sr_client: SchemaRegistryClient,
    topic: str,
    old_partitions: list[int],
    old_consumers,
    replacement_partition_count: int,
    max_workers: int,
    health_store: HealthStateStore,
    scheduler=None,
):
    """Build and publish one complete replacement generation or fail closed."""
    replacement_partitions = list(range(replacement_partition_count))
    try:
        old_consumers.close()
    except Exception as exc:
        raise RuntimeError(
            "Could not retire every consumer from the replaced topic"
        ) from exc

    # Retire the old generation before constructing another one. This avoids a
    # transient double-pool that would violate the configured consumer bound.
    replacement_pool, replacement_executor = (
        _prepare_replacement_runtime(
            kafka_config,
            sr_client,
            topic,
            replacement_partitions,
            max_workers,
        )
    )

    replacement_failures = {partition: 0 for partition in replacement_partitions}
    try:
        scheduler_oldest_overdue = 0.0
        if scheduler is not None:
            scheduler.replace_partitions(replacement_partitions)
            scheduler_oldest_overdue = (
                scheduler.snapshot().oldest_overdue_seconds
            )
        health_store.replace_expected_partitions(
            replacement_partitions,
            preserve_existing=False,
            scheduler_oldest_overdue=scheduler_oldest_overdue,
        )
        metrics.reconcile_partition_metrics(replacement_partitions, reset=True)
    except Exception:
        _cleanup_unpublished_replacement(
            replacement_pool, replacement_executor
        )
        raise
    return (
        replacement_pool,
        replacement_executor,
        replacement_partitions,
        replacement_failures,
    )


def _log_reconciliation_failure(stage: str, exc: Exception) -> None:
    """Report a fatal reconciliation stage without logging exception contents."""
    log.error(
        "Fatal topic reconciliation failure",
        extra={"stage": stage, "error": type(exc).__name__},
    )


def _cleanup_runtime_resources(executor, consumers, producer) -> None:
    """Best-effort cleanup that is safe for a partially constructed runtime."""
    if executor is not None:
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            log.exception("Failed to stop scheduler during final cleanup")

    consumer_items = ()
    if consumers is not None:
        try:
            consumers.close()
        except AttributeError:
            consumer_items = consumers.items()
        except Exception:
            log.exception("Failed to close consumer pool during final cleanup")

    for partition, consumer in consumer_items:
        try:
            consumer.close()
        except Exception:
            log.exception(
                "Failed to close consumer during final cleanup",
                extra={"partition": partition},
            )

    if executor is not None:
        try:
            # WorkerPool.close() marks borrowed consumers for owner-thread
            # cleanup.  Give those cooperative workers the remainder of the
            # process shutdown window; the lifecycle daemon boundary abandons
            # this wait at the already-published absolute deadline if a native
            # dependency call never returns.
            executor.shutdown(wait=True)
        except Exception:
            log.exception("Failed to drain scheduler during final cleanup")

    if producer is not None:
        try:
            producer.flush(timeout=5)
        except Exception:
            log.exception("Failed to flush producer during final cleanup")


def _submit_due_checks(
    scheduler,
    executor,
    active_futures,
    worker_limit,
    check_sequence,
    submit_check,
    generation=None,
) -> tuple[int, ...]:
    """Fill free worker slots in one shutdown-linearized transaction."""
    with _dispatch_guard:
        if _shutdown_requested():
            return ()
        available = max(0, worker_limit - len(active_futures))
        selected = scheduler.acquire_due(available)
        for partition in selected:
            identity = (partition, check_sequence)
            if generation is not None:
                identity = (*identity, generation)
            active_futures[executor.submit(submit_check, partition)] = identity
        return selected


def _dispatch_scheduled_operation(lane, operation) -> bool:
    """Dispatch an SR or reconciliation occurrence before shutdown only."""
    with _dispatch_guard:
        if _shutdown_requested():
            return False
        return lane.dispatch_if_due(operation)


def _run_guarded_daemon_dependency(
    operation, *, thread_name_prefix, rejection_type, rejection_message
):
    """Authorize one dependency call and run it on an abandonable daemon lane.

    The start gate prevents a fast worker from entering native client code
    before the dispatch guard has been released. A rejected submission raises
    immediately so callers cannot advance to a later dependency stage.
    """
    executor = startup_executor_module.DaemonThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix=thread_name_prefix,
    )
    start_native_work = threading.Event()

    def invoke():
        start_native_work.wait()
        return operation()

    try:
        with _dispatch_guard:
            if _shutdown_requested():
                raise rejection_type(rejection_message)
            future = executor.submit(invoke)
        start_native_work.set()
        return future.result()
    finally:
        # Never join a native call here. The lifecycle daemon boundary owns
        # the fixed shutdown deadline and may abandon this daemon worker.
        start_native_work.set()
        executor.shutdown(wait=False, cancel_futures=True)


def _run_startup_dependency(operation):
    """Run one startup dependency call through guarded daemon dispatch."""
    return _run_guarded_daemon_dependency(
        operation,
        thread_name_prefix="cloud-canary-startup",
        rejection_type=_StartupDispatchRejected,
        rejection_message="shutdown rejected startup dependency submission",
    )


def _run_replacement_runtime_transaction(operation):
    """Run replacement runtime construction through guarded daemon dispatch."""
    return _run_guarded_daemon_dependency(
        operation,
        thread_name_prefix="cloud-canary-replacement",
        rejection_type=_ReplacementDispatchRejected,
        rejection_message="shutdown rejected replacement runtime submission",
    )


def _publish_scheduler_observability(scheduler, health_store) -> None:
    """Refresh constant-time scheduling metrics from one current snapshot."""
    snapshot = scheduler.snapshot()
    health_store.record_scheduler_heartbeat()
    health_store.record_scheduler_capacity(snapshot.oldest_overdue_seconds)
    metrics.update_scheduler_metrics(snapshot)


def _publish_shutdown_state(health_store) -> None:
    """Publish an observed stop request outside the signal handler."""
    if _shutdown_requested():
        health_store.begin_shutdown()


def _close_kafka_log_handler(handler) -> None:
    """Detach an optional Kafka handler without masking a fatal exception."""
    if handler is None:
        return
    try:
        logging.getLogger().removeHandler(handler)
        handler.close()
    except Exception:
        log.exception("Failed to close Kafka log handler during final cleanup")


def _register_kafka_log_handler(handler) -> bool:
    """Attach before shutdown, disarming a handler if shutdown wins the race."""
    root_logger = logging.getLogger()
    should_close = False
    with _dispatch_guard:
        if _shutdown_requested():
            should_close = True
        else:
            root_logger.addHandler(handler)
            if _shutdown_requested():
                root_logger.removeHandler(handler)
                should_close = True
    if should_close:
        # Kafka flush is dependency I/O and must remain outside the dispatch
        # guard. The handler's shutdown predicate already disarms emit().
        handler.close()
        return False
    return True


def _update_success_metrics(
    partition: int,
    check_sequence: int,
    latency_ms: int,
    consecutive_failures: dict[int, int],
    is_warming_up: bool,
    *_legacy_cardinality_args,
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
    Additional positional arguments are accepted for compatibility with older
    in-process callers; cardinality is no longer threshold-dependent.
    """
    consecutive_failures[partition] = 0

    metrics.CHECKS_TOTAL.labels(
        host=metrics.HOST, result="success"
    ).inc()
    metrics.record_partition_check(
        partition, "success", consecutive_failures[partition], last_success=time.time()
    )
    metrics.CHECK_SEQUENCE.set(check_sequence)

    if not is_warming_up:
        # Note: Partition label removed to prevent metrics cardinality explosion.
        metrics.E2E_LATENCY.labels(host=metrics.HOST).observe(latency_ms)


def _record_successful_partition_attempt(
    health_store: HealthStateStore,
    partition: int,
    check_sequence: int,
    latency_ms: int,
    consecutive_failures: dict[int, int],
    *_legacy_cardinality_args,
    generation=None,
) -> bool | None:
    """Record a success using the partition's authoritative warmup state."""
    is_warming_up = health_store.record_partition_result(
        partition, success=True, generation=generation
    )
    if is_warming_up is None:
        return None
    _update_success_metrics(
        partition,
        check_sequence,
        latency_ms,
        consecutive_failures,
        is_warming_up,
    )

    if is_warming_up:
        log.info(
            "Warmup check succeeded (latency not recorded)",
            extra={
                "check_sequence": check_sequence,
                "partition": partition,
                "latency_ms": latency_ms,
                "warmup": True,
            },
        )
    else:
        log.info(
            "Check succeeded",
            extra={
                "check_sequence": check_sequence,
                "partition": partition,
                "latency_ms": latency_ms,
            },
        )
    return is_warming_up


def _record_failed_partition_attempt(
    health_store: HealthStateStore,
    partition: int,
    check_sequence: int,
    consecutive_failures: dict[int, int],
    phase: str,
    category: str,
    *,
    failure: str,
    generation=None,
) -> bool:
    """Publish a failed attempt only when its topic generation is current."""
    accepted = health_store.record_partition_result(
        partition,
        success=False,
        failure=failure,
        generation=generation,
    )
    if accepted is None:
        return False
    _update_failure_metrics(
        partition,
        check_sequence,
        consecutive_failures,
        phase,
        category,
    )
    return True


def _record_kafka_completion(
    health_store: HealthStateStore,
    partition: int,
    check_sequence: int,
    generation: int,
    consecutive_failures: dict[int, int],
    completed_future,
) -> None:
    """Record one completed production Kafka check using bounded result data."""
    try:
        latency_ms = completed_future.result()
        _record_successful_partition_attempt(
            health_store,
            partition,
            check_sequence,
            latency_ms,
            consecutive_failures,
            generation=generation,
        )
        return
    except WorkerPoolInvariantError:
        raise
    except (ConsumerFailure, CanaryError) as error:
        failure, failure_label, failure_fields = _bounded_completion_failure(error)
        action = recovery_action(failure, RecoveryContext.RUNTIME)
        accepted = _record_failed_partition_attempt(
            health_store,
            partition,
            check_sequence,
            consecutive_failures,
            failure.phase,
            failure.category,
            failure=failure_label,
            generation=generation,
        )
        if accepted and action is RecoveryAction.MARK_COMPONENT_UNHEALTHY:
            if failure.category is ErrorCategory.CAPACITY:
                health_store.mark_scheduler_capacity_degraded()
            else:
                health_store.mark_partition_deterministic_failure(
                    partition, failure=failure, generation=generation
                )
        if accepted:
            message = (
                "Consumer check failed after replacement"
                if isinstance(error, ConsumerFailure)
                else "Check failed"
            )
            log.error(
                message,
                extra={
                    "consecutive_failures": consecutive_failures[partition],
                    "check_sequence": check_sequence,
                    "partition": partition,
                    **failure_fields,
                },
            )
        if accepted and action is RecoveryAction.TERMINATE_PROCESS:
            _request_fatal_shutdown()
        return
    except Exception:
        # Unknown exception content is deliberately discarded at completion.
        bounded_error = CanaryError(_local_kafka_failure(
            Phase.UNKNOWN,
            ErrorCategory.UNKNOWN,
            Recoverability.INTERNAL_FATAL,
            "CANARY.UNEXPECTED_COMPLETION",
            FailureSummary.INTERNAL_FAILURE,
        ))
        failure, failure_label, failure_fields = _bounded_completion_failure(
            bounded_error
        )
        action = recovery_action(failure, RecoveryContext.RUNTIME)
        accepted = _record_failed_partition_attempt(
            health_store,
            partition,
            check_sequence,
            consecutive_failures,
            failure.phase,
            failure.category,
            failure=failure_label,
            generation=generation,
        )
        if accepted:
            log.error(
                "Check failed (unexpected exception)",
                extra={
                    "consecutive_failures": consecutive_failures[partition],
                    "check_sequence": check_sequence,
                    "partition": partition,
                    **failure_fields,
                },
            )
        if accepted and action is RecoveryAction.TERMINATE_PROCESS:
            _request_fatal_shutdown()


def _update_failure_metrics(
    partition: int,
    check_sequence: int,
    consecutive_failures: dict[int, int],
    phase: str,
    category: str,
    *_legacy_cardinality_args,
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
        Error category, normalized to the bounded metric vocabulary.
    Additional positional arguments are accepted for compatibility with older
    in-process callers; cardinality is no longer threshold-dependent.
    """
    consecutive_failures[partition] += 1

    metrics.CHECKS_TOTAL.labels(
        host=metrics.HOST, result="failure"
    ).inc()
    bounded_phase = metrics.bounded_label(phase, metrics.FAILURE_PHASE_VALUES)
    bounded_category = metrics.bounded_label(
        category,
        metrics.FAILURE_CATEGORY_VALUES,
        aliases={"BROKER": "BROKER_SERVICE"},
    )
    metrics.FAILURES_TOTAL.labels(
        host=metrics.HOST,
        phase=bounded_phase,
        category=bounded_category,
        recoverability="TRANSIENT",
    ).inc()
    metrics.record_partition_check(
        partition, "failure", consecutive_failures[partition]
    )
    metrics.CHECK_SEQUENCE.set(check_sequence)


def validate_ssl_connectivity(validation_operation) -> None:
    """Run retained-client validation through the legacy patch seam.

    The argument is an operation bound to an already constructed long-lived
    client.  Keeping this seam lets lifecycle tests intercept startup network
    work without restoring the removed config-based duplicate-client validator.
    """
    if not callable(validation_operation):
        raise TypeError(
            "validate_ssl_connectivity requires a retained-client operation"
        )
    validation_operation()


def validate_kafka_startup_client(admin: AdminClient) -> None:
    """Validate connectivity through the retained administrative client."""

    def validate() -> None:
        metadata = admin.list_topics(timeout=15)
        log.info(
            "Kafka startup validation succeeded",
            extra={"brokers": len(metadata.brokers)},
        )

    validate_ssl_connectivity(validate)


def validate_schema_registry_startup_client(
    sr_client: SchemaRegistryClient,
) -> None:
    """Validate connectivity through the retained Schema Registry client."""

    def validate() -> None:
        check_sr(sr_client)
        log.info("Schema Registry startup validation succeeded")

    validate_ssl_connectivity(validate)


def _shutdown_http_service(owner) -> None:
    """Make probes unavailable, then bound all HTTP resource cleanup."""
    service = owner.pop("service", None)
    if service is None:
        return
    owner["health_store"].begin_shutdown()
    deadline = _shutdown_deadline()
    if deadline is None:
        deadline = time.monotonic() + _shutdown_timeout_seconds
    service.shutdown(deadline)


def _run_lifecycle() -> None:
    """Own HTTP cleanup across both startup and runtime failures."""
    http_owner = {}
    try:
        _run_lifecycle_owned(http_owner)
    finally:
        _shutdown_http_service(http_owner)


def _run_lifecycle_owned(http_owner) -> None:
    """
    Main entry point: load configuration, initialise all clients, then run the
    check loop until a shutdown signal is received.

    Startup sequence
    ----------------
    1. Load config.ini and extract per-section settings.
    2. Configure structured logging based on environment.
    3. Start the Prometheus HTTP metrics server.
    4. Ensure the canary topic exists (create if missing).
    5. Create the Schema Registry client, producer, and serializers.
    6. Run an initial partition sync to get the current partition count.
    7. Create the worker-owned Consumer pool.
    8. Optionally attach the Kafka log handler after startup completes.
    9. Enter the main loop (canary checks, partition sync, SR checks).

    Shutdown sequence (finally block)
    -----------------------------------
    • All per-partition consumers are closed so each broker removes them from
      the group immediately rather than waiting for the session timeout.
    • ThreadPoolExecutor is shut down.
    • kafka_log_handler.close() flushes any buffered log records to Kafka.
    """
    global _shutdown_timeout_seconds

    config = _execute_startup_operation(
        lambda: load_config(CONFIG_FILE),
        lambda exc: _startup_failure(
            exc,
            phase=Phase.STARTUP,
            component=FailureComponent.INTERNAL_STATE,
        ),
    )
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
    sr_check_timeout    = float(app.get("sr.check.timeout.seconds",             "10"))
    _shutdown_timeout_seconds = float(
        app.get("http.shutdown.timeout.seconds", "10")
    )
    http_socket_timeout = float(app.get("http.socket.timeout.seconds", "5"))
    metrics_port        = int(app.get("metrics.port",                          "8000"))
    metrics_bind_addr   = app.get("metrics.bind.address",                "127.0.0.1")
    metrics_ssl_enabled = app.get("metrics.ssl.enabled", "false").lower() == "true"
    metrics_ssl_cert    = app.get("metrics.ssl.cert")
    metrics_ssl_key     = app.get("metrics.ssl.key")
    http_max_workers    = int(app.get("http.max.workers", "4"))
    http_request_queue_size = int(app.get("http.request.queue.size", "16"))
    log_topic_enabled   = app.get("log.topic.enabled", "false").lower() == "true"
    log_topic           = app.get("log.topic",                    "cloud-canary-logs")
    log_topic_retention_ms = int(app.get("log.topic.retention.ms",        "604800000"))
    warmup_checks          = int(app.get("warmup.checks",                         "2"))
    max_workers            = int(app.get("max.workers",                str(const.MAX_WORKERS)))
    kafka_degraded_after   = float(app.get("health.kafka.degraded.after.seconds", "60"))
    liveness_scheduler_max_staleness = float(
        app.get("liveness.scheduler.max.staleness.seconds", "10")
    )
    health_store = HealthStateStore(
        window_checks=int(app.get("health.failure.window.checks", "20")),
        minimum_checks=int(app.get("health.failure.minimum.checks", "4")),
        failure_threshold=float(app.get("health.failure.threshold", "0.5")),
        max_diagnostic_components=int(app.get("health.max.diagnostic.components", "20")),
        kafka_check_interval=interval,
        kafka_degraded_after=kafka_degraded_after,
        kafka_unhealthy_after=float(app.get("health.kafka.unhealthy.after.seconds", "300")),
        sr_check_interval=sr_check_interval,
        sr_degraded_after=float(app.get("health.sr.degraded.after.seconds", "120")),
        sr_unhealthy_after=float(app.get("health.sr.unhealthy.after.seconds", "300")),
        warmup_checks=warmup_checks,
        liveness_scheduler_max_staleness=liveness_scheduler_max_staleness,
        invariant_failure_callback=_request_fatal_shutdown,
    )
    http_owner["health_store"] = health_store
    configure_health_state(health_store)

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
            "sr_check_timeout_seconds": sr_check_timeout,
            "consumer_timeout_seconds": timeout,
            "metrics_port": metrics_port,
            "warmup_checks": warmup_checks,
            "max_workers": max_workers,
            "log_format": log_format,
        }
    )

    # Start the Prometheus metrics HTTP(S) server (background daemon thread).
    # The /metrics endpoint is available immediately, returning zeros for
    # counters/histograms that haven't been updated yet.
    try:
        http_owner["service"] = start_metrics_server(
            port=metrics_port,
            addr=metrics_bind_addr,
            ssl_enabled=metrics_ssl_enabled,
            ssl_cert=metrics_ssl_cert,
            ssl_key=metrics_ssl_key,
            max_workers=http_max_workers,
            request_queue_size=http_request_queue_size,
            socket_timeout=http_socket_timeout,
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
    def classify_kafka_startup(exc):
        return _startup_failure(
            exc,
            phase=Phase.METADATA_FETCH,
            component=FailureComponent.KAFKA_PARTITION,
        )
    admin = _execute_startup_operation(
        lambda: _run_startup_dependency(lambda: AdminClient(kafka_config)),
        classify_kafka_startup,
    )
    _execute_startup_operation(
        lambda: _run_startup_dependency(
            lambda: validate_kafka_startup_client(admin)
        ),
        classify_kafka_startup,
    )

    # ------------------------------------------------------------------
    # Startup: ensure the canary topic exists before creating clients.
    # ------------------------------------------------------------------
    try:
        _run_startup_dependency(
            lambda: ensure_topic(kafka_config, topic, admin=admin)
        )
    except RuntimeError as exc:
        health_store.record_fatal_internal("topic_reconciliation")
        log.error("Startup failed during topic creation", extra={"error": str(exc)})
        # Let the daemon lifecycle boundary turn failed reconciliation into a
        # non-zero process exit. Unwinding this frame also releases the only
        # startup resource created so far (the AdminClient).
        raise

    # Prepare the optional Kafka log handler, but do not attach it during
    # startup. Once attached, ordinary logging performs Kafka dependency I/O
    # directly and would bypass the guarded startup lane.
    kafka_log_handler = None
    if log_topic_enabled:
        try:
            _run_startup_dependency(
                lambda: ensure_log_topic(
                    kafka_config,
                    log_topic,
                    log_topic_retention_ms,
                    admin=admin,
                )
            )
            kafka_log_handler = _run_startup_dependency(
                lambda: KafkaLogHandler(
                    kafka_config,
                    log_topic,
                    shutdown_requested=_shutdown_requested,
                    shutdown_deadline=_shutdown_deadline,
                )
            )
            kafka_log_handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            ))
        except _StartupDispatchRejected:
            raise
        except RuntimeError as exc:
            log.error(
                "Failed to set up log topic, continuing without it",
                extra={"error": str(exc)}
            )

    # Create the shared Schema Registry client.  The same instance is used by
    # both the producer's AvroSerializer and the consumer's AvroDeserializer,
    # and by check_sr() for independent SR health checks.
    sr_client_config = dict(sr_config)
    sr_client_config["timeout"] = sr_check_timeout
    def classify_sr_startup(exc):
        return _startup_failure(
            exc,
            phase=Phase.SCHEMA_REGISTRY,
            component=FailureComponent.SCHEMA_REGISTRY,
            schema_registry=True,
        )
    sr_client = _execute_startup_operation(
        lambda: _run_startup_dependency(
            lambda: SchemaRegistryClient(sr_client_config)
        ),
        classify_sr_startup,
    )
    _execute_startup_operation(
        lambda: _run_startup_dependency(
            lambda: validate_schema_registry_startup_client(sr_client)
        ),
        classify_sr_startup,
    )

    # Resource placeholders make startup reconciliation and construction failures
    # use the same bounded cleanup path as control-loop failures.
    producer = None
    consumers = {}
    executor = None

    # Create the long-lived producer and serializer.  Thread-safe in librdkafka —
    # shared across all per-partition check threads.
    producer, avro_serializer = _execute_startup_operation(
        lambda: _run_startup_dependency(
            lambda: create_producer(kafka_config, sr_client)
        ),
        classify_kafka_startup,
    )

    # ------------------------------------------------------------------
    # Initial partition sync determines how many workers and consumers to
    # create. Reuse the AdminClient created earlier.
    # ------------------------------------------------------------------
    log.info("Running initial partition sync")
    try:
        result = _run_startup_dependency(
            lambda: sync_topic_partitions(kafka_config, topic, admin=admin)
        )
        if not result:
            raise RuntimeError("partition reconciliation returned no verified state")
    except Exception as exc:
        health_store.record_fatal_internal("topic_reconciliation")
        _log_reconciliation_failure("initial_reconciliation", exc)
        _cleanup_runtime_resources(executor, consumers, producer)
        producer = None
        admin = None
        _close_kafka_log_handler(kafka_log_handler)
        raise
    num_brokers, num_partitions = result
    metrics.BROKER_COUNT.set(num_brokers)
    metrics.TOPIC_PARTITION_COUNT.set(num_partitions)
    partitions = list(range(num_partitions))
    health_store.replace_expected_partitions(
        partitions, scheduler_oldest_overdue=0.0
    )
    metrics.reconcile_partition_metrics(partitions, reset=True)
    # Create one exclusively owned consumer per active worker. The pool
    # manually reassigns each consumer before every partition check.
    try:
        consumers = _execute_startup_operation(
            lambda: _run_startup_dependency(
                lambda: _build_consumer_pool(
                    kafka_config, sr_client, topic, partitions, max_workers
                )
            ),
            classify_kafka_startup,
        )
        # Retaining one executor with the configured upper bound allows safe
        # additive scale-up while _submit_due_checks keeps active work at
        # min(partitions, max_workers).
        executor = DaemonThreadPoolExecutor(max_workers=max_workers)
    except Exception as exc:
        health_store.record_fatal_internal("internal_runtime_build")
        _log_reconciliation_failure("initial_runtime_build", exc)
        _cleanup_runtime_resources(executor, consumers, producer)
        producer = None
        admin = None
        _close_kafka_log_handler(kafka_log_handler)
        raise
    log.info(
        "Worker-owned consumers ready",
        extra={
            "num_brokers": num_brokers,
            "num_partitions": num_partitions,
            "worker_count": len(consumers),
            "partitions": partitions,
        }
    )

    # Startup dependency construction is now complete. Attach Kafka-backed
    # logging only at this boundary so no startup log record can perform
    # unguarded Kafka I/O.
    if kafka_log_handler is not None:
        if not _register_kafka_log_handler(kafka_log_handler):
            kafka_log_handler = None
        else:
            log.info(
                "Log topic enabled",
                extra={
                    "log_topic": log_topic,
                    "retention_ms": log_topic_retention_ms,
                },
            )

    consecutive_failures = {p: 0 for p in partitions}
    scheduler = PartitionScheduler(partitions, interval)
    health_store.record_scheduler_capacity(
        scheduler.snapshot().oldest_overdue_seconds
    )
    active_futures = {}
    check_sequence       = 0
    start_time           = time.monotonic()
    last_health_metrics_update = 0.0
    sr_lane = ScheduledOperationLane(
        sr_check_interval,
        timeout=sr_check_timeout,
        thread_name_prefix="cloud-canary-sr",
    )
    reconciliation_lane = ScheduledOperationLane(
        sync_interval,
        initial_delay=sync_interval,
        thread_name_prefix="cloud-canary-reconciliation",
    )
    recreation_requested = threading.Event()
    recreation_authorized = threading.Event()
    recreation_cancelled = threading.Event()

    def request_recreation_exclusivity() -> None:
        """Ask the control-loop owner to drain Kafka work before deletion."""
        health_store.begin_readiness_transition("topic_recreation")
        recreation_requested.set()
        while not recreation_authorized.wait(0.1):
            if recreation_cancelled.is_set() or _shutdown_requested():
                raise RuntimeError("topic recreation interrupted before authorization")
        if recreation_cancelled.is_set():
            raise RuntimeError("topic recreation interrupted before authorization")

    def reconcile_topic():
        """Reconcile topology and prepare additive capacity on its own lane."""
        result = sync_topic_partitions(
            kafka_config,
            topic,
            admin=admin,
            before_recreate=request_recreation_exclusivity,
        )
        if (
            result
            and not result.recreated
            and result.partition_count != len(partitions)
        ):
            health_store.begin_readiness_transition("worker_rebuild")
            consumers.grow(min(result.partition_count, max_workers))
        return result

    try:
        while not _shutdown_requested():
            # Update uptime gauge at the top of every iteration so it reflects
            # elapsed time even if a check takes a long time.
            metrics.UPTIME_SECONDS.set(time.monotonic() - start_time)

            # ------------------------------------------------------------------
            # Partition sync — detect broker count changes (scale-up/down)
            # Runs on its own cadence, independently of canary checks.
            # Uses incremental updates: only add/remove changed consumers instead
            # of rebuilding the entire pool, preventing monitoring gaps.
            # Reuses the long-lived AdminClient to avoid connection overhead.
            # ------------------------------------------------------------------
            result = None
            _dispatch_scheduled_operation(reconciliation_lane, reconcile_topic)
            reconciliation_future = reconciliation_lane.take_completed()
            if reconciliation_future is not None:
                try:
                    result = reconciliation_future.result()
                except Exception as exc:
                    health_store.record_fatal_internal("topic_reconciliation")
                    _log_reconciliation_failure("periodic_reconciliation", exc)
                    raise
            if result:
                if result.recreated:
                    health_store.begin_readiness_transition("worker_rebuild")
                    try:
                        (
                            consumers,
                            executor,
                            partitions,
                            consecutive_failures,
                        ) = _run_replacement_runtime_transaction(
                            lambda: _replace_recreated_runtime(
                                kafka_config,
                                sr_client,
                                topic,
                                partitions,
                                consumers,
                                result.partition_count,
                                max_workers,
                                health_store,
                                scheduler=scheduler,
                            )
                        )
                    except _ReplacementDispatchRejected:
                        # Shutdown owns the lifecycle now. Leave the old
                        # generation for the centralized cleanup path without
                        # constructing any replacement Kafka clients.
                        break
                    except Exception as exc:
                        health_store.record_fatal_internal("topic_reconciliation")
                        _log_reconciliation_failure(
                            "replacement_runtime_build", exc
                        )
                        raise
                    num_partitions = result.partition_count
                    health_store.end_readiness_transition()
                    recreation_requested.clear()
                    recreation_authorized.clear()
                elif result.partition_count != len(partitions):
                    log.info(
                        "Partition count changed, updating consumer pool incrementally",
                        extra={
                            "old_partition_count": len(partitions),
                            "new_partition_count": result.partition_count,
                        }
                    )
                    partitions = list(range(result.partition_count))
                    consecutive_failures.update(
                        (partition, 0)
                        for partition in partitions
                        if partition not in consecutive_failures
                    )
                    scheduler.reconcile_partitions(partitions)
                    scheduler_snapshot = scheduler.snapshot()
                    health_store.replace_expected_partitions(
                        partitions,
                        scheduler_oldest_overdue=(
                            scheduler_snapshot.oldest_overdue_seconds
                        ),
                    )
                    metrics.reconcile_partition_metrics(partitions)
                    health_store.end_readiness_transition()

                    log.info(
                        "Consumer pool updated",
                        extra={
                            "num_partitions": result.partition_count,
                            "partitions": partitions,
                            "worker_count": len(consumers),
                        }
                    )
                    num_partitions = result.partition_count
                metrics.BROKER_COUNT.set(result.broker_count)
                metrics.TOPIC_PARTITION_COUNT.set(result.partition_count)

            # ------------------------------------------------------------------
            # Schema Registry health check — independent cadence and signal
            # SR availability is not directly exercised by the canary check
            # (the schema is cached after first use), so it must be probed
            # separately to surface SR outages as a distinct metric.
            # ------------------------------------------------------------------
            _dispatch_scheduled_operation(
                sr_lane, lambda: _timed_sr_probe(sr_client)
            )
            sr_future = sr_lane.take_completed()
            if sr_future is not None:
                try:
                    sr_duration_ms, sr_error = sr_future.result()
                except OperationDeadlineExceeded as exc:
                    sr_duration_ms = exc.timeout * 1000
                    sr_error = CanaryError(FailureDescriptor(
                        component=FailureComponent.SCHEMA_REGISTRY,
                        phase=Phase.SCHEMA_REGISTRY,
                        category=ErrorCategory.NETWORK,
                        recoverability=Recoverability.TRANSIENT,
                        code="CANARY.SR_PROBE_TIMEOUT",
                        safe_summary=FailureSummary.OPERATION_TIMED_OUT,
                    ))
                _record_schema_registry_completion(
                    health_store, sr_duration_ms, sr_error
                )

            # ------------------------------------------------------------------
            # Canary check — the core measurement
            # Select directly from the scheduler's single record per partition.
            # No executor-side ready queue is built: at most max.workers oldest
            # due checks are submitted, with deterministic partition tie breaks.
            # ------------------------------------------------------------------
            worker_limit = min(len(partitions), max_workers)
            next_sequence = check_sequence + 1
            due_partitions = ()
            if not recreation_requested.is_set():
                dispatch_generation = health_store.topic_generation
                due_partitions = _submit_due_checks(
                    scheduler,
                    executor,
                    active_futures,
                    worker_limit,
                    next_sequence,
                    lambda p, sequence=next_sequence: consumers.run(
                        p,
                        lambda consumer, deserializer: check_kafka(
                            producer,
                            avro_serializer,
                            consumer,
                            deserializer,
                            topic,
                            timeout,
                            sequence,
                            p,
                        ),
                    ),
                    dispatch_generation,
                )
            if due_partitions:
                check_sequence = next_sequence
                _publish_scheduler_observability(scheduler, health_store)

            if active_futures:
                # Bound the wait so overdue ages continue to advance in metrics
                # and /health while every worker remains occupied.
                delay = SCHEDULER_HEARTBEAT_INTERVAL_SECONDS
                sr_deadline_delay = sr_lane.seconds_until_deadline()
                if sr_deadline_delay is not None:
                    delay = min(delay, sr_deadline_delay)
                for lane in (sr_lane, reconciliation_lane):
                    lane_due_delay = lane.seconds_until_next_due()
                    if lane_due_delay is not None:
                        delay = min(delay, lane_due_delay)
                if len(active_futures) < worker_limit:
                    next_due_delay = scheduler.seconds_until_next_due()
                    if next_due_delay is not None:
                        delay = min(delay, next_due_delay)
                completed, _ = wait(
                    tuple(active_futures),
                    timeout=max(0.0, delay),
                    return_when=FIRST_COMPLETED,
                )
            else:
                completed = set()

            for future in sorted(
                completed, key=lambda item: active_futures[item][0]
            ):
                p, completed_sequence, completed_generation = active_futures.pop(future)
                try:
                    _record_kafka_completion(
                        health_store,
                        p,
                        completed_sequence,
                        completed_generation,
                        consecutive_failures,
                        future,
                    )
                finally:
                    scheduler.complete(p)

                # Refill the freed worker immediately, even if another check
                # from the previous dispatch remains blocked.
                next_sequence = check_sequence + 1
                refilled = ()
                if not recreation_requested.is_set():
                    dispatch_generation = health_store.topic_generation
                    refilled = _submit_due_checks(
                        scheduler,
                        executor,
                        active_futures,
                        worker_limit,
                        next_sequence,
                        lambda partition, sequence=next_sequence: consumers.run(
                            partition,
                            lambda consumer, deserializer: check_kafka(
                                producer,
                                avro_serializer,
                                consumer,
                                deserializer,
                                topic,
                                timeout,
                                sequence,
                                partition,
                            ),
                        ),
                        dispatch_generation,
                    )
                if refilled:
                    check_sequence = next_sequence
                _publish_scheduler_observability(scheduler, health_store)

            # Destructive reconciliation is authorized only by the owner of
            # Kafka dispatch, after every tracked future has been retired from
            # both active_futures and PartitionScheduler.  The reconciliation
            # worker cannot race submission or replace scheduler state early.
            if (
                recreation_requested.is_set()
                and not recreation_authorized.is_set()
                and not active_futures
            ):
                executor.shutdown(wait=True)
                recreation_authorized.set()

            if not completed:
                _publish_scheduler_observability(scheduler, health_store)
                if not active_futures:
                    bounded_delay = SCHEDULER_HEARTBEAT_INTERVAL_SECONDS
                    if not recreation_requested.is_set():
                        delay = scheduler.seconds_until_next_due()
                        if delay is not None:
                            bounded_delay = min(bounded_delay, delay)
                    for lane in (sr_lane, reconciliation_lane):
                        lane_due_delay = lane.seconds_until_next_due()
                        if lane_due_delay is not None:
                            bounded_delay = min(bounded_delay, lane_due_delay)
                    if bounded_delay > 0:
                        sr_deadline_delay = sr_lane.seconds_until_deadline()
                        if sr_deadline_delay is not None:
                            bounded_delay = min(
                                bounded_delay, sr_deadline_delay
                            )
                        log.debug(
                            "Sleeping until next scheduler event",
                            extra={"delay_seconds": bounded_delay},
                        )
                        _shutdown_event.wait(bounded_delay)

            # Full partition-health aggregation is topology-linear, so keep it
            # on a bounded cadence rather than repeating it per completion.
            health_metrics_now = time.monotonic()
            if health_metrics_now - last_health_metrics_update >= 1.0:
                metrics.update_health_metrics(
                    health_store.snapshot(), consecutive_failures
                )
                last_health_metrics_update = health_metrics_now

    except Exception:
        # Runtime failures are terminal internal failures unless a more
        # specific first-write-wins classification was already published.
        health_store.record_fatal_internal("internal_runtime")
        raise
    finally:
        # Signal publication remains lock-free; the lifecycle owner makes
        # shutdown visible before any dependency cleanup can block.
        _publish_shutdown_state(health_store)
        _shutdown_http_service(http_owner)
        # Release a reconciliation callback waiting for authorization without
        # allowing it to begin deletion during exceptional shutdown.
        recreation_cancelled.set()
        recreation_authorized.set()
        sr_lane.close()
        reconciliation_lane.close()
        _cleanup_runtime_resources(executor, consumers, producer)

        # Release the AdminClient to close connections immediately.
        admin = None

        log.info("cloud_canary stopped")

        # Flush and detach the Kafka log handler so the last few log records
        # (including "cloud_canary stopped") reach the log topic before exit.
        _close_kafka_log_handler(kafka_log_handler)


def _run_behind_daemon_boundary(
    lifecycle,
    *,
    monotonic_clock=time.monotonic,
    thread_factory=threading.Thread,
    event_factory=threading.Event,
) -> None:
    """Run startup, runtime, and cleanup behind one abandonable boundary."""
    completed = event_factory()
    outcome = []

    def invoke() -> None:
        try:
            lifecycle()
        except BaseException as exc:
            outcome.append(exc)
        finally:
            completed.set()

    lifecycle_thread = thread_factory(
        target=invoke,
        name="cloud-canary-lifecycle",
        daemon=True,
    )
    lifecycle_thread.start()

    while not completed.is_set():
        request = _active_shutdown_request()
        if request is not None:
            remaining = max(0.0, request.deadline - monotonic_clock())
            if remaining == 0.0:
                if _fatal_shutdown_request is not None:
                    raise SystemExit(1)
                return
            completed.wait(min(0.05, remaining))
        elif not _shutdown_request_claim:
            # Publication has been claimed but its monotonic start has not yet
            # been stored. Do not invent a fresh cleanup window.
            return
        else:
            completed.wait(0.05)

    if _fatal_shutdown_request is not None:
        raise SystemExit(1)
    if outcome:
        raise outcome[0]


def run() -> None:
    """Own signals while the complete application lifecycle runs as a daemon."""
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    _run_behind_daemon_boundary(_run_lifecycle)


if __name__ == "__main__":
    run()
