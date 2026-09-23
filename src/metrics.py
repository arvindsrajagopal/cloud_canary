# ---------------------------------------------------------------------------
# metrics.py — Prometheus metric definitions for the cloud canary
#
# All metrics carry a `host` label set to socket.gethostname() at startup.
# This makes every time series unambiguously tied to the specific canary
# instance that produced it, which is essential when multiple instances run
# concurrently (e.g. one per region or one per host).
#
# Without the host label, Prometheus would only be able to distinguish
# instances via the scrape-target `instance` label it adds automatically —
# which requires Prometheus to be correctly configured for every new instance.
# Embedding the hostname in the metric itself is self-describing and works
# regardless of how the scrape config is set up.
#
# Label strategy
# --------------
# Aggregate metrics never carry a `partition` label.  Their variable labels
# are closed enums so exception text and other unbounded values cannot create
# new time series.
#
# Cluster-level and process-level metrics (broker count, uptime, etc.) carry
#     only `host`.  These are pre-labeled at module load time using HOST.
#     Call sites use the module-level name directly:
#         metrics.UPTIME_SECONDS.set(elapsed)
#
# Metrics WITH other variable labels (result, phase, category) require the
#     caller to include all label dimensions in every .labels() call.
#     HOST is exported as a constant for this purpose.
#
# All metrics are module-level singletons registered with the default
# CollectorRegistry on import.  Importing this module twice (which Python
# prevents via the module cache) is safe.
#
# Metrics are served at http://0.0.0.0:{metrics.port}/metrics by
# start_metrics_server(), which starts a background HTTP thread.
#
# Histogram bucket rationale
# --------------------------
# Buckets are tuned to the expected latency range for Confluent Cloud:
#   E2E latency:      10 ms (excellent) → 10 s (degraded, near timeout)
#   Seek duration:    1 ms  (fast local) → 500 ms (slow broker)
#   Produce duration: 5 ms  (fast write) → 2.5 s  (degraded ack latency)
# ---------------------------------------------------------------------------

import logging
import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

log = logging.getLogger(__name__)

# Hostname of this canary instance.  Embedded as a constant label on every
# metric so that dashboards and alerts can filter or group by instance without
# relying on Prometheus scrape-target configuration.
# Can be overridden via the CANARY_INSTANCE_ID environment variable for
# multi-instance deployments on the same host.
HOST = os.getenv("CANARY_INSTANCE_ID") or socket.gethostname()

RESULT_VALUES = frozenset({"success", "failure"})
STATE_VALUES = frozenset({"healthy", "degraded", "unhealthy"})
FAILURE_PHASE_VALUES = frozenset({
    "ASSIGNMENT", "SEEK", "PRODUCE", "CONSUME", "SCHEMA_REGISTRY",
    "METADATA_FETCH", "TOPIC_CREATE", "TOPIC_EXPAND", "TOPIC_DELETE",
    "TOPIC_VERIFY", "CONSUMER_CREATE", "CONSUMER_REPLACE", "SCHEDULER",
    "STATE_UPDATE", "HTTP_REQUEST", "STARTUP", "SHUTDOWN", "UNKNOWN",
})
FAILURE_CATEGORY_VALUES = frozenset({
    "NETWORK", "BROKER_SERVICE", "AUTHENTICATION", "AUTHORIZATION",
    "TLS_CERTIFICATE", "CONFIGURATION", "SERIALIZATION", "CLIENT_STATE",
    "CAPACITY", "INTERNAL", "UNKNOWN",
})
RECOVERABILITY_VALUES = frozenset({
    "TRANSIENT", "DETERMINISTIC", "INTERNAL_FATAL", "UNKNOWN",
})


def bounded_label(value, allowed: frozenset[str], aliases=None) -> str:
    """Return a closed-enum metric label, mapping unclassified input safely."""
    candidate = getattr(value, "value", value)
    candidate = str(candidate)
    if aliases:
        candidate = aliases.get(candidate, candidate)
    if candidate in allowed:
        return candidate
    if "UNKNOWN" in allowed:
        return "UNKNOWN"
    raise ValueError(f"unsupported metric label value: {candidate!r}")


# ---------------------------------------------------------------------------
# Aggregate latency histograms — labeled only by host at call time.
# ---------------------------------------------------------------------------

E2E_LATENCY = Histogram(
    "canary_e2e_latency_ms",
    "End-to-end latency per check (produce timestamp → consumer receive timestamp) in ms, "
    "aggregated across all partitions per host. Partition label removed to prevent cardinality "
    "explosion on large clusters.",
    ["host"],
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

SEEK_DURATION = Histogram(
    "canary_seek_duration_ms",
    "Time to fetch watermark offsets from the broker and seek the assigned partition in ms, "
    "aggregated across all partitions per host. Partition label removed to prevent cardinality "
    "explosion on large clusters.",
    ["host"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

PRODUCE_DURATION = Histogram(
    "canary_produce_duration_ms",
    "Time from produce() call to delivery callback in ms, aggregated across all "
    "partitions per host. With acks=all this measures the time for all in-sync replicas to "
    "acknowledge the write. Partition label removed to prevent cardinality explosion.",
    ["host"],
    buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500],
)

# ---------------------------------------------------------------------------
# Aggregate reliability counters. These deliberately omit partition identity;
# partition-specific signals use the separately named per-partition metrics.
# ---------------------------------------------------------------------------

CHECKS_TOTAL = Counter(
    "canary_checks_total",
    "Total canary check attempts, labelled by bounded result and host.",
    ["host", "result"],
    # result values: "success" | "failure"
)

FAILURES_TOTAL = Counter(
    "canary_failures_total",
    "Failed canary checks, labelled by the phase where the error occurred, its category, "
    "recoverability, and host. Labels use closed enums and never exception text.",
    ["host", "phase", "category", "recoverability"],
)

# Aggregate health gauges. State children are maintained only for the three
# bounded states; the remaining gauges have no variable label beyond host.
PARTITIONS_BY_STATE = Gauge(
    "canary_partitions_by_state",
    "Current number of expected Kafka partitions in each bounded health state.",
    ["host", "state"],
)

WORST_PARTITION_STALENESS_SECONDS = Gauge(
    "canary_worst_partition_staleness_seconds",
    "Greatest seconds since success among observed expected Kafka partitions.",
    ["host"],
)

MAX_CONSECUTIVE_FAILURES = Gauge(
    "canary_max_consecutive_failures",
    "Greatest current consecutive failure streak among expected Kafka partitions.",
    ["host"],
)

OLDEST_PARTITION_OVERDUE_SECONDS = Gauge(
    "canary_oldest_partition_overdue_seconds",
    "Seconds the oldest due Kafka partition check is overdue.",
    ["host"],
)

SCHEDULER_PENDING_CHECKS = Gauge(
    "canary_scheduler_pending_checks",
    "Number of due Kafka partition checks waiting for worker capacity.",
    ["host"],
)

SCHEDULER_IN_FLIGHT_CHECKS = Gauge(
    "canary_scheduler_in_flight_checks",
    "Number of Kafka partition checks currently executing.",
    ["host"],
)

PARTITION_COVERAGE_DURATION_SECONDS = Gauge(
    "canary_partition_coverage_duration_seconds",
    "Effective seconds covered by the current partition scheduling records.",
    ["host"],
)

# Materialize the complete bounded state vocabulary and scalar aggregate
# series even before the first check completes.
for _state in STATE_VALUES:
    PARTITIONS_BY_STATE.labels(host=HOST, state=_state).set(0)
WORST_PARTITION_STALENESS_SECONDS.labels(host=HOST).set(0)
MAX_CONSECUTIVE_FAILURES.labels(host=HOST).set(0)
OLDEST_PARTITION_OVERDUE_SECONDS.labels(host=HOST).set(0)
SCHEDULER_PENDING_CHECKS.labels(host=HOST).set(0)
SCHEDULER_IN_FLIGHT_CHECKS.labels(host=HOST).set(0)
PARTITION_COVERAGE_DURATION_SECONDS.labels(host=HOST).set(0)


def update_scheduler_metrics(snapshot) -> None:
    """Publish one immutable scheduler snapshot without partition labels."""
    SCHEDULER_PENDING_CHECKS.labels(host=HOST).set(snapshot.pending)
    SCHEDULER_IN_FLIGHT_CHECKS.labels(host=HOST).set(snapshot.in_flight)
    OLDEST_PARTITION_OVERDUE_SECONDS.labels(host=HOST).set(
        snapshot.oldest_overdue_seconds
    )
    PARTITION_COVERAGE_DURATION_SECONDS.labels(host=HOST).set(
        snapshot.coverage_duration_seconds
    )


def update_health_metrics(snapshot, consecutive_failures) -> None:
    """Publish aggregate partition health from one immutable state snapshot."""
    counts = {state: 0 for state in STATE_VALUES}
    staleness = []
    expected_partitions = set()
    for index, component in enumerate(snapshot.partitions):
        state = bounded_label(component.status, STATE_VALUES)
        counts[state] += 1
        component_name = getattr(component, "component", f"kafka:{index}")
        partition = component_name.removeprefix("kafka:")
        expected_partitions.add(partition)
        with _partition_metric_lock:
            _set_partition_state(partition, state)
        if component.staleness_seconds is not None:
            staleness.append(max(0.0, component.staleness_seconds))

    reconcile_partition_metrics(expected_partitions)

    for state, count in counts.items():
        PARTITIONS_BY_STATE.labels(host=HOST, state=state).set(count)
    WORST_PARTITION_STALENESS_SECONDS.labels(host=HOST).set(
        max(staleness, default=0.0)
    )
    MAX_CONSECUTIVE_FAILURES.labels(host=HOST).set(
        max(consecutive_failures.values(), default=0)
    )


# ---------------------------------------------------------------------------
# Stable per-partition metrics. These are the only metric families that carry
# a partition label; their other labels are closed enums.
# ---------------------------------------------------------------------------

PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS = Gauge(
    "canary_partition_last_success_timestamp_seconds",
    "Unix timestamp of the last successful check for an expected partition.",
    ["host", "partition"],
)

PARTITION_CONSECUTIVE_FAILURES = Gauge(
    "canary_partition_consecutive_failures",
    "Current streak of consecutive check failures without an intervening success, "
    "per host and partition.",
    ["host", "partition"],
)

PARTITION_CHECKS_TOTAL = Counter(
    "canary_partition_checks_total",
    "Check attempts for an expected partition, labelled by bounded result.",
    ["host", "partition", "result"],
)

PARTITION_CURRENT_STATE = Gauge(
    "canary_partition_current_state",
    "Current bounded health state for an expected partition; exactly one state is present.",
    ["host", "partition", "state"],
)

_partition_metric_lock = threading.RLock()
_known_partitions: set[str] = set()
_partition_states: dict[str, str] = {}
_partition_results: dict[str, set[str]] = {}
_partitions_with_success: set[str] = set()


def record_partition_check(
    partition: int,
    result: str,
    consecutive_failures: int,
    last_success: float | None = None,
) -> None:
    """Update stable per-partition check metrics using bounded labels."""
    result = bounded_label(result, RESULT_VALUES)
    partition_label = str(partition)
    with _partition_metric_lock:
        _known_partitions.add(partition_label)
        PARTITION_CHECKS_TOTAL.labels(
            host=HOST, partition=partition_label, result=result
        ).inc()
        _partition_results.setdefault(partition_label, set()).add(result)
        PARTITION_CONSECUTIVE_FAILURES.labels(
            host=HOST, partition=partition_label
        ).set(consecutive_failures)
        if last_success is not None:
            PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS.labels(
                host=HOST, partition=partition_label
            ).set(last_success)
            _partitions_with_success.add(partition_label)


def _set_partition_state(partition: str, state: str) -> None:
    """Replace, rather than retain, a partition's previous state child."""
    previous = _partition_states.get(partition)
    if previous is not None and previous != state:
        PARTITION_CURRENT_STATE.remove(HOST, partition, previous)
    PARTITION_CURRENT_STATE.labels(host=HOST, partition=partition, state=state).set(1)
    _partition_states[partition] = state
    _known_partitions.add(partition)


def reconcile_partition_metrics(expected_partitions, *, reset: bool = False) -> None:
    """Remove children for deleted partitions or a replaced topic generation."""
    expected = {str(partition) for partition in expected_partitions}
    with _partition_metric_lock:
        removed = set(_known_partitions) if reset else _known_partitions - expected
        for partition in removed:
            if partition in _partitions_with_success:
                PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS.remove(HOST, partition)
            PARTITION_CONSECUTIVE_FAILURES.remove(HOST, partition)
            for result in _partition_results.get(partition, ()):
                PARTITION_CHECKS_TOTAL.remove(HOST, partition, result)
            state = _partition_states.pop(partition, None)
            if state is not None:
                PARTITION_CURRENT_STATE.remove(HOST, partition, state)
            _partition_results.pop(partition, None)
            _partitions_with_success.discard(partition)
        _known_partitions.difference_update(removed)


# Compatibility aliases reference the required families without registering
# legacy metric names.
CONSECUTIVE_FAILURES = PARTITION_CONSECUTIVE_FAILURES
LAST_SUCCESS_TIMESTAMP = PARTITION_LAST_SUCCESS_TIMESTAMP_SECONDS

# ---------------------------------------------------------------------------
# Schema Registry metrics — host is a variable label; callers must pass host=HOST
# ---------------------------------------------------------------------------

SR_CHECKS_TOTAL = Counter(
    "canary_sr_checks_total",
    "Schema Registry health check attempts, labelled by result and host. "
    "SR checks run on a separate cadence from canary checks, providing an "
    "independent signal of SR availability per instance.",
    ["result", "host"],
    # result values: "success" | "failure"
)

SR_LATENCY = Histogram(
    "canary_sr_latency_ms",
    "Schema Registry health check response time in ms, per host. "
    "Measures HTTP round-trip time for get_subjects() call. "
    "High latency indicates SR performance degradation or network issues.",
    ["host"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500],
)

# ---------------------------------------------------------------------------
# Cluster state gauges — pre-labeled with HOST (not per-partition)
# ---------------------------------------------------------------------------

_BROKER_COUNT = Gauge(
    "canary_broker_count",
    "Number of brokers observed during the last partition sync, per host. "
    "A change here indicates a cluster scaling event. "
    "Updated every partition.sync.interval.seconds.",
    ["host"],
)
BROKER_COUNT = _BROKER_COUNT.labels(host=HOST)

_TOPIC_PARTITION_COUNT = Gauge(
    "canary_topic_partition_count",
    "Current partition count of the canary check topic after the last sync, per host. "
    "Should match canary_broker_count after a successful sync.",
    ["host"],
)
TOPIC_PARTITION_COUNT = _TOPIC_PARTITION_COUNT.labels(host=HOST)

# ---------------------------------------------------------------------------
# Process gauges — pre-labeled with HOST (not per-partition)
# ---------------------------------------------------------------------------

_UPTIME_SECONDS = Gauge(
    "canary_uptime_seconds",
    "Seconds elapsed since this canary instance started. "
    "A reset to near-zero indicates a process restart on that host.",
    ["host"],
)
UPTIME_SECONDS = _UPTIME_SECONDS.labels(host=HOST)

_CHECK_SEQUENCE = Gauge(
    "canary_check_sequence",
    "Sequence number of the last check cycle for this instance. "
    "Each cycle runs one check per partition concurrently. "
    "Gaps indicate a process restart or paused execution on that host.",
    ["host"],
)
CHECK_SEQUENCE = _CHECK_SEQUENCE.labels(host=HOST)

# ---------------------------------------------------------------------------
# Build info — version metadata (set once at startup)
# ---------------------------------------------------------------------------

VERSION_INFO = Info(
    "canary_version",
    "Cloud canary version information",
)
# Note: VERSION_INFO is populated in main.py after importing __version__


def start_metrics_server(
    port: int,
    addr: str = "0.0.0.0",
    ssl_enabled: bool = False,
    ssl_cert: str = None,
    ssl_key: str = None,
    max_workers: int = 4,
    request_queue_size: int = 16,
) -> None:
    """
    Start the Prometheus HTTP(S) metrics server with health endpoints.

    Spawns a background daemon thread that serves multiple endpoints:
    - /metrics — Prometheus metrics exposition format
    - /live    — Process and scheduler-control-plane liveness
    - /health  — Kafka dependency-health summary (healthy/degraded/unhealthy)
    - /ready   — Readiness probe (ready/not_ready)

    The thread exits automatically when the main process exits.

    Parameters
    ----------
    port : int
        TCP port to listen on (configured via metrics.port in config.ini,
        default 8000). Prometheus scrapes http(s)://<host>:<port>/metrics.
        When running multiple instances on the same host, each must use a
        distinct port.

    addr : str
        Bind address for the HTTP server (default "0.0.0.0" for all interfaces).
        Use "127.0.0.1" to bind to localhost only, or a specific IP address
        to bind to a single interface.

    ssl_enabled : bool
        If True, enables HTTPS with TLS encryption. Requires ssl_cert and
        ssl_key to be provided (default False).

    ssl_cert : str, optional
        Path to SSL certificate file in PEM format. Required if ssl_enabled=True.

    ssl_key : str, optional
        Path to SSL private key file in PEM format. Required if ssl_enabled=True.

    max_workers : int
        Maximum number of concurrently executing HTTP handlers.

    request_queue_size : int
        Maximum number of admitted requests waiting for an HTTP worker.

    Raises
    ------
    ValueError
        If ssl_enabled=True but ssl_cert or ssl_key are not provided.
    FileNotFoundError
        If SSL certificate or key files do not exist.
    """
    import json
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from src.health import (
        get_health_status,
        get_liveness_status,
        get_readiness_status,
    )

    class CanaryHTTPHandler(BaseHTTPRequestHandler):
        """
        HTTP handler that serves metrics and health endpoints.
        """

        def do_GET(self):
            """Handle GET requests for /metrics, /live, /health, and /ready."""

            if self.path == "/metrics":
                # Prometheus metrics endpoint
                try:
                    metrics_output = generate_latest()
                    self.send_response(200)
                    self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                    self.end_headers()
                    self.wfile.write(metrics_output)
                except Exception as e:
                    self.send_error(500, f"Error generating metrics: {e}")

            elif self.path == "/live":
                try:
                    liveness = get_liveness_status()
                    self.send_response(liveness.http_code)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    response = {
                        "status": liveness.status.value,
                        "timestamp": liveness.timestamp,
                        "scheduler_heartbeat_age_seconds": (
                            liveness.scheduler_heartbeat_age_seconds
                        ),
                        "shutdown_started": liveness.shutdown_started,
                        "message": liveness.message,
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())
                except Exception:
                    log.error("Liveness evaluation failed")
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    response = {
                        "status": "not_live",
                        "timestamp": None,
                        "scheduler_heartbeat_age_seconds": None,
                        "shutdown_started": False,
                        "message": "Liveness evaluation failed",
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())

            elif self.path == "/health":
                # Kafka dependency-health endpoint
                try:
                    health = get_health_status()
                    self.send_response(health.http_code)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()

                    response = {
                        "status": health.status.value,
                        "timestamp": health.timestamp,
                        "uptime_seconds": health.uptime_seconds,
                        "message": health.message,
                        "checks": health.checks,
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())
                except Exception:
                    log.error("Health evaluation failed")
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    response = {
                        "status": "unhealthy",
                        "timestamp": None,
                        "uptime_seconds": None,
                        "message": "Health evaluation failed",
                        "checks": {},
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())

            elif self.path == "/ready":
                # Readiness probe endpoint
                try:
                    readiness = get_readiness_status()
                    self.send_response(readiness.http_code)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()

                    response = {
                        "status": readiness.status.value,
                        "timestamp": readiness.timestamp,
                        "message": readiness.message,
                        "checks": readiness.checks,
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())
                except Exception:
                    log.error("Readiness evaluation failed")
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    response = {
                        "status": "not_ready",
                        "timestamp": None,
                        "message": "Readiness evaluation failed",
                        "checks": {},
                    }
                    self.wfile.write(json.dumps(response, indent=2).encode())

            else:
                # Unknown endpoint
                self.send_error(404, f"Endpoint not found: {self.path}")

        def log_message(self, format, *args):
            """Suppress default logging - application uses structured logging"""
            # Only log errors
            if args[1].startswith(("4", "5")):
                log.warning(f"{self.address_string()} - {format % args}")

    def configure_bounded_execution(server):
        """Attach fixed-pool request execution to a concrete HTTP server."""
        server._request_slots = threading.BoundedSemaphore(
            max_workers + request_queue_size
        )
        server._request_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="canary-http",
        )

        def process_request(request, client_address):
            # The listening thread never waits for executor capacity. An
            # accepted request either owns one active/waiting slot or is
            # rejected without entering the executor's internal queue.
            if not server._request_slots.acquire(blocking=False):
                server.shutdown_request(request)
                return

            try:
                server._request_executor.submit(
                    process_admitted_request, request, client_address
                )
            except BaseException:
                server._request_slots.release()
                server.shutdown_request(request)
                raise

        def process_admitted_request(request, client_address):
            try:
                try:
                    server.finish_request(request, client_address)
                except Exception:
                    server.handle_error(request, client_address)
                finally:
                    server.shutdown_request(request)
            finally:
                server._request_slots.release()

        server.process_request = process_request
        return server

    # Validate SSL configuration
    if ssl_enabled:
        if not ssl_cert or not ssl_key:
            raise ValueError(
                "metrics.ssl.cert and metrics.ssl.key must be set when metrics.ssl.enabled=true"
            )

        import os
        import ssl

        if not os.path.exists(ssl_cert):
            raise FileNotFoundError(f"SSL certificate not found: {ssl_cert}")
        if not os.path.exists(ssl_key):
            raise FileNotFoundError(f"SSL private key not found: {ssl_key}")

        # Create HTTPS server with SSL context
        server = configure_bounded_execution(
            HTTPServer((addr, port), CanaryHTTPHandler)
        )
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

        # Set secure defaults for TLS 1.2+ only (disable older protocols)
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

        # Load certificate and key
        try:
            ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
        except ssl.SSLError as exc:
            raise ValueError(f"Invalid SSL certificate or key: {exc}")

        server.socket = ssl_context.wrap_socket(server.socket, server_side=True)

    else:
        # Standard HTTP server (no SSL)
        server = configure_bounded_execution(
            HTTPServer((addr, port), CanaryHTTPHandler)
        )

    # Start server in background thread
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.debug(f"Metrics server started on {addr}:{port} (ssl={ssl_enabled})")
