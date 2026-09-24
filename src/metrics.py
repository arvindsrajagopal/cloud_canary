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
# Metrics are served at http://127.0.0.1:{metrics.port}/metrics by default by
# start_metrics_server(), which starts a background HTTP thread.
#
# Histogram bucket rationale
# --------------------------
# Buckets are tuned to the expected latency range for Confluent Cloud:
#   E2E latency:      10 ms (excellent) → 10 s (degraded, near timeout)
#   Seek duration:    1 ms  (fast local) → 500 ms (slow broker)
#   Produce duration: 5 ms  (fast write) → 2.5 s  (degraded ack latency)
# ---------------------------------------------------------------------------

import ipaddress
import logging
import os
import re
import socket
import threading
import time
from types import SimpleNamespace

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from src.bounded_executor import DaemonThreadPoolExecutor

threading = SimpleNamespace(
    BoundedSemaphore=threading.BoundedSemaphore,
    Event=threading.Event,
    Lock=threading.Lock,
    RLock=threading.RLock,
    Thread=threading.Thread,
)

log = logging.getLogger(__name__)
_http_log = logging.getLogger(f"{__name__}.http")
_http_log.propagate = False
if not _http_log.handlers:
    _http_log.addHandler(logging.StreamHandler())

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

HTTP_OVERLOAD_TOTAL = Counter(
    "canary_http_overload_total",
    "HTTP requests rejected because the bounded worker queue was saturated.",
    ["host"],
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
        component_name = getattr(component, "component", f"kafka:{index}")
        if not component_name.startswith("kafka:"):
            continue
        partition = component_name.removeprefix("kafka:")
        if not partition.isdigit():
            continue
        state = bounded_label(component.status, STATE_VALUES)
        counts[state] += 1
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


class HTTPService:
    """Own the HTTP listener and its independent bounded request workers."""

    def __init__(self, server, listener_thread):
        self._server = server
        self._listener_thread = listener_thread
        self._shutdown_lock = threading.Lock()
        self._stopped = False

    def shutdown(self, deadline: float, *, monotonic_clock=time.monotonic) -> None:
        """Stop admission and release HTTP resources by an absolute deadline."""
        with self._shutdown_lock:
            if self._stopped:
                return
            self._stopped = True

        self._server._accepting.clear()

        listener_shutdown = threading.Thread(
            target=self._server.shutdown,
            name="canary-http-listener-shutdown",
            daemon=True,
        )
        listener_shutdown.start()
        listener_shutdown.join(max(0.0, deadline - monotonic_clock()))

        # Closing the socket is unconditional, even if serve_forever failed to
        # acknowledge shutdown before the deadline.
        self._server.server_close()
        self._server._request_executor.shutdown(
            wait=False, cancel_futures=True
        )

        worker_shutdown = threading.Thread(
            target=self._server._request_executor.shutdown,
            kwargs={"wait": True},
            name="canary-http-worker-shutdown",
            daemon=True,
        )
        worker_shutdown.start()
        worker_shutdown.join(max(0.0, deadline - monotonic_clock()))
        self._server._close_active_requests()
        self._listener_thread.join(max(0.0, deadline - monotonic_clock()))

        if listener_shutdown.is_alive() or worker_shutdown.is_alive():
            _http_log.warning("HTTP shutdown exceeded its configured drain bound")


def start_metrics_server(
    port: int,
    addr: str = "127.0.0.1",
    ssl_enabled: bool = False,
    ssl_cert: str = None,
    ssl_key: str = None,
    max_workers: int = 4,
    request_queue_size: int = 16,
    socket_timeout: float = 5.0,
) -> HTTPService:
    """
    Start the Prometheus HTTP(S) metrics server with health endpoints.

    Returns an owned service handle for the background listener and request
    workers that serve multiple endpoints:
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
        Bind address for the HTTP server (default "127.0.0.1" for loopback
        access only). Use a specific non-loopback address only when access is
        restricted by deployment infrastructure.

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

    socket_timeout : float
        Timeout in seconds applied to each accepted connection. The same
        socket is used for request reads and response writes.

    Raises
    ------
    ValueError
        If ssl_enabled=True but ssl_cert or ssl_key are not provided.
    FileNotFoundError
        If SSL certificate or key files do not exist.
    """
    import json
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from src.health import (
        get_health_status,
        get_liveness_status,
        get_readiness_status,
    )

    try:
        loopback_bind = ipaddress.ip_address(addr).is_loopback
    except ValueError:
        loopback_bind = addr.rstrip(".").lower() == "localhost"

    if not ssl_enabled and not loopback_bind:
        log.warning(
            "SECURITY WARNING: Metrics and probe endpoints are exposed over "
            "plaintext HTTP on a non-loopback address. Restrict access using "
            "an access-restricted private network, firewall, network policy, "
            "reverse proxy, or service mesh; the application does not provide "
            "authentication or authorization."
        )

    max_json_bytes = 64 * 1024
    max_metrics_bytes = 1024 * 1024
    sensitive_text = re.compile(
        rb"(?i)(?:"
        rb"(?:password|passwd|token|secret|credential|authorization|"
        rb"api[_-]?key)\s*[:=]|"
        rb"https?://[^\s/@:]+:[^\s/@]+@|"
        rb"traceback|(?:exception|[a-z]+error)(?:\s*:|\s*\()|"
        rb"(?:^|[\s\"'=])/(?:etc|var|srv|run|home|users|private|tmp)(?:/|\b)|"
        rb"(?:^|[\s\"'=])/(?!/)[\w.-]+(?:/[\w.-]+)+|"
        rb"\.(?:pem|key|crt)\b|"
        rb"(?:bootstrap\.servers|schema\.registry|sasl\.|ssl\.)\s*="
        rb")"
    )
    prohibited_field = re.compile(
        r"(?i)(?:credential|password|passwd|token|secret|authorization|"
        r"api[_-]?key|exception|traceback|kafka[_-]?(?:record|message)|"
        r"config(?:uration)?|certificate|filesystem|file[_-]?path|url)"
    )
    configuration_text = re.compile(rb"(?i)\b[a-z][\w.-]{1,64}\s*=")

    def sanitize_json(value, depth=0):
        """Return a bounded JSON-safe copy without suspicious internal text."""
        if depth > 6:
            return None
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            value = value[:512]
            encoded = value.encode("utf-8", "replace")
            if sensitive_text.search(encoded) or configuration_text.search(encoded):
                return "[redacted]"
            return value
        if isinstance(value, dict):
            sanitized = {}
            for index, (key, item) in enumerate(value.items()):
                if index == 64:
                    break
                safe_key = str(key)[:128]
                if prohibited_field.search(safe_key) or sensitive_text.search(
                    safe_key.encode("utf-8", "replace")
                ):
                    continue
                sanitized[safe_key] = sanitize_json(item, depth + 1)
            return sanitized
        if isinstance(value, (list, tuple)):
            return [sanitize_json(item, depth + 1) for item in value[:64]]
        return None

    def sanitize_metrics(payload):
        """Drop suspicious exposition lines and enforce a response-size bound."""
        if not isinstance(payload, bytes) or len(payload) > max_metrics_bytes:
            raise ValueError("invalid metrics response")
        return b"".join(
            line
            for line in payload.splitlines(keepends=True)
            if not sensitive_text.search(line)
        )

    class CanaryHTTPHandler(BaseHTTPRequestHandler):
        """
        HTTP handler that serves metrics and health endpoints.
        """

        def _write_json(self, status, response):
            """Serialize before sending headers so encoding failures are recoverable."""
            body = json.dumps(
                sanitize_json(response), separators=(",", ":")
            ).encode()
            if len(body) > max_json_bytes:
                status = 500
                body = b'{"status":"error","message":"Response unavailable"}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not getattr(self, "_suppress_body", False):
                self.wfile.write(body)

        def _write_failure(self, status, response):
            """Best-effort fixed-shape JSON failure without exception details."""
            try:
                _http_log.error("HTTP handler failed")
            except Exception:
                pass

            try:
                self._write_json(status, response)
            except Exception:
                # The connection may already be unwritable; do not let the
                # reporting attempt escape the handler boundary.
                pass

        def do_GET(self):
            """Handle GET requests for /metrics, /live, /health, and /ready."""

            try:
                self._do_GET()
            except Exception:
                self._write_failure(
                    500, {"status": "error", "message": "HTTP request failed"}
                )

        def do_HEAD(self):
            """Serve GET metadata without a response body or mutation."""
            previous = getattr(self, "_suppress_body", False)
            self._suppress_body = True
            try:
                self.do_GET()
            finally:
                self._suppress_body = previous

        def _reject_mutation(self):
            """Reject mutation methods before any endpoint work is dispatched."""
            response = {"status": "method_not_allowed", "message": "Method not allowed"}
            body = json.dumps(response, separators=(",", ":")).encode()
            self.send_response(405)
            self.send_header("Allow", "GET, HEAD")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = _reject_mutation
        do_PUT = _reject_mutation
        do_PATCH = _reject_mutation
        do_DELETE = _reject_mutation

        def _do_GET(self):
            """Dispatch a GET request inside the sanitizing boundary."""

            if self.path == "/metrics":
                # Prometheus metrics endpoint
                try:
                    metrics_output = sanitize_metrics(generate_latest())
                    self.send_response(200)
                    self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                    self.send_header("Content-Length", str(len(metrics_output)))
                    self.end_headers()
                    if not getattr(self, "_suppress_body", False):
                        self.wfile.write(metrics_output)
                except Exception:
                    self._write_failure(
                        500,
                        {"status": "error", "message": "Metrics generation failed"},
                    )

            elif self.path == "/live":
                try:
                    liveness = get_liveness_status()
                    response = {
                        "status": liveness.status.value,
                        "timestamp": liveness.timestamp,
                        "scheduler_heartbeat_age_seconds": (
                            liveness.scheduler_heartbeat_age_seconds
                        ),
                        "shutdown_started": liveness.shutdown_started,
                        "message": liveness.message,
                    }
                    self._write_json(liveness.http_code, response)
                except Exception:
                    self._write_failure(
                        503,
                        {
                            "status": "not_live",
                            "timestamp": None,
                            "scheduler_heartbeat_age_seconds": None,
                            "shutdown_started": False,
                            "message": "Liveness evaluation failed",
                        },
                    )

            elif self.path == "/health":
                # Kafka dependency-health endpoint
                try:
                    health = get_health_status()
                    response = {
                        "status": health.status.value,
                        "timestamp": health.timestamp,
                        "uptime_seconds": health.uptime_seconds,
                        "message": health.message,
                        "checks": health.checks,
                    }
                    self._write_json(health.http_code, response)
                except Exception:
                    self._write_failure(
                        503,
                        {
                            "status": "unhealthy",
                            "timestamp": None,
                            "uptime_seconds": None,
                            "message": "Health evaluation failed",
                            "checks": {},
                        },
                    )

            elif self.path == "/ready":
                # Readiness probe endpoint
                try:
                    readiness = get_readiness_status()
                    response = {
                        "status": readiness.status.value,
                        "timestamp": readiness.timestamp,
                        "message": readiness.message,
                        "checks": readiness.checks,
                    }
                    self._write_json(readiness.http_code, response)
                except Exception:
                    self._write_failure(
                        503,
                        {
                            "status": "not_ready",
                            "timestamp": None,
                            "message": "Readiness evaluation failed",
                            "checks": {},
                        },
                    )

            else:
                self._write_failure(
                    404, {"status": "not_found", "message": "Endpoint not found"}
                )

        def log_message(self, format, *args):
            """Suppress request-line logging because it contains request targets."""

    def configure_bounded_execution(server):
        """Attach fixed-pool request execution to a concrete HTTP server."""
        overload_body = (
            b'{"status":"unavailable","message":"HTTP service overloaded"}'
        )
        overload_response = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json\r\n"
            b"Connection: close\r\n"
            b"Content-Length: "
            + str(len(overload_body)).encode("ascii")
            + b"\r\n\r\n"
            + overload_body
        )
        server._request_slots = threading.BoundedSemaphore(
            max_workers + request_queue_size
        )
        server._accepting = threading.Event()
        server._accepting.set()
        server._active_requests = set()
        server._active_requests_lock = threading.Lock()
        server._request_executor = DaemonThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="canary-http",
        )

        # Apply the deadline immediately after accept, before the connection
        # can be submitted to a handler. Socket timeouts cover both recv and
        # send operations performed by BaseHTTPRequestHandler.
        original_get_request = getattr(server, "get_request", None)
        if original_get_request is not None:
            def get_request():
                request, client_address = original_get_request()
                request.settimeout(socket_timeout)
                return request, client_address

            server.get_request = get_request

        def process_request(request, client_address):
            # The listening thread never waits for executor capacity. An
            # accepted request either owns one active/waiting slot or is
            # rejected without entering the executor's internal queue.
            if not server._accepting.is_set():
                server.shutdown_request(request)
                return
            if not server._request_slots.acquire(blocking=False):
                HTTP_OVERLOAD_TOTAL.labels(host=HOST).inc()
                try:
                    request.sendall(overload_response)
                except Exception:
                    # The peer may already be gone; saturation must remain
                    # isolated from handlers and dependency health state.
                    pass
                server.shutdown_request(request)
                return

            try:
                with server._active_requests_lock:
                    server._active_requests.add(request)
                server._request_executor.submit(
                    process_admitted_request, request, client_address
                )
            except BaseException:
                with server._active_requests_lock:
                    server._active_requests.discard(request)
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
                    with server._active_requests_lock:
                        server._active_requests.discard(request)
                    server.shutdown_request(request)
            finally:
                server._request_slots.release()

        def close_active_requests():
            with server._active_requests_lock:
                active_requests = tuple(server._active_requests)
                server._active_requests.clear()
            for request in active_requests:
                try:
                    request.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    request.close()
                except Exception:
                    pass

        server.process_request = process_request
        server._close_active_requests = close_active_requests
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
    return HTTPService(server, thread)
