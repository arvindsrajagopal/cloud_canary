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
# Per-partition metrics (latency histograms, checks counters, consecutive
#     failures gauge) carry BOTH `host` and `partition` labels.  Callers must
#     include both in every .labels() call:
#         metrics.E2E_LATENCY.labels(host=metrics.HOST, partition=str(p)).observe(ms)
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

import os
import socket

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Hostname of this canary instance.  Embedded as a constant label on every
# metric so that dashboards and alerts can filter or group by instance without
# relying on Prometheus scrape-target configuration.
# Can be overridden via CANARY_INSTANCE_ID environment variable or instance.id
# in config.ini for multi-instance deployments on the same host.
HOST = os.getenv("CANARY_INSTANCE_ID") or socket.gethostname()

# ---------------------------------------------------------------------------
# Latency histograms — labeled per host AND partition at call time.
# Each partition maps to a distinct broker leader, giving per-broker latency.
# ---------------------------------------------------------------------------

E2E_LATENCY = Histogram(
    "canary_e2e_latency_ms",
    "End-to-end latency per check (produce timestamp → consumer receive timestamp) in ms, "
    "per host and partition. Each partition corresponds to a distinct broker leader, so "
    "this histogram surfaces per-broker latency attribution.",
    ["host", "partition"],
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

SEEK_DURATION = Histogram(
    "canary_seek_duration_ms",
    "Time to fetch watermark offsets from the broker and seek the assigned partition in ms, "
    "per host and partition. A sustained increase here indicates broker-side fetch latency "
    "on the specific partition's leader.",
    ["host", "partition"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

PRODUCE_DURATION = Histogram(
    "canary_produce_duration_ms",
    "Time from produce() call to broker ack (flush return) in ms, per host and partition. "
    "With acks=all this measures the time for all in-sync replicas to acknowledge the write "
    "— an increase on a specific partition may indicate replication lag or leader overload "
    "on that broker.",
    ["host", "partition"],
    buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500],
)

# ---------------------------------------------------------------------------
# Reliability counters — host and partition are variable labels;
# callers must pass both in every .labels() call.
# ---------------------------------------------------------------------------

CHECKS_TOTAL = Counter(
    "canary_checks_total",
    "Total canary check attempts, labelled by result, host, and partition. "
    "Use rate(canary_checks_total[5m]) to compute check throughput per broker.",
    ["result", "host", "partition"],
    # result values: "success" | "failure"
)

FAILURES_TOTAL = Counter(
    "canary_failures_total",
    "Failed canary checks, labelled by the phase where the error occurred, its category, "
    "host, and partition. Use to identify whether failures are connectivity-related (NETWORK) "
    "or cluster-side (BROKER), which pipeline stage is affected, and which broker is at fault.",
    ["phase", "category", "host", "partition"],
    # phase values:    SEEK | PRODUCE | CONSUME | SCHEMA_REGISTRY | ASSIGNMENT | UNKNOWN
    # category values: NETWORK | BROKER | UNKNOWN
)

# ---------------------------------------------------------------------------
# Reliability gauge — labeled per host AND partition at call time.
# Alert fires if ANY partition's streak reaches the threshold.
# ---------------------------------------------------------------------------

CONSECUTIVE_FAILURES = Gauge(
    "canary_consecutive_failures",
    "Current streak of consecutive check failures without an intervening success, "
    "per host and partition. Primary alerting signal — alert when this value exceeds "
    "a threshold (e.g. >= 3). Resets to 0 on any successful check for that partition.",
    ["host", "partition"],
)

LAST_SUCCESS_TIMESTAMP = Gauge(
    "canary_last_success_timestamp_seconds",
    "Unix timestamp of the last successful check per host and partition. "
    "Use (time() - canary_last_success_timestamp_seconds) to detect stale monitoring. "
    "Alert if this value is too far in the past (e.g., > 300s means no success in 5 minutes).",
    ["host", "partition"],
)

# ---------------------------------------------------------------------------
# Schema Registry counter — host is a variable label; callers must pass host=HOST
# ---------------------------------------------------------------------------

SR_CHECKS_TOTAL = Counter(
    "canary_sr_checks_total",
    "Schema Registry health check attempts, labelled by result and host. "
    "SR checks run on a separate cadence from canary checks, providing an "
    "independent signal of SR availability per instance.",
    ["result", "host"],
    # result values: "success" | "failure"
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


def start_metrics_server(
    port: int,
    addr: str = "0.0.0.0",
    ssl_enabled: bool = False,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
) -> None:
    """
    Start the Prometheus HTTP(S) metrics server on the given port and address.

    Spawns a background daemon thread that serves the /metrics endpoint.
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

    ssl_cert : str | None
        Path to SSL certificate file in PEM format. Required if ssl_enabled=True.

    ssl_key : str | None
        Path to SSL private key file in PEM format. Required if ssl_enabled=True.

    Raises
    ------
    ValueError
        If ssl_enabled=True but ssl_cert or ssl_key are not provided.
    FileNotFoundError
        If SSL certificate or key files do not exist.
    """
    if ssl_enabled:
        if not ssl_cert or not ssl_key:
            raise ValueError(
                "metrics.ssl.cert and metrics.ssl.key must be set when metrics.ssl.enabled=true"
            )

        import os
        import ssl
        from http.server import HTTPServer
        from prometheus_client import MetricsHandler

        if not os.path.exists(ssl_cert):
            raise FileNotFoundError(f"SSL certificate not found: {ssl_cert}")
        if not os.path.exists(ssl_key):
            raise FileNotFoundError(f"SSL private key not found: {ssl_key}")

        # Create HTTPS server with SSL context
        server = HTTPServer((addr, port), MetricsHandler)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
        server.socket = ssl_context.wrap_socket(server.socket, server_side=True)

        # Start server in background thread
        import threading
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
    else:
        # Standard HTTP server (no SSL)
        start_http_server(port, addr=addr)
