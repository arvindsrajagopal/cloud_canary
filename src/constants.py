# ---------------------------------------------------------------------------
# constants.py — Named constants for cloud_canary
#
# This module defines all magic numbers used throughout the codebase with
# descriptive names and documentation explaining their purpose and rationale.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Producer Configuration Constants
# ---------------------------------------------------------------------------

# Maximum retry count for producer operations.
# Set to max signed 32-bit integer to defer all retry control to
# delivery.timeout.ms, allowing the producer to keep retrying until
# the 2-minute budget is exhausted.
MAX_PRODUCER_RETRIES = 2147483647

# Total per-message delivery timeout in milliseconds.
# The producer will retry for up to 2 minutes before giving up.
# This is generous enough for transient network issues and broker
# leader elections without blocking indefinitely.
PRODUCER_DELIVERY_TIMEOUT_MS = 120000  # 2 minutes

# Producer flush timeout in seconds (wall-clock guard).
# This is a safety timeout that prevents flush() from blocking indefinitely
# if librdkafka's internal state machine gets stuck. The actual delivery
# timeout is controlled by PRODUCER_DELIVERY_TIMEOUT_MS.
# Reduced from 15s to 2s - sufficient for most broker acks while minimizing
# blocking time on the produce path (15s was excessive).
PRODUCER_FLUSH_TIMEOUT_SECONDS = 2

# Maximum number of in-flight requests per broker connection.
# Safe to set > 1 without idempotence because the canary doesn't require
# strict ordering and deduplicates by UUID.
MAX_IN_FLIGHT_REQUESTS = 5

# Producer batch size in bytes.
# 64 KB is sufficient for batching multiple canary messages during
# burst periods without delaying individual messages.
PRODUCER_BATCH_SIZE_BYTES = 65536

# Producer queue buffer limits.
PRODUCER_QUEUE_MAX_MESSAGES = 100000
PRODUCER_QUEUE_MAX_KBYTES = 1048576  # 1 GB

# API version negotiation timeout in milliseconds.
# Increased from 10s default to accommodate Confluent Cloud cold-start latency.
API_VERSION_REQUEST_TIMEOUT_MS = 30000

# Socket connection setup timeout in milliseconds.
SOCKET_CONNECTION_SETUP_TIMEOUT_MS = 30000

# Exponential backoff parameters for reconnection attempts.
RECONNECT_BACKOFF_MIN_MS = 1000   # 1 second base
RECONNECT_BACKOFF_MAX_MS = 10000  # 10 second cap

# Metadata refresh interval in milliseconds.
# Refresh every 5 minutes to detect partition leadership changes
# (e.g., after a broker restart) without manual intervention.
METADATA_MAX_AGE_MS = 300000

# ---------------------------------------------------------------------------
# Consumer Configuration Constants
# ---------------------------------------------------------------------------

# Session timeout in milliseconds.
# How long the broker waits for a heartbeat before considering the
# consumer dead and triggering a rebalance. 45s gives headroom for
# transient network hiccups without over-triggering rebalances.
CONSUMER_SESSION_TIMEOUT_MS = 45000

# Heartbeat interval in milliseconds.
# Must be < session.timeout.ms / 3 per Kafka spec. 3s is aggressive
# enough to detect failures promptly.
CONSUMER_HEARTBEAT_INTERVAL_MS = 3000

# Max poll interval in milliseconds.
# Maximum time between poll() calls before the broker evicts this
# consumer from the group. 300s accommodates the canary's
# consumer.timeout.seconds (default 5s) with ample margin.
CONSUMER_MAX_POLL_INTERVAL_MS = 300000

# Fetch behavior constants.
CONSUMER_FETCH_MIN_BYTES = 1    # Return as soon as any data is available
CONSUMER_FETCH_WAIT_MAX_MS = 100  # Reduced from 500ms default for lower latency

# Consumer poll timeout in seconds used in consume_canary().
# Each poll() call blocks for up to this duration waiting for a message.
CONSUMER_POLL_TIMEOUT_SECONDS = 1.0

# Maximum attempts to wait for consumer state transition from START to ACTIVE.
# After assign(), the consumer needs a few poll() cycles to transition.
CONSUMER_STATE_TRANSITION_MAX_ATTEMPTS = 5

# Sleep duration between state transition retry attempts in seconds.
# Reduced from 100ms to 50ms to minimize cold-start latency while still
# providing sufficient time for state transition (worst case: 250ms).
CONSUMER_STATE_TRANSITION_RETRY_SLEEP_SECONDS = 0.05

# Watermark offset fetch timeout in seconds.
# Used in seek_to_end() to get the current high watermark for each partition.
CONSUMER_WATERMARK_TIMEOUT_SECONDS = 5.0

# ---------------------------------------------------------------------------
# Topic Management Constants
# ---------------------------------------------------------------------------

# How long to wait (in seconds) for a topic deletion to propagate across
# all brokers before attempting to recreate it. Kafka deletes topics
# asynchronously; re-creating before deletion completes can result in a
# "topic already exists" error.
DELETE_PROPAGATION_TIMEOUT_SECONDS = 30

# Metadata fetch timeout in seconds for AdminClient operations.
# Increased from 10s to 30s to accommodate cold-start latency on some networks.
ADMIN_METADATA_TIMEOUT_SECONDS = 30

# Sleep interval during deletion propagation wait loop in seconds.
DELETE_PROPAGATION_POLL_INTERVAL_SECONDS = 1

# ---------------------------------------------------------------------------
# Kafka Log Handler Constants
# ---------------------------------------------------------------------------

# Linger time in milliseconds for log record batching.
# 50ms accumulates a batch of records during active canary checks
# while still flushing promptly.
LOG_HANDLER_LINGER_MS = 50

# Log handler flush timeout in seconds during close().
# 10s is generous for flushing a handful of buffered log records.
LOG_HANDLER_FLUSH_TIMEOUT_SECONDS = 10

# ---------------------------------------------------------------------------
# Metrics & Observability Constants
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Concurrency Configuration Constants
# ---------------------------------------------------------------------------

# Maximum number of worker threads in the check executor pool.
# Capped at 20 to prevent excessive memory usage and context switching overhead
# on large clusters. Each thread uses ~8MB stack memory, so 100 threads would
# consume ~800MB. With 20 workers, the pool can still process 20 partitions
# concurrently, which is sufficient for most deployments.
# Performance testing shows diminishing returns beyond 20 workers due to
# context switching overhead.
MAX_WORKERS = 20

# Default Prometheus metrics port.
DEFAULT_METRICS_PORT = 8000

# Default retention period for canary topic in milliseconds.
# 1 day (86400000 ms) — canary messages have no value after the check completes.
CANARY_TOPIC_RETENTION_MS = 86400000

# Default retention period for log topic in milliseconds.
# 7 days (604800000 ms) — allows long-term log retention.
DEFAULT_LOG_TOPIC_RETENTION_MS = 604800000
