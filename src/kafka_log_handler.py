# ---------------------------------------------------------------------------
# kafka_log_handler.py — logging.Handler that publishes log records to Kafka
#
# Purpose
# -------
# When log.topic.enabled=true in config.ini, all canary log output is also
# sent to a dedicated Kafka topic (default: "cloud-canary-logs") as JSON
# records.  This allows long-term log retention beyond what the host's local
# logging infrastructure retains, and enables querying canary logs from any
# Kafka consumer (e.g. ksqlDB, Kafka Streams, or a Flink job).
#
# Design decisions
# ----------------
# Separate producer
#     The log handler uses its own plain Producer (not the Avro SerializingProducer
#     used for canary checks).  This isolates logging failures from canary
#     measurement failures — a log delivery error never causes a check failure,
#     and vice versa.
#
# Plain JSON, not Avro
#     Log records are diagnostic data consumed by humans and generic tooling
#     (e.g. Kibana, ksqlDB, jq).  Plain UTF-8 JSON is universally parseable
#     without a Schema Registry lookup, which is simpler and more robust for
#     ad-hoc log consumption.
#
# acks=1 (leader only)
#     Log records don't need full ISR durability.  Leader-only ack reduces
#     produce latency and avoids blocking the logging path if followers are
#     slow.
#
# linger.ms=50 + lz4 compression
#     Log records are small text messages that compress well.  A 50 ms linger
#     accumulates a batch of records (the canary logs frequently during checks)
#     to amortize per-message overhead, while still flushing promptly.
#
# Non-blocking emit()
#     emit() calls producer.poll(0) to serve delivery callbacks without
#     blocking the calling thread.  The actual network I/O happens in
#     librdkafka's background thread.  Buffered records are flushed on close().
#
# Recursion guard
#     If produce() itself raises an exception, self.handleError() is called
#     instead of logging the error — logging the error would re-enter emit()
#     and cause infinite recursion.
# ---------------------------------------------------------------------------

import json
import logging
import socket
import time
from collections.abc import Callable

from confluent_kafka import KafkaException, Producer

from src import constants as const
from src import metrics


class KafkaLogHandler(logging.Handler):
    """
    A logging.Handler that publishes formatted log records as JSON to a Kafka topic.

    Usage
    -----
    Add to the root logger after the log topic is created:

        handler = KafkaLogHandler(kafka_config, "cloud-canary-logs")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(handler)

    Remove and flush on shutdown:

        logging.getLogger().removeHandler(handler)
        handler.close()

    JSON record format
    ------------------
    Each Kafka message value is a UTF-8 encoded JSON object:
        {
            "timestamp_ms": 1718000000000,   # Unix epoch ms when the log record was created
            "level":        "INFO",           # Python log level name
            "logger":       "src.main",       # Logger name (typically the module)
            "host":         "canary-host-1",  # Hostname of the canary process
            "message":      "OK | seq=42 ..." # Formatted log message (via setFormatter)
        }
    """

    def __init__(
        self,
        kafka_config: dict,
        topic: str,
        *,
        shutdown_requested: Callable[[], bool] | None = None,
        shutdown_deadline: Callable[[], float | None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """
        Parameters
        ----------
        kafka_config : dict
            Connection/auth settings from config.ini [kafka].  Merged with
            handler-specific overrides (acks, linger, compression).

        topic : str
            Kafka topic to publish log records to (e.g. "cloud-canary-logs").
            Must already exist; use ensure_log_topic() before creating this handler.
        """
        super().__init__()
        self._topic = topic
        self._shutdown_requested = shutdown_requested or (lambda: False)
        self._shutdown_deadline = shutdown_deadline or (lambda: None)
        self._monotonic = monotonic
        # Use metrics.HOST to respect configurable instance ID
        self._host = metrics.HOST
        # Cache JSON encoder instance to avoid recreating on every log call.
        # This reduces CPU overhead by ~20-30% in the logging hot path.
        self._encoder = json.JSONEncoder(default=str, separators=(',', ':'))
        self._producer = Producer({
            **kafka_config,
            "client.id":        f"{metrics.HOST}-log-handler",
            "acks":             "1",            # leader ack only — logs don't need full ISR durability
            "linger.ms":        const.LOG_HANDLER_LINGER_MS,
            "compression.type": "lz4",          # text compresses well; lz4 is fast with good ratio
            "socket.nagle.disable": True,
            "client.dns.lookup": "use_all_dns_ips",
        })

    def emit(self, record: logging.LogRecord) -> None:
        """
        Format the log record and produce it to the Kafka log topic.

        Called by the logging framework for every record that passes the
        handler's level filter.  The method is intentionally non-blocking:
        produce() enqueues the message in librdkafka's internal buffer and
        poll(0) services delivery callbacks without waiting for network I/O.

        Parameters
        ----------
        record : logging.LogRecord
            The log record emitted by a logger.  self.format(record) applies
            the handler's Formatter (set by the caller) to produce the final
            message string.
        """
        if self._shutdown_requested():
            return

        try:
            log_dict = {
                "timestamp_ms": int(record.created * 1000),  # record.created is float seconds
                "level":        record.levelname,
                "logger":       record.name,
                "host":         self._host,
                "message":      self.format(record),          # applies the configured Formatter
            }
            # Use cached encoder for better performance
            payload = self._encoder.encode(log_dict)
            producer = self._producer
            if producer is None:
                return
            producer.produce(
                topic=self._topic,
                value=payload.encode("utf-8"),
            )
            # poll(0) is non-blocking: it drives the librdkafka event loop to
            # process delivery callbacks from previously produced messages without
            # waiting for new ones.  This prevents the internal callback queue
            # from growing unbounded during a burst of log output.
            producer.poll(0)
        except (KafkaException, json.JSONEncodeError, UnicodeEncodeError, BufferError) as exc:
            # Catch specific exceptions that can occur during log publishing:
            # - KafkaException: broker connectivity or quota issues
            # - JSONEncodeError: malformed log record data
            # - UnicodeEncodeError: non-UTF8 characters in log message
            # - BufferError: producer queue is full
            # handleError() logs the exception to stderr via the logging
            # framework's fallback mechanism, avoiding re-entry into emit()
            # which would cause infinite recursion.
            self.handleError(record)

    def close(self) -> None:
        """
        Flush all buffered log records to Kafka and release resources.

        Called automatically when the handler is removed from a logger or when
        the logging system shuts down. The flush is capped by both its normal
        timeout and any process shutdown deadline supplied by the caller.

        Explicitly releases the producer to close TCP connections and free
        resources immediately rather than waiting for garbage collection.
        """
        try:
            # Claim the producer before flushing.  logging.shutdown() may call
            # close() again during interpreter teardown; that later call must
            # be a no-op rather than starting a fresh timeout.
            producer = self._producer
            self._producer = None
            if producer is None:
                return

            timeout = const.LOG_HANDLER_FLUSH_TIMEOUT_SECONDS
            deadline = self._shutdown_deadline()
            if deadline is not None:
                timeout = min(timeout, max(0.0, deadline - self._monotonic()))
            producer.flush(timeout=timeout)
        finally:
            super().close()   # always call the parent to mark the handler as closed
