# ---------------------------------------------------------------------------
# config.py — Configuration loader
#
# Reads config/config.ini (INI format, parsed by Python's configparser) and
# returns its three sections as plain dicts.
#
# Section layout
# --------------
#   [kafka]           — librdkafka connection and authentication settings
#                       (bootstrap.servers, SASL credentials, etc.)
#   [schema_registry] — Confluent Schema Registry URL and credentials
#   [app]             — Application-level knobs (topic name, intervals, ports)
#
# The kafka dict is passed directly to AdminClient, Producer, and
# Consumer, so its keys must match librdkafka configuration
# property names exactly (see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
# The schema_registry dict is passed to SchemaRegistryClient.
# ---------------------------------------------------------------------------

import configparser
import logging
import math
import os
import sys
from urllib.parse import urlsplit

log = logging.getLogger(__name__)

SCHEDULER_HEARTBEAT_INTERVAL_SECONDS = 1.0


def validate_config(config: dict) -> None:
    """
    Validate that all required configuration keys are present and have valid values.

    Parameters
    ----------
    config : dict
        Configuration dictionary with 'kafka', 'schema_registry', and 'app' sections.

    Raises
    ------
    ValueError
        If any required configuration key is missing or has an invalid value.
    """
    # Required keys per section
    required_keys = {
        'kafka': [
            'bootstrap.servers',
            'security.protocol',
            'sasl.mechanisms',
            'sasl.username',
            'sasl.password',
        ],
        'schema_registry': [
            'url',
            'basic.auth.user.info',
        ],
        'app': [
            'topic',
        ],
    }

    # Check for missing required keys
    for section, keys in required_keys.items():
        if section not in config:
            raise ValueError(f"Missing required config section: [{section}]")
        for key in keys:
            if key not in config[section] or not config[section][key]:
                raise ValueError(f"Missing or empty required config: [{section}].{key}")

    # Validate specific formats
    kafka = config['kafka']
    sr = config['schema_registry']
    app = config['app']

    # Validate bootstrap.servers format
    if ':' not in kafka['bootstrap.servers']:
        raise ValueError(
            "[kafka].bootstrap.servers must be in format 'host:port' "
            f"(got: '{kafka['bootstrap.servers']}')"
        )

    # Validate security.protocol is SASL_SSL for Confluent Cloud
    if kafka['security.protocol'] != 'SASL_SSL':
        log.warning(
            "[kafka].security.protocol is '%s' — Confluent Cloud requires 'SASL_SSL'",
            kafka['security.protocol']
        )

    # Validate sasl.mechanisms is PLAIN for Confluent Cloud
    if kafka['sasl.mechanisms'] != 'PLAIN':
        log.warning(
            "[kafka].sasl.mechanisms is '%s' — Confluent Cloud requires 'PLAIN'",
            kafka['sasl.mechanisms']
        )

    # Schema Registry credentials must never be sent over plaintext transport.
    parsed_sr_url = urlsplit(sr['url'])
    if parsed_sr_url.scheme.lower() != 'https' or not parsed_sr_url.hostname:
        raise ValueError(
            "[schema_registry].url must be a valid HTTPS URL"
        )

    # Validate basic.auth.user.info format (KEY:SECRET)
    if ':' not in sr['basic.auth.user.info']:
        raise ValueError(
            "[schema_registry].basic.auth.user.info must be in format 'KEY:SECRET'"
        )

    # Validate SSL/TLS certificate configuration
    if 'ssl.ca.location' in kafka:
        ca_path = kafka['ssl.ca.location']
        if not ca_path:
            raise ValueError(
                "[kafka].ssl.ca.location cannot be empty. "
                "Remove the setting to use system default CA bundle."
            )
        # Note: We don't check if the file exists here because it might be
        # mounted at runtime (Docker volumes). File existence is validated
        # during SSL connectivity check at startup.

    # Validate SSL certificate verification setting (security critical)
    ssl_verify = kafka.get('enable.ssl.certificate.verification', 'true').lower()
    if ssl_verify not in ('true', 'false'):
        raise ValueError(
            "[kafka].enable.ssl.certificate.verification must be 'true' or 'false' "
            f"(got: '{kafka.get('enable.ssl.certificate.verification')}')"
        )
    if ssl_verify == 'false':
        log.warning(
            "SSL certificate verification is DISABLED - this is a critical security risk! "
            "Only disable for local testing. NEVER use in production."
        )

    # Validate mTLS configuration (if client cert is provided)
    if 'ssl.certificate.location' in kafka or 'ssl.key.location' in kafka:
        if not kafka.get('ssl.certificate.location'):
            raise ValueError(
                "[kafka].ssl.certificate.location must be set when using client certificates (mTLS)"
            )
        if not kafka.get('ssl.key.location'):
            raise ValueError(
                "[kafka].ssl.key.location must be set when using client certificates (mTLS)"
            )

    # Validate numeric app settings (if present)
    numeric_settings = {
        'consumer.timeout.seconds': (1, 300),
        'check.interval.seconds': (1, 3600),
        'partition.sync.interval.seconds': (60, 604800),
        'sr.check.interval.seconds': (10, 3600),
        'warmup.checks': (0, 100),
        'max.workers': (1, 200),  # Thread pool size
        'metrics.port': (1024, 65535),
        'log.topic.retention.ms': (60000, 2592000000),  # 1 min to 30 days
    }

    for key, (min_val, max_val) in numeric_settings.items():
        if key in app:
            try:
                value = float(app[key]) if 'seconds' in key else int(app[key])
                if not (min_val <= value <= max_val):
                    raise ValueError(
                        f"[app].{key} must be between {min_val} and {max_val} "
                        f"(got: {value})"
                    )
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"[app].{key} must be a valid number (got: '{app[key]}')"
                ) from exc

    def positive_integer(key: str, default: str) -> int:
        raw_value = app.get(key, default)
        try:
            value = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"[app].{key} must be a positive integer") from exc
        if value <= 0:
            raise ValueError(f"[app].{key} must be a positive integer")
        return value

    def finite_number(key: str, default: str) -> float:
        raw_value = app.get(key, default)
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"[app].{key} must be a valid number") from exc
        if not math.isfinite(value):
            raise ValueError(f"[app].{key} must be a finite number")
        return value

    window_checks = positive_integer("health.failure.window.checks", "20")
    minimum_checks = positive_integer("health.failure.minimum.checks", "4")
    if minimum_checks > window_checks:
        raise ValueError(
            "[app].health.failure.minimum.checks must be no greater than "
            "[app].health.failure.window.checks"
        )

    failure_threshold = finite_number("health.failure.threshold", "0.5")
    if not 0 < failure_threshold <= 1:
        raise ValueError(
            "[app].health.failure.threshold must be greater than 0 and at most 1"
        )
    positive_integer("health.max.diagnostic.components", "20")

    kafka_degraded = finite_number("health.kafka.degraded.after.seconds", "60")
    kafka_unhealthy = finite_number("health.kafka.unhealthy.after.seconds", "300")
    kafka_interval = finite_number("check.interval.seconds", "15")
    liveness_staleness = finite_number(
        "liveness.scheduler.max.staleness.seconds", "10"
    )
    if liveness_staleness <= SCHEDULER_HEARTBEAT_INTERVAL_SECONDS:
        raise ValueError(
            "[app].liveness.scheduler.max.staleness.seconds must be greater "
            "than the scheduler heartbeat interval"
        )
    if kafka_degraded <= kafka_interval:
        raise ValueError(
            "[app].health.kafka.degraded.after.seconds must be greater than "
            "[app].check.interval.seconds"
        )
    if kafka_unhealthy <= kafka_degraded:
        raise ValueError(
            "[app].health.kafka.unhealthy.after.seconds must be greater than "
            "[app].health.kafka.degraded.after.seconds"
        )

    sr_degraded = finite_number("health.sr.degraded.after.seconds", "120")
    sr_unhealthy = finite_number("health.sr.unhealthy.after.seconds", "300")
    sr_interval = finite_number("sr.check.interval.seconds", "60")
    sr_timeout = finite_number("sr.check.timeout.seconds", "10")
    if not 0 < sr_timeout < sr_interval:
        raise ValueError(
            "[app].sr.check.timeout.seconds must be greater than 0 and less than "
            "[app].sr.check.interval.seconds"
        )
    if sr_degraded <= sr_interval:
        raise ValueError(
            "[app].health.sr.degraded.after.seconds must be greater than "
            "[app].sr.check.interval.seconds"
        )
    if sr_unhealthy <= sr_degraded:
        raise ValueError(
            "[app].health.sr.unhealthy.after.seconds must be greater than "
            "[app].health.sr.degraded.after.seconds"
        )

    # Validate boolean settings (if present)
    boolean_settings = ['log.topic.enabled', 'metrics.ssl.enabled']
    for key in boolean_settings:
        if key in app:
            value = app[key].lower()
            if value not in ('true', 'false'):
                raise ValueError(
                    f"[app].{key} must be 'true' or 'false' (got: '{app[key]}')"
                )

    # Validate SSL configuration consistency
    ssl_enabled = app.get('metrics.ssl.enabled', 'false').lower() == 'true'
    if ssl_enabled:
        if not app.get('metrics.ssl.cert'):
            raise ValueError(
                "[app].metrics.ssl.cert must be set when metrics.ssl.enabled=true"
            )
        if not app.get('metrics.ssl.key'):
            raise ValueError(
                "[app].metrics.ssl.key must be set when metrics.ssl.enabled=true"
            )

    log.debug("Configuration validation passed")


def load_config(path: str = "config/config.ini") -> dict:
    """
    Load and parse the INI configuration file.

    Parameters
    ----------
    path : str
        Path to the INI file, relative to the working directory from which
        the application is launched (typically the project root).

    Returns
    -------
    dict
        A dict with three keys: "kafka", "schema_registry", and "app".
        Each value is itself a dict of the corresponding INI section's
        key-value pairs (all values are strings; callers cast as needed).

    Exits
    -----
    Calls sys.exit() with an actionable error message if the config file is
    missing.  This is intentional — the application cannot start without
    credentials, and a clean exit is preferable to a later AttributeError
    on a missing section.
    """
    if not os.path.exists(path):
        # Sanitize path for error message to prevent information leakage.
        # Only show the basename if it's outside expected directories.
        safe_path = path if path.startswith(("config/", "./config/")) else os.path.basename(path)
        sys.exit(
            f"Config file not found: '{safe_path}'\n"
            "Copy config/config.ini.template to config/config.ini and fill in your credentials."
        )

    parser = configparser.ConfigParser()
    parser.read(path)

    config = {
        "kafka":           dict(parser["kafka"]),
        "schema_registry": dict(parser["schema_registry"]),
        "app":             dict(parser["app"]),
    }

    # Validate configuration before returning
    try:
        validate_config(config)
    except ValueError as exc:
        sys.exit(f"Configuration error: {exc}")

    return config
