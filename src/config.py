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
import os
import sys

log = logging.getLogger(__name__)


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

    # Validate Schema Registry URL format
    if not sr['url'].startswith(('http://', 'https://')):
        raise ValueError(
            "[schema_registry].url must start with 'http://' or 'https://' "
            f"(got: '{sr['url']}')"
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
        'metrics.partition.threshold': (0, 10000),  # Cardinality control
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
