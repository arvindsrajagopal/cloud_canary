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
# The kafka dict is passed directly to AdminClient, SerializingProducer, and
# DeserializingConsumer, so its keys must match librdkafka configuration
# property names exactly (see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
# The schema_registry dict is passed to SchemaRegistryClient.
# ---------------------------------------------------------------------------

import configparser
import os
import sys


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
        sys.exit(
            f"Config file not found: '{path}'\n"
            "Copy config/config.ini.template to config/config.ini and fill in your credentials."
        )

    parser = configparser.ConfigParser()
    parser.read(path)

    return {
        "kafka":           dict(parser["kafka"]),
        "schema_registry": dict(parser["schema_registry"]),
        "app":             dict(parser["app"]),
    }
