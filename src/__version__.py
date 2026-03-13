# ---------------------------------------------------------------------------
# __version__.py — Version metadata for cloud_canary
#
# This module defines the canonical version number for the application.
# It is imported by main.py to include version info in logs and metrics,
# and can be used by packaging tools (setup.py, Docker labels, etc.).
# ---------------------------------------------------------------------------

__version__ = "1.2.3"
__version_info__ = (1, 2, 3)

# Human-readable build metadata (optional)
__author__ = "Cloud Canary Team"
__description__ = "Production-grade Kafka canary for Confluent Cloud monitoring"
