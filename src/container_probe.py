"""Configuration-aware, container-local liveness probe."""

from __future__ import annotations

import configparser
import os
import ssl
import urllib.request
from collections.abc import Mapping
from typing import Callable


DEFAULT_CONFIG_PATH = "config/config.ini"
DEFAULT_METRICS_PORT = 8000
PROBE_TIMEOUT_SECONDS = 3.0


class ProbeConfigurationError(ValueError):
    """Raised when the probe cannot construct a safe local target."""


def build_probe_url(app_config: Mapping[str, str]) -> str:
    """Build the loopback liveness URL from the application configuration."""
    raw_port = app_config.get("metrics.port", str(DEFAULT_METRICS_PORT))
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as exc:
        raise ProbeConfigurationError("metrics.port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ProbeConfigurationError("metrics.port is outside the TCP port range")

    ssl_enabled = app_config.get("metrics.ssl.enabled", "false").lower()
    if ssl_enabled not in {"true", "false"}:
        raise ProbeConfigurationError(
            "metrics.ssl.enabled must be either true or false"
        )

    scheme = "https" if ssl_enabled == "true" else "http"
    return f"{scheme}://localhost:{port}/live"


def load_probe_url(config_path: str | None = None) -> str:
    """Read only the non-secret application settings needed by the probe."""
    path = config_path or os.getenv("CANARY_CONFIG_FILE", DEFAULT_CONFIG_PATH)
    parser = configparser.ConfigParser(interpolation=None)
    try:
        loaded_paths = parser.read(path)
    except (OSError, configparser.Error) as exc:
        raise ProbeConfigurationError("unable to read probe configuration") from exc
    if not loaded_paths or not parser.has_section("app"):
        raise ProbeConfigurationError("probe configuration has no app section")
    return build_probe_url(parser["app"])


def request_liveness(
    url: str,
    *,
    timeout: float = PROBE_TIMEOUT_SECONDS,
    opener: Callable[..., object] = urllib.request.urlopen,
    context_factory: Callable[[], ssl.SSLContext] = ssl.create_default_context,
) -> bool:
    """Return whether the configured liveness endpoint responds with HTTP 200."""
    if timeout <= 0 or timeout > PROBE_TIMEOUT_SECONDS:
        raise ValueError("probe timeout must be positive and bounded")

    request = urllib.request.Request(url, method="GET")
    kwargs: dict[str, object] = {"timeout": timeout}
    if url.startswith("https://"):
        # The default context validates the certificate chain and hostname.
        kwargs["context"] = context_factory()

    with opener(request, **kwargs) as response:
        return response.getcode() == 200


def main() -> int:
    """Run one bounded probe, returning a conventional healthcheck exit code."""
    try:
        return 0 if request_liveness(load_probe_url()) else 1
    except Exception:
        # Healthcheck failures are intentionally silent and never expose config.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
