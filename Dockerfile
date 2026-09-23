# Dockerfile for cloud_canary
#
# Build metadata arguments are required; see README.md for the release command.
#
# Pin base image with SHA256 digest to prevent supply chain attacks
# Image: python:3.11-slim
# To update: docker pull python:3.11-slim && docker inspect python:3.11-slim --format='{{index .RepoDigests 0}}'
FROM python:3.11-slim@sha256:9358444059ed78e2975ada2c189f1c1a3144a5dab6f35bff8c981afb38946634

# Release metadata has no fallback values: a release build must supply the
# application version, Git revision, and RFC 3339 creation timestamp.
ARG VERSION
ARG REVISION
ARG CREATED

LABEL org.opencontainers.image.title="Cloud Canary"
LABEL org.opencontainers.image.description="Prototype Kafka canary for monitoring Confluent Cloud health"
LABEL org.opencontainers.image.source="https://github.com/arvindsrajagopal/cloud_canary"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${REVISION}"
LABEL org.opencontainers.image.created="${CREATED}"
LABEL org.opencontainers.image.vendor="Cloud Canary Team"

WORKDIR /app

# Install the only required operating-system package. Interactive diagnostic
# tools belong in an ephemeral debug container, not the production image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy application source
COPY src/ src/

# Fail the build when release metadata is missing, malformed, or disagrees with
# the application's authoritative version module. Import native dependencies on
# the build platform so a release cannot publish an unusable native wheel.
RUN python -c 'import datetime, importlib, re, sys; from src.__version__ import __version__; version, revision, created = sys.argv[1:]; assert version == __version__, "image version does not match src.__version__"; assert re.fullmatch(r"[0-9a-f]{40}", revision), "revision must be a full Git SHA"; assert created.endswith("Z"); datetime.datetime.fromisoformat(created[:-1] + "+00:00"); [importlib.import_module(name) for name in ("confluent_kafka", "fastavro", "cryptography")]' "$VERSION" "$REVISION" "$CREATED"

# Copy config template (actual config.ini should be mounted at runtime)
COPY config/config.ini.template config/

# Expose Prometheus metrics port (configurable via config.ini)
EXPOSE 8000

# The runtime needs no home directory and no writable path under /app.
RUN useradd --no-create-home --uid 1000 --user-group canary
USER 1000:1000

# Environment variables
# Use JSON logging in Docker containers for log aggregators
ENV CANARY_LOG_FORMAT=json \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Health check - verify the process is live
# The probe reads metrics.port and metrics.ssl.enabled from config.ini.
# start-period gives warmup time before first check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-m", "src.container_probe"]

# Direct exec semantics deliver termination signals to the Python process.
ENTRYPOINT ["python", "-m", "src.main"]
