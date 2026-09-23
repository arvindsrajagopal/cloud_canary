# Dockerfile for cloud_canary
#
# Build:
#   docker build -t cloud-canary:1.2.3 .
#
# Pin base image with SHA256 digest to prevent supply chain attacks
# Image: python:3.11-slim
# To update: docker pull python:3.11-slim && docker inspect python:3.11-slim --format='{{index .RepoDigests 0}}'
FROM python:3.11-slim@sha256:9358444059ed78e2975ada2c189f1c1a3144a5dab6f35bff8c981afb38946634

# Build argument for version (can be overridden during build)
ARG VERSION=1.2.3

LABEL org.opencontainers.image.title="Cloud Canary"
LABEL org.opencontainers.image.description="Prototype Kafka canary for monitoring Confluent Cloud health"
LABEL org.opencontainers.image.source="https://github.com/yourusername/cloud_canary"
LABEL org.opencontainers.image.version="${VERSION}"
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
