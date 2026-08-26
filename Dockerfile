# Dockerfile for cloud_canary
#
# Build:
#   docker build -t cloud-canary:latest .
#
# Run:
#   docker run --rm \
#     -e CANARY_INSTANCE_ID=my-canary-1 \
#     -v $(pwd)/config/config.ini:/app/config/config.ini:ro \
#     -p 8000:8000 \
#     cloud-canary:latest
#
# Run with HTTPS:
#   docker run --rm \
#     -v $(pwd)/config/config.ini:/app/config/config.ini:ro \
#     -v $(pwd)/certs:/certs:ro \
#     -p 8000:8000 \
#     cloud-canary:latest
#   (Ensure config.ini has metrics.ssl.enabled=true and cert/key paths)
#
# DNS Troubleshooting:
#   If you encounter DNS resolution issues with Confluent Cloud:
#
#   1. Run diagnostics:
#      docker run --rm cloud-canary:latest diagnose
#
#   2. Use custom DNS servers (recommended method):
#      docker run --rm \
#        --dns 8.8.8.8 --dns 8.8.4.4 \
#        -v $(pwd)/config/config.ini:/app/config/config.ini:ro \
#        -p 8000:8000 \
#        cloud-canary:latest
#
#   3. Or use docker-compose.yml with DNS configuration:
#      services:
#        cloud-canary:
#          dns:
#            - 8.8.8.8
#            - 8.8.4.4
#
# Environment Variables:
#   CANARY_INSTANCE_ID - Override instance hostname (optional)
#   DNS_CHECK - Run DNS check before startup (true=enabled)
#   KAFKA_BOOTSTRAP_SERVER - Bootstrap server for DNS testing

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

# Install system dependencies
# - curl: healthcheck endpoint testing
# - dnsutils: DNS troubleshooting (nslookup, dig)
# - iputils-ping: network connectivity testing
# - ca-certificates: ensure SSL/TLS certificates are up to date
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        dnsutils \
        iputils-ping \
        ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ src/

# Copy config template (actual config.ini should be mounted at runtime)
COPY config/config.ini.template config/

# Copy and configure entrypoint script for DNS handling
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose Prometheus metrics port (configurable via config.ini)
EXPOSE 8000

# Run as non-root user for security
RUN useradd -m -u 1000 canary && chown -R canary:canary /app
USER canary

# Environment variables
# Use JSON logging in Docker containers for log aggregators
ENV CANARY_LOG_FORMAT=json

# Health check - verify application is healthy
# Uses /health endpoint which returns 200 only when healthy
# start-period gives warmup time before first check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Use entrypoint script to handle DNS configuration and diagnostics
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Default command - can be overridden (e.g., "diagnose" for DNS troubleshooting)
CMD ["python", "-m", "src.main"]
