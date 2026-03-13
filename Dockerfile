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
# Environment Variables:
#   CANARY_INSTANCE_ID - Override instance hostname (optional)

FROM python:3.11-slim

# Build argument for version (can be overridden during build)
ARG VERSION=1.2.3

LABEL org.opencontainers.image.title="Cloud Canary"
LABEL org.opencontainers.image.description="Production-grade Kafka canary for Confluent Cloud monitoring"
LABEL org.opencontainers.image.source="https://github.com/yourusername/cloud_canary"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.vendor="Cloud Canary Team"

WORKDIR /app

# Install system dependencies (curl for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ src/

# Copy config template (actual config.ini should be mounted at runtime)
COPY config/config.ini.template config/

# Expose Prometheus metrics port (configurable via config.ini)
EXPOSE 8000

# Run as non-root user for security
RUN useradd -m -u 1000 canary && chown -R canary:canary /app
USER canary

# Environment variables
# Use JSON logging in Docker containers for log aggregators
ENV CANARY_LOG_FORMAT=json

# Health check - verify metrics endpoint is responding
# Using curl instead of python for reliability and security
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/metrics || exit 1

CMD ["python", "-m", "src.main"]
