#!/bin/bash
set -e

# Docker Entrypoint Script for Cloud Canary
# Handles DNS configuration and runtime initialization

# Function to check if DNS resolution is working
check_dns() {
    local bootstrap="${KAFKA_BOOTSTRAP_SERVER:-pkc-xxxxx.region.provider.confluent.cloud}"
    echo "Testing DNS resolution for: $bootstrap"

    if timeout 5 nslookup "$bootstrap" > /dev/null 2>&1; then
        echo "✓ DNS resolution working"
        return 0
    else
        echo "✗ DNS resolution failed"
        return 1
    fi
}

# Note: USE_CUSTOM_DNS environment variable is deprecated and removed for security.
# Container runs as non-root user (UID 1000) and cannot modify /etc/resolv.conf.
# To use custom DNS servers, use Docker's built-in --dns flag instead:
#   docker run --dns 8.8.8.8 --dns 8.8.4.4 ...
# Or in docker-compose.yml:
#   dns:
#     - 8.8.8.8
#     - 8.8.4.4
if [ -n "$USE_CUSTOM_DNS" ]; then
    echo "WARNING: USE_CUSTOM_DNS is deprecated and has no effect."
    echo "Use Docker's --dns flag instead: docker run --dns 8.8.8.8 --dns 8.8.4.4 ..."
fi

# Diagnostic mode - run DNS checks and exit
if [ "$1" = "diagnose" ]; then
    echo "=== Cloud Canary DNS Diagnostics ==="
    echo ""
    echo "Current DNS configuration:"
    cat /etc/resolv.conf
    echo ""

    echo "Testing DNS resolution:"
    check_dns || true

    echo ""
    echo "Attempting to resolve common Confluent Cloud domains:"
    for domain in \
        "confluent.cloud" \
        "pkc-xxxxx.us-east-1.aws.confluent.cloud" \
        "psrc-xxxxx.us-east-1.aws.confluent.cloud"; do
        echo -n "  $domain ... "
        if timeout 3 nslookup "$domain" > /dev/null 2>&1; then
            echo "✓"
        else
            echo "✗ FAILED"
        fi
    done

    echo ""
    echo "Network interfaces:"
    ip addr show || ifconfig

    exit 0
fi

# Normal startup - log DNS status
echo "Cloud Canary starting..."
echo "Instance ID: ${CANARY_INSTANCE_ID:-$(hostname)}"
echo "DNS servers: $(grep nameserver /etc/resolv.conf | awk '{print $2}' | tr '\n' ' ')"

# Optional: Check DNS before starting (set DNS_CHECK=true to enable)
if [ "$DNS_CHECK" = "true" ]; then
    if ! check_dns; then
        echo "WARNING: DNS resolution check failed. Application may not be able to connect to Confluent Cloud."
        echo "Consider using custom DNS: docker run --dns 8.8.8.8 --dns 8.8.4.4 ..."
        # Don't exit - let the application try anyway
    fi
fi

# Execute the main application
exec "$@"
