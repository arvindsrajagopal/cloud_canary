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

# If USE_CUSTOM_DNS is set, configure DNS servers
# This is useful when Docker's default DNS (127.0.0.11) has issues
if [ -n "$USE_CUSTOM_DNS" ]; then
    echo "Configuring custom DNS servers..."

    # Backup original resolv.conf (Docker-managed)
    cp /etc/resolv.conf /etc/resolv.conf.docker-backup

    # Write new resolv.conf with custom DNS
    cat > /etc/resolv.conf <<EOF
# Custom DNS configuration for container
# Original Docker DNS backed up to /etc/resolv.conf.docker-backup
options timeout:2 attempts:3 rotate
nameserver 8.8.8.8
nameserver 8.8.4.4
nameserver 1.1.1.1
EOF

    echo "Custom DNS configured: 8.8.8.8, 8.8.4.4, 1.1.1.1"
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
        echo "Consider running with: docker run -e USE_CUSTOM_DNS=1 ..."
        # Don't exit - let the application try anyway
    fi
fi

# Execute the main application
exec "$@"
