#!/usr/bin/env bash
# run.sh — Start cloud_canary together with its monitoring stack.
#
# Usage:
#   ./run.sh [INSTANCE_ID]
#
# Examples:
#   ./run.sh                    # Uses hostname as instance ID
#   ./run.sh canary-dev-1       # Sets custom instance ID
#   CANARY_INSTANCE_ID=my-id ./run.sh  # Via environment variable
#
# What it does:
#   1. Starts Prometheus + Grafana via docker compose (detached)
#   2. Launches the canary app in the foreground
#   3. Tears down the monitoring stack on exit — including SIGKILL and crashes,
#      because the trap fires in the shell process even when the child is killed.
#
# Prometheus : http://localhost:9090
# Grafana    : http://localhost:3000  (admin / admin)
# Metrics    : http://localhost:8000/metrics

set -euo pipefail

# Optional: Set instance ID from command-line argument
# Usage: ./run.sh canary-dev-1
if [ $# -ge 1 ]; then
    export CANARY_INSTANCE_ID="$1"
    echo "Using instance ID: $CANARY_INSTANCE_ID"
elif [ -n "${CANARY_INSTANCE_ID:-}" ]; then
    echo "Using instance ID from environment: $CANARY_INSTANCE_ID"
else
    echo "Using default instance ID (hostname)"
fi

# Ensure Colima (container runtime) is running before attempting docker compose.
if ! colima status &>/dev/null; then
    echo "Colima is not running — starting it now..."
    colima start
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/monitoring/docker-compose.yml"

cleanup() {
    echo ""
    echo "Stopping monitoring stack..."
    docker compose -f "$COMPOSE_FILE" down
}

trap cleanup EXIT

echo "Starting monitoring stack..."
docker compose -f "$COMPOSE_FILE" up -d

echo ""
echo "  Prometheus : http://localhost:9090"
echo "  Grafana    : http://localhost:3000  (admin / admin)"
echo "  Metrics    : http://localhost:8000/metrics"
echo ""

"$SCRIPT_DIR/.venv/bin/python" -m src.main
