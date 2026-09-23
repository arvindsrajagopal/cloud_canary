#!/usr/bin/env bash
# Manage only the optional local Prometheus and Grafana stack.
# The canary is started separately with: python -m src.main

set -euo pipefail

usage() {
    echo "Usage: $0 {start|stop}" >&2
}

if [ "$#" -ne 1 ]; then
    usage
    exit 2
fi

case "$1" in
    start|stop) action="$1" ;;
    *)
        usage
        exit 2
        ;;
esac

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: Docker CLI is required for local monitoring but was not found in PATH." >&2
    echo "Install and start a supported container runtime, then retry '$0 $action'." >&2
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "Error: Docker Compose V2 is required, but 'docker compose' is unavailable." >&2
    echo "Install the Compose V2 plugin for your existing runtime, then retry." >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Error: the Docker runtime is unavailable." >&2
    echo "Start your container runtime outside this helper, then retry '$0 $action'." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/monitoring/docker-compose.yml"

case "$action" in
    start)
        echo "Starting local monitoring stack..."
        docker compose -f "$COMPOSE_FILE" up -d
        echo "Prometheus: http://localhost:9090"
        echo "Grafana:    http://localhost:3000 (admin / admin; local development only)"
        ;;
    stop)
        echo "Stopping local monitoring stack..."
        docker compose -f "$COMPOSE_FILE" down
        ;;
esac
