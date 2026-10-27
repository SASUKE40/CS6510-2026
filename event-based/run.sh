#!/usr/bin/env bash
# Builds and starts Kafka and every microservice for manual use; Ctrl+C stops
# the services (the broker keeps running: docker compose -f
# event-based/docker-compose.yml down -v removes it).
# Usage: ./event-based/run.sh [--reset]
set -euo pipefail
cd "$(dirname "$0")/.."
source event-based/scripts/services.sh
data="${CHECKOUT_DATA:-event-based/data/dev}"
reset=false
[[ "${1:-}" == "--reset" ]] && reset=true
build_services
require_free_ports
start_broker "$reset"
trap 'stop_services' EXIT
trap 'exit 130' INT TERM
start_services "$data" event-based/data/logs "$reset"
echo "Self-checkout API: http://localhost:$GATEWAY_PORT (docs at /docs). Ctrl+C to stop."
wait
