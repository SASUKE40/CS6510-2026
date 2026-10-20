#!/usr/bin/env bash
# Builds and starts every microservice for manual use; Ctrl+C stops them all.
# Usage: ./microservices/run.sh [--reset]
set -euo pipefail
cd "$(dirname "$0")/.."
source microservices/scripts/services.sh
data="${CHECKOUT_DATA:-microservices/data/dev}"
reset=false
[[ "${1:-}" == "--reset" ]] && reset=true
build_services
require_free_ports
trap 'stop_services' EXIT
trap 'exit 130' INT TERM
start_services "$data" microservices/data/logs "$reset"
echo "Self-checkout API: http://localhost:$GATEWAY_PORT (docs at /docs). Ctrl+C to stop."
wait
