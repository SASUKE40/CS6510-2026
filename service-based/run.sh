#!/usr/bin/env bash
# Builds and starts every deployable for manual use; Ctrl+C stops them all.
# Usage: ./service-based/run.sh [--reset]
set -euo pipefail
cd "$(dirname "$0")/.."
source service-based/scripts/services.sh
db="${CHECKOUT_DB:-service-based/data/checkout.sqlite}"
reset=false
[[ "${1:-}" == "--reset" ]] && reset=true
build_services
require_free_ports
mkdir -p "$(dirname "$db")"
"$SB_BIN/checkout-db-admin" --db="$db" --reset="$reset"
trap 'stop_services' EXIT
trap 'exit 130' INT TERM
start_services "$db" service-based/data/logs
echo "Self-checkout API: http://localhost:$GATEWAY_PORT (docs at /docs). Ctrl+C to stop."
wait
