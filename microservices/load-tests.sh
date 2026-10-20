#!/usr/bin/env bash
# Requires Rust, JDK 21+, Python 3, and curl. The Java client is unchanged.
# Runs the default and stress workloads, each against freshly reset
# per-service databases, then verifies all four stopped databases together.
set -euo pipefail
cd "$(dirname "$0")/.."
source microservices/scripts/services.sh
build_services
bash load-client/build.sh
mkdir -p microservices/reports/default microservices/reports/stress
base_url="http://localhost:$GATEWAY_PORT"
trap 'exit_code=$?; stop_services; exit "$exit_code"' EXIT
require_free_ports
for mode in ${LOAD_MODES:-default stress}; do
  data="microservices/data/$mode"
  out="microservices/reports/$mode"
  start_services "$data" "$out/logs" true
  if [[ "$mode" == stress ]]; then
    java -cp load-client/out Main --baseUrl="$base_url" --stations=100 --duration=120 --reportDir="$out" >"$out/client.log" 2>&1
  else
    java -cp load-client/out Main --baseUrl="$base_url" --reportDir="$out" >"$out/client.log" 2>&1
  fi
  stop_services
  report=$(python3 -c 'import pathlib,sys; print(max(pathlib.Path(sys.argv[1]).glob("report-*.json"), key=lambda p:p.stat().st_mtime))' "$out")
  python3 microservices/scripts/verify_database.py "$data" "$report" | tee "$out/verification.txt"
  python3 scripts/summarize_failures.py "$report" "$out/client.log" | tee "$out/failures.txt"
  tail -35 "$out/client.log"
done
