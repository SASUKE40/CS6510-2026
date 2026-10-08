#!/usr/bin/env bash
# Requires Rust, JDK 21+, Python 3, and curl. The Java client is unchanged.
# Runs the default and stress workloads, each against a freshly reset shared
# database with all five processes, then verifies the stopped database.
set -euo pipefail
cd "$(dirname "$0")/.."
source service-based/scripts/services.sh
build_services
bash load-client/build.sh
mkdir -p service-based/data service-based/reports/default service-based/reports/stress
base_url="http://localhost:$GATEWAY_PORT"
trap 'exit_code=$?; stop_services; exit "$exit_code"' EXIT
require_free_ports
for mode in ${LOAD_MODES:-default stress}; do
  db="service-based/data/$mode.sqlite"
  out="service-based/reports/$mode"
  "$SB_BIN/checkout-db-admin" --db="$db" --reset=true
  start_services "$db" "$out/logs"
  if [[ "$mode" == stress ]]; then
    java -cp load-client/out Main --baseUrl="$base_url" --stations=100 --duration=120 --reportDir="$out" >"$out/client.log" 2>&1
  else
    java -cp load-client/out Main --baseUrl="$base_url" --reportDir="$out" >"$out/client.log" 2>&1
  fi
  stop_services
  report=$(python3 -c 'import pathlib,sys; print(max(pathlib.Path(sys.argv[1]).glob("report-*.json"), key=lambda p:p.stat().st_mtime))' "$out")
  python3 service-based/scripts/verify_database.py "$db" "$report" | tee "$out/verification.txt"
  python3 scripts/summarize_failures.py "$report" "$out/client.log" | tee "$out/failures.txt"
  tail -35 "$out/client.log"
done
