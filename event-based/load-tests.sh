#!/usr/bin/env bash
# Requires Rust, Docker, JDK 21+, Python 3, and curl. The Java client is unchanged.
# Runs the default and stress workloads, each against a fresh Kafka broker and
# freshly reset per-service databases, then verifies all four stopped
# databases together with the scan topic.
set -euo pipefail
cd "$(dirname "$0")/.."
source event-based/scripts/services.sh
build_services
bash load-client/build.sh
mkdir -p event-based/reports/default event-based/reports/stress
base_url="http://localhost:$GATEWAY_PORT"
trap 'exit_code=$?; stop_services; exit "$exit_code"' EXIT
require_free_ports
for mode in ${LOAD_MODES:-default stress}; do
  data="event-based/data/$mode"
  out="event-based/reports/$mode"
  start_broker true
  start_services "$data" "$out/logs" true
  if [[ "$mode" == stress ]]; then
    java -cp load-client/out Main --baseUrl="$base_url" --stations=100 --duration=120 --reportDir="$out" >"$out/client.log" 2>&1
  else
    java -cp load-client/out Main --baseUrl="$base_url" --reportDir="$out" >"$out/client.log" 2>&1
  fi
  stop_services
  scans_end=$(kafka_cli kafka-get-offsets.sh --topic "$TOPIC_PREFIX.basket.scans" | awk -F: '{print $3}')
  report=$(python3 -c 'import pathlib,sys; print(max(pathlib.Path(sys.argv[1]).glob("report-*.json"), key=lambda p:p.stat().st_mtime))' "$out")
  python3 event-based/scripts/verify_database.py "$data" "$report" "$scans_end" | tee "$out/verification.txt"
  python3 scripts/summarize_failures.py "$report" "$out/client.log" | tee "$out/failures.txt"
  tail -35 "$out/client.log"
done
