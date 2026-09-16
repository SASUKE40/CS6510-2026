#!/usr/bin/env bash
# Requires Rust, JDK 21+, Python 3, and curl. The Java client is unchanged.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --locked --manifest-path monolith/Cargo.toml
bash load-client/build.sh
mkdir -p data reports/default reports/stress
server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill -INT "$server_pid" 2>/dev/null || true
    wait "$server_pid" || true
  fi
}
trap 'exit_code=$?; cleanup; exit "$exit_code"' EXIT
for mode in default stress; do
  db="data/$mode.sqlite"
  monolith/target/release/checkout-monolith --db="$db" --reset >"reports/$mode/server.log" 2>&1 &
  server_pid=$!
  ready=false
  for _ in {1..100}; do
    if ! kill -0 "$server_pid" 2>/dev/null; then cat "reports/$mode/server.log"; exit 1; fi
    if curl -fsS http://localhost:8080/items -o /dev/null 2>/dev/null; then ready=true; break; fi
    sleep 0.1
  done
  [[ "$ready" == true ]] || { echo 'Server did not become ready'; exit 1; }
  # Check it is still our server after probing; avoid testing a different listener.
  kill -0 "$server_pid"
  if [[ "$mode" == stress ]]; then
    java -cp load-client/out Main --stations=100 --duration=120 --reportDir="reports/$mode" >"reports/$mode/client.log" 2>&1
  else
    java -cp load-client/out Main --reportDir="reports/$mode" >"reports/$mode/client.log" 2>&1
  fi
  cleanup
  server_pid=''
  report=$(python3 -c 'import pathlib,sys; print(max(pathlib.Path(sys.argv[1]).glob("report-*.json"), key=lambda p:p.stat().st_mtime))' "reports/$mode")
  python3 scripts/verify_database.py "$db" "$report" | tee "reports/$mode/verification.txt"
  python3 scripts/summarize_failures.py "$report" "reports/$mode/client.log" | tee "reports/$mode/failures.txt"
  tail -35 "reports/$mode/client.log"
done
