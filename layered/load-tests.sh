#!/usr/bin/env bash
# Requires Rust, JDK 21+, Python 3, and curl. The Java client is unchanged.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --locked --manifest-path layered/Cargo.toml
bash load-client/build.sh
mkdir -p layered/data layered/reports/default layered/reports/stress
test_port="${CHECKOUT_PORT:-8080}"
base_url="http://localhost:$test_port"
server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill -INT "$server_pid" 2>/dev/null || true
    wait "$server_pid" || true
  fi
}
trap 'exit_code=$?; cleanup; exit "$exit_code"' EXIT
python3 - "$test_port" <<'PORT_CHECK'
import socket
import sys
port = int(sys.argv[1])
with socket.socket() as listener:
    try:
        listener.bind(('127.0.0.1', port))
    except OSError as error:
        raise SystemExit(f'Port {port} is in use; stop the existing server or set CHECKOUT_PORT: {error}')
PORT_CHECK
for mode in default stress; do
  db="layered/data/$mode.sqlite"
  layered/target/release/checkout-layered --db="$db" --port="$test_port" --reset >"layered/reports/$mode/server.log" 2>&1 &
  server_pid=$!
  ready=false
  for _ in {1..100}; do
    if ! kill -0 "$server_pid" 2>/dev/null; then cat "layered/reports/$mode/server.log"; exit 1; fi
    if curl -fsS "$base_url/items" -o /dev/null 2>/dev/null; then ready=true; break; fi
    sleep 0.1
  done
  [[ "$ready" == true ]] || { echo 'Server did not become ready'; exit 1; }
  # Check it is still our server after probing; avoid testing a different listener.
  kill -0 "$server_pid"
  if [[ "$mode" == stress ]]; then
    java -cp load-client/out Main --baseUrl="$base_url" --stations=100 --duration=120 --reportDir="layered/reports/$mode" >"layered/reports/$mode/client.log" 2>&1
  else
    java -cp load-client/out Main --baseUrl="$base_url" --reportDir="layered/reports/$mode" >"layered/reports/$mode/client.log" 2>&1
  fi
  cleanup
  server_pid=''
  report=$(python3 -c 'import pathlib,sys; print(max(pathlib.Path(sys.argv[1]).glob("report-*.json"), key=lambda p:p.stat().st_mtime))' "layered/reports/$mode")
  python3 scripts/verify_database.py "$db" "$report" | tee "layered/reports/$mode/verification.txt"
  python3 scripts/summarize_failures.py "$report" "layered/reports/$mode/client.log" | tee "layered/reports/$mode/failures.txt"
  tail -35 "layered/reports/$mode/client.log"
done
