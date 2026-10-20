# Sourced by run.sh and load-tests.sh (bash 3.2 compatible). Starts each
# microservice as its own OS process with its own database file.
MS_BIN="microservices/target/release"
GATEWAY_PORT="${CHECKOUT_PORT:-8080}"
INVENTORY_PORT="${INVENTORY_PORT:-8081}"
BASKET_PORT="${BASKET_PORT:-8082}"
PAYMENT_PORT="${PAYMENT_PORT:-8083}"
ANALYTICS_PORT="${ANALYTICS_PORT:-8084}"
SERVICE_PIDS=()

build_services() {
  cargo build --release --locked --manifest-path microservices/Cargo.toml
}

require_free_ports() {
  python3 - "$GATEWAY_PORT" "$INVENTORY_PORT" "$BASKET_PORT" "$PAYMENT_PORT" "$ANALYTICS_PORT" <<'PORT_CHECK'
import socket
import sys
# Servers bind with SO_REUSEADDR, so TIME_WAIT leftovers from a previous run
# are fine; only a live listener is a conflict.
for port in map(int, sys.argv[1:]):
    with socket.socket() as probe:
        live = probe.connect_ex(('127.0.0.1', port)) == 0
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            if live:
                raise OSError('a server is already listening')
            listener.bind(('127.0.0.1', port))
        except OSError as error:
            raise SystemExit(f'Port {port} is in use; stop the existing server or override its *_PORT variable: {error}')
PORT_CHECK
}

# start_one NAME PORT LOGDIR ARGS...
start_one() {
  local name=$1 port=$2 logdir=$3
  shift 3
  "$MS_BIN/$name" --port="$port" "$@" >"$logdir/$name.log" 2>&1 &
  local pid=$!
  SERVICE_PIDS+=("$pid")
  for _ in {1..300}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$logdir/$name.log"
      echo "$name exited during startup" >&2
      return 1
    fi
    if curl -fsS "http://127.0.0.1:$port/health" -o /dev/null 2>/dev/null; then
      echo "  $name (pid $pid) ready on :$port"
      return 0
    fi
    sleep 0.1
  done
  echo "$name did not become ready" >&2
  return 1
}

# start_services DATADIR LOGDIR RESET: each service gets DATADIR/<service>.sqlite.
# Inventory starts first because basket replicates its catalog at startup.
start_services() {
  local data=$1 logdir=$2 reset=$3
  local inventory="http://127.0.0.1:$INVENTORY_PORT" basket="http://127.0.0.1:$BASKET_PORT"
  mkdir -p "$data" "$logdir"
  start_one inventory-service "$INVENTORY_PORT" "$logdir" --db="$data/inventory.sqlite" --reset="$reset"
  start_one basket-service "$BASKET_PORT" "$logdir" --db="$data/basket.sqlite" --reset="$reset" \
    --inventory="$inventory"
  start_one payment-service "$PAYMENT_PORT" "$logdir" --db="$data/payment.sqlite" --reset="$reset" \
    --basket="$basket" --inventory="$inventory"
  start_one analytics-service "$ANALYTICS_PORT" "$logdir" --db="$data/analytics.sqlite" --reset="$reset" \
    --basket="$basket"
  start_one checkout-gateway "$GATEWAY_PORT" "$logdir" \
    --inventory="$inventory" --basket="$basket" \
    --payment="http://127.0.0.1:$PAYMENT_PORT" --analytics="http://127.0.0.1:$ANALYTICS_PORT"
}

# Graceful SIGINT in reverse start order: the gateway stops taking requests
# first; analytics pulls the last scan events before basket goes away; payment
# drains its sagas before basket and inventory stop.
stop_services() {
  local i
  for ((i = ${#SERVICE_PIDS[@]} - 1; i >= 0; i--)); do
    kill -INT "${SERVICE_PIDS[$i]}" 2>/dev/null || true
    wait "${SERVICE_PIDS[$i]}" 2>/dev/null || true
  done
  SERVICE_PIDS=()
}
