# Assignment 4 — Self-Checkout: Service-Based Architecture

The checkout system is split by business function into four **domain services**.
Each service is its own Rust binary and runs as its own OS process. All four use
**one centralized SQLite database**. Services never call each other. They couple
only through the database, using a shared, versioned data-access library.

Submission directory: <https://github.com/SASUKE40/CS6510-2026/tree/main/service-based>

## Services defined in the system

| # | Domain service | Deployable (binary) | Port | API routes it owns |
| - | --- | --- | --- | --- |
| 1 | **Inventory service**: product catalog and low-stock alerts | `inventory-service` | 8081 | `GET /items`, `GET /inventory/low-stock` |
| 2 | **Basket service**: start a transaction, scan items into the basket, basket status | `basket-service` | 8082 | `POST /transactions`, `POST /transactions/{id}/items`, `GET /transactions/{id}` |
| 3 | **Payment service**: complete the purchase (payment, all-or-nothing stock decrement, receipt) | `payment-service` | 8083 | `POST /transactions/{id}/complete` |
| 4 | **Analytics service**: popular items over the hopping window of scans | `analytics-service` | 8084 | `GET /analytics/popular-items` |

Supporting deployables and libraries (not domain services):

- **API gateway** (`checkout-gateway`, port 8080): the single URL the unchanged
  load client knows. It routes each request by path to the owning service and
  relays the response. It has no business logic and no database connection.
  It also serves `/docs` and `/openapi.yaml`.
- **Database admin** (`checkout-db-admin`): a one-shot tool that owns the schema.
  It creates, seeds, and resets the shared database.
- **`checkout-data`** library ([`data-access/`](data-access/)): the shared
  database-access layer. It is the only code that contains SQL.
- **`service-kit`** library ([`service-kit/`](service-kit/)): shared HTTP
  plumbing (JSON errors, CLI options, `/health`, graceful shutdown).

## Load-test reports (unchanged Java client)

- Default parameters (10 stations, 60 s):
  [`reports/default/report-20261007-203911.json`](reports/default/report-20261007-203911.json)
- Stress mode (`--stations=100 --duration=120`):
  [`reports/stress/report-20261007-204114.json`](reports/stress/report-20261007-204114.json)

Each directory also contains `verification.txt` (database invariant checks),
`failures.txt` (every client error accounted for), and `logs/` (one log per process).

## Architecture

```text
                       load client (unchanged, --baseUrl=http://localhost:8080)
                                         │
                              ┌──────────▼──────────┐
                              │  checkout-gateway   │  routing only
                              └──┬──────┬──────┬──┬─┘
          ┌──────────────────────┘      │      │  └───────────────────────┐
┌─────────▼─────────┐ ┌─────────────────▼─┐ ┌──▼────────────────┐ ┌───────▼───────────┐
│ inventory-service │ │  basket-service   │ │  payment-service  │ │ analytics-service │
└─────────┬─────────┘ └─────────┬─────────┘ └─────────┬─────────┘ └─────────┬─────────┘
          │      checkout-data library (one connection per process)         │
          └──────────────────────────┬─────────────────────────────────────┘
                         ┌───────────▼───────────┐
                         │  checkout.sqlite (WAL)│  ◄── checkout-db-admin (schema owner)
                         └───────────────────────┘
```

### No direct service-to-service communication

Two business flows cross service boundaries. Both are carried by the database:

- **Scan → analytics.** The basket service appends every accepted scan to the
  `scans` table, in the same database transaction as the basket line. The
  database assigns the global, gap-free sequence number. The analytics service
  polls that log every 100 ms and publishes a ranking at each 500-scan boundary.
  It then trims scans that no future window can include. A
  `GET /analytics/popular-items` also refreshes before answering. Every
  acknowledged scan is already committed, so readers never see a stale window.
- **Basket → payment.** The payment service reads the basket and its lines
  directly from the `transactions` and `lines` tables. Completion and a racing
  scan are serialized by the database write lock, even though they run in
  different processes.

The gateway's forwarding is client routing; services do not call each other
through it.

### Sharing the database-access logic

All SQL lives in one library crate, `checkout-data`, which every service links
at build time. Its repositories are split into modules by bounded context, and
each service imports only the modules it needs:

| Module | Tables | Written by | Read by |
| --- | --- | --- | --- |
| `catalog` | `items`, `stock_changes` | payment (guarded stock decrement) | inventory, basket, payment |
| `baskets` | `transactions`, `lines` | basket, payment (status → COMPLETED) | basket, payment |
| `scans` | `scans` | basket (append), analytics (trim) | analytics |
| `popularity` | `popularity` | analytics | analytics |
| `admin` | all (DDL, seed, reset) | db-admin only | — |

Design decisions:

- **One shared library, not copies of SQL in each service.** The `stock >= qty`
  guard on stock decrements is part of the inventory invariant. It has to be
  written once, not re-implemented differently in two services. The cost is
  build-time coupling: a library change means rebuilding and redeploying every
  service that uses it. That is the usual service-based trade-off.
- **Schema ownership is centralized in `db-admin`.** No domain service creates
  or migrates tables. `db-admin` stamps `PRAGMA user_version`, and every service
  refuses to start against a database with a different `SCHEMA_VERSION`. A
  service built against an old schema therefore fails fast instead of corrupting
  data.
- **Configuration is stored in the database.** The `settings` row holds the
  low-stock threshold and the window/slide sizes. Each service reads it at
  startup, so all services always agree on these values.
- **Units of work stay inside the database.** `Store::write` runs the
  service's closure in one `BEGIN IMMEDIATE` transaction and commits only on
  success. The repository handle cannot escape the closure. Payment's
  check-status → decrement-every-line → mark-completed sequence is therefore
  atomic. A shortage on any line rolls back the whole basket. No distributed
  transaction or saga is needed, which is the main benefit of keeping a single
  database.
- **Cross-process lock fairness.** SQLite's default busy handler backs off up to
  100 ms per retry, which can let one process starve another. The library installs
  a busy handler that polls every 200 µs (giving up after 10 s), so handoff of
  the write lock between processes stays close to FIFO.

## Results and comparison

Same laptop as earlier assignments: Rust 1.98.0 release build, Temurin JDK 21.0.12,
ARM64 macOS 27.0. SQLite uses WAL and `synchronous=FULL`. Each workload starts
from its own freshly reset database (2,000 SKUs × 10,000 units).

| Run | Scans/s | Completed tx/s | Scan p95 / p99 (ms) | Complete p95 / p99 (ms) | Errors |
| --- | --- | --- | --- | --- | --- |
| Default (10 × 60 s) | 5,200.3 | 252.2 | 3.08 / 4.72 | 6.66 / 9.44 | 14,511 × 409 `INSUFFICIENT_STOCK` |
| Stress (100 × 120 s) | 5,042.6 | 185.9 | 36.11 / 56.22 | 7.35 / 11.86 | 35,381 × 409 `INSUFFICIENT_STOCK` |
| *A3 pipeline, stress* | *5,599.6* | *198.1* | *33.07 / 62.26* | *34.19 / 62.83* | *same cause* |

- **Correctness held under load.** After each run, the stopped database passed:
  integrity and foreign-key checks; stock conservation for all 2,000 SKUs; basket
  totals; scan-log sequence = accepted scans; and the final window and top 10
  matching the client report. There were no transport, start, or scan errors.
  Every error was a whole-basket rejection after a popular SKU sold out, which is
  the same behaviour as in Assignments 1–3.
- **The gateway hop is cheap.** Default-mode throughput and latency match the
  single-process servers (A3 default: 5,130 scans/s, scan p99 5.5 ms). The extra
  loopback hop does not show up in these numbers.
- **Separate services have separate queues.** At 100 stations, completion p99
  dropped from about 62 ms to 12 ms. The most likely reason: in the earlier
  single-process designs, completions queued behind scans for one connection.
  Now the payment process has its own queue and waits only for SQLite's write
  lock. Scans and starts make up about 92% of requests and all go to the basket
  service, so they still
  queue on its single connection (p99 about 56 ms). The bottleneck is the single
  database writer that every service shares. Overall throughput is the same as
  before (single runs on a shared laptop; differences of a few percent are noise).
- **Trade-offs.** Each service can be deployed, restarted, and scaled on its
  own. For example, analytics can restart without losing scans, because they
  are in the shared log. But the shared database is a single point of failure,
  a scalability ceiling, and a coupling point: a schema change touches every
  service that reads the table. Replicating the basket service would not help,
  because the database lock serializes all writers.

## Build, test, and run

From the repository root, with stable Rust installed:

```bash
./service-based/run.sh --reset     # build, seed the shared DB, start all 5 processes
```

The API is then at <http://localhost:8080> (docs at `/docs`). Ctrl+C stops every
process gracefully. Without `--reset`, existing data is kept. Each deployable can
also be started on its own, for example:

```bash
cargo build --release --locked --manifest-path service-based/Cargo.toml
B=service-based/target/release DB=service-based/data/checkout.sqlite
$B/checkout-db-admin --db=$DB            # --reset=true to reseed
$B/inventory-service --db=$DB --port=8081 &
$B/basket-service    --db=$DB --port=8082 &
$B/payment-service   --db=$DB --port=8083 &
$B/analytics-service --db=$DB --port=8084 &
$B/checkout-gateway  --port=8080         # --inventory=URL --basket=URL ... to relocate services
```

Tests and the load-test reproduction (requires JDK 21+, Python 3, and curl; ports
8080–8084 must be free, or override them with `CHECKOUT_PORT`, `INVENTORY_PORT`,
`BASKET_PORT`, `PAYMENT_PORT`, `ANALYTICS_PORT`):

```bash
cargo test --locked --manifest-path service-based/Cargo.toml
cargo clippy --locked --manifest-path service-based/Cargo.toml --workspace --all-targets -- -D warnings
./service-based/load-tests.sh
```

The 11 system tests start every service with its own database connection on an
ephemeral port and drive them through the gateway over TCP. They cover:

- the API contract, validation, and 404/405 responses;
- last-unit and duplicate-completion races across services;
- whole-basket rollback;
- hopping-window boundaries;
- scans racing completion in different services;
- concurrent scans each counted exactly once;
- restart and reset;
- analytics resuming from the shared scan log, and the background publisher;
- gateway routing and the 502 response when a service is down.

The load-test script resets a separate database per workload, runs the unchanged
client, stops all processes, and then verifies the database with
[`scripts/verify_database.py`](scripts/verify_database.py).
