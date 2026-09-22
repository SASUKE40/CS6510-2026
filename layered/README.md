# Assignment 2 — Self-Checkout Supermarket: Layered Architecture

This directory contains the Rust layered implementation and its two required
load-client JSON reports. The Assignment 1 implementation remains in
[`../monolith/`](../monolith/) with its original reports in `../reports/`.

Submission directory: <https://github.com/SASUKE40/CS6510-2026/tree/main/layered>.

## Layers and responsibilities

- **API — [`src/api.rs`](src/api.rs):** Axum routes, request extraction,
  JSON/HTTP responses, status-code mapping, Swagger UI, and dispatch to blocking
  workers. Handlers call application services and contain no SQL or stock rules.
- **Transactions — [`src/transactions.rs`](src/transactions.rs):** station and
  transaction validation, basket state transitions, scanning, receipts, and
  all-or-nothing checkout. The service defines each atomic unit of work.
- **Analytics — [`src/analytics.rs`](src/analytics.rs):** scan-window eviction
  policy, 500-scan recomputation cadence, ranking with deterministic ties,
  snapshot bounds, and result limits. Transactions invokes this service during
  a scan using the same repository/unit of work.
- **Database access — [`src/database.rs`](src/database.rs):** the only module
  importing `rusqlite` or issuing SQL. It owns schema initialization, reset,
  SQLite locking/transactions, repository queries, and persistence. It exposes
  domain records and repository operations, never a raw connection to services.
- **Inventory service — [`src/inventory.rs`](src/inventory.rs):** catalog
  responses and low-stock threshold validation/results.
- **Shared types — [`src/domain.rs`](src/domain.rs), [`src/error.rs`](src/error.rs),
  and [`src/config.rs`](src/config.rs):** records, monetary/timestamp helpers,
  transport-independent errors, and configuration. Monetary state uses integer
  cents; service result values are JSON-ready but contain no Axum types.
- **Composition — [`src/lib.rs`](src/lib.rs):** constructs and connects services
  with one shared database. [`src/main.rs`](src/main.rs) handles CLI/startup.

Dependencies flow from API to application services to database access. The
transaction and analytics services collaborate within the application layer.
The database layer does not call services or the API. This remains one process
and one database, with in-process calls between layers, rather than separate
network services.

## Atomicity and behavior preserved

`Database::write` obtains the mutex, begins an immediate SQLite transaction,
executes a service-supplied closure, and commits only on success. Its repository
cannot outlive that closure or independently commit. Scanning a basket and
updating analytics happen in the same unit of work. A shortage during checkout
rolls back **all** earlier SKU decrements and leaves the basket OPEN. Repeated
completion cannot charge stock twice. WAL and `synchronous=FULL` preserve the
original durability settings.

The API contract, seed catalog/prices, initial stock, Java client, and SQLite
schema are unchanged. Popularity follows the OpenAPI's **scan-based** semantics:
last 1,000 scans, recomputed every 500; scans in unpaid/rejected baskets count.
The first snapshot at scan 500 is partial. Querying between hops returns the
previous persisted snapshot. Low-stock means strictly below the threshold.

Layering adds navigable responsibilities and separates SQL from checkout policy.
It does not remove the single-writer bottleneck or provide horizontal scaling.
The transaction service intentionally coordinates analytics within one database
transaction to retain consistency; changing storage technologies would require
reimplementing the repository and its atomic unit-of-work contract.

## Build and run

From the **repository root**, with stable Rust installed:

```bash
mkdir -p layered/data
cargo run --release --locked --manifest-path layered/Cargo.toml -- \
  --db=layered/data/checkout.sqlite
```

The default address is <http://localhost:8080>. Stop another server on that port
first, or supply `--port=8081` for manual use. Documentation is at `/docs`, and the
original OpenAPI YAML is at `/openapi.yaml`. Swagger UI's assets require internet
access. `--help` lists all options. To reset the application tables and seed
2,000 items with 10,000 units each, stop the server and repeat the command with
`--reset`. Otherwise restarts preserve inventory, baskets, and analytics.

## Tests and the two required reports

```bash
cargo test --locked --manifest-path layered/Cargo.toml
cargo clippy --locked --manifest-path layered/Cargo.toml --all-targets -- -D warnings
./layered/load-tests.sh
```

The load-test script requires JDK 21+ (`java` and `javac` on PATH), Python 3,
curl, and an available port (8080 by default). It runs the unchanged Java client against an optimized
Rust build, with a separate reset database for each workload:

```bash
java -cp load-client/out Main --reportDir=layered/reports/default
java -cp load-client/out Main --stations=100 --duration=120 --reportDir=layered/reports/stress
```

If port 8080 is already in use, select another local port:

```bash
CHECKOUT_PORT=8081 ./layered/load-tests.sh
```

Only the server address changes; the workload settings remain the same. The
submitted runs used port 8081 because another server occupied port 8080.

The default workload is **10 stations for 60 seconds**. Stress is **100 stations
for 120 seconds**. Both use 1–20 units per basket and 10,000 starting units per
SKU. The script takes approximately three minutes plus build time, stops its
server after each test, then checks SQLite integrity, every SKU's stock
conservation, basket totals, persisted analytics, and agreement with client
counts. It also accounts for every reported error from the client log.

- [Default JSON report](reports/default/report-20260921-201120.json)
- [Stress JSON report](reports/stress/report-20260921-201322.json)
- [Measured results and interpretation](reports/README.md)

The seven API regression tests cover catalog/validation, receipts, last-unit
races, duplicate completion, scan/completion races, whole-basket rollback,
restart/reset, and hopping windows. An additional service-level test injects a
failure after the basket and analytics have both changed and confirms that the
shared unit of work rolls both back before the next successful scan.

The client keeps selecting popular items after they sell out. Those checkouts
correctly return `409 INSUFFICIENT_STOCK`; the raw reports preserve these errors.
Latency percentiles cover successful requests only, and items/sec includes scans
in baskets that later fail. See the results notes before interpreting throughput.
