# Self-Checkout Supermarket assignments

- **Assignment 2 — Layered architecture:** [`layered/`](layered/) contains the
  implementation, run instructions, and both required load-test JSON reports.
  Submit <https://github.com/SASUKE40/CS6510-2026/tree/main/layered>.
- **Assignment 1 — Monolithic architecture:** [`monolith/`](monolith/),
  [`ARCHITECTURE.md`](ARCHITECTURE.md), and [`reports/`](reports/).

The instructions below describe Assignment 1. For the layered server, use the
[Assignment 2 README](layered/README.md).

# Rust monolithic implementation

The monolithic assignment implementation is in `monolith/`: one Axum/Tokio Rust
process with embedded SQLite persistence. The original course client, mock
server, and OpenAPI specification below are unchanged.

## Assignment deliverables

- [`ARCHITECTURE.md`](ARCHITECTURE.md): required quality attributes, measurable
  requirements, three priorities, and explicit trade-offs.
- [`reports/default/`](reports/default/): original JSON output from 10 stations
  for 60 seconds.
- [`reports/stress/`](reports/stress/): original JSON output from 100 stations
  for 120 seconds.
- [`reports/README.md`](reports/README.md): run environment, results, and validation.

## Run the Rust backend

Install a current stable Rust toolchain. SQLite is bundled by `rusqlite`; a
separate SQLite installation or server is not needed.

```bash
cargo build --release --locked --manifest-path monolith/Cargo.toml
mkdir -p data
./monolith/target/release/checkout-monolith --db=data/checkout.sqlite
```

The server listens at `http://localhost:8080`. First startup seeds 2,000 items with
10,000 units each. Later startups preserve data. To explicitly reinitialize this
application's tables before another test, stop the server and run:

```bash
./monolith/target/release/checkout-monolith --db=data/checkout.sqlite --reset
```

`--help` lists port, database, catalog size, stock, low-stock threshold, window
size, and slide interval options. Use the same settings when reopening an
existing database; incompatible settings are rejected rather than silently
mixing experiments. Database files and build outputs are git-ignored.

## Interactive API documentation

From the repository root, start the server with the documentation routes. If an
older version is running, stop it with Ctrl+C first, then rebuild and restart:

```bash
mkdir -p data
cargo run --release --locked --manifest-path monolith/Cargo.toml -- \
  --db=data/checkout.sqlite
```

This preserves existing inventory and transactions. Then open:

- [Swagger UI](http://localhost:8080/docs): interactive API documentation.
- [Raw OpenAPI YAML](http://localhost:8080/openapi.yaml): the API specification.

In Swagger UI, expand an endpoint, select **Try it out**, fill in any parameters
or request body, and click **Execute**. For a checkout, start a transaction first,
then use the returned `transactionId` to scan items and complete the purchase.
These requests operate on the running server's database. Use your configured
port in the links if you changed `--port`.

The HTML and original YAML specification are embedded in the Rust binary at
compile time. Swagger UI's JavaScript and CSS load from the pinned CDN version,
so the interactive page requires internet access. Rebuild/restart the server
after changing the specification. Integration follows the
[Swagger UI standalone installation documentation](https://swagger.io/docs/open-source-tools/swagger-ui/usage/installation/).

## Test and reproduce the submission

The supplied client requires JDK 21+ (`java` and `javac` on `PATH`). The script
also requires Python 3 and curl. With port 8080 free:

```bash
cargo test --locked --manifest-path monolith/Cargo.toml
cargo clippy --locked --manifest-path monolith/Cargo.toml --all-targets -- -D warnings
./scripts/load-tests.sh
```

The script builds release mode, rebuilds the unchanged client, and runs:

```bash
java -cp load-client/out Main --reportDir=reports/default
java -cp load-client/out Main --stations=100 --duration=120 --reportDir=reports/stress
```

Each workload uses its own freshly reset database (`data/default.sqlite` and
`data/stress.sqlite`). The client drains in-flight baskets at the deadline, so
wall time can slightly exceed the requested duration. Reports retain their
original timestamped names and are never edited to remove errors. The older
files under `load-client/reports/` came with the starter and are not the Rust
submission results.

The script stops its server after each workload and checks database integrity,
all 2,000 SKU stock-conservation equations, basket totals, persisted analytics,
and agreement with the client totals. To repeat this check manually:

```bash
python3 scripts/verify_database.py data/default.sqlite reports/default/report-TIMESTAMP.json
```

See `ARCHITECTURE.md` for scan-based hopping-window semantics, shortage handling,
and durability/scalability trade-offs. No real payment service is involved.

---

# Self-Checkout System — Semester Project

A single API contract, to be implemented by a different
architecture style each week. Each implementation is tested every week by the **same**
unmodified load-testing client. Because the client and the contract never
change, the differences you observe week to week come entirely from the
architecture, not from a different test tool.

## What's in this folder

```
self-checkout-project/
├── spec/
│   └── self-checkout-openapi.yaml   ← the shared API contract (OpenAPI 3.0.3)
├── load-client/
│   ├── src/*.java                   ← the load-testing client (zero dependencies)
│   ├── build.sh
│   └── run.sh
└── mockserver/
    ├── MockServer.java              ← optional reference server (see below)
    ├── build.sh
    └── run.sh
```

## The API contract (`spec/self-checkout-openapi.yaml`)

This is what every weekly implementation must satisfy, regardless of its
internal architecture:

| Endpoint                           | Purpose                                                                   |
| ---------------------------------- | ------------------------------------------------------------------------- |
| `GET /items`                       | Full catalog (SKU, name, price) — the client fetches this once at startup |
| `POST /transactions`               | Start a transaction at a station                                          |
| `POST /transactions/{id}/items`    | Scan one unit of an item into the basket                                  |
| `POST /transactions/{id}/complete` | Pay, decrement stock, return a receipt                                    |
| `GET /transactions/{id}`           | Debugging/instructor use only, not exercised by the client                |
| `GET /inventory/low-stock`         | Current low-stock alerts                                                  |
| `GET /analytics/popular-items`     | Most-scanned items in the current sliding window                          |

Some issues to note:

- **Stock is decremented at  transaction*completion*, not at scan time.** The physical
  metaphor is that the customer already has the item in hand when they scan
  it — the backend's job is just to keep an accurate count, not to gate the
  scan. 
- **Every endpoint is synchronous**, no matter what a given week's internal
  architecture does. Event-driven or orchestration-driven weeks are free to
  use events, queues, or an orchestration engine *internally*, but the
  client-facing contract never changes. This is what keeps the same load
  client valid for every week.
- **Popular items use a hopping window**: the server considers the most
  recent `windowSize` scans (spec default 1000) and recomputes every
  `slideInterval` scans (spec default 500). The response includes
  `windowStart`/`windowEnd` so students can show their work.

## The load client (`load-client/`)

A Java client using the JDK's built-in java.net.http.HttpClient` and a hand-rolled JSON
reader/writer — this removes all external dependencies. The CLI options below default to values to use for each weekly submission. 

### Build & run

Requires a full JDK 21+ (not just a JRE), since javac` needs to be installed:

```bash
cd load-client
./build.sh
./run.sh --baseUrl=http://localhost:8080 --stations=10 --duration=60
```

### CLI options

| Flag                        | Default                 | Meaning                                         |
| --------------------------- | ----------------------- | ----------------------------------------------- |
| `--baseUrl`                 | `http://localhost:8080` | Base URL of the system under test               |
| `--stations`                | `10`                    | Concurrent simulated checkout stations          |
| `--duration`                | `60`                    | Test length in seconds                          |
| `--minItems` / `--maxItems` | `1` / `20`              | Basket size range per transaction               |
| `--popularLimit`            | `10`                    | How many popular items to request at the end    |
| `--requestTimeout`          | `10`                    | Per-request timeout, seconds                    |
| `--verbose`                 | `false`                 | Print every completed transaction as it happens |
| `--reportDir`               | `./reports`             | Where the JSON report file is written           |

Run `./run.sh --help` for the same, from the tool itself.

**Stress mode** is just the same client with a bigger `--stations` value —
e.g. `--stations=200 --duration=180` — useful specifically for the weeks
where scalability differences between styles are the point (microservices,
event-driven), since at the default 10-station scale most architectures
will feel instantly fast regardless of style.

### What the report shows

At the end of a run, the client prints a console report and writes a JSON
file to `--reportDir` (default `./reports`), so results from different
weeks can be diffed or charted later:

- Per-operation (`START_TRANSACTION`, `SCAN_ITEM`, `COMPLETE_TRANSACTION`):
  success/error counts, mean, p50, p95, p99, max latency, and error rate.
  **Use the percentiles, not just the mean** — tail latency is usually
  where an architecture's weaknesses (lock contention, network hops,
  orchestration overhead) actually show up.
- Overall throughput (transactions/sec, items/sec).
- Current low-stock alerts.
- Current most-popular items.

The item-popularity sampling is intentionally **not uniform random** — it
uses a Zipf-like weighting (`ItemSampler.java`) so a small number of items
get scanned disproportionately often, the same way real retail sales work.
Without this, the popular-items feature would have nothing meaningful to
detect.

## The mock server (`mockserver/`)

`MockServer.java` is a bare-bones, single-file reference implementation of
the contract, built the same zero-dependency way as the client (just the
JDK's built-in `com.sun.net.httpserver`). This gives you something real to 

point the client at **before ** any implementation exists. You can see a full 

report end to end and understand the contract by example. 

It is **not** an example of good architecture — it's a handful of
`ConcurrentHashMap`s behind an HTTP server, deliberately uninteresting.


```bash
cd mockserver
./build.sh
./run.sh 8080 2000 10000 50   # port, catalogSize, stockPerItem, lowStockThreshold
```

## The concurrency gotcha

With 10+ stations completing transactions concurrently against the sameinventory, a naive "read stock, check it, then write stock minus one" doneas two separate steps (a read call followed by a write call, or even twonon-atomic statements against a shared database row without appropriatelocking) can let two stations both succeed in buying the last unit of anitem. In the monolith and layered weeks this is easy to get right byaccident, because it's all one process talking to one local transaction. Itgets *much* easier to get wrong once inventory becomes its own service(service-based, microservices) and the check-then-decrement happens acrossa network call.

Suggested correctness check for grading, independent of any performancenumber: **for every SKU, `initial_stock - final_stock` must equal the totalnumber of completed-transaction line items for that SKU, and final stockmust never go negative.** A student's implementation can pass everyfunctional test and still fail this invariant under load — that's thepoint.



## Grading - things to look for

Submit the timestamped JSON report for each architecture in a 
`reports/report-*.json` directory alongside your code, and compare:

- Does the system complete runs under normal and stress mode workloads. How do latencies compare?
- How do p95/p99 latencies move relative to the previous week's numbers on
  the same hardware? 
- Does the popular-items ranking stay stable across implementations (it
  should — it's testing the analytics feature, not the architecture)?

