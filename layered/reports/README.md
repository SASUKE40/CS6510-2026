# Layered architecture load-test reports

These are fresh reports from the **layered Rust server**, produced by the
unchanged Java client. The Assignment 1 reports remain in `../../reports/`.

## Setup

- Rust 1.98.0, release build; exact dependencies in `../Cargo.lock`.
- Temurin OpenJDK 21.0.12.1+1 on ARM64 macOS 27.0 (26A428).
- Client and server on the same development machine, using
  `http://localhost:8081` because port 8080 was occupied.
- SQLite with WAL and `synchronous=FULL`; one shared connection protected by a
  mutex; synchronous services run on blocking workers.
- Each workload starts from its own reset database with 2,000 SKUs and 10,000
  units per SKU. Low-stock threshold 50; popularity window 1,000 scans, hop 500.
- Default: 10 stations, 60 seconds, 1–20 items per basket.
- Stress: 100 stations, 120 seconds, 1–20 items per basket.

From the repository root, with JDK 21+ on PATH:

```bash
CHECKOUT_PORT=8081 ./layered/load-tests.sh
```

The script checks every SKU's inventory conservation, database integrity,
basket totals, persisted window metadata/ranking, and agreement with the client's
completed-transaction/scan counts. See `verification.txt` in each run directory.
`failures.txt` accounts for every client error using the original local log.
Large repetitive `client.log` files and databases are git-ignored; raw JSON
reports, startup logs, and verification summaries are included.

## Interpretation

The client continues choosing sold-out SKUs. The server accepts scans but rejects
checkout with `409 INSUFFICIENT_STOCK` if any basket item lacks stock, rolling
back the entire basket. These rejections are retained in the JSON error counts.
Scan-based analytics includes those scans, as the OpenAPI contract requires.

The client records latency only for successful requests; completion percentiles
do not include shortage rejections. Items/sec counts scans, not purchased units.
The workloads are single runs on a shared laptop, not controlled repeated trials.
Longer runs can exhaust more SKUs, so completed-transactions/sec alone is not a
clean measure of scaling or of an architectural improvement.

## Measured results

### Default

[report-20260921-201120.json](default/report-20260921-201120.json) — generated 2026-09-22T03:11:20.666793Z.

- 15,263 completed purchases; 316,544 accepted scans over 60.02 seconds.
- 254.30 completed transactions/sec; 5274.04 scans/sec.
- START_TRANSACTION: p95 3.10 ms, p99 4.53 ms; 30,261 successes, 0 errors (0.00%).
- SCAN_ITEM: p95 3.13 ms, p99 4.54 ms; 316,544 successes, 0 errors (0.00%).
- COMPLETE_TRANSACTION: p95 3.57 ms, p99 4.91 ms; 15,263 successes, 14,998 errors (49.56%).
- Every error was an HTTP 409 stock-shortage rejection; no transport, start, or scan errors were recorded. All 2,000 SKU conservation checks passed.

### Stress

[report-20260921-201322.json](stress/report-20260921-201322.json) — generated 2026-09-22T03:13:22.482119Z.

- 23,099 completed purchases; 643,694 accepted scans over 120.13 seconds.
- 192.29 completed transactions/sec; 5358.41 scans/sec.
- START_TRANSACTION: p95 31.32 ms, p99 44.22 ms; 61,244 successes, 0 errors (0.00%).
- SCAN_ITEM: p95 31.40 ms, p99 44.75 ms; 643,694 successes, 0 errors (0.00%).
- COMPLETE_TRANSACTION: p95 32.63 ms, p99 44.60 ms; 23,099 successes, 38,145 errors (62.28%).
- Every error was an HTTP 409 stock-shortage rejection; no transport, start, or scan errors were recorded. All 2,000 SKU conservation checks passed.

Both workloads completed and preserved inventory correctness, but they are not zero-error runs: popular items sold out. More stations increased queueing latency while accepted-scan throughput remained similar, consistent with retaining the same serialized database writer. These runs do not establish a statistically significant performance difference from Assignment 1.
