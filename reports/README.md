# Monolithic Rust load-test results

The timestamped JSON files in `default/` and `stress/` are the unmodified output
of the supplied Java client against the Rust implementation, not the mock server.
The default workload uses 10 stations for 60 seconds. Stress uses exactly
`--stations=100 --duration=120`. Both retain the default 1–20-unit baskets and
start with a fresh database containing 2,000 SKUs and 10,000 units per SKU.

## Environment and reproduction

- Local ARM64 macOS 27.0 (build 26A428), with client and server on the same machine.
- Rust 1.98.0; optimized `cargo build --release --locked`.
- Temurin OpenJDK 21.0.12.1+1; unchanged course client compiled using `javac`.
- Axum/Tokio, bundled SQLite, WAL, `synchronous=FULL`; exact Rust dependencies
  are recorded in `monolith/Cargo.lock`.
- One server process and one serialized SQLite connection per run; database
  work runs on Tokio blocking workers. Default 1,000-scan window, 500-scan hop,
  and low-stock threshold 50.
- No artificial delays, replenishment, increased initial stock, modified item
  distribution, or client retries were introduced to improve the reported score.
- Run date: 2026-09-15 Pacific / 2026-09-16 UTC. JSON timestamps use UTC; report
  filenames use the client's local time.

Run `./scripts/load-tests.sh` from the repository root with a JDK 21+ on PATH.
For this machine, a temporary official Temurin JDK was downloaded to
`/tmp/cs6510-jdk21/Contents/Home`; it is not a project dependency or committed file.

## How to interpret errors and timing

The client continuously samples a Zipf distribution without considering remaining
stock. SKU-000001 receives roughly 12% of scans. With only 10,000 initial units,
a fast server can sell out popular SKUs during either workload. Scanning remains
valid even when stock is zero, as required by the API's physical-item model.
Completion rejects the entire basket with `409 INSUFFICIENT_STOCK` when any SKU
cannot supply the requested quantity. Those responses appear in the client's
completion error count. Failed baskets remain OPEN and do not decrement any SKU.

These are application-level shortage rejections, not transport failures or lost
updates. `failures.txt` accounts for every error using the original client log.
The full logs remain local as ignored `client.log` files because they contain
many repetitive shortage messages. The JSON results preserve all error counts.
`server.log` records startup configuration. `verification.txt` records database
checks after the server was stopped.

The client computes latency statistics for **successful requests only**; rejected
completion latency is not represented in its completion percentiles. Items/sec
counts accepted scans, including scans in rejected baskets; transactions/sec
counts only completed purchases. Thus this workload is not a pure steady-state
checkout benchmark after stock exhaustion. Do not interpret its error rate as
server unavailability or compare throughput without considering this effect.

These are single measured runs on a shared development laptop, not repeated
controlled trials. Client and server compete for the same CPU and storage.

## Measured results

### Default: 10 stations, 60 seconds

Raw report: [report-20260915-200422.json](default/report-20260915-200422.json).

- Completed purchases: 15,329; accepted scans: 319,722.
- Throughput: 255.39 completed transactions/sec; 5326.72 scans/sec.
- START_TRANSACTION: p95 3.06 ms, p99 4.62 ms; 30,453 successes, 0 errors (0.00%).
- SCAN_ITEM: p95 3.09 ms, p99 4.57 ms; 319,722 successes, 0 errors (0.00%).
- COMPLETE_TRANSACTION: p95 3.51 ms, p99 4.90 ms; 15,329 successes, 15,124 errors (49.66%).
- All errors were HTTP 409 inventory-shortage rejections; no start/scan or transport errors were recorded.
- Sold-out SKUs at the end: 1. All 2,000 inventory conservation checks passed.

### Stress: 100 stations, 120 seconds

Raw report: [report-20260915-200624.json](stress/report-20260915-200624.json).

- Completed purchases: 23,352; accepted scans: 648,240.
- Throughput: 194.43 completed transactions/sec; 5397.34 scans/sec.
- START_TRANSACTION: p95 31.34 ms, p99 47.07 ms; 61,862 successes, 0 errors (0.00%).
- SCAN_ITEM: p95 31.35 ms, p99 45.75 ms; 648,240 successes, 0 errors (0.00%).
- COMPLETE_TRANSACTION: p95 31.84 ms, p99 48.74 ms; 23,352 successes, 38,510 errors (62.25%).
- All errors were HTTP 409 inventory-shortage rejections; no start/scan or transport errors were recorded.
- Sold-out SKUs at the end: 2. All 2,000 inventory conservation checks passed.

### Interpretation

Ten times as many stations increased accepted-scan throughput by only 1.3%, while successful completion p99 rose from 4.90 ms to 48.74 ms. This is consistent with queueing behind a serialized database writer, although these client reports alone do not isolate the bottleneck. Both runs meet the stated latency targets for successful requests. Completion rejection rates are high because finite stock was exhausted; they cannot be treated as a zero-error functional pass. Inventory integrity held for every successful purchase, and no inventory was decremented for rejected baskets.

The longer stress run sold out a second SKU, reducing the fraction of baskets that could complete. This makes completed-transactions/sec unsuitable as a direct scaling comparison between these two different-duration, stock-depleting runs. A future supplemental scaling experiment could use repeated fixed-duration runs and sufficient stock, but the required reports here retain the assignment's exact 10,000-unit initialization.
