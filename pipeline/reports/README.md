# Pipeline architecture load-test reports

Fresh reports from the **pipeline Rust server**, produced by the unchanged Java
client. Earlier assignments' reports remain in `../../reports/` and
`../../layered/reports/`.

## Setup

- Rust 1.98.0, release build; exact dependencies in `../Cargo.lock`.
- Temurin OpenJDK 21.0.12 on ARM64 macOS 27.0.
- Client and server on the same development machine at `http://localhost:8080`
  (the client's default `--baseUrl`).
- SQLite with WAL and `synchronous=FULL`, one shared connection behind a mutex.
  Popular-items analytics runs in five filter threads connected by bounded channels.
- Each workload starts from its own reset database: 2,000 SKUs, 10,000 units each,
  low-stock threshold 50, popularity window 1,000 scans with a hop of 500.
- Default: 10 stations, 60 seconds, 1–20 items per basket.
- Stress: 100 stations, 120 seconds, 1–20 items per basket.

Reproduce from the repository root with `./pipeline/load-tests.sh`. After each
run, the script stops the server, which drains the pipeline. It then checks:

- database integrity and foreign keys;
- stock conservation for every SKU, and basket totals;
- that the journal's final sequence number equals the client's scan count, so
  every committed scan passed through the pipeline exactly once;
- that the persisted window metadata, ranking, and top 10 match the client report.

See `verification.txt` and `failures.txt` in each run directory.

## Results

### Default

[report-20260929-200339.json](default/report-20260929-200339.json)

- 14,787 completed purchases and 308,059 accepted scans in 60.05 s.
- 246.2 completed transactions/s and 5,129.8 scans/s.
- START_TRANSACTION: p95 3.34 ms, p99 5.41 ms, 0 errors.
- SCAN_ITEM: p95 3.33 ms, p99 5.51 ms, 0 errors.
- COMPLETE_TRANSACTION: p95 3.97 ms, p99 6.14 ms; 14,436 errors (49.40%).
- Final window: scans 307,001–308,000.

### Stress

[report-20260929-200540.json](stress/report-20260929-200540.json)

- 23,795 completed purchases and 672,524 accepted scans in 120.10 s.
- 198.1 completed transactions/s and 5,599.6 scans/s.
- START_TRANSACTION: p95 32.86 ms, p99 62.49 ms, 0 errors.
- SCAN_ITEM: p95 33.07 ms, p99 62.26 ms, 0 errors.
- COMPLETE_TRANSACTION: p95 34.19 ms, p99 62.83 ms; 40,412 errors (62.94%).
- Final window: scans 671,501–672,500.

In both runs, every error was an HTTP 409 `INSUFFICIENT_STOCK` rejection at
checkout. The client keeps choosing popular SKUs after they sell out, and the server
rolls back the whole basket. There were no transport, start, or scan errors.

## Interpretation

Throughput and median latency are close to Assignment 2's layered runs (5,274 and
5,358 scans/s). Stress p99 is higher here (about 62 ms versus 45 ms). These are
single runs on a shared laptop, so neither difference is significant. That result
is expected. The pipeline takes the window bookkeeping out of each scan's SQLite
transaction, but checkout still serializes on one SQLite writer. The journal and
publish filters use that same writer for batched commits. The architectural gain
is separation and extensibility: each stage has one job, and each can be changed,
replaced, or given more buffering without touching checkout code. It does not add
raw capacity.
