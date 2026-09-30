# Assignment 3 — Pipeline architecture (popular-items analytics)

This is the Assignment 2 layered Rust server with one piece of functionality,
the **windowed popular-items analytics**, refactored into a pipes-and-filters
pipeline. Checkout itself (start, scan, complete, low stock) is unchanged; it is
request/response work and does not pipeline naturally.

## Load-test reports (unchanged Java client)

- Default parameters (10 stations, 60 s):
  [`reports/default/report-20260929-200339.json`](reports/default/report-20260929-200339.json)
- Stress mode (`--stations=100 --duration=120`):
  [`reports/stress/report-20260929-200540.json`](reports/stress/report-20260929-200540.json)
- Results, verification, and interpretation: [`reports/README.md`](reports/README.md)

## The pipeline

The system has a 5-stage pipeline:

```text
scan handler ─▶ sequence ─▶ journal ─▶ window ─▶ rank ─▶ publish
  (source)                                                 (sink)
```

| Filter | Purpose |
| --- | --- |
| `sequence` (ingest) | Receives each committed scan's SKU from the checkout service and stamps it with the next global scan sequence number. |
| `journal` | Buffers whatever scans are waiting and appends the whole batch to the SQLite `scans` table in one transaction, trimming scans older than the window. This lets a restarted server rebuild its window. |
| `window` | Holds the most recent 1,000 scans (ring buffer plus running per-SKU counts). Every 500th scan it emits the window's counts with `windowStart`/`windowEnd`. This is the hopping window. |
| `rank` | Sorts a window's counts (most scans first, ties by SKU), assigns ranks, and attaches item names to build the popular-items snapshot. |
| `publish` (sink) | Persists the newest snapshot to the `popularity` table and answers `GET /analytics/popular-items`. |

**Pipes.** Each filter runs on its own OS thread and owns its state. Filters share
no memory. Each pair of neighbours is connected by a bounded Rust
`std::sync::mpsc::sync_channel` (capacity 16,384), which is the Rust equivalent of a
Java `BlockingQueue`. A filter drains every message already waiting (up to 1,024)
as one batch, so the journal commits many scans per SQLite transaction. A full pipe
blocks its producer, which applies backpressure instead of letting memory grow
without limit.

**Asynchronous writes, consistent reads.** `POST /transactions/{id}/items` commits
the basket change and then only enqueues the SKU; it no longer updates analytics
inside its SQLite transaction. Scans that fail validation never enter the pipe. To
keep the synchronous API contract, `GET /analytics/popular-items` sends a *barrier*
message through the same pipes. Because the pipes are FIFO, the barrier reaches
`publish` only after every scan acknowledged earlier. `publish` answers it with the
current snapshot. Readers therefore never see a stale window, while scans do not
wait for the analytics work.

**Shutdown and restart.** On Ctrl+C the HTTP server stops accepting requests. Then a
`Stop` message drains the pipeline stage by stage, and the server joins every
filter thread. Startup reloads the journaled scans, the last sequence number, and
the last published snapshot.

Code: [`src/analytics/pipes.rs`](src/analytics/pipes.rs) (generic filter and pipe
runtime), [`src/analytics/filters.rs`](src/analytics/filters.rs) (the five filters),
and [`src/analytics/mod.rs`](src/analytics/mod.rs) (wiring and the API-facing
entry point).

### Trade-offs compared with Assignment 2

- In the layered server, a scan and its analytics update committed atomically.
  Here a scan commits first and its analytics follow asynchronously. After a
  graceful shutdown nothing is lost. After a crash, scans that were accepted but
  not yet journaled are missing from the window. The baskets, stock, and receipts
  are unaffected. Analytics is derived data, so this is the accepted trade-off.
- The window order is the order in which committed scans reach the pipe. That is
  still a single global sequence, as the contract requires.
- The journal and publish filters still write through the one shared SQLite
  connection. The single-writer bottleneck from Assignments 1 and 2 remains; the
  pipeline moves analytics work off the scan request path but does not remove
  that bottleneck.

## Build, test, and reproduce

From the repository root, with stable Rust installed:

```bash
mkdir -p pipeline/data
cargo run --release --locked --manifest-path pipeline/Cargo.toml -- \
  --db=pipeline/data/checkout.sqlite
```

The server listens on <http://localhost:8080> and prints the pipeline stages at
startup. Documentation is at `/docs`. Add `--reset` to reseed 2,000 SKUs with
10,000 units each. `--help` lists all options.

```bash
cargo test --locked --manifest-path pipeline/Cargo.toml
cargo clippy --locked --manifest-path pipeline/Cargo.toml --all-targets -- -D warnings
./pipeline/load-tests.sh            # CHECKOUT_PORT=8082 if 8080 is taken
```

The load-test script requires JDK 21+, Python 3, and curl. It resets a separate
database for each workload and runs:

```bash
java -cp load-client/out Main --reportDir=pipeline/reports/default
java -cp load-client/out Main --stations=100 --duration=120 --reportDir=pipeline/reports/stress
```

It then verifies the stopped server's database. The ten tests include the
Assignment 2 API regression suite and three new pipeline checks:

- Failed scans never enter the pipe.
- Concurrent scans are each counted exactly once.
- Shutdown drains the pipeline, and a restart resumes the window from the journal.
