# Quality attributes: Rust self-checkout monolith

Assignment: [Self-Checkout Supermarket — Monolithic Architecture](https://northeastern.instructure.com/courses/260761/assignments/3398707).

## Architectural characteristics and concrete requirements

These are design/acceptance targets, not claims about measured performance; the
supplied client's actual measurements are recorded separately in `reports/`.

- **Correctness and consistency:** for every SKU, initial stock minus final stock
  equals units in successfully completed transactions. Stock never becomes
  negative. Completion is atomic across the whole basket; simultaneous/repeated
  completion can decrement inventory only once. Scans do not change stock.
  Prices and basket totals use integer cents internally.
- **Performance:** with 10 stations for 60 seconds, target p95 below 100 ms and
  p99 below 1 second for each operation. At 100 stations for 120 seconds, target
  p99 below 1 second and no request exceeding the client's 10-second timeout.
  Run the supplied client unmodified, with 1–20 units per basket.
- **Durability and recoverability:** an acknowledged start, scan, or completion
  survives a process restart. Persist all 2,000 catalog items, inventory, and
  popularity results in a local database. Reopening preserves transactions;
  explicit `--reset` restores exactly 10,000 units per SKU by default.
- **Concurrency and scalability:** support at least 10 simultaneous station
  clients and exercise 100, without a hardcoded station limit. Queue database
  work safely and preserve the inventory invariant at either concurrency level.
- **Interoperability:** implement all seven routes and response fields in
  `spec/self-checkout-openapi.yaml`; use JSON errors and the specified status
  codes. Return 409 on insufficient inventory as an uncompletable transaction.
- **Analytics accuracy:** count successful item scans with global, one-based
  sequence numbers. Persist a ranking every 500 scans over the latest at most
  1,000 scans; rank by count descending, SKU ascending for deterministic ties.
  Return the latest stored snapshot, including its inclusive bounds and timestamp.
  The first snapshot is at scan 500 (a partial window); before then it is empty
  with bounds 0/0. The default result contains the top 10.
- **Observability and testability:** retain both raw JSON load reports, record
  failures without suppressing them, and verify inventory conservation against
  the completed transaction ledger after each run. Test last-unit races,
  duplicate completion, rollback, restart, and exact window boundaries.
- **Maintainability and deployability:** build and run one Rust executable plus
  one SQLite database file, with no separate database service. Keep the client
  and API specification unchanged so later architecture assignments are comparable.
- **Security:** bind to loopback for the laptop experiment, use parameterized SQL,
  validate request types and query arguments, and avoid returning database details
  in errors. Authentication, TLS, authorization, and real payment processing are
  outside this simulation; those would be required before public deployment.

## Three priorities and their trade-offs

1. **Correctness and consistency.** A process-wide mutex serializes database
   operations, and each scan/completion uses a SQLite `BEGIN IMMEDIATE`
   transaction. Guarded stock updates, basket lines, stock history, and the
   COMPLETED state commit together. A shortage rolls back every update in the
   basket. This sacrifices write parallelism and can increase tail latency as
   station count grows. Keeping the accurate 409 failure is preferable to
   reporting a sale that has no inventory backing it.
2. **Durability and recoverability.** SQLite uses WAL and `synchronous=FULL`.
   Open baskets, scans, item quantities, and the latest analytics result are
   durable, rather than reconstructing them from in-memory state after a crash.
   Per-scan commits cost disk work and throughput; retaining transaction and
   stock-change history grows the database. A single local database also remains
   a machine-level point of failure: this design does not provide high availability
   or protect against disk loss. Backups and archival are future work.
3. **Performance within the assignment's workload.** Axum/Tokio accepts
   concurrent HTTP requests and sends synchronous SQLite work to blocking workers.
   Indexed SKU lookups, aggregated basket lines, and a scan buffer capped at 1,000
   entries keep request work small. Popularity recomputation happens only at
   500-scan boundaries. This trades analytics freshness (up to 499 scans stale)
   for lower work per request. Retaining a single DB connection favors simplicity
   over horizontal scalability; multi-process replicas are not the deployment
   model being evaluated.

## Implementation decisions

One process owns HTTP routing, transaction logic, inventory, analytics, and the
embedded database. There are no independently deployable services or queues.
`monolith/src/lib.rs` contains the application and routes;
`monolith/src/schema.sql` defines persistence; `monolith/src/main.rs` handles
configuration and startup.

The assignment prose mentions purchased-item popularity, whereas the supplied
OpenAPI explicitly requires **scans**. This implementation follows that precise
contract, so scans in abandoned or stock-rejected baskets still count. Stock is
only decremented after successful completion. Stock is not reserved at scan time.
The database stores the full ranking at each hop (including the required top ten),
allowing larger `limit` queries without recomputing a different window.

Low-stock means strictly below the requested threshold. Stock-decrement history
preserves the timestamp at which a SKU first crossed that threshold, including
query overrides; repeated reads do not fabricate new trigger times. This assumes
the assignment's decrement-only inventory model between resets.

There is no 20-item server-side cap: 1–20 is the load generator's basket range,
not an API constraint. Payment always succeeds unless inventory prevents checkout.
A retry after an ambiguous completion response returns 409; the debugging GET
route exposes whether that transaction completed. Production payment idempotency
would require extending this protocol.
