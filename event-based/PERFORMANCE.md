# Why the performance improved

## Basic rule

- Each station sends one request. Then it waits for the response.
- Thus, a shorter wait gives more requests each second.

## 1. Scans do not wait for Kafka

- Before: a scan waited for Kafka to keep its event.
- Now: a scan returns after the database commit.
- The event is safe in the outbox, so no event is lost.
- Scans are approximately 84% of all requests.
- Result (default load): the median scan time decreased from 3.08 ms to 1.06 ms.
- Result (default load): throughput increased from 1,498 to 2,903 scans/s.

## 2. The receipt comes at the pivot

- Before: payment waited for basket to complete the basket.
- Now: payment sends the receipt when it records the payment.
- After this point, the checkout cannot fail. Recovery completes the basket.
- This removes 2 of the 6 Kafka messages from the wait.
- Result (default load): the median completion time decreased from 20.8 ms to 15.0 ms.

## 3. Consumers handle many events at the same time

- Before: each service handled one event at a time.
- Each event waited behind the previous one.
- Now: events with different keys run in parallel (up to 64).
- Events with the same key stay in order.
- Result (stress): the median completion time decreased from 329 ms to 67 ms.

## 4. Group commit (on by default)

- At stress load, the limit is the basket database, not Kafka.
- Each write waits for its own disk sync (fsync).
- 100 stations make a long queue in front of this one writer.
- With group commit, the writes in the queue share one disk sync.
- Each write is still on the disk before its caller gets a response.
- The submitted code sets `CHECKOUT_MAX_BATCH=256`. The `per-write-commit` run uses 1.
- Result (stress, one producer): throughput increased from 1,601 to 6,360 scans/s.
- At default load, the queue is short. Thus, group commit does not help.

## 5. Two producers: saga and bulk

- A checkout waits for 4 broker hops. Each hop waited for `linger.ms=1`.
- Now: saga commands and replies use a producer with `linger.ms=0`.
- Scans and catalog snapshots use a second producer with `linger.ms=5`. Nothing waits for them.
- One producer with `linger.ms=0` is not better: the scan relay then sends many small requests, and scans get slower.
- Result (default load): throughput increased from 3,119–3,562 to 4,274 scans/s.
- Result (default load): the median completion time decreased from 13 ms to 9.4 ms.
- Result (stress): the p99 completion time decreased from 391–432 ms to 137 ms.

## Cost of these changes

- Analytics can show a scan a few milliseconds after the scan response.
- The basket can show `OPEN` for a few milliseconds after the receipt.
