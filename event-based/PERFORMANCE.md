# Why the performance improved

## Basic rule

- Each station sends one request. Then it waits for the response.
- Thus, a shorter wait gives more requests each second.

## 1. Scans do not wait for Kafka

- Before: a scan waited for Kafka to keep its event.
- Now: a scan returns after the database commit.
- The event is safe in the outbox, so no event is lost.
- Scans are approximately 90% of all requests.
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

## 4. Group commit (comparison run only)

- At stress load, the limit is the basket database, not Kafka.
- Each write waits for its own disk sync (fsync).
- 100 stations make a long queue in front of this one writer.
- With group commit, the writes in the queue share one disk sync.
- Each write is still on the disk before its caller gets a response.
- Result (stress): throughput increased from 1,601 to 6,360 scans/s.
- At default load, the queue is short. Thus, group commit does not help.

## Cost of these changes

- Analytics can show a scan a few milliseconds after the scan response.
- The basket can show `OPEN` for a few milliseconds after the receipt.
