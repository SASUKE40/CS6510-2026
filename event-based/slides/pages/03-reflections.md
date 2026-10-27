---
layout: divider
part: 3
minutes: 3 min
hotkey: 3
section:
  duration: 3m15s
  title: Reflections
---

# Reflections

Results, challenges, and future work

<!--
[0:05]
Part three is reflections.
-->

---

# Results: A1–A6 under the same load test

<ResultsChart />

<div class="insight mt-3 small">
  <div class="cue c-orange"><tabler-scale class="icon" /> Trade-off</div>
  <div>Under stress, A6 completes the most transactions: group commit removes the SQLite limit. At default load, A6 is the slowest: four broker hops come before the pivot.</div>
</div>

<div class="src mt-2">comparison/data/performance.json · all A6 runs passed the cross-database verifier</div>

<!--
[0:45]
This chart compares all six assignments under the same load test.
It shows completed transactions per second.
Each station waits for a response before it sends the next request.
Thus, each millisecond of wait reduces the throughput.
At default load, A1 to A5 complete about 250 transactions per second.
A6 completes 221, the lowest value.
Four broker hops, that is two request-reply round trips, come before the pivot.
Under stress, A6 completes 232 per second, the highest value.
It also has the most scans: 7,239 per second.
Group commit lets queued writes share one fsync.
Thus, the basket SQLite writer is not the limit.
But group commit is a configuration, not an architecture.
A1 to A5 commit each write alone. With group commit, A5 reaches 304 per second under stress.
All A6 runs passed the cross-database verifier.
-->

---

# Challenges and how I solved them

<div class="ch">
  <div class="ch-head c-red"><tabler-alert-triangle class="icon" /> PROBLEM</div>
  <div />
  <div class="ch-head c-green"><tabler-circle-check class="icon" /> FIX</div>

  <div v-click="1" class="ch-cell red"><tabler-x class="icon c-red" /><span><b>Basket stopped consuming.</b> Kafka 4.1.0 failed an offset fetch. librdkafka kept the partition but did not fetch.</span></div>
  <div v-click="1" class="ch-arrow"><tabler-arrow-right /></div>
  <div v-click="1" class="ch-cell green"><tabler-check class="icon c-green" /><span><code>Consumer::rejoin()</code> after any consume error. Broker upgrade to 4.3.2.</span></div>

  <div v-click="2" class="ch-cell red"><tabler-x class="icon c-red" /><span><b>Completions took seconds under stress.</b> One record at a time.</span></div>
  <div v-click="2" class="ch-arrow"><tabler-arrow-right /></div>
  <div v-click="2" class="ch-cell green"><tabler-check class="icon c-green" /><span>Per-key order, 64 keys in parallel. p99 <b>4.2 s → 1.09 s</b>.</span></div>

  <div v-click="3" class="ch-cell red"><tabler-x class="icon c-red" /><span><b>Every scan waited for Kafka.</b> Half the completed tx/s of the HTTP version.</span></div>
  <div v-click="3" class="ch-arrow"><tabler-arrow-right /></div>
  <div v-click="3" class="ch-cell green"><tabler-check class="icon c-green" /><span>Acknowledge after the outbox commit. The relay publishes right after.</span></div>

  <div v-click="4" class="ch-cell red"><tabler-x class="icon c-red" /><span><b>The receipt waited for basket.</b> Two more broker hops.</span></div>
  <div v-click="4" class="ch-arrow"><tabler-arrow-right /></div>
  <div v-click="4" class="ch-cell green"><tabler-check class="icon c-green" /><span>Answer at the pivot. Recovery sends <code>Complete</code> again if necessary.</span></div>

  <div v-click="5" class="ch-cell red"><tabler-x class="icon c-red" /><span><b>Each saga hop waited for batching.</b> One producer with <code>linger.ms=1</code>.</span></div>
  <div v-click="5" class="ch-arrow"><tabler-arrow-right /></div>
  <div v-click="5" class="ch-cell green"><tabler-check class="icon c-green" /><span>Two producers: saga <code>linger.ms=0</code>, scans <code>linger.ms=5</code>. p50 <b>13 → 9.4 ms</b>.</span></div>
</div>

<div v-click="6" class="insight mt-4 small">
  <div class="cue c-orange"><tabler-scale class="icon" /> Cost</div>
  <div>Analytics and the basket status trail by a few milliseconds. I traded read-your-writes for throughput.</div>
</div>

<style>
.ch {
  display: grid;
  grid-template-columns: 1.15fr 2rem 1fr;
  gap: 0.45rem 0.3rem;
  font-size: 0.82rem;
}
.ch-head {
  font-weight: 700;
  letter-spacing: 0.08em;
  display: flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.85rem;
}
.ch-cell {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.4rem 0.7rem;
  color: var(--text-2);
}
.ch-cell.red { border-color: var(--red); }
.ch-cell.green { border-color: var(--green); }
.ch-cell b { color: var(--text); }
.ch-arrow {
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--muted);
}
</style>

<!--
[1:25]
I had five main challenges.
First, the basket consumer stopped during one run.
The Kafka 4.1 coordinator failed an offset fetch with an internal error.
After that, librdkafka kept the partition but did not fetch from it.
My fix was to rejoin the group after any consume error.
I also upgraded the broker to 4.3.2.
Second, completions took seconds under stress.
Each consumer handled one record at a time, so a command waited behind many scans.
Per-key concurrency fixed this.
Third, each scan waited for Kafka.
Now a scan returns after the outbox commit, and the relay publishes it right after.
Fourth, the receipt waited for basket.
Now payment answers at the pivot.
Fifth, each saga hop waited for the producer to batch records.
Now saga records go out at once, and only scans wait 5 milliseconds.
The cost is clear.
Analytics and the basket status can trail by a few milliseconds.
-->

---

# Future improvements

<div class="grid grid-cols-2 gap-6 mt-2 future">
<div
  v-motion
  :initial="{ opacity: 0, x: -40 }"
  :enter="{ opacity: 1, x: 0, transition: { duration: 500 } }"
  class="panel orange"
>
  <div class="head c-orange"><tabler-shield-check class="icon" /> Robustness</div>
  <div class="item"><tabler-trash-x class="icon c-orange" /><span><b>Dead-letter topic.</b> Today a poison record retries forever and blocks its key.</span></div>
  <div class="item"><tabler-file-code class="icon c-orange" /><span><b>Schema contract.</b> JSON with a <code>type</code> field is not versioned. Use a schema registry.</span></div>
  <div class="item"><tabler-server-2 class="icon c-orange" /><span><b>Multi-node broker</b> with replication factor 3.</span></div>
  <div class="item"><tabler-route-2 class="icon c-orange" /><span><b>Tracing</b> with the saga id as the trace id.</span></div>
</div>
<div
  v-motion
  :initial="{ opacity: 0, x: 40 }"
  :enter="{ opacity: 1, x: 0, transition: { delay: 150, duration: 500 } }"
  class="panel blue"
>
  <div class="head c-blue"><tabler-gauge class="icon" /> Performance</div>
  <div class="item"><tabler-columns-3 class="icon c-blue" /><span><b>More partitions</b> for command topics, keyed by transaction id or saga id.</span></div>
  <div class="item"><tabler-server-bolt class="icon c-blue" /><span><b>Broker outside the Docker Desktop VM:</b> each saga hop crosses the VM with <code>acks=all</code>.</span></div>
  <div class="item"><tabler-arrow-guide class="icon c-blue" /><span><b>Fewer hops before the pivot:</b> basket could send <code>Reserve</code> directly. This is a choreography trade-off.</span></div>
</div>
</div>

<style>
.future .panel {
  display: flex;
  flex-direction: column;
  gap: 0.7rem;
  padding: 1rem 1.1rem;
}
.future .item {
  display: flex;
  gap: 0.7rem;
  align-items: flex-start;
  font-size: 0.98rem;
  color: var(--text-2);
}
.future .item b {
  color: var(--text);
}
</style>

<!--
[1:00]
If I had more time, I would make these changes.
For robustness, I would add a dead-letter topic.
Today, a poison record retries forever and blocks its key.
I would also add a versioned schema, for example Avro with a schema registry.
A real deployment needs a broker cluster with replication.
A trace id equal to the saga id would help to find a stalled saga.
For performance, I would add partitions to the command topics.
The key stays the same, so the order per saga does not change.
I would also run the broker outside the Docker Desktop VM.
Today, each saga hop crosses this VM.
-->
