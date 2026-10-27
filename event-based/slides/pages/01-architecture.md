---
layout: divider
part: 1
minutes: 4 min
hotkey: 1
section:
  duration: 4m5s
  title: Architecture
---

# Architecture and design

Components, topics, and design decisions

<!--
[0:05]
Part one is the architecture and the design decisions.
-->

---

# What changed from Assignment 5

<div class="cmp mt-2">
  <div class="cmp-head c-blue"><tabler-world-www class="icon" /> ASSIGNMENT 5 · HTTP</div>
  <div class="cmp-mid" />
  <div v-click="1" class="cmp-head c-orange"><tabler-stack-2 class="icon" /> ASSIGNMENT 6 · EVENTS</div>

  <div class="cmp-cell blue"><tabler-arrows-exchange class="icon c-blue" /><span><b>Traffic</b> · HTTP calls between services</span></div>
  <div class="cmp-mid"><tabler-arrow-right class="c-muted" /></div>
  <div v-click="1" class="cmp-cell orange"><tabler-stack-2 class="icon c-orange" /><span><b>Kafka topics only</b></span></div>

  <div class="cmp-cell blue"><tabler-address-book class="icon c-blue" /><span><b>Knowledge</b> · the URLs of other services</span></div>
  <div class="cmp-mid"><tabler-arrow-right class="c-muted" /></div>
  <div v-click="1" class="cmp-cell orange"><tabler-tag class="icon c-orange" /><span><b>Topic names only</b></span></div>

  <div class="cmp-cell blue"><tabler-plug-connected-x class="icon c-blue" /><span><b>A service is down</b> · the request fails at once</span></div>
  <div class="cmp-mid"><tabler-arrow-right class="c-muted" /></div>
  <div v-click="1" class="cmp-cell orange"><tabler-clock-pause class="icon c-orange" /><span><b>Commands wait in Kafka</b></span></div>

  <div class="cmp-cell blue"><tabler-send class="icon c-blue" /><span><b>Delivery</b> · one call, one answer</span></div>
  <div class="cmp-mid"><tabler-arrow-right class="c-muted" /></div>
  <div v-click="1" class="cmp-cell orange"><tabler-repeat class="icon c-orange" /><span><b>At least once</b> · idempotent handlers</span></div>
</div>

<div v-click="2" class="insight mt-5">
  <div class="cue"><tabler-equal class="icon" /> No change</div>
  <div>The Java load client · the public API · one SQLite file per service · the routing-only gateway</div>
</div>

<style>
.cmp {
  display: grid;
  grid-template-columns: 1fr 2.2rem 1fr;
  gap: 0.55rem 0.4rem;
  align-items: stretch;
}
.cmp-head {
  font-weight: 700;
  letter-spacing: 0.06em;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.95rem;
  padding-bottom: 0.2rem;
}
.cmp-mid {
  display: flex;
  align-items: center;
  justify-content: center;
}
.cmp-cell {
  display: flex;
  align-items: center;
  gap: 0.7rem;
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.55rem 0.8rem;
}
.cmp-cell.blue { border-color: rgba(47, 128, 237, 0.55); }
.cmp-cell.orange { border-color: var(--orange); }
.cmp-cell b { color: var(--text); }
</style>

<!--
[0:45]
This slide shows the change from Assignment 5.
In Assignment 5, services called each other over HTTP.
Now, services communicate only through Kafka topics.
A service does not know the address of another service.
It knows only the topic names.
If a service is down, its commands wait in Kafka.
The request does not fail at once.
The cost is at-least-once delivery.
So every handler has to be idempotent.
The load client, the public API, and the databases did not change.
-->

---
class: wide
clicks: 3
---

# Architecture

<ArchitectureDiagram />

<!--
[1:15]
This is the architecture.
The client sends HTTP requests to the gateway on port 8080.
The gateway only routes each request to the service that owns the path.
It has no business logic, no database, and no broker connection.
Next, there are four services.
Each service has its own process and its own SQLite file.
Payment is the orchestrator of the checkout saga.
Below the services, there are six Kafka topics.
Each topic has one partition, so the order of events is the order of production.
Now the saga starts. Each pulse is one Kafka record.
Payment sends a command, and the service sends a reply to payment.
Then I show the two data feeds: the catalog feed and the scan feed.
Last, the crate service-kit holds the shared code.
It has the event bus and the SQLite connection code, but no domain types.
-->

---

# Event catalog

<div
  v-motion
  :initial="{ opacity: 0, y: 24 }"
  :enter="{ opacity: 1, y: 0, transition: { duration: 450 } }"
  class="catalog"
>

| Topic | From → to | Key | Messages | Pattern |
| --- | --- | --- | --- | --- |
| `inventory.catalog` | inventory → basket | `catalog` | `CatalogPublished` | state transfer, compacted |
| `basket.scans` | basket → analytics | sequence | `ItemScanned` | outbox relay, choreography |
| `basket.commands` | payment → basket | tx id | <span class="pill cmd">Checkout</span><span class="pill cmd">Release</span><span class="pill cmd">Complete</span> | saga commands |
| `basket.events` | basket → payment | tx id | <span class="pill evt">BasketLocked</span><span class="pill evt">…Released</span><span class="pill evt">…Completed</span><span class="pill warn">…Rejected</span> | saga replies |
| `inventory.commands` | payment → inventory | saga id | <span class="pill cmd">Reserve</span><span class="pill cmd">Cancel</span> | saga commands |
| `inventory.events` | inventory → payment | saga id | <span class="pill evt">StockReserved</span><span class="pill warn">StockRejected</span><span class="pill evt">…Cancelled</span> | saga replies |

</div>

<div
  v-motion
  :initial="{ opacity: 0 }"
  :enter="{ opacity: 1, transition: { delay: 350, duration: 450 } }"
  class="grid grid-cols-3 gap-4 mt-5 small"
>
  <div class="panel blue"><span class="pill cmd">command</span> payment asks a service to act.</div>
  <div class="panel orange"><span class="pill evt">reply</span> the service answers payment.</div>
  <div class="panel red"><span class="pill warn">rejection</span> same code as Assignment 5, e.g. <code>409</code>.</div>
</div>

<style>
.catalog table {
  font-size: 0.76rem;
}
.catalog td:nth-child(2) {
  white-space: nowrap;
}
.catalog td,
.catalog th {
  padding-top: 0.4rem;
  padding-bottom: 0.4rem;
}
.catalog td:nth-child(4) {
  white-space: nowrap;
}
</style>

<!--
[0:45]
This table is the event contract.
Blue items are commands, and orange items are replies.
Payment sends commands to basket and to inventory.
Basket and inventory send replies to payment.
The key is the transaction id or the saga id.
That way, all messages for one saga stay in order.
There are also two data feeds.
The catalog topic is compacted, and basket reads the newest catalog at startup.
The scan topic carries one event for each accepted scan.
A rejection reply carries the same error code as before.
So the client sees the same errors as in Assignment 5.
-->

---

# Key design decisions

<div class="grid grid-cols-3 gap-4 mt-2 decisions">
  <div v-click class="panel blue">
    <div class="head c-blue"><tabler-sitemap class="icon" /> Orchestration for checkout</div>
    payment holds the saga state. One place shows each step and each rollback.
  </div>
  <div v-click class="panel orange">
    <div class="head c-orange"><tabler-broadcast class="icon" /> Choreography for scans</div>
    analytics only listens. A new consumer needs no change in basket.
  </div>
  <div v-click class="panel blue">
    <div class="head c-blue"><tabler-inbox class="icon" /> Transactional outbox</div>
    The scan and its event commit in one local transaction. No event is lost.
  </div>
  <div v-click class="panel orange">
    <div class="head c-orange"><tabler-stack-2 class="icon" /> Compacted catalog topic</div>
    basket validates SKUs and prices without a call to inventory.
  </div>
  <div v-click class="panel orange">
    <div class="head c-orange"><tabler-repeat class="icon" /> At least once + idempotent</div>
    This is simpler than exactly-once across Kafka and SQLite.
  </div>
  <div v-click class="panel orange">
    <div class="head c-orange"><tabler-list-numbers class="icon" /> One partition per topic</div>
    Total order is easy to verify. Different keys still run in parallel.
  </div>
</div>

<div v-click class="insight mt-5">
  <div class="cue c-orange"><tabler-scale class="icon" /> Trade-off</div>
  <div>Events give temporal decoupling and a durable log. They cost one more stateful system, idempotent handlers, and latency on each hop.</div>
</div>

<div class="small c-muted mt-3"><span class="c-blue">■</span> decision inside a service · <span class="c-orange">■</span> decision about the broker</div>

<style>
.decisions .panel {
  font-size: 0.86rem;
  color: var(--text-2);
}
.decisions .head {
  font-size: 0.95rem;
}
</style>

<!--
[1:15]
Here are the key decisions.
First, checkout uses orchestration.
Payment holds the saga state, so one place shows each step and each rollback.
Second, scans use choreography.
Analytics only listens to the scan topic.
A new consumer, for example fraud detection, needs no change in basket.
Third, basket uses a transactional outbox.
The scan and its event commit in the same local transaction, so no event is lost.
Fourth, the catalog is a compacted topic.
Basket checks SKUs and prices locally, without a call to inventory.
Fifth, delivery is at least once, and every handler is idempotent.
Sixth, each topic has one partition, so the order is easy to verify.
The trade-off is clear.
Events give temporal decoupling and a durable log.
But they add a broker to operate, and each hop adds latency.
-->
