---
theme: default
title: 'Event-based Self-Checkout: Code Walkthrough'
info: |
  CS6510 code walkthrough of Assignment 6: Rust microservices that
  communicate only through Apache Kafka events.
author: Shilin
layout: cover
colorSchema: dark
mdc: true
transition: slide-left
lineNumbers: false
duration: 20min
timer: countdown
drawings:
  persist: false
fonts:
  sans: Inter
  serif: Inter
  mono: JetBrains Mono
  provider: none
addons:
  - slidev-addon-timing-bar
  - '@alukach/slidev-addon-laser'
  - '@alukach/slidev-addon-hotkeys'
themeConfig:
  laserColor: '#FF7900'
  laserWidth: 6
  laserFade: 900
section:
  duration: 1m
  title: Story
---

# Event-based Self-Checkout

<p class="text-xl c-text2">Code walkthrough · Assignment 6 · Rust and Apache Kafka</p>
<p class="small c-muted mt-3">Shilin · CS6510 · Fall 2026</p>

<!--
[0:20]
Good evening. My name is Shilin.
Today I show the code of my Assignment 6.
In this version, the microservices communicate only through Kafka events.
I use one story from the start to the end of the talk.
-->

---

# One story for the full talk

<div
  v-motion
  :initial="{ opacity: 0, y: 20 }"
  :enter="{ opacity: 1, y: 0, transition: { duration: 450 } }"
  class="insight mt-5"
>
  <div class="cue c-orange"><tabler-barcode class="icon" /> Station 7</div>
  <div class="text-lg">Starts <code>tx-42</code>. Scans 3 items. Then completes <code>tx-42</code>.</div>
</div>

<div class="grid grid-cols-3 gap-5 mt-8">
  <div
    v-motion
    :initial="{ opacity: 0, y: 36 }"
    :enter="{ opacity: 1, y: 0, transition: { delay: 200, duration: 450 } }"
    class="panel blue"
  >
    <div class="head c-blue"><tabler-topology-star-3 class="icon" /> 1 · Architecture</div>
    <div class="small c-muted mb-2">4 min</div>
    <p>Components, topics, and design decisions</p>
  </div>
  <div
    v-motion
    :initial="{ opacity: 0, y: 36 }"
    :enter="{ opacity: 1, y: 0, transition: { delay: 320, duration: 450 } }"
    class="panel orange"
  >
    <div class="head c-orange"><tabler-code class="icon" /> 2 · Code</div>
    <div class="small c-muted mb-2">10 min</div>
    <p>Entry point, saga, event bus, errors, popular-items window</p>
  </div>
  <div
    v-motion
    :initial="{ opacity: 0, y: 36 }"
    :enter="{ opacity: 1, y: 0, transition: { delay: 440, duration: 450 } }"
    class="panel green"
  >
    <div class="head c-green"><tabler-bulb class="icon" /> 3 · Reflections</div>
    <div class="small c-muted mb-2">3 min</div>
    <p>Results, challenges, and future work</p>
  </div>
</div>

<div class="story-row mt-8">
  <div
    v-for="(r, i) in [
      { verb: 'POST', path: '/transactions', note: 'station 7 → tx-42', cls: 'blue' },
      { verb: 'POST', path: '…/tx-42/items', note: '× 3 scans', cls: 'blue' },
      { verb: 'POST', path: '…/tx-42/complete', note: 'saga starts', cls: 'blue' },
      { verb: '200', path: 'receipt', note: 'at the pivot', cls: 'green' },
    ]"
    :key="i"
    v-motion
    class="story-step"
    :class="r.cls"
    :initial="{ opacity: 0, x: -24 }"
    :enter="{ opacity: 1, x: 0, transition: { delay: 650 + i * 160, duration: 400 } }"
  >
    <div class="mono"><b>{{ r.verb }}</b> {{ r.path }}</div>
    <div class="small c-muted">{{ r.note }}</div>
  </div>
</div>

<style>
.story-row {
  display: grid;
  grid-template-columns: 1fr 1fr 1.1fr 0.8fr;
  gap: 1.1rem;
}
.story-step {
  position: relative;
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.45rem 0.7rem;
  font-size: 0.8rem;
  white-space: nowrap;
}
.story-step b {
  color: var(--blue);
}
.story-step.green {
  border-color: var(--green);
}
.story-step.green b {
  color: var(--green);
}
.story-step:not(:last-child)::after {
  content: '';
  position: absolute;
  right: -1.05rem;
  top: 50%;
  width: 0.9rem;
  border-top: 2px dashed var(--line);
}
</style>

<!--
[0:40]
I use one scenario for the full talk.
Station 7 starts transaction tx-42.
It scans 3 items.
Then it completes the transaction.
We follow these requests through the code.
First, I show the architecture and the design decisions.
Next, I start at the entry point and follow the checkout saga.
Then I show the event bus, the error cases, and the popular-items window.
At the end, I talk about the results, the challenges, and the future work.
-->

---
src: ./pages/01-architecture.md
---

---
src: ./pages/02-deep-dive.md
---

---
src: ./pages/03-reflections.md
---

---
layout: center
class: text-center
hotkey: q
section:
  duration: 10s
  title: Questions
---

<div
  v-motion
  :initial="{ opacity: 0, scale: 0.92 }"
  :enter="{ opacity: 1, scale: 1, transition: { duration: 500 } }"
>

# Questions?

</div>

<div class="flex justify-center mt-8">
  <div
    v-motion
    :initial="{ opacity: 0, y: 20 }"
    :enter="{ opacity: 1, y: 0, transition: { delay: 200, duration: 450 } }"
    class="text-center small"
  >
    <div class="head c-blue flex items-center justify-center gap-2 font-bold mb-2"><tabler-brand-github class="icon" /> Code</div>
    <code>github.com/SASUKE40/CS6510-2026/tree/main/event-based</code>
    <div class="c-text2 mt-6">Backup slides: exactly-once, partitions,<br />SQLite, and a crash after the pivot.</div>
  </div>
</div>

<!--
[0:10]
Thank you. That is the end of my walkthrough.
I am ready for your questions.
I also have backup slides for some frequent questions.
-->

---
hotkey: b
---

# Backup · Why not Kafka exactly-once transactions?

<div class="grid grid-cols-[3fr_2fr] gap-6 mt-2">
<div>

<v-clicks>

- Kafka transactions make a read, process, and write cycle atomic **inside Kafka**.
- Each handler here also writes to **SQLite**. Kafka cannot include SQLite in its transaction.
- Thus, a crash can still occur between the SQLite commit and the offset commit.
- The design accepts redelivery and makes each handler idempotent.

</v-clicks>

</div>
<div class="flex flex-col gap-3">
  <div v-click class="panel purple"><div class="head c-purple"><tabler-arrows-exchange class="icon" /> payment</div>compare-and-set on the saga state</div>
  <div v-click class="panel blue"><div class="head c-blue"><tabler-grave-2 class="icon" /> basket · inventory</div>tombstone rows</div>
  <div v-click class="panel orange"><div class="head c-orange"><tabler-list-numbers class="icon" /> analytics</div>a sequence cursor</div>
</div>
</div>

<div v-click class="key mt-5">At-least-once delivery plus idempotent handlers gives the same visible result, with fewer moving parts.</div>

<!--
[backup]
Kafka transactions make a cycle atomic only inside Kafka.
My handlers also write to SQLite.
Kafka cannot put the SQLite commit in the same transaction.
So a crash can still happen between the database commit and the offset commit.
For this reason, I accept redelivery.
Each handler is idempotent.
Payment uses compare-and-set.
Basket and inventory use tombstones.
Analytics uses a sequence cursor.
-->

---

# Backup · Why one partition per topic?

<div class="grid grid-cols-2 gap-5 mt-3">
  <div v-click class="panel orange"><div class="head c-orange"><tabler-list-numbers class="icon" /> Total order</div>One partition gives a total order for each topic. The design is easy to explain and to verify.</div>
  <div v-click class="panel blue"><div class="head c-blue"><tabler-arrows-split class="icon" /> Still parallel</div>The consumer runs different keys in parallel (up to 64). One partition is not one thread.</div>
  <div v-click class="panel dim"><div class="head"><tabler-database class="icon" /> Real bottleneck</div>Under stress, the limit was the <b>SQLite writer</b> in basket, not Kafka. Group commit removed it.</div>
  <div v-click class="panel dim"><div class="head"><tabler-arrow-big-right-lines class="icon" /> Next step</div>More partitions for command topics, keyed by transaction id or saga id. <code>basket.scans</code> keeps one: its sequence is the window clock.</div>
</div>

<!--
[backup]
One partition gives a total order for each topic.
This order makes the design simple to verify.
The consumer still handles up to 64 different keys at the same time.
Under stress, the limit was the basket SQLite writer, not Kafka.
Group commit removed this limit.
More partitions are possible for the command topics.
The key stays the transaction id or the saga id, so the order per key does not change.
The scan topic must keep one partition, because the scan sequence is the clock of the window.
-->

---

# Backup · Why SQLite? Why a crash after the pivot is safe

<div class="grid grid-cols-2 gap-6 mt-2">
<div v-click class="panel blue">

<div class="head c-blue"><tabler-database class="icon" /> SQLite</div>

- One private file per service: **database per service**.
- WAL and `synchronous=FULL`: each commit is durable.
- One writer per service makes the reservation check and the decrement atomic.
- Group commit (`CHECKOUT_MAX_BATCH`, default 256) shares one fsync between queued writes.

</div>
<div v-click class="panel purple">

<div class="head c-purple"><tabler-refresh-alert class="icon" /> payment crashes after the pivot</div>

1. The saga row is `STOCK_RESERVED` and the payment row exists.
2. On restart, `recover(true)` reads every unfinished saga.
3. It sends `Complete` again.
4. Basket applies it once. Later copies change nothing.
5. `BasketCompleted` moves the saga to `COMPLETED`.

</div>
</div>

<!--
[backup]
Each service has one private SQLite file.
WAL mode with full sync makes each commit durable.
One writer per service makes the stock check and the decrement atomic.
Queued writes share one fsync, up to 256 for each commit.
Now, about a crash after the pivot.
The saga row is STOCK_RESERVED, and the payment row exists.
At restart, payment reads every unfinished saga.
It sends Complete again.
Basket applies it one time, and copies change nothing.
Then the reply moves the saga to COMPLETED.
-->
