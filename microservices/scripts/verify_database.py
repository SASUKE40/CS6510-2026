#!/usr/bin/env python3
"""Read-only checks of all four service databases after every service has stopped.

Each database is checked on its own, then against the others: the
cross-database equations are what the checkout saga and the scan outbox must
keep true without a shared transaction.
"""
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

data = Path(sys.argv[1]).resolve()
report = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else None


def open_db(service):
    db = sqlite3.connect(f"{(data / f'{service}.sqlite').as_uri()}?mode=ro", uri=True)
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok", f"{service}: integrity"
    assert not db.execute("PRAGMA foreign_key_check").fetchall(), f"{service}: foreign keys"
    return db


inventory, basket, payment, analytics = (open_db(s) for s in ("inventory", "basket", "payment", "analytics"))

# Inventory, locally: stock = initial - active reservations, never negative.
items = {sku: (name, price, initial, stock) for sku, name, price, initial, stock in
         inventory.execute("SELECT sku, name, price_cents, initial_stock, stock FROM items")}
reserved = Counter(dict(inventory.execute("""
    SELECT l.sku, SUM(l.quantity) FROM reservation_lines l
    JOIN reservations r ON r.id=l.reservation_id WHERE r.status='RESERVED' GROUP BY l.sku""")))
bad = [sku for sku, (_, _, initial, stock) in items.items() if stock < 0 or initial - stock != reserved[sku]]
assert not bad, f"Inventory conservation failed: {bad[:10]}"

# Basket, locally: totals match lines; nothing is still held by a saga.
assert basket.execute("SELECT COUNT(*) FROM transactions WHERE status='CHECKING_OUT'").fetchone()[0] == 0, \
    "A basket is still held by an unfinished saga"
assert not basket.execute("""
    SELECT t.id FROM transactions t LEFT JOIN (
        SELECT l.transaction_id, SUM(l.quantity) units, SUM(l.quantity*c.price_cents) cents
        FROM lines l JOIN catalog c ON c.sku=l.sku GROUP BY l.transaction_id
    ) b ON b.transaction_id=t.id
    WHERE t.item_count != COALESCE(b.units,0) OR t.total_cents != COALESCE(b.cents,0)
       OR (t.status='COMPLETED' AND (t.completed_at IS NULL OR t.item_count=0 OR t.checkout_id IS NULL))
""").fetchall(), "Basket totals or completion fields are inconsistent"
replica = {sku: (name, price) for sku, name, price in basket.execute("SELECT sku, name, price_cents FROM catalog")}
assert replica == {sku: (n, p) for sku, (n, p, _, _) in items.items()}, "Catalog replica differs from inventory"

# Payment, locally: every saga finished; one payment per completed saga.
unfinished = payment.execute("SELECT COUNT(*) FROM sagas WHERE state NOT IN ('COMPLETED','ABORTED')").fetchone()[0]
assert unfinished == 0, f"{unfinished} sagas never finished"
assert payment.execute("""SELECT COUNT(*) FROM sagas s LEFT JOIN payments p ON p.saga_id=s.id
    WHERE (s.state='COMPLETED') != (p.saga_id IS NOT NULL)""").fetchone()[0] == 0, \
    "Payments do not match completed sagas"

# Across services: the saga kept basket, inventory, and payment in agreement.
completed = dict(basket.execute(
    "SELECT checkout_id, id FROM transactions WHERE status='COMPLETED'"))
sagas_done = {f"saga-{i}": tx for i, tx in payment.execute(
    "SELECT id, transaction_id FROM sagas WHERE state='COMPLETED'")}
active = {r for (r,) in inventory.execute("SELECT id FROM reservations WHERE status='RESERVED'")}
assert set(completed) == set(sagas_done) == active, \
    "Completed baskets, completed sagas, and active reservations differ"
assert all(sagas_done[s] == f"tx-{tx}" for s, tx in completed.items()), "Saga/basket ids differ"
sold = Counter(dict(basket.execute("""
    SELECT l.sku, SUM(l.quantity) FROM lines l JOIN transactions t ON t.id=l.transaction_id
    WHERE t.status='COMPLETED' GROUP BY l.sku""")))
assert sold == reserved, "Units in completed baskets differ from reserved stock"
basket_cents = basket.execute("SELECT COALESCE(SUM(total_cents),0) FROM transactions WHERE status='COMPLETED'").fetchone()[0]
paid_cents = payment.execute("SELECT COALESCE(SUM(amount_cents),0) FROM payments").fetchone()[0]
assert basket_cents == paid_cents, "Payments differ from completed basket totals"

# Across services: the outbox and analytics agree on every accepted scan.
scans = basket.execute("SELECT COALESCE(SUM(item_count),0) FROM transactions").fetchone()[0]
outbox = basket.execute("SELECT seq FROM sqlite_sequence WHERE name='scan_events'").fetchone()
outbox = outbox[0] if outbox else 0
assert outbox == basket.execute("SELECT COUNT(*) FROM scan_events").fetchone()[0] == scans, \
    "Scan outbox and baskets disagree"
cursor = analytics.execute("SELECT last_sequence FROM cursor").fetchone()[0]
assert cursor == outbox, f"Analytics consumed {cursor} of {outbox} scan events"
snapshot = json.loads(analytics.execute("SELECT snapshot FROM popularity WHERE id=1").fetchone()[0])
size, slide = snapshot['windowSize'], snapshot['slideInterval']
end = scans // slide * slide
assert snapshot['windowEnd'] == end, "Analytics did not publish the final hop"
assert snapshot['windowStart'] == (max(1, end - size + 1) if end else 0)
assert sum(i['scanCount'] for i in snapshot['items']) == min(end, size)
assert snapshot['items'] == sorted(snapshot['items'], key=lambda i: (-i['scanCount'], i['sku']))
assert [i['rank'] for i in snapshot['items']] == list(range(1, len(snapshot['items']) + 1))
window = Counter(dict(basket.execute(
    "SELECT sku, COUNT(*) FROM scan_events WHERE sequence BETWEEN ? AND ? GROUP BY sku",
    (snapshot['windowStart'], end))))
assert {i['sku']: i['scanCount'] for i in snapshot['items']} == window, "Window differs from the outbox"
retained = analytics.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
assert retained == scans - max(0, end - size), f"Unexpected retained scans: {retained}"

aborted = Counter(f for (f,) in payment.execute("SELECT failure FROM sagas WHERE state='ABORTED'"))
if report:
    assert len(completed) == report['totalTransactions']
    assert scans == report['totalItemsScanned']
    expected = [{k: i[k] for k in ('rank', 'sku', 'name', 'scanCount')} for i in snapshot['items'][:10]]
    assert expected == report['popularItems']
print(f"PASS: integrity and foreign keys in all 4 databases; inventory conservation for all {len(items)} SKUs; "
      "basket totals and catalog replica; every saga finished; completed baskets = completed sagas = active "
      "reservations = payments (per SKU and in cents); scan outbox = analytics cursor; hopping-window metadata/ranks")
print(f"Completed transactions: {len(completed)}; purchased units: {sum(sold.values())}; accepted scans: {scans}")
print(f"Aborted sagas by cause: {dict(sorted(aborted.items()))}")
print(f"Latest window: {snapshot['windowStart']}..{snapshot['windowEnd']}; retained scans in analytics: {retained}")
print(f"Out-of-stock SKUs: {sum(1 for v in items.values() if v[3] == 0)}")
