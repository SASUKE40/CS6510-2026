#!/usr/bin/env python3
"""Read-only checks of the shared database after every service has stopped."""
import json
import sqlite3
import sys
from pathlib import Path

path = Path(sys.argv[1]).resolve()
report = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else None
with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as db:
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert not db.execute("PRAGMA foreign_key_check").fetchall()
    bad = db.execute("""
        SELECT i.sku FROM items i LEFT JOIN (
            SELECT l.sku, SUM(l.quantity) sold FROM lines l
            JOIN transactions t ON t.id=l.transaction_id
            WHERE t.status='COMPLETED' GROUP BY l.sku
        ) s ON s.sku=i.sku
        WHERE i.stock<0 OR i.initial_stock-i.stock != COALESCE(s.sold, 0)
    """).fetchall()
    assert not bad, f"Inventory conservation failed: {bad}"
    assert not db.execute("""
        SELECT t.id FROM transactions t LEFT JOIN (
            SELECT l.transaction_id, SUM(l.quantity) units,
                   SUM(l.quantity*i.price_cents) cents
            FROM lines l JOIN items i ON i.sku=l.sku GROUP BY l.transaction_id
        ) b ON b.transaction_id=t.id
        WHERE t.item_count != COALESCE(b.units,0) OR t.total_cents != COALESCE(b.cents,0)
           OR (t.status='COMPLETED' AND (t.completed_at IS NULL OR t.item_count=0))
    """).fetchall(), "Basket totals or completed timestamps are inconsistent"
    completed, sold = db.execute("SELECT COUNT(*), COALESCE(SUM(item_count),0) FROM transactions WHERE status='COMPLETED'").fetchone()
    scans = db.execute("SELECT COALESCE(SUM(item_count),0) FROM transactions").fetchone()[0]
    # The basket service logs each accepted scan in the same unit of work as the
    # basket line, so the scan log's sequence must equal the units in baskets.
    sequence = db.execute("SELECT seq FROM sqlite_sequence WHERE name='scans'").fetchone()
    assert (sequence[0] if sequence else 0) == scans, "Scan log and baskets disagree"
    snapshot = json.loads(db.execute("SELECT snapshot FROM popularity WHERE id=1").fetchone()[0])
    size, slide = snapshot['windowSize'], snapshot['slideInterval']
    end = scans // slide * slide
    assert snapshot['windowEnd'] == end, "Analytics did not publish the final hop"
    assert snapshot['windowStart'] == (max(1, end-size+1) if end else 0)
    assert sum(i['scanCount'] for i in snapshot['items']) == min(end, size)
    assert snapshot['items'] == sorted(snapshot['items'], key=lambda i: (-i['scanCount'], i['sku']))
    assert [i['rank'] for i in snapshot['items']] == list(range(1, len(snapshot['items'])+1))
    # Analytics trims scans no future window can include: those <= end - size.
    retained = db.execute("SELECT COUNT(*), MIN(sequence) FROM scans").fetchone()
    assert retained[0] == scans - max(0, end - size), f"Unexpected retained scans: {retained}"
    if report:
        assert completed == report['totalTransactions']
        assert scans == report['totalItemsScanned']
        expected = [{k: i[k] for k in ('rank','sku','name','scanCount')} for i in snapshot['items'][:10]]
        assert expected == report['popularItems']
    print(f"PASS: SQLite integrity, foreign keys, inventory conservation for all {db.execute('SELECT COUNT(*) FROM items').fetchone()[0]} SKUs, basket totals, scan log, and persisted hopping-window metadata/ranks")
    print(f"Completed transactions: {completed}; purchased units: {sold}; accepted scans: {scans}")
    print(f"Latest window: {snapshot['windowStart']}..{snapshot['windowEnd']}; retained scans: {retained[0]}")
    print(f"Out-of-stock SKUs: {db.execute('SELECT COUNT(*) FROM items WHERE stock=0').fetchone()[0]}")
