#!/usr/bin/env python3
"""Read-only checks of inventory conservation and durable load-test state."""
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
    sequence = db.execute("SELECT seq FROM sqlite_sequence WHERE name='scans'").fetchone()
    assert (sequence[0] if sequence else 0) == scans
    snapshot = json.loads(db.execute("SELECT snapshot FROM popularity WHERE id=1").fetchone()[0])
    end = scans // snapshot['slideInterval'] * snapshot['slideInterval']
    assert snapshot['windowEnd'] == end
    assert snapshot['windowStart'] == (max(1, end-snapshot['windowSize']+1) if end else 0)
    assert sum(i['scanCount'] for i in snapshot['items']) == min(end, snapshot['windowSize'])
    assert snapshot['items'] == sorted(snapshot['items'], key=lambda i: (-i['scanCount'], i['sku']))
    assert [i['rank'] for i in snapshot['items']] == list(range(1, len(snapshot['items'])+1))
    assert db.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == min(scans, snapshot['windowSize'])
    if report:
        assert completed == report['totalTransactions']
        assert scans == report['totalItemsScanned']
        expected = [{k: i[k] for k in ('rank','sku','name','scanCount')} for i in snapshot['items'][:10]]
        assert expected == report['popularItems']
    print(f"PASS: SQLite integrity, foreign keys, inventory conservation for all {db.execute('SELECT COUNT(*) FROM items').fetchone()[0]} SKUs, basket totals, and persisted hopping-window metadata/ranks")
    print(f"Completed transactions: {completed}; purchased units: {sold}; accepted scans: {scans}")
    print(f"Latest window: {snapshot['windowStart']}..{snapshot['windowEnd']}; retained scans: {min(scans,snapshot['windowSize'])}")
    print(f"Out-of-stock SKUs: {db.execute('SELECT COUNT(*) FROM items WHERE stock=0').fetchone()[0]}")
