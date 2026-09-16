#!/usr/bin/env python3
"""Account for every reported error without changing the load client's JSON."""
from collections import Counter
from pathlib import Path
import json
import sys

report = json.loads(Path(sys.argv[1]).read_text())
counts = Counter()
with Path(sys.argv[2]).open() as log:
    for line in log:
        if 'transaction failed:' in line:
            if '"error":"INSUFFICIENT_STOCK"' in line:
                counts['INSUFFICIENT_STOCK (HTTP 409)'] += 1
            else:
                counts['OTHER'] += 1
assert sum(counts.values()) == sum(op['errorCount'] for op in report['operations']), 'Error totals do not match the client report'
for cause, count in sorted(counts.items()):
    print(f'{cause}: {count}')
print('All client-recorded errors accounted for.')
assert not counts['OTHER'], 'Unexpected errors occurred; inspect client.log'
