#!/usr/bin/env python3
"""Summarize instrument.py output; retain per-block raw files as evidence."""
import json
import math
import re
import sys

def percentile(rows, key, p=99):
    values = sorted(r[key] for r in rows)
    return values[math.ceil(len(values)*p/100)-1]

result = {}
for path in sys.argv[1:]:
    groups = []
    for line in open(path):
        row = {k: float(v) for k, v in re.findall(r'(\w+)=([0-9.]+)', line)}
        if 'block' not in row:
            continue
        if row['block'] == 0:
            groups.append([])
        groups[-1].append(row)
    runs = []
    for rows in groups:
        if len(rows) < 700:  # Exclude Go benchmark calibration.
            continue
        warm = [r for r in rows if r['block'] >= 36]
        top = sorted(rows, key=lambda r: r['write_ms'], reverse=True)[:7]
        runs.append({
            'blocks': len(rows),
            'write_p99_ms': percentile(rows, 'write_ms'),
            'write_warm_p99_ms': percentile(warm, 'write_ms'),
            'commit_p99_ms': percentile(rows, 'commit_ms'),
            'undo_growth_calls': sum(r['grows'] for r in rows),
            'undo_growth_ms': sum(r['grow_ms'] for r in rows),
            'undo_growth_blocks': [int(r['block']) for r in rows if r['grows']],
            'ring_pending_blocks': sum(r['ring_pending'] for r in rows),
            'waits': {name: {
                'calls': sum(r[name+'_calls'] for r in rows),
                'slow_calls_ge_1us': sum(r[name+'_slow'] for r in rows),
                'ms': sum(r[name+'_ms'] for r in rows),
                'ms_per_block': sum(r[name+'_ms'] for r in rows)/len(rows),
            } for name in ('ring', 'hash', 'meta_hash', 'meta_disk', 'failure', 'copy', 'submit')},
            'slowest_write_blocks': [{k: r[k] for k in ('block', 'write_ms', 'grows', 'grow_ms', 'gc', 'assist_ms', 'ring_pending', 'ring_ms')} for r in top],
        })
    result[path] = runs
print(json.dumps(result, indent=2))
