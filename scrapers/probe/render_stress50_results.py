#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path('reports/stress_50_live')
SRC=ROOT/'extract_report.json'
OUT=ROOT/'results_compact.md'

def one_line(s):
    return ' '.join(str(s or '').split())

def main():
    p=json.loads(SRC.read_text(encoding='utf-8'))
    rows=p.get('verified_open_unseen_records') or []
    lines=[
        '# 50-Record Live Stress Test Results','',
        f"Status: **{p.get('status')}**  ",
        f"Verified OPEN records: **{len(rows)}**  ",
        f"Runtime: **{p.get('runtime_seconds')} sec**  ",
        f"Parent groups inspected: **{p.get('parent_groups_inspected')}**  ",'',
        '| # | Case | Property | Occupancy | Complaint / key detail | Citation assessed |',
        '|---:|---|---|---|---|---:|'
    ]
    for i,r in enumerate(rows,1):
        occ=', '.join(r.get('recent_window_occupancies') or []) or '—'
        desc=one_line(r.get('description_raw'))
        comments='; '.join(one_line(x) for x in (r.get('inspector_comments') or [])[:2])
        detail=desc
        if comments:
            detail += ' | Inspector: ' + comments
        detail=detail.replace('|','/').replace('\n',' ')
        if len(detail)>240:
            detail=detail[:237]+'...'
        assessed=float(r.get('citation_assessed_total') or 0)
        money=f"${assessed:,.0f}" if assessed else '—'
        lines.append(f"| {i} | {r.get('case_number')} | {one_line(r.get('property_address'))} | {occ} | {detail} | {money} |")
    OUT.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(OUT)
if __name__=='__main__': main()
