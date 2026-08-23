#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
ROOT=Path('reports/stress_50_live'); SRC=ROOT/'extract_report.json'
def one_line(s): return ' '.join(str(s or '').split())
def row(i,r):
    occ=', '.join(r.get('recent_window_occupancies') or []) or '—'
    desc=one_line(r.get('description_raw'))
    comments='; '.join(one_line(x) for x in (r.get('inspector_comments') or [])[:2])
    detail=desc + ((' / Inspector: '+comments) if comments else '')
    detail=detail.replace('|','/').replace('\n',' ')
    if len(detail)>220: detail=detail[:217]+'...'
    assessed=float(r.get('citation_assessed_total') or 0); money=f'${assessed:,.0f}' if assessed else '—'
    return f"| {i} | {r.get('case_number')} | {one_line(r.get('property_address'))} | {occ} | {detail} | {money} |"
def main():
    p=json.loads(SRC.read_text(encoding='utf-8')); rows=p.get('verified_open_unseen_records') or []
    header=['| # | Case | Property | Occupancy | Complaint / key detail | Citation assessed |','|---:|---|---|---|---|---:|']
    for c in range(5):
        start=c*10; lines=[f'# Stress50 Results {start+1}-{start+10}','']+header
        for i,r in enumerate(rows[start:start+10],start+1): lines.append(row(i,r))
        (ROOT/f'results_{start+1:02d}_{start+10:02d}.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('done')
if __name__=='__main__': main()
