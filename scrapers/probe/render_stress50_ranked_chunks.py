#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path('reports/stress_50_live')
SRC=ROOT/'final_ranked_report.json'

def main():
    p=json.loads(SRC.read_text(encoding='utf-8'))
    rows=p.get('ranked_live_leads') or []
    if len(rows)!=50:
        raise RuntimeError(f'expected_50_ranked_got_{len(rows)}')
    for start in range(0,50,10):
        end=start+10
        lines=[f'# Ranked Stress50 {start+1}-{end}','',
               '| Rank | Property | Priority | Distress | Saturation | Freshness | Why | Citation assessed |',
               '|---:|---|---:|---:|---:|---:|---|---:|']
        for x in rows[start:end]:
            why=' '.join(str(x.get('ai_summary') or '').split()).replace('|','/')
            assessed=float(x.get('citation_assessed_total') or 0)
            money=f'${assessed:,.0f}' if assessed else '—'
            lines.append(f"| {x.get('rank')} | {x.get('property_address')} | {x.get('priority_score')} {x.get('priority_tier')} | {x.get('distress_score')} | {x.get('saturation_score')} | {x.get('freshness_score')} {x.get('freshness_label')} | {why} | {money} |")
        (ROOT/f'ranked_{start+1:02d}_{end:02d}.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('rendered ranked chunks')
if __name__=='__main__': main()
