#!/usr/bin/env python3
from __future__ import annotations
import json,sys
from pathlib import Path
from scrapers.probe import land_rank_acceptance

ROOT=Path('reports/land_stress_50')

def load_best_source():
    candidates=[]
    for name in ('extract_report.json','reprocessed_report.json'):
        p=ROOT/name
        if not p.exists(): continue
        d=json.loads(p.read_text(encoding='utf-8'))
        rows=d.get('private_land_records') or d.get('private_unseen_land_records') or []
        candidates.append((d.get('status')=='PASS',len(rows),p,d,rows))
    if not candidates: raise RuntimeError('no_land_stress_source_report')
    candidates.sort(key=lambda x:(x[0],x[1]),reverse=True)
    return candidates[0]

def main()->int:
    passed,count,source,d,rows=load_best_source()
    if not passed or count<50:
        raise RuntimeError(f'no_passing_50_record_source: source={source.name} status={d.get("status")} count={count}')
    # land_rank_acceptance has a stable input contract of extract_report.json + private_land_records.
    normalized=dict(d)
    normalized['status']='PASS'
    normalized['private_land_records']=rows[:50]
    normalized['ranking_source_report']=source.name
    (ROOT/'extract_report.json').write_text(json.dumps(normalized,indent=2,ensure_ascii=False),encoding='utf-8')
    old=list(sys.argv)
    try:
        sys.argv=['land_stress_50_rank','--root',str(ROOT),'--target','50']
        return land_rank_acceptance.main()
    finally:
        sys.argv=old
if __name__=='__main__':raise SystemExit(main())
