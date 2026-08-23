#!/usr/bin/env python3
from __future__ import annotations
import json,sys
from pathlib import Path
from scrapers.probe import land_rank_acceptance

ROOT=Path('reports/land_stress_50')

def main()->int:
    p=ROOT/'extract_report.json'
    d=json.loads(p.read_text(encoding='utf-8'))
    if 'private_land_records' not in d:
        d['private_land_records']=d.get('private_unseen_land_records') or []
        p.write_text(json.dumps(d,indent=2,ensure_ascii=False),encoding='utf-8')
    old=list(sys.argv)
    try:
        sys.argv=['land_stress_50_rank','--root',str(ROOT),'--target','50']
        return land_rank_acceptance.main()
    finally:
        sys.argv=old
if __name__=='__main__':raise SystemExit(main())
