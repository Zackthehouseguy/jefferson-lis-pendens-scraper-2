#!/usr/bin/env python3
from __future__ import annotations
import json,sys
from pathlib import Path
from scrapers.probe import land_rank_acceptance

ROOT=Path('reports/land_private_live')

def main()->int:
    p=ROOT/'extract_report.json'; d=json.loads(p.read_text(encoding='utf-8'))
    if not d.get('generated_at_et'):
        raw=ROOT/'raw'/'extract_report.json'
        if raw.exists():
            rd=json.loads(raw.read_text(encoding='utf-8'))
            d['generated_at_et']=rd.get('generated_at_et'); d['generated_at_utc']=rd.get('generated_at_utc')
            p.write_text(json.dumps(d,indent=2,ensure_ascii=False),encoding='utf-8')
    old=list(sys.argv)
    try:
        sys.argv=['land_private_rank_fix','--root',str(ROOT),'--target','10']
        return land_rank_acceptance.main()
    finally: sys.argv=old
if __name__=='__main__':raise SystemExit(main())
