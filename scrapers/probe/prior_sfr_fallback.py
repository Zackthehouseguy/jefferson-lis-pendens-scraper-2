#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.lojic_land import enrich_parcel

PRIOR=Path('reports/full_system_live/final_ranked_report.json')
CURRENT=Path('reports/mixed_daily_current/mixed_qualification.json')
OUT=Path('reports/sfr_prior_fallback')

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    p=json.loads(PRIOR.read_text(encoding='utf-8')); c=json.loads(CURRENT.read_text(encoding='utf-8'))
    used={r.get('parcel_id') for r in c.get('houses') or []}
    accepted=[]; rejected=[]
    for r0 in p.get('ranked_live_leads') or []:
        r=dict(r0)
        if int(r.get('priority_score') or 0)<60:
            rejected.append({'case_number':r.get('case_number'),'reason':'priority_below_60'});continue
        pid=r.get('parcel_id')
        if not pid or pid in used:
            rejected.append({'case_number':r.get('case_number'),'reason':'missing_or_already_used'});continue
        e,fail=enrich_parcel(pid);r.update(e)
        if not r.get('lojic_parcel_verified') or str(r.get('landuse_name') or '').upper()!='SINGLE FAMILY':
            rejected.append({'case_number':r.get('case_number'),'parcel_id':pid,'reason':'not_single_family_landuse','landuse_name':r.get('landuse_name'),'failures':fail});continue
        # Prior report predates the explicit occupancy card field. Preserve Unknown
        # unless the evidence itself gives a source-backed status.
        narrative=str(r.get('description_raw') or '').upper()
        r['occupancy']='Vacant' if ('VACANT' in narrative or 'ABANDONED' in narrative) else 'Unknown'
        r['recent_window_occupancies']=[];r['property_type']='SFR';r['tax_delinquent_verified']=None
        accepted.append(r)
    report={'status':'PASS','source_generated_at_et':p.get('source_extract_generated_at_et'),'accepted':accepted,'accepted_count':len(accepted),'rejected':rejected,
            'guardrails':{'priority_floor_60':True,'lojic_single_family_required':True,'current_22_excluded':True}}
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':'PASS','accepted':len(accepted),'rejected':len(rejected)},indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
