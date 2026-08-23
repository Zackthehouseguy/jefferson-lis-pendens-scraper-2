#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.land_filters import private_owner_screen

RAW=Path('reports/land_stress_50/raw/extract_report.json')
RANKED=Path('reports/land_stress_50/ranked_report.json')
OUT=Path('reports/land_fill_live')
TARGET=20

def load(p): return json.loads(p.read_text(encoding='utf-8'))

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    raw=load(RAW); ranked=load(RANKED)
    used_cases={r.get('case_number') for r in ranked.get('ranked_land') or []}
    used_parcels={r.get('parcel_id') for r in ranked.get('ranked_land') or []}
    accepted=[]; rejected=[]; seen=set()
    for r0 in raw.get('verified_land_records') or []:
        r=dict(r0); case=r.get('case_number'); pid=r.get('parcel_id')
        if case in used_cases or pid in used_parcels:
            rejected.append({'case_number':case,'parcel_id':pid,'reason':'already_in_first_50'});continue
        if not pid or pid in seen: continue
        seen.add(pid)
        screen=private_owner_screen(r.get('owner_name'));r.update(screen)
        if not screen.get('private_owner_screen_passed'):
            rejected.append({'case_number':case,'parcel_id':pid,'reason':'obvious_public_owner','owner':r.get('owner_name')});continue
        # Raw stress extraction already performed LOJIC enrichment. Reuse it;
        # do not create another burst of redundant parcel requests.
        if not r.get('lojic_parcel_verified') or r.get('lot_sqft') is None:
            rejected.append({'case_number':case,'parcel_id':pid,'reason':'raw_lojic_not_verified'});continue
        accepted.append(r)
        if len(accepted)>=TARGET: break
    status='PASS' if len(accepted)>=10 else 'PARTIAL'
    report={'status':status,'source_generated_at_et':raw.get('generated_at_et'),'raw_runtime_seconds':raw.get('runtime_seconds'),'target':TARGET,
            'first_50_cases_excluded':len(used_cases),'records':accepted,'rejected':rejected,
            'guardrails':{'no_rescrape':True,'reuse_verified_raw_lojic':True,'private_owner_required':True,'first_50_excluded':True}}
    (OUT/'extract_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    compact=[]
    for i,r in enumerate(accepted,1):
        compact.append({'index':i,'case_number':r.get('case_number'),'parcel_id':r.get('parcel_id'),'property_address':r.get('property_address'),'city':r.get('city'),'state':r.get('state'),'zip':r.get('zip'),
          'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),'owner_mailing_differs':r.get('owner_mailing_differs'),'event_date':r.get('event_date'),
          'description_raw':r.get('description_raw'),'inspector_comments':r.get('inspector_comments') or [],'violation_codes':r.get('violation_codes') or [],
          'citation_event_count':r.get('citation_event_count') or 0,'citation_assessed_total':float(r.get('citation_assessed_total') or 0),'outstanding_balance':r.get('outstanding_balance'),
          'lot_sqft':r.get('lot_sqft'),'lot_acres':r.get('lot_acres'),'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),'zoning_type':r.get('zoning_type'),'landuse_name':r.get('landuse_name'),
          'confirmed_vacant_lot':r.get('confirmed_vacant_lot'),'demolition_verified':r.get('demolition_verified'),'tax_delinquent_verified':r.get('tax_delinquent_verified'),'source_url':r.get('source_url')})
    (OUT/'ai_input.json').write_text(json.dumps({'records':compact},indent=2,ensure_ascii=False),encoding='utf-8')
    for start in range(0,len(compact),10):
        chunk=compact[start:start+10]
        (OUT/f'ai_input_{start+1:02d}_{start+len(chunk):02d}.json').write_text(json.dumps({'records':chunk},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'accepted':len(accepted),'excluded_first50':len(used_cases)},indent=2));return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
