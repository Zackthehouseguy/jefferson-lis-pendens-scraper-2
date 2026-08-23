#!/usr/bin/env python3
from __future__ import annotations
import json,sys
from pathlib import Path
from scrapers.probe import full_system_live_extract as live
from scrapers.lojic_land import enrich_parcel

ROOT=Path('reports/sfr_fill_live')
PREV=Path('reports/stress_50_live/extract_report.json')

def main()->int:
    ROOT.mkdir(parents=True,exist_ok=True); raw=ROOT/'raw'; raw.mkdir(parents=True,exist_ok=True)
    previous=set()
    if PREV.exists():
        d=json.loads(PREV.read_text(encoding='utf-8'))
        previous={r.get('case_number') for r in d.get('verified_open_unseen_records') or [] if r.get('case_number')}
    live.KNOWN_TUNING_CASES.update(previous)
    old=list(sys.argv)
    try:
        sys.argv=['sfr_fill_extract','--target-open','24','--arcgis-limit','1800','--max-parent-attempts','100','--out',str(raw)]
        rc=live.main()
    finally: sys.argv=old
    rr=json.loads((raw/'extract_report.json').read_text(encoding='utf-8'))
    sfr=[]; rejected=[]
    for r0 in rr.get('verified_open_unseen_records') or []:
        r=dict(r0); pid=r.get('parcel_id')
        if not pid:
            rejected.append({'case_number':r.get('case_number'),'reason':'missing_parcel'});continue
        enrich,fail=enrich_parcel(pid); r.update(enrich)
        if not r.get('lojic_parcel_verified'):
            rejected.append({'case_number':r.get('case_number'),'parcel_id':pid,'reason':'lojic_not_verified','failures':fail});continue
        if str(r.get('landuse_name') or '').strip().upper()!='SINGLE FAMILY':
            rejected.append({'case_number':r.get('case_number'),'parcel_id':pid,'reason':'not_single_family_landuse','landuse_name':r.get('landuse_name')});continue
        vals=[str(x or '').upper() for x in r.get('recent_window_occupancies') or []]
        r['occupancy']='Vacant' if any('VACANT STRUCTURE' in x for x in vals) else ('Occupied' if any('OCCUPIED STRUCTURE' in x for x in vals) else 'Unknown')
        r['property_type']='SFR'; sfr.append(r)
    status='PASS' if len(sfr)>=3 else 'PARTIAL'
    report={'status':status,'raw_status':rr.get('status'),'generated_at_et':rr.get('generated_at_et'),'runtime_seconds':rr.get('runtime_seconds'),
            'previous_cases_excluded':len(previous),'raw_records':len(rr.get('verified_open_unseen_records') or []),'sfr_candidates':sfr,'rejected':rejected,
            'guardrails':{'previous_50_cases_excluded':True,'sfr_requires_lojic_landuse_single_family':True,'no_ai_scores_in_extract_stage':True}}
    (ROOT/'extract_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    compact=[]
    for i,r in enumerate(sfr,1):
        compact.append({'index':i,'case_number':r.get('case_number'),'parcel_id':r.get('parcel_id'),'property_address':r.get('property_address'),
          'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),'owner_mailing_differs':r.get('owner_mailing_differs'),
          'occupancy':r.get('occupancy'),'event_date':r.get('event_date'),'description_raw':r.get('description_raw'),'inspector_comments':r.get('inspector_comments') or [],
          'open_case_count':r.get('recent_window_distinct_parent_groups_same_parcel') or 1,'citation_event_count':r.get('citation_event_count') or 0,
          'citation_assessed_total':r.get('citation_assessed_total') or 0,'landuse_name':r.get('landuse_name'),'zoning_code':r.get('zoning_code'),'source_url':r.get('source_url')})
    (ROOT/'ai_input.json').write_text(json.dumps({'records':compact},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'raw':report['raw_records'],'sfr_candidates':len(sfr),'rejected':len(rejected)},indent=2))
    return 0 if status=='PASS' and rc==0 else 2
if __name__=='__main__': raise SystemExit(main())
