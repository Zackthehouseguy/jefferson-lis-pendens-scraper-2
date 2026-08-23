#!/usr/bin/env python3
from __future__ import annotations
import json,sys
from pathlib import Path
from scrapers.probe import land_live_extract as live
from scrapers.land_filters import private_owner_screen

ROOT=Path('reports/land_fresh_deeper')
RANKED=Path('reports/land_stress_50/ranked_report.json')
BENCH=Path('reports/land_private_live/ai_classifications.json')
TARGET_PRIVATE=15
RAW_TARGET=26

def load(p): return json.loads(p.read_text(encoding='utf-8'))

def main()->int:
    ROOT.mkdir(parents=True,exist_ok=True); rawdir=ROOT/'raw';rawdir.mkdir(parents=True,exist_ok=True)
    ranked=load(RANKED); bench=load(BENCH) if BENCH.exists() else {'classifications':{}}
    exclude_cases={r.get('case_number') for r in ranked.get('ranked_land') or [] if r.get('case_number')}
    exclude_parcels={r.get('parcel_id') for r in ranked.get('ranked_land') or [] if r.get('parcel_id')}
    exclude_cases.update((bench.get('classifications') or {}).keys())
    # Prior benchmark parcel IDs may overlap cases outside the ranked 50. Pull them
    # from the old benchmark extraction so the browser never opens them again.
    bench_extract=Path('reports/land_private_live/extract_report.json')
    if bench_extract.exists():
        bd=load(bench_extract)
        for r in bd.get('private_land_records') or []:
            if r.get('parcel_id'):exclude_parcels.add(r['parcel_id'])
    ex={'case_numbers':sorted(exclude_cases),'parcel_ids':sorted(exclude_parcels)}
    ex_path=ROOT/'exclude.json';ex_path.write_text(json.dumps(ex,indent=2),encoding='utf-8')
    old=list(sys.argv)
    try:
        sys.argv=['land_live_extract','--target',str(RAW_TARGET),'--pm-limit','20000','--max-attempts','260','--out',str(rawdir),'--exclude-json',str(ex_path)]
        live_rc=live.main()
    finally:sys.argv=old
    raw=load(rawdir/'extract_report.json')
    accepted=[];rejected=[];seen=set()
    for r0 in raw.get('verified_land_records') or []:
        r=dict(r0);pid=r.get('parcel_id');case=r.get('case_number')
        if not pid or pid in seen:continue
        seen.add(pid)
        screen=private_owner_screen(r.get('owner_name'));r.update(screen)
        if not screen.get('private_owner_screen_passed'):
            rejected.append({'case_number':case,'parcel_id':pid,'reason':'obvious_public_owner','owner':r.get('owner_name')});continue
        if not r.get('lojic_parcel_verified') or r.get('lot_sqft') is None:
            rejected.append({'case_number':case,'parcel_id':pid,'reason':'required_lojic_missing'});continue
        accepted.append(r)
        if len(accepted)>=TARGET_PRIVATE:break
    status='PASS' if len(accepted)>=10 else 'PARTIAL'
    report={'status':status,'raw_status':raw.get('status'),'source_generated_at_et':raw.get('generated_at_et'),'runtime_seconds':raw.get('runtime_seconds'),
      'prebrowser_excluded':raw.get('prebrowser_excluded'),'raw_verified':len(raw.get('verified_land_records') or []),'target_private':TARGET_PRIVATE,'records':accepted,'rejected':rejected,
      'guardrails':{'ranked_50_excluded':True,'benchmark_cases_excluded':True,'private_owner_required':True,'lojic_required':True,'no_duplicate_parcels':True}}
    (ROOT/'extract_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    compact=[]
    for i,r in enumerate(accepted,1):
        compact.append({'index':i,'case_number':r.get('case_number'),'parcel_id':r.get('parcel_id'),'property_address':r.get('property_address'),'city':r.get('city'),'state':r.get('state'),'zip':r.get('zip'),
          'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),'owner_mailing_differs':r.get('owner_mailing_differs'),'event_date':r.get('event_date'),
          'description_raw':r.get('description_raw'),'inspector_comments':r.get('inspector_comments') or [],'violation_codes':r.get('violation_codes') or [],
          'open_case_count':r.get('open_case_groups_in_source_window_same_parcel') or 1,'citation_event_count':r.get('citation_event_count') or 0,
          'citation_assessed_total':float(r.get('citation_assessed_total') or 0),'outstanding_balance':r.get('outstanding_balance'),
          'lot_sqft':r.get('lot_sqft'),'lot_acres':r.get('lot_acres'),'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),'zoning_type':r.get('zoning_type'),'landuse_name':r.get('landuse_name'),
          'confirmed_vacant_lot':r.get('confirmed_vacant_lot'),'demolition_verified':r.get('demolition_verified'),'tax_delinquent_verified':r.get('tax_delinquent_verified'),'source_url':r.get('source_url')})
    (ROOT/'ai_input.json').write_text(json.dumps({'records':compact},indent=2,ensure_ascii=False),encoding='utf-8')
    for start in range(0,len(compact),5):
        ch=compact[start:start+5]
        (ROOT/f'ai_input_{start+1:02d}_{start+len(ch):02d}.json').write_text(json.dumps({'records':ch},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'private':len(accepted),'raw_verified':report['raw_verified'],'runtime':report['runtime_seconds']},indent=2))
    return 0 if status=='PASS' and live_rc==0 else 2
if __name__=='__main__':raise SystemExit(main())
