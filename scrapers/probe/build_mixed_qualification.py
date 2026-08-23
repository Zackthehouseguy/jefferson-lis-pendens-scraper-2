#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.lojic_land import enrich_parcel

OUT=Path('reports/mixed_daily_current')
HOUSE_RANK=Path('reports/stress_50_live/final_ranked_report.json')
HOUSE_EXTRACT=Path('reports/stress_50_live/extract_report.json')
LAND_RANK=Path('reports/land_stress_50/ranked_report.json')
LAND_REPROCESSED=Path('reports/land_stress_50/reprocessed_report.json')
LAND_EXTRACT=Path('reports/land_stress_50/extract_report.json')
MIN_PRIORITY=60
MIN_HOUSE_DISTRESS=50
MIN_LAND_MOTIVATION=50
MIN_LAND_BUILDER_FIT=50
TARGET=25

def load(p): return json.loads(p.read_text(encoding='utf-8'))
def clean(v): return ' '.join(str(v or '').split()).strip()
def occupancy(rec):
    vals=[clean(x).upper() for x in (rec.get('recent_window_occupancies') or []) if clean(x)]
    if any('VACANT STRUCTURE' in x for x in vals): return 'Vacant'
    if any('OCCUPIED STRUCTURE' in x for x in vals): return 'Occupied'
    return 'Unknown'

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    hr=load(HOUSE_RANK); he=load(HOUSE_EXTRACT)
    hsrc={r.get('case_number'):r for r in he.get('verified_open_unseen_records') or []}
    houses=[]; house_reject=[]
    for ranked in hr.get('ranked_live_leads') or []:
        if int(ranked.get('priority_score') or 0)<MIN_PRIORITY or int(ranked.get('distress_score') or 0)<MIN_HOUSE_DISTRESS:
            house_reject.append({'case_number':ranked.get('case_number'),'reason':'score_floor','priority':ranked.get('priority_score'),'distress':ranked.get('distress_score')});continue
        pid=ranked.get('parcel_id')
        if not pid:
            house_reject.append({'case_number':ranked.get('case_number'),'reason':'missing_parcel'});continue
        enrich,fail=enrich_parcel(pid)
        use=clean(enrich.get('landuse_name')).upper()
        if not enrich.get('lojic_parcel_verified') or use!='SINGLE FAMILY':
            house_reject.append({'case_number':ranked.get('case_number'),'parcel_id':pid,'reason':'not_single_family_landuse','landuse_name':enrich.get('landuse_name'),'failures':fail});continue
        src=hsrc.get(ranked.get('case_number')) or {}
        row={**ranked,**enrich,'property_type':'SFR','occupancy':occupancy(src),'recent_window_occupancies':src.get('recent_window_occupancies') or [],
             'description_raw':ranked.get('description_raw') or src.get('description_raw'),'inspector_comments':ranked.get('inspector_comments') or src.get('inspector_comments') or [],
             'owner_mailing_address':ranked.get('owner_mailing_address') or src.get('owner_mailing_address'),'tax_delinquent_verified':None}
        houses.append(row)
        if len(houses)>=TARGET: break

    lr=load(LAND_RANK)
    lsource_path=LAND_REPROCESSED if LAND_REPROCESSED.exists() else LAND_EXTRACT
    ls=load(lsource_path)
    lraw=ls.get('private_unseen_land_records') or ls.get('private_land_records') or []
    lsrc={r.get('case_number'):r for r in lraw}
    land=[]; land_reject=[]
    for ranked in lr.get('ranked_land') or []:
        p=int(ranked.get('priority_score') or 0); m=int(ranked.get('motivation_score') or 0); b=int(ranked.get('builder_fit_score') or 0)
        if p<MIN_PRIORITY or m<MIN_LAND_MOTIVATION or b<MIN_LAND_BUILDER_FIT:
            land_reject.append({'case_number':ranked.get('case_number'),'reason':'quality_floor','priority':p,'motivation':m,'builder_fit':b});continue
        src=lsrc.get(ranked.get('case_number')) or {}
        row={**src,**ranked,'property_type':'LAND','description_raw':src.get('description_raw'),'owner_mailing_address':src.get('owner_mailing_address'),
             'possible_structure_to_lot_transition':src.get('possible_structure_to_lot_transition',False)}
        land.append(row)
        if len(land)>=TARGET: break

    for i,r in enumerate(houses,1):r['rank']=i
    for i,r in enumerate(land,1):r['rank']=i
    status='PASS' if len(houses)==TARGET and len(land)==TARGET else 'PARTIAL'
    report={'status':status,'minimum_priority':MIN_PRIORITY,'minimum_house_distress':MIN_HOUSE_DISTRESS,'minimum_land_motivation':MIN_LAND_MOTIVATION,
            'minimum_land_builder_fit':MIN_LAND_BUILDER_FIT,'target_each':TARGET,'houses_count':len(houses),'land_count':len(land),
            'house_source_status':hr.get('status'),'house_source_generated_at_et':hr.get('source_extract_generated_at_et'),'land_source_status':lr.get('status'),
            'houses':houses,'land':land,'house_rejections':house_reject,'land_rejections':land_reject,
            'guardrails':{'sfr_requires_lojic_landuse_single_family':True,'priority_must_be_at_least_60':True,'house_distress_must_be_at_least_50':True,
                          'land_motivation_must_be_at_least_50':True,'land_builder_fit_must_be_at_least_50':True,'multifamily_not_allowed_in_house_lane':True,
                          'assessed_citation_is_not_current_balance':True,'tax_unknown_stays_null':True}}
    (OUT/'mixed_qualification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'houses':len(houses),'land':len(land),'house_rejections':len(house_reject),'land_rejections':len(land_reject)},indent=2))
    return 0 if status=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
