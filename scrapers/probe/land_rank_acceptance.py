#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from collections import Counter
from pathlib import Path
from statistics import mean
from scrapers.land_decision_layer import validate_land_ai,rank_land
from scrapers.land_filters import private_owner_screen


def tier(n:int)->str:
    if n>=75:return 'CALL FIRST'
    if n>=60:return 'STRONG'
    if n>=45:return 'REVIEW'
    return 'LOW'

def main()->int:
    ap=argparse.ArgumentParser(); ap.add_argument('--root',default='reports/land_private_live'); ap.add_argument('--target',type=int,default=10)
    args=ap.parse_args(); root=Path(args.root)
    extract=json.loads((root/'extract_report.json').read_text(encoding='utf-8'))
    ai_payload=json.loads((root/'ai_classifications.json').read_text(encoding='utf-8'))
    rows=extract.get('private_land_records') or []; cls=ai_payload.get('classifications') or {}
    today=(extract.get('generated_at_et') or '')[:10]
    assertions=[]
    def ck(name,ok,**d):assertions.append({'name':name,'passed':bool(ok),**d})
    cases=[r.get('case_number') for r in rows]
    ck('extract_pass',extract.get('status')=='PASS',status=extract.get('status'))
    ck('target_private_records',len(rows)==args.target,count=len(rows))
    ck('unique_parcels',len({r.get('parcel_id') for r in rows})==len(rows))
    ck('unique_cases',len(set(cases))==len(rows))
    ck('exact_ai_count',len(cls)==args.target,count=len(cls))
    ck('ai_case_set_matches',set(cls)==set(cases),missing=sorted(set(cases)-set(cls)),extra=sorted(set(cls)-set(cases)))
    ranked=[]
    for r in rows:
      case=r.get('case_number'); raw_ai=cls.get(case)
      try: ai=validate_land_ai(raw_ai or {}); ok=True; err=None
      except Exception as e: ai=None;ok=False;err=str(e)
      ck('ai_contract_'+str(case),ok,error=err)
      if not ai:continue
      required=all(r.get(k) for k in ('case_number','property_address','city','state','zip','owner_name','parcel_id','source_url')) and r.get('lot_sqft') is not None
      ck('required_fields_'+case,required)
      ck('open_'+case,str(r.get('record_status')).lower()=='open',status=r.get('record_status'))
      screen=private_owner_screen(r.get('owner_name')); ck('private_owner_'+case,screen['private_owner_screen_passed'],owner=r.get('owner_name'))
      ck('vacant_lot_'+case,bool(r.get('confirmed_vacant_lot')))
      ck('not_fake_demolition_'+case,not (r.get('demolition_verified') and not r.get('demolition_source_url')))
      tax_signal=bool(r.get('tax_delinquent_verified') is True)
      scored=rank_land(ai=ai,event_date=r.get('event_date'),today=today,zoning_type=r.get('zoning_type'),zoning_code=r.get('zoning_code'),
         landuse_name=r.get('landuse_name'),lot_sqft=r.get('lot_sqft'),confirmed_vacant_lot=bool(r.get('confirmed_vacant_lot')),parcel_type=r.get('parcel_type'),
         open_case_count=int(r.get('open_case_groups_in_source_window_same_parcel') or 1),citation_event_count=int(r.get('citation_event_count') or 0),
         owner_mailing_differs=bool(r.get('owner_mailing_differs')),custom_code_signal=True,demolition_transition=bool(r.get('demolition_verified')),tax_signal=tax_signal)
      ck('score_ranges_'+case,all(0<=scored[k]<=100 for k in ('motivation_score','builder_fit_score','saturation_score','freshness_score','priority_score')),scores=scored)
      ck('freshness_date_precision_'+case,scored['freshness_precision']=='date')
      ck('saturation_nonzero_'+case,scored['saturation_score']>=5)
      assessed=float(r.get('citation_assessed_total') or 0); current=r.get('outstanding_balance')
      money=(f'${assessed:,.0f} citation assessed; current balance unverified' if assessed else 'No assessed citation in current extract')
      ck('no_fake_owed_'+case,not (current is None and ' owed' in money.lower()),label=money)
      ranked.append({**scored,'priority_tier':tier(scored['priority_score']),'case_number':case,'property_address':r.get('property_address'),
        'city':r.get('city'),'state':r.get('state'),'zip':r.get('zip'),'owner_name':r.get('owner_name'),'parcel_id':r.get('parcel_id'),
        'lot_sqft':r.get('lot_sqft'),'lot_acres':r.get('lot_acres'),'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),
        'zoning_type':r.get('zoning_type'),'landuse_name':r.get('landuse_name'),'ai_motivation_level':ai.get('motivation_level'),'ai_signals':ai.get('signals'),
        'ai_summary':ai.get('summary'),'confirmed_facts':ai.get('confirmed_facts'),'speculative_claims':ai.get('speculative_claims'),
        'citation_assessed_total':assessed,'verified_current_outstanding_balance':current,'money_label':money,'source_url':r.get('source_url'),
        'event_date':r.get('event_date'),'tax_delinquent_verified':r.get('tax_delinquent_verified'),'demolition_verified':r.get('demolition_verified')})
    ranked.sort(key=lambda x:(x['priority_score'],x['motivation_score'],x['builder_fit_score']),reverse=True)
    for i,x in enumerate(ranked,1):x['rank']=i
    ck('ranked_target',len(ranked)==args.target,count=len(ranked)); ck('rank_order',all(ranked[i]['priority_score']>=ranked[i+1]['priority_score'] for i in range(len(ranked)-1)))
    hi=[x['motivation_score'] for x in ranked if x['ai_motivation_level']=='HIGH'];lo=[x['motivation_score'] for x in ranked if x['ai_motivation_level']=='LOW']
    if hi and lo:ck('high_avg_above_low',mean(hi)>mean(lo),high=mean(hi),low=mean(lo))
    failed=[x for x in assertions if not x['passed']];status='PASS' if not failed and len(ranked)==args.target else 'FAIL'
    report={'status':status,'target':args.target,'assertions_passed':len(assertions)-len(failed),'assertions_failed':len(failed),'failed_assertions':failed,
      'motivation_distribution':dict(Counter(x['ai_motivation_level'] for x in ranked)),'priority_distribution':dict(Counter(x['priority_tier'] for x in ranked)),
      'ranked_land':ranked,'ai_provider_for_bench':ai_payload.get('provider'),'ai_contract_version':ai_payload.get('contract_version')}
    (root/'ranked_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    lines=['# TheReaper Land Ranking', '', f"Status: **{status}**", f"Assertions: **{report['assertions_passed']} passed / {report['assertions_failed']} failed**",'',
      '| Rank | Property | Priority | Motivation | Builder Fit | Saturation | Freshness | Why |','|---:|---|---:|---:|---:|---:|---:|---|']
    for x in ranked:lines.append(f"| {x['rank']} | {x['property_address']}, {x['city']}, {x['state']} {x['zip']} | {x['priority_score']} {x['priority_tier']} | {x['motivation_score']} | {x['builder_fit_score']} | {x['saturation_score']} | {x['freshness_score']} {x['freshness_label']} | {x['ai_summary']} |")
    (root/'ranked_report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps({'status':status,'ranked':len(ranked),'passed':report['assertions_passed'],'failed':report['assertions_failed'],'top':ranked[:5]},indent=2,ensure_ascii=False))
    return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
