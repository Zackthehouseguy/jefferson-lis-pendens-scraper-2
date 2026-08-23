#!/usr/bin/env python3
from __future__ import annotations
import json
from collections import Counter
from pathlib import Path
from scrapers.land_decision_layer import validate_land_ai,rank_land

ROOT=Path('reports/land_fresh_deeper')

def tier(n:int)->str:
    if n>=75:return 'CALL FIRST'
    if n>=60:return 'STRONG'
    if n>=45:return 'REVIEW'
    return 'LOW'

def main()->int:
    ex=json.loads((ROOT/'extract_report.json').read_text(encoding='utf-8'))
    aip=json.loads((ROOT/'ai_classifications.json').read_text(encoding='utf-8'))
    rows=ex.get('records') or []; cls=aip.get('classifications') or {}
    today=(ex.get('source_generated_at_et') or '')[:10]
    assertions=[]
    def ck(name,ok,**d):assertions.append({'name':name,'passed':bool(ok),**d})
    cases=[r.get('case_number') for r in rows]
    ck('extract_pass_or_usable',ex.get('status') in ('PASS','PARTIAL') and len(rows)>=5,status=ex.get('status'),count=len(rows))
    ck('unique_cases',len(set(cases))==len(rows))
    ck('exact_ai_count',len(cls)==len(rows),ai_count=len(cls),record_count=len(rows))
    ck('ai_case_set_matches',set(cls)==set(cases),missing=sorted(set(cases)-set(cls)),extra=sorted(set(cls)-set(cases)))
    ranked=[]
    for r in rows:
        case=r.get('case_number');raw=cls.get(case)
        try: ai=validate_land_ai(raw or {});ok=True;err=None
        except Exception as e: ai=None;ok=False;err=str(e)
        ck('ai_contract_'+str(case),ok,error=err)
        ck('open_'+str(case),str(r.get('record_status') or '').lower()=='open',status=r.get('record_status'))
        ck('vacant_lot_'+str(case),bool(r.get('confirmed_vacant_lot')))
        ck('parcel_verified_'+str(case),bool(r.get('lojic_parcel_verified')) and r.get('lot_sqft') is not None)
        if not ai:continue
        scored=rank_land(ai=ai,event_date=r.get('event_date'),today=today,zoning_type=r.get('zoning_type'),zoning_code=r.get('zoning_code'),
          landuse_name=r.get('landuse_name'),lot_sqft=r.get('lot_sqft'),confirmed_vacant_lot=bool(r.get('confirmed_vacant_lot')),parcel_type=r.get('parcel_type'),
          open_case_count=int(r.get('open_case_groups_in_source_window_same_parcel') or 1),citation_event_count=int(r.get('citation_event_count') or 0),
          owner_mailing_differs=bool(r.get('owner_mailing_differs')),custom_code_signal=True,demolition_transition=bool(r.get('demolition_verified')),tax_signal=bool(r.get('tax_delinquent_verified') is True))
        ck('score_range_'+str(case),all(0<=scored[k]<=100 for k in ('motivation_score','builder_fit_score','saturation_score','freshness_score','priority_score')),scores=scored)
        qualified=scored['priority_score']>=60 and scored['motivation_score']>=50 and scored['builder_fit_score']>=50
        ranked.append({**r,**scored,'priority_tier':tier(scored['priority_score']),'ai_motivation_level':ai.get('motivation_level'),'ai_signals':ai.get('signals'),
          'ai_summary':ai.get('summary'),'confirmed_facts':ai.get('confirmed_facts'),'speculative_claims':ai.get('speculative_claims'),'acquisition_relevant':ai.get('acquisition_relevant'),
          'qualified_daily_land':qualified,'verified_current_outstanding_balance':r.get('outstanding_balance')})
    ranked.sort(key=lambda x:(x['qualified_daily_land'],x['priority_score'],x['motivation_score'],x['builder_fit_score']),reverse=True)
    for i,r in enumerate(ranked,1):r['rank']=i
    qualified=[r for r in ranked if r['qualified_daily_land']]
    ck('at_least_five_qualified',len(qualified)>=5,count=len(qualified))
    failed=[a for a in assertions if not a['passed']]
    status='PASS' if not failed and len(qualified)>=5 else 'FAIL'
    report={'status':status,'records':len(rows),'qualified_count':len(qualified),'assertions_passed':len(assertions)-len(failed),'assertions_failed':len(failed),
      'failed_assertions':failed,'motivation_distribution':dict(Counter(r['ai_motivation_level'] for r in ranked)),'ranked_land':ranked,'qualified_land':qualified,
      'quality_floors':{'priority':60,'motivation':50,'builder_fit':50},'ai_provider':aip.get('provider'),'ai_contract_version':aip.get('contract_version')}
    (ROOT/'ranked_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'records':len(rows),'qualified':len(qualified),'failed':len(failed)},indent=2));return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
