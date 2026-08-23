#!/usr/bin/env python3
from __future__ import annotations
import json
from datetime import date
from pathlib import Path
from scrapers.decision_layer import validate_ai_classification,distress_score,freshness_date,saturation_score,priority_score

ROOT=Path('reports/sfr_fill_live')

def tier(n):
    if n>=75:return 'CALL FIRST'
    if n>=60:return 'STRONG'
    if n>=45:return 'REVIEW'
    return 'LOW'

def main()->int:
    ex=json.loads((ROOT/'extract_report.json').read_text(encoding='utf-8'))
    aip=json.loads((ROOT/'ai_classifications.json').read_text(encoding='utf-8'))
    rows=ex.get('sfr_candidates') or []; cls=aip.get('classifications') or {}
    today=date.fromisoformat((ex.get('generated_at_et') or '')[:10])
    assertions=[]
    def ck(name,ok,**d): assertions.append({'name':name,'passed':bool(ok),**d})
    cases=[r.get('case_number') for r in rows]
    ck('extract_has_at_least_3_sfr',len(rows)>=3,count=len(rows))
    ck('unique_cases',len(set(cases))==len(rows))
    ck('exact_ai_count',len(cls)==len(rows),ai_count=len(cls),record_count=len(rows))
    ck('ai_case_set_matches',set(cls)==set(cases),missing=sorted(set(cases)-set(cls)),extra=sorted(set(cls)-set(cases)))
    ranked=[]
    for r in rows:
        case=r.get('case_number'); raw=cls.get(case)
        try: ai=validate_ai_classification(raw or {}); ok=True;err=None
        except Exception as e: ai=None;ok=False;err=str(e)
        ck('ai_contract_'+str(case),ok,error=err)
        ck('sfr_landuse_'+str(case),str(r.get('landuse_name') or '').upper()=='SINGLE FAMILY',landuse=r.get('landuse_name'))
        ck('open_'+str(case),str(r.get('record_status') or '').lower()=='open',status=r.get('record_status'))
        if not ai: continue
        f=freshness_date(r.get('event_date'),today=today)
        oc=int(r.get('recent_window_distinct_parent_groups_same_parcel') or 1); cc=int(r.get('citation_event_count') or 0)
        d=distress_score(ai,open_case_count=oc,citation_event_count=cc,owner_mailing_differs=bool(r.get('owner_mailing_differs')))
        s=saturation_score(source_type='code_enforcement',freshness_score=f.score,has_free_text_description=bool(r.get('description_raw')),open_case_count=oc,new_transition_event=False)
        p=priority_score(distress=d,freshness_score=f.score,saturation=s)
        ck('score_range_'+str(case),all(0<=v<=100 for v in (d,s,f.score,p)))
        ranked.append({
          'rank':None,'priority_score':p,'priority_tier':tier(p),'distress_score':d,'saturation_score':s,'freshness_score':f.score,'freshness_label':f.label,
          'case_number':case,'property_address':r.get('property_address'),'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),
          'parcel_id':r.get('parcel_id'),'landuse_name':r.get('landuse_name'),'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),
          'occupancy':r.get('occupancy'),'recent_window_occupancies':r.get('recent_window_occupancies') or [],'event_date':r.get('event_date'),
          'ai_distress_level':ai.get('distress_level'),'ai_signals':ai.get('signals'),'ai_summary':ai.get('summary'),'confirmed_facts':ai.get('confirmed_facts'),
          'speculative_claims':ai.get('speculative_claims'),'description_raw':r.get('description_raw'),'inspector_comments':r.get('inspector_comments') or [],
          'citation_event_count':cc,'citation_assessed_total':float(r.get('citation_assessed_total') or 0),'verified_current_outstanding_balance':r.get('outstanding_balance'),
          'tax_delinquent_verified':None,'source_url':r.get('source_url'),'property_type':'SFR'})
    ranked.sort(key=lambda x:(x['priority_score'],x['distress_score'],x['freshness_score']),reverse=True)
    for i,r in enumerate(ranked,1):r['rank']=i
    ck('ranked_count_matches',len(ranked)==len(rows),ranked=len(ranked),rows=len(rows))
    failed=[a for a in assertions if not a['passed']]
    status='PASS' if not failed and len(ranked)==len(rows) and len(rows)>=3 else 'FAIL'
    report={'status':status,'source_generated_at_et':ex.get('generated_at_et'),'records':len(rows),'assertions_passed':len(assertions)-len(failed),'assertions_failed':len(failed),'failed_assertions':failed,
            'strong_or_better':sum(1 for r in ranked if r['priority_score']>=60),'ranked_live_leads':ranked,'ai_provider':aip.get('provider'),'ai_contract_version':aip.get('contract_version')}
    (ROOT/'ranked_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'ranked':len(ranked),'strong_or_better':report['strong_or_better'],'failed':len(failed)},indent=2))
    return 0 if status=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
