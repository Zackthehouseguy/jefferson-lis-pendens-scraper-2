#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.land_decision_layer import validate_land_ai, motivation_score, builder_fit_score, land_saturation_score, land_priority_score, rank_land
from scrapers.land_filters import private_owner_screen
from scrapers.decision_layer import freshness_date

OUT=Path('reports/land_decision_bench')

def ai(level='MEDIUM', signals=None, relevant=True):
    return {'motivation_level':level,'signals':signals or [],'confirmed_facts':['fixture fact'],
            'speculative_claims':[],'summary':'fixture summary','acquisition_relevant':relevant}

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True); checks=[]
    def ck(name, ok, **d): checks.append({'name':name,'passed':bool(ok),**d})
    try: validate_land_ai(ai()); ck('valid_ai_contract',True)
    except Exception as e: ck('valid_ai_contract',False,error=str(e))
    try:
        bad=ai(); bad['motivation_score']=99; validate_land_ai(bad); ck('forbid_ai_numeric_score',False)
    except ValueError: ck('forbid_ai_numeric_score',True)
    try:
        bad=ai(signals=['invented_signal']); validate_land_ai(bad); ck('reject_unknown_signal',False)
    except ValueError: ck('reject_unknown_signal',True)

    f0=freshness_date('2026-08-23',today='2026-08-23'); f2=freshness_date('2026-08-21',today='2026-08-23'); f10=freshness_date('2026-08-13',today='2026-08-23')
    ck('same_day_freshness',f0.score==100 and f0.age_hours is None and f0.date_precision=='date',value=f0.__dict__)
    ck('1_3_day_freshness',f2.score==82 and f2.age_hours is None,value=f2.__dict__)
    ck('8_30_day_freshness',f10.score==42,value=f10.__dict__)

    weak=motivation_score(ai('LOW',['vacant_lot','overgrown_vegetation']))
    hot=motivation_score(ai('HIGH',['vacant_lot','demolition_transition','tax_delinquent','repeat_abatement']),open_case_count=3,citation_event_count=2,owner_mailing_differs=True)
    ck('hot_motivation_above_weak',hot>weak,hot=hot,weak=weak)
    ck('motivation_clamped',0<=hot<=100 and 0<=weak<=100)
    irrelevant=motivation_score(ai('HIGH',['vacant_lot','demolition_transition'],False))
    ck('irrelevant_capped',irrelevant<=20,value=irrelevant)

    good=builder_fit_score(zoning_type='RESIDENTIAL',zoning_code='R5',landuse_name='SINGLE FAMILY',lot_sqft=5000,confirmed_vacant_lot=True,parcel_type=0)
    row=builder_fit_score(zoning_type='RESIDENTIAL',zoning_code='R5',landuse_name='RIGHT-OF-WAY',lot_sqft=5000,confirmed_vacant_lot=True,parcel_type=0)
    public=builder_fit_score(zoning_type='RESIDENTIAL',zoning_code='R6',landuse_name='PUBLIC AND SEMI-PUBLIC',lot_sqft=5000,confirmed_vacant_lot=True,parcel_type=0)
    tiny=builder_fit_score(zoning_type='RESIDENTIAL',zoning_code='R5',landuse_name='VACANT',lot_sqft=900,confirmed_vacant_lot=True,parcel_type=0)
    ck('res_infill_scores_high',good>=80,value=good)
    ck('row_penalized',row<good-30,row=row,good=good)
    ck('public_use_penalized',public<good-30,public=public,good=good)
    ck('tiny_lot_penalized',tiny<good,tiny=tiny,good=good)

    s_new=land_saturation_score(freshness_score=100,custom_code_signal=True,open_case_count=2,demolition_transition=True)
    s_old=land_saturation_score(freshness_score=20,custom_code_signal=False,open_case_count=1)
    ck('fresh_custom_lower_saturation',s_new<s_old,new=s_new,old=s_old)
    ck('saturation_floor',s_new>=5,value=s_new)

    p_hot=land_priority_score(motivation=95,builder_fit=90,freshness_score=100,saturation=15)
    p_weak=land_priority_score(motivation=20,builder_fit=40,freshness_score=82,saturation=35)
    ck('priority_orders_hot_above_weak',p_hot>p_weak,hot=p_hot,weak=p_weak)

    ranked=rank_land(ai=ai('HIGH',['vacant_lot','repeat_abatement','absentee_owner']),event_date='2026-08-21',today='2026-08-23',zoning_type='RESIDENTIAL',zoning_code='R5',landuse_name='SINGLE FAMILY',lot_sqft=5200,confirmed_vacant_lot=True,owner_mailing_differs=True)
    ck('rank_land_ranges',all(0<=ranked[k]<=100 for k in ('motivation_score','builder_fit_score','saturation_score','freshness_score','priority_score')),value=ranked)
    ck('rank_land_date_precision',ranked['freshness_precision']=='date' and ranked['freshness_label']=='1-3 DAYS',value=ranked)

    ck('filter_louisville_public',not private_owner_screen('LOUISVILLE')['private_owner_screen_passed'])
    ck('filter_landbank',not private_owner_screen('Louisville Metro Land Bank Authority')['private_owner_screen_passed'])
    ck('keep_private_llc',private_owner_screen('LT & JT III INVESTORS LLC')['private_owner_screen_passed'])
    ck('keep_individual',private_owner_screen('BROWN MICHAEL LEE')['private_owner_screen_passed'])

    failed=[x for x in checks if not x['passed']]
    report={'status':'PASS' if not failed else 'FAIL','passed':len(checks)-len(failed),'failed':len(failed),'checks':checks}
    (OUT/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2))
    return 0 if not failed else 2
if __name__=='__main__': raise SystemExit(main())
