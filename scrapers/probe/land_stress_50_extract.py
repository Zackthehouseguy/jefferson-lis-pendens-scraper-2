#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
from scrapers.probe import land_live_extract as live
from scrapers.land_filters import private_owner_screen
from scrapers.lojic_land import enrich_parcel

ROOT=Path('reports/land_stress_50')
BENCH_AI=Path('reports/land_private_live/ai_classifications.json')

def main()->int:
 ap=argparse.ArgumentParser();ap.add_argument('--target',type=int,default=50);ap.add_argument('--raw-target',type=int,default=85);ap.add_argument('--pm-limit',type=int,default=12000);ap.add_argument('--max-attempts',type=int,default=500);args=ap.parse_args()
 ROOT.mkdir(parents=True,exist_ok=True);raw=ROOT/'raw';raw.mkdir(parents=True,exist_ok=True)
 excluded_cases=set()
 if BENCH_AI.exists(): excluded_cases=set((json.loads(BENCH_AI.read_text(encoding='utf-8')).get('classifications') or {}).keys())
 old=list(sys.argv)
 try:
  sys.argv=['land_live_extract','--target',str(args.raw_target),'--pm-limit',str(args.pm_limit),'--max-attempts',str(args.max_attempts),'--out',str(raw)]
  live.main()
 finally:sys.argv=old
 rr=json.loads((raw/'extract_report.json').read_text(encoding='utf-8'))
 accepted=[];excluded=[];seen=set();enrich_fail=[]
 for r0 in rr.get('verified_land_records') or []:
  r=dict(r0);case=r.get('case_number');pid=r.get('parcel_id')
  if case in excluded_cases:excluded.append({'case_number':case,'parcel_id':pid,'reason':'benchmark_fixture_excluded'});continue
  if not pid or pid in seen:continue
  seen.add(pid)
  screen=private_owner_screen(r.get('owner_name'));r.update(screen)
  if not screen['private_owner_screen_passed']:excluded.append({'case_number':case,'parcel_id':pid,'reason':'obvious_public_owner','owner':r.get('owner_name')});continue
  enrich,fail=enrich_parcel(pid);r.update(enrich)
  if fail:enrich_fail.append({'case_number':case,'parcel_id':pid,'failures':fail})
  transport=any(any(t in (x.get('reason') or '') for t in ('ConnectionError','HTTPError','Timeout')) for x in fail)
  if not r.get('lojic_parcel_verified') or r.get('lot_sqft') is None or transport:
   excluded.append({'case_number':case,'parcel_id':pid,'reason':'required_lojic_enrichment_failed','detail':fail});continue
  accepted.append(r)
  if len(accepted)>=args.target:break
 status='PASS' if len(accepted)==args.target else 'FAIL'
 report={'status':status,'generated_at_utc':rr.get('generated_at_utc'),'generated_at_et':rr.get('generated_at_et'),'target':args.target,'raw_target':args.raw_target,
   'benchmark_cases_excluded':sorted(excluded_cases),'pm_features_fetched':rr.get('pm_features_fetched'),'parent_groups_discovered':rr.get('parent_groups_discovered'),
   'vacant_lot_parent_groups':rr.get('vacant_lot_parent_groups'),'demolition_transition_watch_groups':rr.get('demolition_transition_watch_groups'),
   'raw_runtime_seconds':rr.get('runtime_seconds'),'private_unseen_land_records':accepted,'excluded':excluded,'enrichment_failures':enrich_fail,
   'raw_failures':rr.get('failures') or [],'demolition_watch_preview':rr.get('demolition_watch_preview') or [],
   'guardrails':{'landbank_excluded':True,'private_owner_queue_only':True,'benchmark_cases_excluded':True,'required_parcel_area_enrichment':True,'demolition_not_inferred':True}}
 (ROOT/'extract_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8');print(json.dumps({'status':status,'accepted':len(accepted),'raw_runtime':rr.get('runtime_seconds'),'excluded':len(excluded),'enrich_failures':len(enrich_fail)},indent=2))
 return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
