#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.land_filters import private_owner_screen
from scrapers.lojic_land import enrich_parcel
ROOT=Path('reports/land_stress_50'); RAW=ROOT/'raw'/'extract_report.json'; BENCH=Path('reports/land_private_live/ai_classifications.json')
def main()->int:
 rr=json.loads(RAW.read_text(encoding='utf-8')); excluded_cases=set((json.loads(BENCH.read_text(encoding='utf-8')).get('classifications') or {}).keys()) if BENCH.exists() else set()
 accepted=[];excluded=[];seen=set();fallback=0;reused=0
 for r0 in rr.get('verified_land_records') or []:
  r=dict(r0);case=r.get('case_number');pid=r.get('parcel_id')
  if case in excluded_cases: excluded.append({'case_number':case,'parcel_id':pid,'reason':'benchmark_fixture_excluded'});continue
  if not pid or pid in seen:continue
  seen.add(pid); screen=private_owner_screen(r.get('owner_name'));r.update(screen)
  if not screen['private_owner_screen_passed']:excluded.append({'case_number':case,'parcel_id':pid,'reason':'obvious_public_owner'});continue
  if r.get('lojic_parcel_verified') and r.get('lot_sqft') is not None: reused+=1
  else:
   fallback+=1;enrich,fail=enrich_parcel(pid);r.update(enrich)
  if not r.get('lojic_parcel_verified') or r.get('lot_sqft') is None: excluded.append({'case_number':case,'parcel_id':pid,'reason':'required_lojic_enrichment_failed'});continue
  accepted.append(r)
  if len(accepted)>=50:break
 status='PASS' if len(accepted)==50 else 'FAIL'
 out={'status':status,'target':50,'accepted':len(accepted),'source_raw_status':rr.get('status'),'source_raw_runtime_seconds':rr.get('runtime_seconds'),'pm_features_fetched':rr.get('pm_features_fetched'),'vacant_lot_parent_groups':rr.get('vacant_lot_parent_groups'),'private_unseen_land_records':accepted,'excluded':excluded,'enrichment_efficiency':{'raw_enrichment_reused':reused,'fallback_requeries':fallback},'generated_at_et':rr.get('generated_at_et'),'generated_at_utc':rr.get('generated_at_utc')}
 (ROOT/'reprocessed_report.json').write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding='utf-8');print(json.dumps({'status':status,'accepted':len(accepted),'reused':reused,'fallback':fallback,'excluded':len(excluded)},indent=2));return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
