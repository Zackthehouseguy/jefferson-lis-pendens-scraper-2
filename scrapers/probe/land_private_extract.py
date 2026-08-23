#!/usr/bin/env python3
"""Private-owner acquisition acceptance wrapper around live land extraction."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from scrapers.probe import land_live_extract as live
from scrapers.land_filters import private_owner_screen
from scrapers.lojic_land import enrich_parcel


def main()->int:
    ap=argparse.ArgumentParser()
    ap.add_argument('--target',type=int,default=10)
    ap.add_argument('--raw-extra',type=int,default=10)
    ap.add_argument('--pm-limit',type=int,default=5000)
    ap.add_argument('--max-attempts',type=int,default=120)
    ap.add_argument('--out',default='reports/land_private_live')
    args=ap.parse_args()
    out=Path(args.out); raw=out/'raw'; raw.mkdir(parents=True,exist_ok=True)
    raw_target=args.target+args.raw_extra
    old=list(sys.argv)
    try:
        sys.argv=['land_live_extract','--target',str(raw_target),'--pm-limit',str(args.pm_limit),'--max-attempts',str(args.max_attempts),'--out',str(raw)]
        live.main()
    finally: sys.argv=old
    raw_report=json.loads((raw/'extract_report.json').read_text(encoding='utf-8'))
    accepted=[]; excluded=[]; enrichment_failures=[]
    seen=set()
    for r0 in raw_report.get('verified_land_records') or []:
        r=dict(r0); pid=r.get('parcel_id')
        if not pid or pid in seen: continue
        seen.add(pid)
        screen=private_owner_screen(r.get('owner_name')); r.update(screen)
        if not screen['private_owner_screen_passed']:
            excluded.append({'parcel_id':pid,'owner_name':r.get('owner_name'),'property_address':r.get('property_address'),'reason':'obvious_public_owner'})
            continue
        enrich,fail=enrich_parcel(pid); r.update(enrich)
        if fail: enrichment_failures.append({'parcel_id':pid,'property_address':r.get('property_address'),'failures':fail})
        # Parcel identity + area are mandatory. Zoning/land-use may be unknown if
        # no overlay intersects, but a network failure cannot masquerade as valid.
        transport_failure=any('ConnectionError' in x.get('reason','') or 'Timeout' in x.get('reason','') or 'HTTPError' in x.get('reason','') for x in fail)
        if not r.get('lojic_parcel_verified') or r.get('lot_sqft') is None or transport_failure:
            excluded.append({'parcel_id':pid,'owner_name':r.get('owner_name'),'property_address':r.get('property_address'),'reason':'required_lojic_enrichment_failed','detail':fail})
            continue
        accepted.append(r)
        if len(accepted)>=args.target: break
    status='PASS' if len(accepted)==args.target else 'FAIL'
    report={
      'status':status,'target_private_land':args.target,'raw_target':raw_target,
      'raw_status':raw_report.get('status'),'raw_runtime_seconds':raw_report.get('runtime_seconds'),
      'pm_features_fetched':raw_report.get('pm_features_fetched'),'parent_groups_discovered':raw_report.get('parent_groups_discovered'),
      'vacant_lot_parent_groups':raw_report.get('vacant_lot_parent_groups'),
      'demolition_transition_watch_groups':raw_report.get('demolition_transition_watch_groups'),
      'private_land_records':accepted,'exclusions':excluded,'enrichment_failures':enrichment_failures,
      'raw_failures':raw_report.get('failures') or [],'demolition_watch_preview':raw_report.get('demolition_watch_preview') or [],
      'guardrails':{**(raw_report.get('guardrails') or {}),'private_owner_queue_only':True,'required_parcel_area_enrichment':True},
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/'extract_report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if status=='PASS' else 2

if __name__=='__main__': raise SystemExit(main())
