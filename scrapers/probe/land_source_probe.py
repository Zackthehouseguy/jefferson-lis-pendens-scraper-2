#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.probe.land_live_extract import fetch_pm_rows, is_vacant_lot_group
from scrapers.probe import accela_engine_probe_v2 as base
from scrapers.lojic_land import enrich_parcel

OUT=Path('reports/land_source_probe')

def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    report={'status':'FAIL','errors':[],'samples':[]}
    try:
        features=fetch_pm_rows(2000); groups=base.build_groups(features)
        land=[g for g in groups if is_vacant_lot_group(g) and g.get('parcel')]
        seen=set(); attempts=0
        for g in land:
            pid=g.get('parcel')
            if not pid or pid in seen: continue
            seen.add(pid); attempts+=1
            enrich, failures=enrich_parcel(pid)
            ok=bool(enrich.get('lojic_parcel_verified') and enrich.get('lot_sqft') is not None and not any(
                x in (f.get('reason') or '') for f in failures for x in ('ConnectionError','HTTPError','Timeout')))
            sample={'parcel_id':pid,'address':g.get('address'),'occupancies':g.get('occupancies'),'enrichment':enrich,
                    'enrichment_failures':failures,'accepted':ok}
            if ok: report['samples'].append(sample)
            else: report.setdefault('rejected_samples',[]).append(sample)
            if len(report['samples'])>=5 or attempts>=15: break
        report.update({'status':'PASS' if len(report['samples'])==5 else 'FAIL','pm_features':len(features),'parent_groups':len(groups),
          'vacant_lot_groups':len(land),'unique_vacant_lot_parcels_in_window':len({g.get('parcel') for g in land if g.get('parcel')}),
          'attempted_parcels':attempts,'successful_enrichments':len(report['samples'])})
    except Exception as exc: report['errors'].append(f'{type(exc).__name__}:{exc}')
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
