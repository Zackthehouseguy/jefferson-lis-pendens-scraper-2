#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from scrapers.probe.land_live_extract import fetch_pm_rows, parcel_enrichment, is_vacant_lot_group
from scrapers.probe import accela_engine_probe_v2 as base

OUT=Path('reports/land_source_probe')

def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    report={'status':'FAIL','errors':[],'samples':[]}
    try:
        features=fetch_pm_rows(2000)
        groups=base.build_groups(features)
        land=[g for g in groups if is_vacant_lot_group(g) and g.get('parcel')]
        seen=set()
        for g in land:
            pid=g.get('parcel')
            if not pid or pid in seen: continue
            seen.add(pid)
            enrich, failures=parcel_enrichment(pid)
            report['samples'].append({
                'parcel_id':pid,
                'address':g.get('address'),
                'occupancies':g.get('occupancies'),
                'enrichment':enrich,
                'enrichment_failures':failures,
            })
            if len(report['samples'])>=5: break
        report.update({
            'status':'PASS' if len(report['samples'])==5 else 'PARTIAL',
            'pm_features':len(features),
            'parent_groups':len(groups),
            'vacant_lot_groups':len(land),
            'unique_vacant_lot_parcels_in_window':len({g.get('parcel') for g in land if g.get('parcel')}),
        })
    except Exception as exc:
        report['errors'].append(f'{type(exc).__name__}:{exc}')
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 2

if __name__=='__main__':
    raise SystemExit(main())
