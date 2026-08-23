#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
ROOT=Path('reports/land_private_live')

def clean(v): return ' '.join(str(v or '').split())
def main()->int:
 p=json.loads((ROOT/'extract_report.json').read_text(encoding='utf-8')); rows=p.get('private_land_records') or []
 compact=[]; md=['# Private Land AI Review','']
 for i,r in enumerate(rows,1):
  x={k:r.get(k) for k in ('case_number','property_address','city','state','zip','parcel_id','owner_name','owner_mailing_address','owner_mailing_differs','description_raw','inspector_comments','violation_codes','citation_event_count','citation_assessed_total','event_date','lot_sqft','lot_acres','zoning_code','zoning_name','zoning_type','landuse_name','confirmed_vacant_lot','demolition_verified','possible_structure_to_lot_transition','open_case_groups_in_source_window_same_parcel','tax_delinquent_verified')}
  compact.append(x)
  md += [f"## {i}. {x['property_address']}, {x['city']}, {x['state']} {x['zip']}",f"Case: {x['case_number']} | Parcel: {x['parcel_id']}",f"Owner: {x['owner_name']} | Mailing: {x['owner_mailing_address']} | Differs: {x['owner_mailing_differs']}",f"Lot: {x['lot_sqft']} sqft / {x['lot_acres']} ac | Zoning: {x['zoning_code']} {x['zoning_name']} ({x['zoning_type']}) | Land use: {x['landuse_name']}",f"Complaint: {clean(x['description_raw']) or '—'}",f"Inspector: {'; '.join(clean(y) for y in (x['inspector_comments'] or [])) or '—'}",f"Citation assessed: ${float(x['citation_assessed_total'] or 0):,.0f} | Tax verified: {x['tax_delinquent_verified']} | Demolition verified: {x['demolition_verified']}",'']
 out={'source_status':p.get('status'),'generated_at_et':p.get('generated_at_et'),'record_count':len(compact),'records':compact}
 (ROOT/'ai_input.json').write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding='utf-8');(ROOT/'ai_input.md').write_text('\n'.join(md),encoding='utf-8')
 return 0 if len(compact)==10 else 2
if __name__=='__main__':raise SystemExit(main())
