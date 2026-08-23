#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path('reports/land_live')
SRC=ROOT/'extract_report.json'
OUT=ROOT/'ai_input.json'
MD=ROOT/'ai_input.md'

def clean(v): return ' '.join(str(v or '').split())

def main() -> int:
    p=json.loads(SRC.read_text(encoding='utf-8'))
    rows=p.get('verified_land_records') or []
    compact=[]
    lines=['# Land 10 AI Review','']
    for i,r in enumerate(rows,1):
        item={
          'case_number':r.get('case_number'),'property_address':r.get('property_address'),
          'parcel_id':r.get('parcel_id'),'description_raw':r.get('description_raw'),
          'inspector_comments':r.get('inspector_comments') or [],
          'violation_codes':r.get('violation_codes') or [],
          'occupancies':r.get('source_window_occupancies') or [],
          'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),
          'owner_mailing_differs':bool(r.get('owner_mailing_differs')),
          'open_case_count':r.get('open_case_groups_in_source_window_same_parcel') or 1,
          'citation_event_count':r.get('citation_event_count') or 0,
          'citation_assessed_total':r.get('citation_assessed_total') or 0,
          'event_date':r.get('event_date'),'lot_sqft':r.get('lot_sqft'),'lot_acres':r.get('lot_acres'),
          'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),
          'zoning_type':r.get('zoning_type'),'landuse_name':r.get('landuse_name'),
          'possible_structure_to_lot_transition':bool(r.get('possible_structure_to_lot_transition')),
          'demolition_verified':bool(r.get('demolition_verified')),
        }
        compact.append(item)
        lines += [f"## {i}. {item['property_address']}",
          f"Case: {item['case_number']} | Parcel: {item['parcel_id']}",
          f"Owner: {item['owner_name']} | Mailing differs: {item['owner_mailing_differs']}",
          f"Lot: {item['lot_sqft']} sqft / {item['lot_acres']} ac | Zoning: {item['zoning_code']} {item['zoning_name']} | Land use: {item['landuse_name']}",
          f"Complaint: {clean(item['description_raw']) or '—'}",
          f"Inspector: {'; '.join(clean(x) for x in item['inspector_comments']) or '—'}",
          f"Citation assessed: ${item['citation_assessed_total']:,.0f}", '']
    payload={'source_status':p.get('status'),'record_count':len(compact),'generated_at_et':p.get('generated_at_et'),'records':compact}
    OUT.write_text(json.dumps(payload,indent=2,ensure_ascii=False),encoding='utf-8')
    MD.write_text('\n'.join(lines),encoding='utf-8')
    return 0 if len(compact)==10 else 2
if __name__=='__main__': raise SystemExit(main())
