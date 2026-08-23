#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path('reports/land_stress_50')

def money(r):
    assessed=float(r.get('citation_assessed_total') or 0)
    return f'${assessed:,.0f} assessed' if assessed else 'none assessed'

def main()->int:
    candidates=[ROOT/'reprocessed_report.json',ROOT/'extract_report.json']
    p=next((x for x in candidates if x.exists() and json.loads(x.read_text(encoding='utf-8')).get('status')=='PASS'),None)
    if p is None:
        p=next((x for x in candidates if x.exists()),None)
    if p is None: raise SystemExit('missing land stress source report')
    d=json.loads(p.read_text(encoding='utf-8'))
    rows=d.get('private_unseen_land_records') or []
    lines=['# Land Stress50 AI Review Packet','',f"Source: **{p.name}**",f"Extraction status: **{d.get('status')}**",f"Records: **{len(rows)}**",'']
    compact=[]
    for i,r in enumerate(rows,1):
        item={
            'index':i,'case_number':r.get('case_number'),'parcel_id':r.get('parcel_id'),
            'property_address':r.get('property_address'),'city':r.get('city'),'state':r.get('state'),'zip':r.get('zip'),
            'owner_name':r.get('owner_name'),'owner_mailing_address':r.get('owner_mailing_address'),'owner_mailing_differs':r.get('owner_mailing_differs'),
            'event_date':r.get('event_date'),'description_raw':r.get('description_raw'),'inspector_comments':r.get('inspector_comments') or [],
            'violation_codes':r.get('violation_codes') or [],'citation_event_count':r.get('citation_event_count') or 0,
            'citation_assessed_total':float(r.get('citation_assessed_total') or 0),'outstanding_balance':r.get('outstanding_balance'),
            'lot_sqft':r.get('lot_sqft'),'lot_acres':r.get('lot_acres'),'zoning_code':r.get('zoning_code'),'zoning_name':r.get('zoning_name'),
            'zoning_type':r.get('zoning_type'),'landuse_name':r.get('landuse_name'),'confirmed_vacant_lot':r.get('confirmed_vacant_lot'),
            'demolition_verified':r.get('demolition_verified'),'tax_delinquent_verified':r.get('tax_delinquent_verified'),
            'source_url':r.get('source_url')
        }
        compact.append(item)
        lines += [
            f"## {i}. {r.get('property_address')}, {r.get('city')}, {r.get('state')} {r.get('zip')}",
            f"Case: {r.get('case_number')} | Parcel: {r.get('parcel_id')}",
            f"Owner: {r.get('owner_name')} | Mailing: {r.get('owner_mailing_address') or '—'} | Differs: {bool(r.get('owner_mailing_differs'))}",
            f"Lot: {r.get('lot_sqft')} sqft / {r.get('lot_acres')} ac | Zoning: {r.get('zoning_code')} {r.get('zoning_name')} ({r.get('zoning_type')}) | Land use: {r.get('landuse_name')}",
            f"Complaint: {r.get('description_raw') or '—'}",
            f"Inspector: {' || '.join(r.get('inspector_comments') or []) or '—'}",
            f"Citation: {money(r)} | Current balance: {'verified '+str(r.get('outstanding_balance')) if r.get('outstanding_balance') is not None else 'unverified'} | Tax delinquent verified: {r.get('tax_delinquent_verified')} | Demolition verified: {r.get('demolition_verified')}",
            ''
        ]
    (ROOT/'ai_input.json').write_text(json.dumps({'source_report':p.name,'records':compact},indent=2,ensure_ascii=False),encoding='utf-8')
    (ROOT/'ai_input.md').write_text('\n'.join(lines),encoding='utf-8')
    for start in range(0,len(compact),10):
        chunk=compact[start:start+10]
        (ROOT/f'ai_input_{start+1:02d}_{start+len(chunk):02d}.json').write_text(json.dumps({'records':chunk},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':'PASS' if len(rows)==50 else 'PARTIAL','source':p.name,'records':len(rows),'chunks':(len(rows)+9)//10},indent=2))
    return 0 if len(rows)==50 else 2

if __name__=='__main__':raise SystemExit(main())
