#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json
from pathlib import Path

BASE=Path('reports/mixed_daily_current/mixed_qualification.json')
FILL=Path('reports/sfr_fill_live/ranked_report.json')
OUT=Path('reports/daily_mix_final')
LEDGER=Path('data/reaper_delivery_ledger.json')
TARGET=25; MIN_PRIORITY=60

def load(p): return json.loads(p.read_text(encoding='utf-8'))
def clean(v): return ' '.join(str(v or '').split()).strip()
def fp(r):
    payload='|'.join([clean(r.get('parcel_id')),clean(r.get('case_number')),clean(r.get('event_date')),str(float(r.get('citation_assessed_total') or 0)),clean(r.get('description_raw'))])
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()
def key(r): return 'JEFFERSON|'+clean(r.get('parcel_id') or r.get('property_address')).upper()

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    b=load(BASE); f=load(FILL) if FILL.exists() else {'ranked_live_leads':[]}
    houses=list(b.get('houses') or [])
    existing={(r.get('parcel_id'),r.get('case_number')) for r in houses}
    for r in f.get('ranked_live_leads') or []:
        if int(r.get('priority_score') or 0)<MIN_PRIORITY: continue
        ident=(r.get('parcel_id'),r.get('case_number'))
        if ident in existing: continue
        houses.append(r); existing.add(ident)
    houses=[r for r in houses if int(r.get('priority_score') or 0)>=MIN_PRIORITY]
    houses.sort(key=lambda r:(int(r.get('priority_score') or 0),int(r.get('distress_score') or 0)),reverse=True)
    land=[r for r in (b.get('land') or []) if int(r.get('priority_score') or 0)>=MIN_PRIORITY]
    land.sort(key=lambda r:(int(r.get('priority_score') or 0),int(r.get('motivation_score') or 0)),reverse=True)

    ledger=load(LEDGER) if LEDGER.exists() else {'version':1,'market':'Jefferson County, Kentucky','properties':{}}
    props=ledger.setdefault('properties',{})
    fill_stamp=f.get('source_generated_at_et') or ''
    batch_id='|'.join([clean(b.get('house_source_generated_at_et')),clean(fill_stamp),max([clean(r.get('event_date')) for r in land] or [''])])

    def fresh(rows):
        out=[]; skipped=[]
        for r in rows:
            k=key(r); fingerprint=fp(r); prior=props.get(k)
            if prior and prior.get('fingerprint')==fingerprint and prior.get('batch_id')!=batch_id:
                skipped.append({'property_key':k,'case_number':r.get('case_number'),'reason':'unchanged_since_prior_delivery'});continue
            rr=dict(r);rr['event_fingerprint']=fingerprint;out.append(rr)
            if len(out)>=TARGET:break
        return out,skipped

    fresh_h,skip_h=fresh(houses); fresh_l,skip_l=fresh(land)
    for i,r in enumerate(fresh_h,1):r['rank']=i
    for i,r in enumerate(fresh_l,1):r['rank']=i
    for r in fresh_h+fresh_l:
        props[key(r)]={'fingerprint':r['event_fingerprint'],'batch_id':batch_id,'last_case_number':r.get('case_number'),'last_event_date':r.get('event_date'),'property_type':r.get('property_type')}
    ledger['last_batch_id']=batch_id; ledger['last_finalized_counts']={'houses':len(fresh_h),'land':len(fresh_l)}
    LEDGER.write_text(json.dumps(ledger,indent=2,ensure_ascii=False),encoding='utf-8')

    status='PASS' if len(fresh_h)==TARGET and len(fresh_l)==TARGET else 'PARTIAL'
    report={'status':status,'batch_id':batch_id,'minimum_priority':MIN_PRIORITY,'target_each':TARGET,'houses_count':len(fresh_h),'land_count':len(fresh_l),
            'houses':fresh_h,'land':fresh_l,'unchanged_skipped':{'houses':skip_h,'land':skip_l},
            'guardrails':{'fresh_event_fingerprint_required':True,'unchanged_repeat_suppressed_across_new_batches':True,'same_batch_rerender_idempotent':True,'priority_floor_60':True}}
    (OUT/'current.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'houses':len(fresh_h),'land':len(fresh_l),'skipped_unchanged':len(skip_h)+len(skip_l)},indent=2))
    return 0 if status=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
