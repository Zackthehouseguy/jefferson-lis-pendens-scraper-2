#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,re
from pathlib import Path

PVA_SEARCH='https://jeffersonpva.ky.gov/property-search/'

def clean(v): return ' '.join(str(v or '').split()).strip()
def money(x): return f"${float(x):,.0f}" if x is not None else '—'
def excerpt(v,n=320):
    s=clean(v)
    return s if len(s)<=n else s[:n-1].rstrip()+"…"
def full_address(x):
    raw=clean(x.get('property_address')); city=clean(x.get('city')); state=clean(x.get('state')); zipcode=clean(x.get('zip'))
    up=raw.upper()
    if zipcode and zipcode in raw and state and state.upper() in up and city and city.upper() in up:
        return raw
    parts=[raw]
    if city and city.upper() not in up: parts.append(city)
    tail=' '.join(v for v in (state,zipcode) if v)
    if tail and tail.upper() not in up: parts.append(tail)
    return ', '.join(p for p in parts if p)

def pva_lines(x):
    parcel=clean(x.get('parcel_id')) or '—'
    return [f"**Parcel:** {parcel}", f"**PVA parcel search:** {PVA_SEARCH}  ·  Search Parcel ID: `{parcel}`"]

def occupancy_from(x):
    if clean(x.get('occupancy')): return clean(x.get('occupancy'))
    vals=[clean(v).upper() for v in (x.get('recent_window_occupancies') or []) if clean(v)]
    if any('VACANT STRUCTURE' in v for v in vals): return 'Vacant'
    if any('OCCUPIED STRUCTURE' in v for v in vals): return 'Occupied'
    return 'Unknown'

def citation_line(x):
    assessed=float(x.get('citation_assessed_total') or 0)
    balance=x.get('verified_current_outstanding_balance',x.get('outstanding_balance'))
    if assessed:
        return f"{money(assessed)} citation assessed; current balance {'verified '+money(balance) if balance is not None else 'unverified'}"
    return 'No assessed citation in current extract'

def tax_line(x):
    if x.get('tax_delinquent_verified') is True:
        return f"VERIFIED — {money(x.get('tax_bill_total'))}" if x.get('tax_bill_total') is not None else 'VERIFIED'
    if x.get('tax_delinquent_verified') is False: return 'No delinquent bill returned for the verified search scope'
    return 'Not verified in this run'

def land_card(x:dict)->str:
    lines=[
      f"## #{x.get('rank')} — {full_address(x)}",
      f"**LAND | {x.get('priority_tier')} | Priority {x.get('priority_score')}/100**",
      f"Motivation **{x.get('motivation_score')}/100** · Builder Fit **{x.get('builder_fit_score')}/100** · Saturation **{x.get('saturation_score')}/100** · Freshness **{x.get('freshness_score')}/100 ({x.get('freshness_label')})**",
      "**Occupancy:** Vacant lot",
      f"**Why:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Complaint / distress:** {excerpt(x.get('description_raw') or ((x.get('confirmed_facts') or ['—'])[0]))}",
      f"**Owner:** {clean(x.get('owner_name')) or '—'}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
    ]+pva_lines(x)+[
      f"**Lot:** {x.get('lot_sqft') or '—'} SF / {x.get('lot_acres') or '—'} ac",
      f"**Zoning:** {clean(x.get('zoning_code')) or '—'} {clean(x.get('zoning_name'))} · **Land use:** {clean(x.get('landuse_name')) or '—'}",
      f"**Citation:** {citation_line(x)}",
      f"**Tax delinquent:** {tax_line(x)}",
      f"**Demolition:** {'VERIFIED COMPLETED' if x.get('demolition_verified') is True else ('Transition/watch signal' if x.get('possible_structure_to_lot_transition') else 'Not verified')}",
      f"**Official enforcement source:** {clean(x.get('source_url')) or '—'}",
      ''
    ]
    return '\n'.join(lines)

def house_card(x:dict)->str:
    lines=[
      f"## #{x.get('rank')} — {full_address(x)}",
      f"**SINGLE-FAMILY | {x.get('priority_tier')} | Priority {x.get('priority_score')}/100**",
      f"Distress **{x.get('distress_score')}/100** · Saturation **{x.get('saturation_score')}/100** · Freshness **{x.get('freshness_score')}/100 ({x.get('freshness_label')})**",
      f"**Occupancy:** {occupancy_from(x)}",
      f"**Why:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Complaint / distress:** {excerpt(x.get('description_raw')) or '—'}",
      f"**Owner:** {clean(x.get('owner_name')) or '—'}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
    ]+pva_lines(x)+[
      f"**SFR screen:** LOJIC land use = {clean(x.get('landuse_name')) or '—'}",
      f"**Citation:** {citation_line(x)}",
      f"**Tax delinquent:** {tax_line(x)}",
      f"**Official enforcement source:** {clean(x.get('source_url')) or '—'}",
      ''
    ]
    return '\n'.join(lines)

def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument('--input',required=True);ap.add_argument('--output',required=True);ap.add_argument('--kind',choices=['land','house','mixed'],required=True);args=ap.parse_args()
    d=json.loads(Path(args.input).read_text(encoding='utf-8'))
    if args.kind=='land':
        rows=d.get('ranked_land') or d.get('land') or []
        blocks=[land_card(x) for x in rows]
    elif args.kind=='house':
        rows=d.get('ranked_live_leads') or d.get('houses') or []
        blocks=[house_card(x) for x in rows]
    else:
        houses=d.get('houses') or []; land=d.get('land') or []; rows=houses+land
        blocks=['# SINGLE-FAMILY HOMES','']+[house_card(x) for x in houses]+['# LAND','']+[land_card(x) for x in land]
    cards=['# TheReaper Lead Cards','',f"Records: **{len(rows)}**",'']+blocks
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text('\n'.join(cards),encoding='utf-8')
    print(json.dumps({'status':'PASS','cards':len(rows),'output':args.output,'kind':args.kind},indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
