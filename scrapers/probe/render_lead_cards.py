#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,re
from pathlib import Path

def clean(v): return ' '.join(str(v or '').split()).strip()
def money(x): return f"${float(x):,.0f}" if x is not None else '—'
def full_address(x):
    raw=clean(x.get('property_address')); city=clean(x.get('city')); state=clean(x.get('state')); zipcode=clean(x.get('zip'))
    # Accela currently often returns a full situs string in property_address.
    # Do not duplicate city/state/ZIP if already embedded.
    up=raw.upper()
    if zipcode and zipcode in raw and state and state.upper() in up and city and city.upper() in up:
        return raw
    parts=[raw]
    if city and city.upper() not in up: parts.append(city)
    tail=' '.join(v for v in (state,zipcode) if v)
    if tail and tail.upper() not in up: parts.append(tail)
    return ', '.join(p for p in parts if p)

def land_card(x:dict)->str:
    address=full_address(x)
    assessed=float(x.get('citation_assessed_total') or 0)
    balance=x.get('verified_current_outstanding_balance')
    money_line=(f"{money(assessed)} citation assessed; current balance {'verified '+money(balance) if balance is not None else 'unverified'}" if assessed else 'No assessed citation in current extract')
    tax=(f"VERIFIED — {money(x.get('tax_bill_total'))}" if x.get('tax_delinquent_verified') is True and x.get('tax_bill_total') is not None else ('VERIFIED' if x.get('tax_delinquent_verified') is True else ('No delinquent bill returned for tested search' if x.get('tax_delinquent_verified') is False else 'Not verified in this run')))
    return '\n'.join([
      f"## #{x.get('rank')} — {address}",
      f"**LAND | {x.get('priority_tier')} | Priority {x.get('priority_score')}/100**",
      f"Motivation **{x.get('motivation_score')}/100** · Builder Fit **{x.get('builder_fit_score')}/100** · Saturation **{x.get('saturation_score')}/100** · Freshness **{x.get('freshness_score')}/100 ({x.get('freshness_label')})**",
      f"**Occupancy:** Vacant lot",
      f"**Why:** {clean(x.get('ai_summary')) or '—'}",
      f"**Owner:** {clean(x.get('owner_name')) or '—'}",
      f"**Parcel:** {clean(x.get('parcel_id')) or '—'} · **Lot:** {x.get('lot_sqft') or '—'} SF / {x.get('lot_acres') or '—'} ac",
      f"**Zoning:** {clean(x.get('zoning_code')) or '—'} {clean(x.get('zoning_name'))} · **Land use:** {clean(x.get('landuse_name')) or '—'}",
      f"**Citation:** {money_line}",
      f"**Tax delinquent:** {tax}",
      f"**Demolition:** {'VERIFIED COMPLETED' if x.get('demolition_verified') is True else ('Transition/watch signal' if x.get('possible_structure_to_lot_transition') else 'Not verified')}",
      f"**Source:** {clean(x.get('source_url')) or '—'}",
      ''
    ])

def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument('--input',required=True);ap.add_argument('--output',required=True);ap.add_argument('--kind',choices=['land'],default='land');args=ap.parse_args()
    d=json.loads(Path(args.input).read_text(encoding='utf-8'))
    rows=d.get('ranked_land') or d.get('ranked_live_leads') or []
    cards=['# TheReaper Lead Cards','',f"Records: **{len(rows)}**",'']+[land_card(x) for x in rows]
    Path(args.output).write_text('\n'.join(cards),encoding='utf-8')
    print(json.dumps({'status':'PASS','cards':len(rows),'output':args.output},indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
