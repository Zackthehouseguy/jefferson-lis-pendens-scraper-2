#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,re
from pathlib import Path

PVA_SEARCH='https://jeffersonpva.ky.gov/property-search/'
OCCUPANCY_SOURCE='Louisville Metro Property Maintenance / Accela'
PARCEL_SOURCE='LOJIC / Jefferson County parcel data'

def clean(v): return ' '.join(str(v or '').split()).strip()
def money(x): return f"${float(x):,.0f}" if x is not None else '—'
def excerpt(v,n=360):
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
    return [
        f"**Parcel ID:** `{parcel}`",
        f"**PVA lookup:** {PVA_SEARCH}  ·  search Parcel ID `{parcel}`",
        f"**Parcel-data source:** {PARCEL_SOURCE}",
    ]

def occupancy_from(x):
    vals=[clean(v).upper() for v in (x.get('recent_window_occupancies') or []) if clean(v)]
    structured='Vacant' if any('VACANT STRUCTURE' in v for v in vals) else ('Occupied' if any('OCCUPIED STRUCTURE' in v for v in vals) else clean(x.get('occupancy')) or 'Unknown')
    narrative=(clean(x.get('description_raw'))+' '+' '.join(clean(v) for v in (x.get('inspector_comments') or []))).upper()
    narrative_vacant=bool(re.search(r'\bVACANT\b|\bABANDONED\b|\bUNOCCUPIED\b',narrative))
    narrative_occupied=bool(re.search(r'\bTENANT\b|\bRENTER\b|\bOCCUPIED\b|\bLIVES? (?:HERE|THERE|IN)\b',narrative))
    if structured=='Occupied' and narrative_vacant:
        return 'CONFLICT — structured source says Occupied; narrative reports vacant/abandoned. Verify before relying on occupancy.'
    if structured=='Vacant' and narrative_occupied:
        return 'CONFLICT — structured source says Vacant; narrative indicates occupancy. Verify before relying on occupancy.'
    return structured

def land_occupancy(x):
    narrative=(clean(x.get('description_raw'))+' '+' '.join(clean(v) for v in (x.get('inspector_comments') or []))).upper()
    building_terms=('HOUSE','BUILDING','STRUCTURE','ROOF','BEDROOM','KITCHEN','WINDOW','FURNACE','CEILING')
    if any(t in narrative for t in building_terms):
        return 'Vacant lot per structured source — narrative references a structure; verify parcel/structure status'
    return 'Vacant lot'

def citation_line(x):
    assessed=float(x.get('citation_assessed_total') or 0)
    balance=x.get('verified_current_outstanding_balance',x.get('outstanding_balance'))
    if assessed:
        return f"{money(assessed)} assessed; current outstanding balance {'VERIFIED '+money(balance) if balance is not None else 'NOT VERIFIED'}"
    return 'No assessed citation in current extract'

def tax_line(x):
    if x.get('tax_delinquent_verified') is True:
        return f"VERIFIED — {money(x.get('tax_bill_total'))}" if x.get('tax_bill_total') is not None else 'VERIFIED'
    if x.get('tax_delinquent_verified') is False: return 'No delinquent bill returned for the verified search scope'
    return 'UNKNOWN / not verified in this run'

def ai_level(x, land=False):
    return clean(x.get('ai_motivation_level') if land else x.get('ai_distress_level')) or '—'

def common_sources(x):
    return [
      f"**Official case/source:** {clean(x.get('source_url')) or '—'}",
      f"**Occupancy/site-status source:** {OCCUPANCY_SOURCE}",
    ]

def land_card(x:dict)->str:
    lines=[
      f"## #{x.get('rank')}  🔥  {full_address(x)}",
      f"**PRIVATE LAND · {x.get('priority_tier')}**",
      '',
      f"**Overall Priority Score:** **{x.get('priority_score')}/100** — combined call-order score",
      f"**AI Motivation Classification:** **{ai_level(x, True)}** — GPT evidence classification, not a model-invented numeric score",
      f"**Land Motivation Score:** **{x.get('motivation_score')}/100** — deterministic score from verified/grounded motivation evidence",
      f"**Builder Fit Score:** **{x.get('builder_fit_score')}/100** — estimated parcel usefulness; not a buildability guarantee",
      f"**Saturation Risk:** **{x.get('saturation_score')}/100** — higher means more likely to be on generic investor lists",
      f"**Freshness Score:** **{x.get('freshness_score')}/100** — {x.get('freshness_label')}",
      '',
      f"**Site status:** {land_occupancy(x)}  _(according to {OCCUPANCY_SOURCE})_",
      f"**Why this lead:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Key complaint / distress:** {excerpt(x.get('description_raw') or ((x.get('confirmed_facts') or ['—'])[0]))}",
      f"**Confirmed facts:** {excerpt('; '.join(clean(v) for v in (x.get('confirmed_facts') or []) if clean(v))) or '—'}",
      f"**Owner:** {clean(x.get('owner_name')) or '—'}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
    ]+pva_lines(x)+[
      f"**Lot:** {x.get('lot_sqft') or '—'} SF / {x.get('lot_acres') or '—'} ac",
      f"**Zoning:** {clean(x.get('zoning_code')) or '—'} {clean(x.get('zoning_name'))} · **Land use:** {clean(x.get('landuse_name')) or '—'}",
      f"**Citation:** {citation_line(x)}",
      f"**Tax delinquency:** {tax_line(x)}",
      f"**Demolition:** {'VERIFIED COMPLETED' if x.get('demolition_verified') is True else ('Transition/watch signal only — completion not verified' if x.get('possible_structure_to_lot_transition') else 'Not verified')}",
    ]+common_sources(x)+['','---','']
    return '\n'.join(lines)

def house_card(x:dict)->str:
    occ=occupancy_from(x)
    lines=[
      f"## #{x.get('rank')}  🔥  {full_address(x)}",
      f"**SINGLE-FAMILY · {x.get('priority_tier')}**",
      '',
      f"**Overall Priority Score:** **{x.get('priority_score')}/100** — combined call-order score",
      f"**AI Distress Classification:** **{ai_level(x)}** — GPT evidence classification, not a model-invented numeric score",
      f"**Distress Score:** **{x.get('distress_score')}/100** — deterministic score from AI-classified + source-grounded distress evidence",
      f"**Saturation Risk:** **{x.get('saturation_score')}/100** — higher means more likely to be on generic investor lists",
      f"**Freshness Score:** **{x.get('freshness_score')}/100** — {x.get('freshness_label')}",
      '',
      f"**Occupancy:** {occ}  _(according to {OCCUPANCY_SOURCE}; conflicts are shown, never silently resolved)_",
      f"**Why this lead:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Key complaint / distress:** {excerpt(x.get('description_raw')) or '—'}",
      f"**Inspector / confirmed evidence:** {excerpt('; '.join(clean(v) for v in (x.get('inspector_comments') or []) if clean(v))) or '—'}",
      f"**Owner:** {clean(x.get('owner_name')) or '—'}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
    ]+pva_lines(x)+[
      f"**SFR screen:** LOJIC land use = {clean(x.get('landuse_name')) or '—'}",
      f"**Citation:** {citation_line(x)}",
      f"**Tax delinquency:** {tax_line(x)}",
    ]+common_sources(x)+['','---','']
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
        blocks=['# 🏠 SINGLE-FAMILY HOMES','']+[house_card(x) for x in houses]+['# 🌱 LAND','']+[land_card(x) for x in land]
    cards=['# TheReaper — Assigned Lead Cards','',f"Records: **{len(rows)}**",'','> Numeric scores are deterministic. GPT classifies evidence; it does not invent the score values.','']+blocks
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text('\n'.join(cards),encoding='utf-8')
    print(json.dumps({'status':'PASS','cards':len(rows),'output':args.output,'kind':args.kind},indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
