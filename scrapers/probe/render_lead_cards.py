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

def owner_name(x):
    return clean(x.get('owner_name') or x.get('pva_owner')) or 'Owner'

def owner_confidence(x):
    if x.get('pva_verified') is True and owner_name(x) != 'Owner':
        return 'CONFIRMED CURRENT PVA OWNER — verify the contacted phone/person before discussing private details'
    if owner_name(x) != 'Owner':
        return 'PUBLIC-RECORD OWNER NAME — current ownership not fully confirmed in this row'
    return 'UNKNOWN — verify ownership before outreach'

def mailing_comparison(x):
    situs=clean(x.get('pva_situs_address') or x.get('property_address'))
    mailing=clean(x.get('owner_mailing_address') or x.get('pva_mailing_address'))
    if not mailing: return 'UNKNOWN — mailing address unavailable'
    if not situs: return 'UNKNOWN — situs comparison unavailable'
    norm=lambda v: re.sub(r'[^A-Z0-9]','',v.upper())
    return 'SAME AS PROPERTY' if norm(situs)==norm(mailing) else 'DIFFERENT FROM PROPERTY — possible absentee-owner signal; verify'

def source_presence(x, source, label):
    evidence=[v for v in (x.get('evidence') or []) if isinstance(v,dict) and clean(v.get('source'))==source]
    sources={clean(v) for v in (x.get('sources') or [])}
    if source not in sources and not evidence: return f"**{label}:** Not returned in this scrape"
    dates=sorted({clean(v.get('signal_date')) for v in evidence if clean(v.get('signal_date'))})
    return f"**{label}:** PRESENT" + (f" — {', '.join(dates)}" if dates else '')

def distress_status_lines(x):
    demolition=(
        'VERIFIED COMPLETED' if x.get('demolition_verified') is True
        else 'Transition/watch signal only — completion not verified' if x.get('possible_structure_to_lot_transition')
        else 'Not verified'
    )
    return [
        source_presence(x,'lis_pendens','Lis Pendens'),
        source_presence(x,'louisville_code_violations','Louisville code/property-maintenance record'),
        f"**Tax delinquency:** {tax_line(x)}",
        source_presence(x,'wills','Will/probate-source signal') + (' — a will filing is not proof of an active probate case' if 'wills' in (x.get('sources') or []) else ''),
        f"**Citation:** {citation_line(x)}",
        f"**Demolition:** {demolition}",
        '**Current market status:** NOT CHECKED — listing-status screening is disabled by Reaper policy',
    ]

def official_source_lines(x):
    seen=set(); lines=[]
    for item in (x.get('evidence') or []):
        if not isinstance(item,dict): continue
        url=clean(item.get('source_url')); source=clean(item.get('source')) or 'public record'
        if not url or url in seen: continue
        seen.add(url); lines.append(f"- **{source}:** {url}")
    for source,url in (('primary source',clean(x.get('source_url'))),('Jefferson PVA',clean(x.get('pva_url')))):
        if url and url not in seen:
            seen.add(url); lines.append(f"- **{source}:** {url}")
    return lines or ['- No direct URL retained — verify through the parcel/PVA links above before outreach']

def specific_questions(x, land=False):
    signals={clean(v) for v in (x.get('ai_signals') or [])}
    sources={clean(v) for v in (x.get('sources') or [])}
    questions=[]
    def add(v):
        if v not in questions: questions.append(v)
    if land:
        add('Is the parcel completely vacant today, or is there still any structure or debris on it?')
        add('Do you have a recent survey, and is there confirmed legal road access?')
        add('What utilities are available at the parcel—water, sewer, electric, or septic/perc information?')
        if {'municipal_cleanup','trash_or_dumping','overgrown_vegetation','repeat_abatement'} & signals:
            add('What cleanup, mowing, dumping, or abatement work is still needed?')
        add('Are there any easements, restrictions, floodplain issues, HOA rules, or adjacent parcels involved?')
        add('Were you planning to build, hold it, or sell it—and what changed?')
    else:
        add('What kind of shape is the property in right now, beyond normal cosmetic work?')
        add('Is anybody living there right now, and if not, how long has it been vacant?')
        if {'structural_damage','unsafe_structure','demolition_risk','habitability'} & signals:
            add('What happened with the structural or safety issues, and has anyone quoted the repairs?')
        if {'roof_risk','water_damage','mold'} & signals:
            add('Has the roof or water intrusion been repaired, and is there any remaining mold or interior damage?')
        if {'electrical','utility_issue','fire_risk'} & signals:
            add('Are the utilities currently on, and are there known electrical, fire, plumbing, HVAC, or permit issues?')
        if {'infestation','trash_or_debris','overgrown_vegetation','nuisance'} & signals:
            add('What cleanup, pest, debris, or exterior work still needs to be handled?')
        if 'louisville_code_violations' in sources or 'repeated_noncompliance' in signals:
            add('What has happened with the city/property-maintenance matter, and are you planning to fix it or sell as-is?')
        if 'lis_pendens' in sources:
            add('Are you leaning toward keeping the property, or would selling it now solve a problem for you?')
        add('If the numbers made sense, how quickly would you realistically want it sold?')
    return questions[:7]

def source_phrase(x):
    sources={clean(v) for v in (x.get('sources') or [])}
    if 'louisville_code_violations' in sources: return 'a Louisville Metro property-maintenance record'
    if 'lis_pendens' in sources: return 'a Jefferson County Lis Pendens filing'
    if 'wills' in sources: return 'a Jefferson County public filing record'
    if 'tax_delinquent' in sources: return 'Jefferson County public property records'
    return 'Jefferson County public property records'

def outreach_package(x, land=False):
    owner=owner_name(x); address=full_address(x); city=clean(x.get('city')) or 'Louisville'
    noun='parcel' if land else 'property'; phrase=source_phrase(x)
    return [
        '### Frozen outreach package',
        '',
        '**Call opener:**',
        f'> “Hey {owner}, this is Zack. I wanted to call you about your {noun} over on {address}. Just wanted to see, man — would you consider selling that {noun} if the price made sense? I’m not here to waste your time, I’m just here to make you a good offer.”',
        '',
        '**If the seller asks what you are offering:**',
        '> “Absolutely. I don’t want to throw something random at you without knowing anything about the place. What’s the property like right now?”',
        '',
        '**How did you find me?:**',
        f'> “Man, I do quite a bit of property research around {city}. I was looking through public property records, including {phrase}, and came across your {noun}. I figured I’d reach out directly and see if selling might make sense for you. If it did, I can buy it as-is, close quickly, and you wouldn’t have to worry about repairs or agent commissions.”',
        '',
        '**Confirmed-owner voicemail:**',
        f'> “Hey {owner}, this is Zack. I was giving you a quick call regarding your {noun} over on {address} in {city}. Just had a quick question for you about it. Whenever you get a chance, give me a call or shoot me a text back. Again, this is Zack. Thanks.”',
        '',
        '**Unconfirmed-owner voicemail:**',
        f'> “Hey, this is Zack. I’m trying to get in touch with {owner} regarding a {noun} over on {address} in {city}. I’m not sure if I’ve got the right number or not, but I wanted to see if {owner} would consider selling the {noun} if the price made sense. If this is {owner}, whenever you get a chance, give me a call or shoot me a text back. If I’ve got the wrong person, no worries at all. Thanks.”',
        '',
        '**Confirmed-owner text:**',
        f'> “Hey {owner}, this is Zack. I’m reaching out about the {noun} you own over on {address} in {city}. Just wanted to see if you’d consider selling it if the price made sense. I can buy it as-is and keep the process pretty simple. Thanks!”',
        '',
        '**Unconfirmed-owner text:**',
        f'> “Hey, my name’s Zack. I’m trying to get in touch with {owner} regarding a {noun} over on {address} in {city}. I was reaching out to see if they’d consider selling it if the price made sense. Not sure if I’ve got the right number or not, but if this is {owner}, if you could shoot me a text or call I’d really appreciate it. If I’ve got the wrong person, sorry to bother you & thanks!”',
        '',
    ]

def universal_seller_flow():
    return [
        '### Universal seller qualification flow',
        '',
        '1. **Condition:** “What kind of shape is the property in right now?”',
        '2. **Occupancy:** “Is anybody living there right now?”',
        '3. **Motivation:** “What has you open to selling it instead of just keeping it?”',
        '4. **Timeline:** “If we could agree on something that made sense, when would you ideally want to have it sold?”',
        '5. **Price:** “What were you hoping to get for it?”',
        '6. **Debt/terms:** “Do you still have any financing on the property?”',
        '7. **Decision:** “Besides yourself, is there anybody else who would need to be involved in deciding whether to sell?”',
        '8. **Close:** “Gotcha. Based on everything you told me, let me run the numbers and see what I can realistically make work. If I can put something together that makes sense for both of us, are you open to moving forward pretty quickly?”',
        '',
    ]

def land_card(x:dict)->str:
    questions=specific_questions(x,True)
    action='Call first and use both identity-safe and confirmed-owner outreach; verify parcel access, utilities, cleanup burden, and sell-vs-hold motivation.'
    lines=[
      f"## #{x.get('rank')}  🔥  {full_address(x)}",
      f"**PRIVATE LAND · {x.get('priority_tier')}**",
      '',
      f"**Overall Priority Score:** **{x.get('priority_score')}/100** — combined call-order score",
      f"**AI Motivation Classification:** **{ai_level(x, True)}** — GPT evidence classification, not a model-invented numeric score",
      f"**Land Motivation Score:** **{x.get('motivation_score')}/100** — deterministic score from verified/grounded motivation evidence",
      f"**Builder Fit Score:** **{x.get('builder_fit_score')}/100** — estimated parcel usefulness; not a buildability guarantee",
      f"**Public-Source Exposure:** **{x.get('saturation_score')}/100** — deterministic saturation heuristic; not observed investor competition",
      f"**Exposure method:** {clean(x.get('saturation_method')) or '—'}",
      f"**Exposure factors:** {excerpt('; '.join(clean(v) for v in (x.get('saturation_factors') or []) if clean(v)),700) or '—'}",
      f"**Freshness Score:** **{x.get('freshness_score')}/100** — {x.get('freshness_label')}",
      '',
      f"**Site status:** {land_occupancy(x)}  _(according to {OCCUPANCY_SOURCE})_",
      f"**Why this lead:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Key complaint / distress:** {excerpt(x.get('description_raw') or ((x.get('confirmed_facts') or ['—'])[0]))}",
      f"**Confirmed facts:** {excerpt('; '.join(clean(v) for v in (x.get('confirmed_facts') or []) if clean(v))) or '—'}",
      f"**Owner:** {owner_name(x)}",
      f"**Ownership confidence:** {owner_confidence(x)}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
      f"**Mailing vs property:** {mailing_comparison(x)}",
    ]+pva_lines(x)+[
      f"**Lot:** {x.get('lot_sqft') or '—'} SF / {x.get('lot_acres') or '—'} ac",
      f"**Zoning:** {clean(x.get('zoning_code')) or '—'} {clean(x.get('zoning_name'))} · **Land use:** {clean(x.get('landuse_name')) or '—'}",
      '',
      '### Distress/source status',
      '',
    ]+distress_status_lines(x)+[
      '',
      '### Exact official/public sources',
      '',
    ]+official_source_lines(x)+[
      '',
      f"**Recommended action:** {action}",
      '**GOAL:** Confirm that this is a privately controlled usable parcel, uncover the owner’s carrying burden and motivation, establish timeline/price, and secure a follow-up or written offer opportunity.',
      '',
      '### Property-specific questions',
      '',
    ]+[f"{i}. {q}" for i,q in enumerate(questions,1)]+['']+outreach_package(x,True)+universal_seller_flow()+['---','']
    return '\n'.join(lines)

def house_card(x:dict)->str:
    occ=occupancy_from(x)
    questions=specific_questions(x,False)
    action='Call first and use the confirmed-owner version only after matching the contacted person; otherwise use the unconfirmed voicemail/text and verify identity before discussing details.'
    lines=[
      f"## #{x.get('rank')}  🔥  {full_address(x)}",
      f"**SINGLE-FAMILY · {x.get('priority_tier')}**",
      '',
      f"**Overall Priority Score:** **{x.get('priority_score')}/100** — combined call-order score",
      f"**AI Distress Classification:** **{ai_level(x)}** — GPT evidence classification, not a model-invented numeric score",
      f"**Distress Score:** **{x.get('distress_score')}/100** — deterministic score from AI-classified + source-grounded distress evidence",
      f"**Public-Source Exposure:** **{x.get('saturation_score')}/100** — deterministic saturation heuristic; not observed investor competition",
      f"**Exposure method:** {clean(x.get('saturation_method')) or '—'}",
      f"**Exposure factors:** {excerpt('; '.join(clean(v) for v in (x.get('saturation_factors') or []) if clean(v)),700) or '—'}",
      f"**Freshness Score:** **{x.get('freshness_score')}/100** — {x.get('freshness_label')}",
      '',
      f"**Occupancy:** {occ}  _(according to {OCCUPANCY_SOURCE}; conflicts are shown, never silently resolved)_",
      f"**Why this lead:** {excerpt(x.get('ai_summary')) or '—'}",
      f"**Key complaint / distress:** {excerpt(x.get('description_raw')) or '—'}",
      f"**Inspector / confirmed evidence:** {excerpt('; '.join(clean(v) for v in (x.get('inspector_comments') or []) if clean(v))) or '—'}",
      f"**Owner:** {owner_name(x)}",
      f"**Ownership confidence:** {owner_confidence(x)}",
      f"**Owner mailing:** {clean(x.get('owner_mailing_address')) or '—'}",
      f"**Mailing vs property:** {mailing_comparison(x)}",
    ]+pva_lines(x)+[
      f"**SFR screen:** LOJIC land use = {clean(x.get('landuse_name')) or '—'}",
      '',
      '### Distress/source status',
      '',
    ]+distress_status_lines(x)+[
      '',
      '### Exact official/public sources',
      '',
    ]+official_source_lines(x)+[
      '',
      f"**Recommended action:** {action}",
      '**GOAL:** Confirm identity, condition, occupancy, motivation, timeline, price, financing, and decision-makers; finish with a clear follow-up, appointment, or offer commitment.',
      '',
      '### Property-specific questions',
      '',
    ]+[f"{i}. {q}" for i,q in enumerate(questions,1)]+['']+outreach_package(x,False)+universal_seller_flow()+['---','']
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
