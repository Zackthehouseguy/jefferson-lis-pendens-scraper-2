#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,re
from datetime import datetime,timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ET=ZoneInfo('America/New_York')
SOURCE_FILES={
 'lis_pendens':'lis_pendens_results.csv','wills':'wills_results.csv',
 'louisville_code_violations':'louisville_code_violations_results.csv',
 'tax_delinquent':'jefferson_tax_delinquent_results.csv',
 'louisville_landbank':'louisville_landbank_results.csv'}
ENTITY_TERMS=(' LLC',' L.L.C',' INC',' CORPORATION',' CORP',' COMPANY',' TRUST',' ESTATE',' HEIRS',' DEVISEES',' CHURCH',' MINISTRY',' FOUNDATION',' HOLDINGS',' PROPERTIES',' ASSETS',' INVESTMENTS',' DEVELOPMENT',' REALTY',' ASSOCIATION',' PARTNERSHIP',' LLP',' AUTHORITY',' METRO',' COUNTY',' CITY OF ',' COMMONWEALTH',' BANK ',' MORTGAGE ',' FINANCIAL ',' CREDIT UNION ',' SERVICING ')

def clean(v): return re.sub(r'\s+',' ',str(v or '')).strip()
def norm_addr(v):
 s=clean(v).upper()
 if not s or s in {'ADDRESS NOT FOUND','N/A','NONE'}: return ''
 for a,b in {' STREET':' ST',' AVENUE':' AVE',' ROAD':' RD',' DRIVE':' DR',' LANE':' LN',' COURT':' CT',' BOULEVARD':' BLVD',' PLACE':' PL',' TERRACE':' TER',' HIGHWAY':' HWY',' PARKWAY':' PKWY'}.items(): s=s.replace(a,b)
 s=re.sub(r'\bLOUISVILLE\b.*$','',s).strip();s=re.sub(r'[^A-Z0-9 ]',' ',s)
 return re.sub(r'\s+',' ',s).strip()
def money(v):
 s=re.sub(r'[^0-9.]','',clean(v))
 try:return float(s) if s else None
 except:return None

def one_party_individual(p):
 u=' '+clean(p).upper()+' '
 if not clean(p): return None
 if any(t in u for t in ENTITY_TERMS): return False
 alpha=[x for x in re.findall(r'[A-Z]+',u) if len(x)>1]
 return len(alpha)>=2

def likely_individual(name):
 n=clean(name)
 if not n:return None
 parts=[clean(x) for x in re.split(r';|\||\n',n) if clean(x)]
 flags=[one_party_individual(x) for x in parts]
 # Deeds filings often include a lender plus a human defendant; one clear
 # human party is enough to keep the property for PVA verification.
 if True in flags:return True
 if flags and all(x is False for x in flags):return False
 return None

def read_csv(p):
 if not p.exists():return []
 with p.open('r',encoding='utf-8-sig',newline='') as f:return [dict(r) for r in csv.DictReader(f)]

def parse_source(source,rows):
 out=[]
 for r in rows:
  if source=='wills':
   addr=clean(r.get('Property Address'));party=clean(r.get('Decedent') or r.get('Parties'));date=clean(r.get('Filing Date')) or None;url=clean(r.get('PDF Link'));notes=clean(r.get('Notes'));parcel='';amount=None;status='';details=' | '.join(x for x in [notes,clean(r.get('Complexity Flag')),clean(r.get('Complexity Reasons'))] if x)
  elif source in {'lis_pendens','louisville_landbank'}:
   addr=clean(r.get('Property Address'));party=clean(r.get('Defendants/Parties'));date=clean(r.get('Date')) or None;url=clean(r.get('PDF Link'));notes=clean(r.get('Notes'));parcel='';amount=None;status='';details=notes
  elif source=='louisville_code_violations':
   addr=clean(r.get('Property Address'));party=clean(r.get('Parties'));date=clean(r.get('Filing Date')) or None;url=clean(r.get('Source Link') or r.get('PDF Link'));notes=clean(r.get('Notes'));parcel=clean(r.get('Parcel'));amount=money(r.get('Citation Total'));status=clean(r.get('Status'));details=' | '.join(x for x in [clean(r.get('Distress Signals')),clean(r.get('Violation Codes')),notes] if x)
  elif source=='tax_delinquent':
   addr=clean(r.get('Property Address'));party=clean(r.get('Parties'));date=clean(r.get('Filing Date')) or None;url=clean(r.get('Source Link'));notes=clean(r.get('Notes'));parcel=clean(r.get('Parcel ID'));amount=money(r.get('Amount Due'));status=clean(r.get('Status'));details=' | '.join(x for x in [f"Tax year {clean(r.get('Tax Year'))}" if clean(r.get('Tax Year')) else '',f"Amount due {clean(r.get('Amount Due'))}" if clean(r.get('Amount Due')) else '',notes] if x)
  else:continue
  if not addr and not parcel:continue
  rawscore=None
  if source=='louisville_code_violations' and clean(r.get('Distress Score')):
   try:rawscore=int(float(r.get('Distress Score')))
   except:pass
  out.append({'source':source,'signal_date':date,'property_address':addr or None,'normalized_address':norm_addr(addr),'parcel_id':parcel or None,'party_or_owner':party or None,'individual_party_likely':likely_individual(party),'status':status or None,'amount':amount,'details':details or None,'source_url':url or None,'raw_distress_score':rawscore})
 return out

def assigned_state(path):
 parcels,addrs=set(),set()
 try:data=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
 except:return parcels,addrs
 for rec in (data.get('properties') or {}).values():
  if clean(rec.get('status') or 'active').lower()=='released':continue
  p=clean(rec.get('parcel_id')).upper();a=norm_addr(rec.get('property_address'))
  if p:parcels.add(p)
  if a:addrs.add(a)
 return parcels,addrs

def seen_state(path):
 try:data=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
 except:return set()
 return {clean(k).upper() for k in (data.get('parcels') or {}) if clean(k)}

def score(evidence):
 sources={e['source'] for e in evidence};s=0;why=[]
 if 'lis_pendens' in sources:s+=48;why.append('recent lis pendens filing')
 if 'tax_delinquent' in sources:
  s+=24;mx=max([e.get('amount') or 0 for e in evidence if e['source']=='tax_delinquent'] or [0])
  if mx>=10000:s+=16;why.append(f'large published delinquent-tax balance (${mx:,.0f})')
  elif mx>=5000:s+=10;why.append(f'meaningful published delinquent-tax balance (${mx:,.0f})')
  else:why.append('published delinquent-tax listing')
 if 'wills' in sources:s+=18;why.append('will/probate-related filing')
 if 'louisville_landbank' in sources:s+=10;why.append('landbank inventory')
 if 'louisville_code_violations' in sources:
  cr=[e for e in evidence if e['source']=='louisville_code_violations'];raw=max([e.get('raw_distress_score') or 0 for e in cr] or [0]);s+=min(34,max(18,round(raw*.34)))
  text=' '.join(clean(e.get('details')).upper() for e in cr);terms=('CONDEMN','UNSAFE','STRUCTURAL','FOUNDATION','ABANDON','VACANT','TERMINATED UTIL','FIRE','NO WATER','NO ELECTRIC','ROOF');hits=sorted({t for t in terms if t in text})
  if hits:s+=16;why.append('severe code indicators: '+', '.join(hits[:4]).lower())
  else:why.append('active code-enforcement distress')
 if len(sources)>=3:s+=32;why.append(f'{len(sources)} independent signal types stacked')
 elif len(sources)==2:s+=24;why.append('2 independent signal types stacked')
 return min(100,s),why

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--root',default='reaper_live_sources');ap.add_argument('--out',default='reports/reaper_multi_source_live/stacked.json');ap.add_argument('--assignments',default='state/lead_assignments.json');ap.add_argument('--seen',default='state/full_system_seen.json');ap.add_argument('--start-date');ap.add_argument('--end-date');ap.add_argument('--top',type=int,default=150);a=ap.parse_args()
 root=Path(a.root);counts={};statuses={};ev=[]
 for src,fn in SOURCE_FILES.items():
  p=root/src/fn;rows=read_csv(p);counts[src]=len(rows);statuses[src]='OK' if p.exists() else 'MISSING_OR_FAILED';ev+=parse_source(src,rows)
 assigned_p,assigned_a=assigned_state(Path(a.assignments));seen=seen_state(Path(a.seen));groups={}
 for e in ev:
  p=clean(e.get('parcel_id')).upper();ad=e.get('normalized_address') or '';k='PARCEL::'+p if p else ('ADDR::'+ad if ad else '')
  if k:groups.setdefault(k,[]).append(e)
 addrmap={}
 for k,rows in groups.items():
  if k.startswith('PARCEL::'):
   for r in rows:
    if r.get('normalized_address'):addrmap.setdefault(r['normalized_address'],k)
 for k in list(groups):
  if k.startswith('ADDR::'):
   tgt=addrmap.get(k.split('::',1)[1])
   if tgt and tgt!=k:groups[tgt]+=groups.pop(k)
 props=[]
 for k,evidence in groups.items():
  sc,why=score(evidence);ps=[clean(e.get('parcel_id')).upper() for e in evidence if clean(e.get('parcel_id'))];parcel=ps[0] if ps else None;ads=[clean(e.get('property_address')) for e in evidence if clean(e.get('property_address'))];addr=ads[0] if ads else None;names=[]
  for e in evidence:
   n=clean(e.get('party_or_owner'))
   if n and n not in names:names.append(n)
  flags=[e.get('individual_party_likely') for e in evidence if e.get('individual_party_likely') is not None];individual=True if True in flags else (False if flags and all(x is False for x in flags) else None);norm=norm_addr(addr);assigned=bool((parcel and parcel in assigned_p) or (norm and norm in assigned_a))
  props.append({'property_key':k,'property_address':addr,'parcel_id':parcel,'parties_or_owners':names,'individual_owner_or_party_likely':individual,'sources':sorted({e['source'] for e in evidence}),'signal_count':len({e['source'] for e in evidence}),'motivation_score':sc,'motivation_class':'HIGH' if sc>=70 else ('MEDIUM' if sc>=50 else 'LOW'),'score_reasons':why,'previously_assigned':assigned,'previously_seen_in_code_queue':bool(parcel and parcel in seen),'fresh_unworked_candidate':not assigned,'evidence':evidence})
 props.sort(key=lambda r:(r['motivation_score'],r['signal_count']),reverse=True);fresh=[r for r in props if r['fresh_unworked_candidate'] and r['individual_owner_or_party_likely'] is True];high=[r for r in fresh if r['motivation_score']>=70];med=[r for r in fresh if 50<=r['motivation_score']<70]
 report={'status':'PASS' if any(v=='OK' for v in statuses.values()) else 'FAIL','generated_at_et':datetime.now(timezone.utc).astimezone(ET).isoformat(),'query_window':{'start_date':a.start_date,'end_date':a.end_date},'source_status':statuses,'source_record_counts':counts,'properties_after_stacking':len(props),'fresh_individual_candidates':len(fresh),'fresh_individual_high_count':len(high),'fresh_individual_medium_count':len(med),'top_fresh_individual':fresh[:a.top],'top_all':props[:min(a.top,100)],'scoring_note':'Deterministic pre-score only; ChatGPT performs final evidence-grounded AI ranking.','tax_note':'Published delinquent-tax list may contain bills paid after publication; verify current status before outreach.'}
 out=Path(a.out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8');print(json.dumps({k:report[k] for k in ('status','source_record_counts','properties_after_stacking','fresh_individual_candidates','fresh_individual_high_count','fresh_individual_medium_count')},indent=2));return 0 if report['status']=='PASS' else 2
if __name__=='__main__':raise SystemExit(main())
