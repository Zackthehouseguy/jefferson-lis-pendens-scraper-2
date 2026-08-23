#!/usr/bin/env python3
from __future__ import annotations
import io, json, re
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

OUT=Path('reports/land_external_sources')
PERMITS='https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/active_construction_permits/FeatureServer/0/query'
TAX_PAGE='https://www.jeffersoncountyclerk.org/delinquent-tax-lottery/'

def clean(v): return ' '.join(str(v or '').split())

def permit_probe():
    params={
      'where':"PERMIT_NUMBER LIKE 'BLD-WRE-%' OR UPPER(PERMIT_TYPE) LIKE '%WRECK%' OR UPPER(WORK_TYPE) LIKE '%DEMOL%'",
      'outFields':'PERMIT_NUMBER,PERMIT_TYPE,PERMIT_STATUS,CATEGORY_NAME,WORK_TYPE,ZONING,SQFT,PROJECT_COSTS,ADDRESS,CITY,STATE,ZIPCODE,ISSUE_DATE',
      'returnGeometry':'false','orderByFields':'ISSUE_DATE DESC','resultRecordCount':'25','f':'json'}
    r=requests.get(PERMITS,params=params,timeout=30); r.raise_for_status(); p=r.json()
    if p.get('error'): raise RuntimeError(p['error'])
    rows=[]
    for f in p.get('features') or []:
      a=f.get('attributes') or {}
      rows.append({k:a.get(k) for k in ('PERMIT_NUMBER','PERMIT_TYPE','PERMIT_STATUS','CATEGORY_NAME','WORK_TYPE','ZONING','SQFT','PROJECT_COSTS','ADDRESS','CITY','STATE','ZIPCODE','ISSUE_DATE')})
    return {'status':'PASS' if rows else 'FAIL','count':len(rows),'samples':rows[:10]}

def tax_probe():
    r=requests.get(TAX_PAGE,timeout=30,headers={'User-Agent':'Mozilla/5.0'}); r.raise_for_status()
    soup=BeautifulSoup(r.text,'html.parser')
    links=[]
    for a in soup.find_all('a',href=True):
      text=clean(a.get_text(' ',strip=True)); href=urljoin(TAX_PAGE,a['href'])
      low=(text+' '+href).lower()
      if any(x in low for x in ('current year available bills','all available bills','eligible bills real estate','.xlsx','.xls')):
        links.append({'text':text,'url':href})
    # prefer current-year/all-available spreadsheet, never TDDP/Landbank-specific list
    usable=[x for x in links if 'tddp' not in (x['text']+' '+x['url']).lower()]
    chosen=None
    for needle in ('current year available bills','all available bills','eligible bills real estate'):
      chosen=next((x for x in usable if needle in x['text'].lower()),None)
      if chosen: break
    preview={'page_status':r.status_code,'resource_links':usable[:20],'chosen':chosen,'headers':[],'rows':[]}
    if chosen and re.search(r'\.(xlsx?|csv)(?:\?|$)',chosen['url'],re.I):
      rr=requests.get(chosen['url'],timeout=60,headers={'User-Agent':'Mozilla/5.0'}); rr.raise_for_status()
      try:
        import pandas as pd
        if '.csv' in chosen['url'].lower(): df=pd.read_csv(io.BytesIO(rr.content))
        else: df=pd.read_excel(io.BytesIO(rr.content))
        preview['headers']=[clean(x) for x in df.columns]
        preview['rows']=[{clean(k): (None if str(v)=='nan' else clean(v)) for k,v in row.items()} for row in df.head(5).to_dict('records')]
        preview['row_count']=len(df)
      except Exception as exc:
        preview['parse_error']=f'{type(exc).__name__}:{exc}'
    return {'status':'PASS' if usable else 'PARTIAL',**preview}

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    report={'permits':{},'tax':{},'errors':[],'guardrails':{'landbank_excluded':True,'tax_balance_not_inferred':True,'wrecking_permit_not_equal_completed_demolition':True}}
    try: report['permits']=permit_probe()
    except Exception as exc: report['errors'].append(f'permits:{type(exc).__name__}:{exc}')
    try: report['tax']=tax_probe()
    except Exception as exc: report['errors'].append(f'tax:{type(exc).__name__}:{exc}')
    report['status']='PASS' if report.get('permits',{}).get('status')=='PASS' and report.get('tax',{}).get('status') in {'PASS','PARTIAL'} and not report['errors'] else 'PARTIAL'
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
