#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup

OUT=Path('reports/cclix_probe')
URL='https://cclix.us/Public/DTAX/Bills/Search'

def clean(v): return ' '.join(str(v or '').split())

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    report={'url':URL,'status':'FAIL','http_status':None,'forms':[],'text_preview':None,'error':None}
    try:
        s=requests.Session(); s.headers.update({'User-Agent':'Mozilla/5.0 (compatible; TheREaperPublicRecords/1.0)'})
        r=s.get(URL,timeout=30,allow_redirects=True)
        report['http_status']=r.status_code; report['final_url']=r.url
        r.raise_for_status()
        soup=BeautifulSoup(r.text,'html.parser')
        report['text_preview']=clean(soup.get_text(' ',strip=True))[:3000]
        for form in soup.find_all('form'):
            report['forms'].append({
                'action':form.get('action'),'method':form.get('method'),
                'inputs':[{'name':x.get('name'),'type':x.get('type'),'value':x.get('value')} for x in form.find_all(['input','select']) if x.get('name')][:100]
            })
        report['status']='PASS' if r.status_code==200 and report['forms'] else 'PARTIAL'
    except Exception as exc:
        report['error']=f'{type(exc).__name__}:{exc}'
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
