#!/usr/bin/env python3
from __future__ import annotations
import json,re
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUT=Path('reports/cclix_browser_probe'); URL='https://cclix.us/Public/DTAX/Bills/Search'
def clean(v): return re.sub(r'\s+',' ',str(v or '')).strip()

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    report={'status':'FAIL','url':URL,'inputs':[],'selects':[],'buttons':[],'labels':[],'text_preview':'','error':None}
    try:
      with sync_playwright() as p:
        b=p.chromium.launch(headless=True); page=b.new_page(viewport={'width':1440,'height':1600})
        resp=page.goto(URL,wait_until='domcontentloaded',timeout=45000)
        try: page.wait_for_load_state('networkidle',timeout=20000)
        except PWTimeout: pass
        page.wait_for_timeout(3000)
        text=page.locator('body').inner_text(timeout=10000)
        report['http_status']=resp.status if resp else None; report['final_url']=page.url; report['text_preview']=clean(text)[:7000]
        for el in page.locator('input').all():
          try: report['inputs'].append({'name':el.get_attribute('name'),'id':el.get_attribute('id'),'type':el.get_attribute('type'),'placeholder':el.get_attribute('placeholder'),'aria_label':el.get_attribute('aria-label')})
          except Exception: pass
        for el in page.locator('select').all():
          try: report['selects'].append({'name':el.get_attribute('name'),'id':el.get_attribute('id'),'aria_label':el.get_attribute('aria-label')})
          except Exception: pass
        for el in page.locator('button').all():
          try: report['buttons'].append({'text':clean(el.inner_text()),'type':el.get_attribute('type'),'id':el.get_attribute('id')})
          except Exception: pass
        for el in page.locator('label').all():
          try: report['labels'].append({'text':clean(el.inner_text()),'for':el.get_attribute('for')})
          except Exception: pass
        report['status']='PASS' if ('Search Criteria' in text or 'Map Id' in text) else 'PARTIAL'
        (OUT/'page.html').write_text(page.content(),encoding='utf-8',errors='ignore')
        try: page.screenshot(path=str(OUT/'page.png'),full_page=True)
        except Exception: pass
        b.close()
    except Exception as exc: report['error']=f'{type(exc).__name__}:{exc}'
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
