#!/usr/bin/env python3
from __future__ import annotations
import json,re
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUT=Path('reports/cclix_mapid_search_probe')
URL='https://cclix.us/Public/DTAX/Bills/Search'
MAP_IDS=['001E00940000','048E00960000','046K00380000','039B01220000']

def clean(v): return re.sub(r'\s+',' ',str(v or '')).strip()

def select_jefferson(page):
    diag={'method':None,'comboboxes':[]}
    try:
        for cb in page.get_by_role('combobox').all():
            try:
                diag['comboboxes'].append({'aria':cb.get_attribute('aria-label'),'text':clean(cb.inner_text()),'id':cb.get_attribute('id')})
            except Exception: pass
    except Exception: pass
    # Kendo dropdowns commonly expose the current value as visible text.
    try:
        loc=page.get_by_text('Select County',exact=True)
        if loc.count():
            loc.first.click(); page.wait_for_timeout(350)
            j=page.get_by_text('Jefferson',exact=True)
            if j.count():
                j.last.click(); page.wait_for_timeout(350); diag['method']='visible_text'; return True,diag
    except Exception as exc: diag['visible_text_error']=f'{type(exc).__name__}:{exc}'
    # Accessible-role fallback.
    try:
        opts=page.get_by_role('option',name='Jefferson')
        if opts.count(): opts.first.click(); page.wait_for_timeout(350); diag['method']='role_option'; return True,diag
    except Exception as exc: diag['role_error']=f'{type(exc).__name__}:{exc}'
    return False,diag

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    results=[]
    with sync_playwright() as p:
      b=p.chromium.launch(headless=True); page=b.new_page(viewport={'width':1440,'height':1600})
      for mid in MAP_IDS:
        rec={'map_id':mid,'status':'FAIL','county_selected':False,'county_diag':{},'text':'','rows':[],'error':None}
        try:
          page.goto(URL,wait_until='domcontentloaded',timeout=45000)
          try: page.wait_for_load_state('networkidle',timeout=15000)
          except PWTimeout: pass
          page.wait_for_timeout(1200)
          ok,diag=select_jefferson(page); rec['county_selected']=ok; rec['county_diag']=diag
          page.locator('#mapId').fill(mid)
          page.locator('#btnSubmitDtaxBillSearchViewModel').click()
          page.wait_for_timeout(2500)
          try: page.wait_for_load_state('networkidle',timeout=10000)
          except PWTimeout: pass
          text=clean(page.locator('body').inner_text(timeout=10000)); rec['text']=text[:7000]
          for tr in page.locator('table tbody tr').all():
            try:
              t=clean(tr.inner_text())
              if t and 'No records available' not in t: rec['rows'].append(t)
            except Exception: pass
          low=text.lower()
          # Required validation always wins over an empty grid.
          if ('county' in low and 'required' in low) and not rec['county_selected']:
            rec['status']='NEEDS_COUNTY'
          elif rec['rows']:
            rec['status']='PASS_ROWS'
          elif rec['county_selected'] and 'No records available' in text:
            rec['status']='PASS_NO_DELINQUENT_RECORD'
          else:
            rec['status']='PARTIAL'
        except Exception as exc: rec['error']=f'{type(exc).__name__}:{exc}'
        results.append(rec)
      b.close()
    status='PASS' if all(r['status'].startswith('PASS') for r in results) else 'PARTIAL'
    report={'status':status,'source':'CCLIX official Public Delinquent Tax Bill Search','results':results,'guardrails':{'county_must_be_selected':True,'no_record_means_no_delinquent_bill_returned_for_that_county_mapid_search':True,'bill_total_only_used_when_returned_by_cclix':True}}
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if status=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
