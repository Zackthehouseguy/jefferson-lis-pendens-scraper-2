#!/usr/bin/env python3
from __future__ import annotations
import json,re
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUT=Path('reports/cclix_mapid_search_probe')
URL='https://cclix.us/Public/DTAX/Bills/Search'
MAP_IDS=['001E00940000','048E00960000','046K00380000','039B01220000']
TAX_YEARS=['2025','2024','2023']

def clean(v): return re.sub(r'\s+',' ',str(v or '')).strip()

def choose_visible(page,current_text,value):
    loc=page.get_by_text(current_text,exact=True)
    if not loc.count(): return False
    loc.first.click(); page.wait_for_timeout(250)
    opt=page.get_by_text(value,exact=True)
    if not opt.count(): return False
    opt.last.click(); page.wait_for_timeout(250); return True

def main()->int:
    OUT.mkdir(parents=True,exist_ok=True)
    results=[]
    with sync_playwright() as p:
      b=p.chromium.launch(headless=True); page=b.new_page(viewport={'width':1440,'height':1600})
      for mid in MAP_IDS:
       for year in TAX_YEARS:
        rec={'map_id':mid,'tax_year':year,'status':'FAIL','county_selected':False,'tax_year_selected':False,'map_value':None,'text':'','rows':[],'error':None}
        try:
          page.goto(URL,wait_until='domcontentloaded',timeout=45000)
          try: page.wait_for_load_state('networkidle',timeout=15000)
          except PWTimeout: pass
          page.wait_for_timeout(900)
          rec['county_selected']=choose_visible(page,'Select County','Jefferson')
          rec['tax_year_selected']=choose_visible(page,'Select Tax Year',year)
          page.locator('#mapId').fill(mid); page.locator('#mapId').press('Tab'); page.wait_for_timeout(150)
          rec['map_value']=page.locator('#mapId').input_value()
          page.locator('#btnSubmitDtaxBillSearchViewModel').click(); page.wait_for_timeout(2200)
          try: page.wait_for_load_state('networkidle',timeout=8000)
          except PWTimeout: pass
          text=clean(page.locator('body').inner_text(timeout=10000)); rec['text']=text[:7000]
          for tr in page.locator('table tbody tr').all():
            try:
              t=clean(tr.inner_text())
              if t and 'No records available' not in t: rec['rows'].append(t)
            except Exception: pass
          low=text.lower()
          validation=('required' in low and ('one other search criterion' in low or 'county' in low))
          if validation:
            rec['status']='VALIDATION_FAILED'
          elif rec['rows']:
            rec['status']='PASS_ROWS'
          elif rec['county_selected'] and rec['tax_year_selected'] and rec['map_value']==mid and 'No records available' in text:
            rec['status']='PASS_NO_DELINQUENT_RECORD'
          else:
            rec['status']='PARTIAL'
        except Exception as exc: rec['error']=f'{type(exc).__name__}:{exc}'
        results.append(rec)
      b.close()
    valid=[r for r in results if r['status'].startswith('PASS')]
    status='PASS' if len(valid)==len(results) else 'PARTIAL'
    report={'status':status,'source':'CCLIX official Public Delinquent Tax Bill Search','tax_years_tested':TAX_YEARS,'results':results,'guardrails':{'county_and_tax_year_required_for_this_probe':True,'validation_message_invalidates_empty_grid':True,'no_record_means_no_delinquent_bill_returned_for_that_exact_county_year_mapid_search':True,'bill_total_only_used_when_returned_by_cclix':True}}
    (OUT/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':status,'searches':len(results),'rows_found':sum(bool(r['rows']) for r in results),'valid_searches':len(valid)},indent=2))
    return 0 if status=='PASS' else 2
if __name__=='__main__': raise SystemExit(main())
