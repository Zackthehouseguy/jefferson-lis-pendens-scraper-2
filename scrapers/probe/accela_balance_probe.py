#!/usr/bin/env python3
"""Read-only probe for public Accela fee/payment/balance visibility."""
from __future__ import annotations
import json, re, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from scrapers.probe.accela_engine_probe_v3 import resolve_parent_from_rendered

ARCGIS="https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/PM_SiteVisit_Violations/FeatureServer/0/query"
ACCELA="https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx"
CHILDREN=["ENF-PMNT-26-014474-8","ENF-PMNT-24-017394-74"]
OUT=Path("reports/accela_balance_probe"); OUT.mkdir(parents=True,exist_ok=True)

def clean(s): return re.sub(r"\s+"," ",str(s or "")).strip()

def arc(case):
    p={"where":f"B1_ALT_ID='{case}'","outFields":"B1_PER_ID1,B1_PER_ID2,B1_PER_ID3,B1_ALT_ID,PARCEL_ID,FullAddress,CitationAmount,G6A_G6_STATUS","returnGeometry":"false","f":"json"}
    r=requests.get(ARCGIS,params=p,timeout=30); r.raise_for_status(); rows=r.json().get("features",[])
    if not rows: return None
    return rows[0].get("attributes") or {}

def child_url(a):
    return ACCELA+"?"+urlencode({"Module":"Enforcement","TabName":"Enforcement","capID1":a["B1_PER_ID1"],"capID2":a["B1_PER_ID2"],"capID3":a["B1_PER_ID3"],"agencyCode":"LJCMG","IsToShowInspection":""})

def interesting_lines(text):
    keys=("payment","payments","fee","fees","balance","amount due","amount paid","invoice","paid","due")
    out=[]
    for ln in text.splitlines():
        c=clean(ln)
        if c and any(k in c.lower() for k in keys) and c not in out: out.append(c)
    return out[:100]

def main():
    started=time.perf_counter(); cases=[]
    with sync_playwright() as p:
        b=p.chromium.launch(headless=True); ctx=b.new_context(viewport={"width":1280,"height":1200},locale="en-US"); page=ctx.new_page()
        for child in CHILDREN:
            a=arc(child)
            if not a:
                cases.append({"child_case":child,"error":"arcgis_not_found"}); continue
            cu=child_url(a); expected=re.sub(r"-\d+$","",child)
            page.goto(cu,wait_until="domcontentloaded",timeout=30000)
            try: page.wait_for_load_state("networkidle",timeout=7000)
            except PlaywrightTimeoutError: pass
            for _ in range(2):
                if expected in page.content(): break
                try: page.evaluate("() => typeof ExpandRelatedPermitSection==='function' ? ExpandRelatedPermitSection(true) : false")
                except Exception: pass
                page.wait_for_timeout(1000)
            pc,pu=resolve_parent_from_rendered(page.content(),cu,expected)
            if not pu:
                cases.append({"child_case":child,"error":"parent_not_resolved"}); continue
            page.goto(pu,wait_until="domcontentloaded",timeout=30000)
            try: page.wait_for_load_state("networkidle",timeout=7000)
            except PlaywrightTimeoutError: pass
            page.wait_for_timeout(700)
            base_text=page.locator("body").inner_text()
            links=[]
            for ael in page.locator("a").all():
                try:
                    txt=clean(ael.inner_text(timeout=500)); href=ael.get_attribute("href")
                    if txt and any(k in txt.lower() for k in ("payment","fee","balance","invoice")):
                        links.append({"text":txt,"href":href})
                except Exception: pass
            tabs=[]
            # Read-only clicks on links/tabs with fee/payment wording, if publicly available.
            for label in ("Payments","Fees","Fees and Payments","Payment"):
                try:
                    loc=page.get_by_text(re.compile(rf"^{re.escape(label)}$",re.I)).first
                    if loc.count()>0:
                        loc.click(timeout=2500); page.wait_for_timeout(1000)
                        tabs.append({"label":label,"url":page.url,"lines":interesting_lines(page.locator("body").inner_text())})
                        page.go_back(wait_until="domcontentloaded",timeout=15000); page.wait_for_timeout(500)
                except Exception: pass
            cases.append({
                "child_case":child,"parent_case":pc,"parent_url":pu,"parcel":clean(a.get("PARCEL_ID")),"address":clean(a.get("FullAddress")),
                "arcgis_citation_amount":a.get("CitationAmount"),"parent_keyword_lines":interesting_lines(base_text),"fee_payment_links":links,"clicked_readonly_tabs":tabs,
            })
        ctx.close(); b.close()
    public_balance_detected=any(any(any(k in ln.lower() for k in ("amount due","balance due","outstanding balance")) for ln in c.get("parent_keyword_lines",[])) or c.get("clicked_readonly_tabs") for c in cases)
    report={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"runtime_seconds":round(time.perf_counter()-started,3),"public_balance_ui_detected":public_balance_detected,"cases":cases,"conclusion":"Do not call CitationAmount outstanding debt unless a public balance/payment field is explicitly found."}
    (OUT/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8"); print(json.dumps(report,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
