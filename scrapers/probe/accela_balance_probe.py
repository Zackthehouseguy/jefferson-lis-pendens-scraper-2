#!/usr/bin/env python3
"""Read-only deep probe of Accela fee/payment UI on a known cited parent case."""
from __future__ import annotations
import json, re, time
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

OUT=Path("reports/accela_balance_probe"); OUT.mkdir(parents=True,exist_ok=True)
CASES=[
 {"case":"ENF-PMNT-26-014474","url":"https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=B1662&agencyCode=LJCMG","known_assessed_event":700.0},
 {"case":"ENF-PMNT-26-019301","url":"https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&TabName=Enforcement&capID1=26REC&capID2=00000&capID3=E2186&agencyCode=LJCMG&IsToShowInspection=","known_assessed_event":0.0},
]

def clean(s): return re.sub(r"\s+"," ",str(s or "")).strip()

def lines(text):
    keys=("payment","fee","balance","amount due","amount paid","invoice","paid","due","total")
    out=[]
    for ln in text.splitlines():
        c=clean(ln)
        if c and any(k in c.lower() for k in keys) and c not in out: out.append(c)
    return out[:150]

def main():
    started=time.perf_counter(); out=[]
    with sync_playwright() as p:
        b=p.chromium.launch(headless=True); ctx=b.new_context(viewport={"width":1440,"height":1400},locale="en-US"); page=ctx.new_page()
        for item in CASES:
            page.goto(item["url"],wait_until="domcontentloaded",timeout=30000)
            try: page.wait_for_load_state("networkidle",timeout=8000)
            except PlaywrightTimeoutError: pass
            page.wait_for_timeout(800)
            links=[]
            for a in page.locator("a").all():
                try:
                    txt=clean(a.inner_text(timeout=400)); href=a.get_attribute("href"); onclick=a.get_attribute("onclick"); cls=a.get_attribute("class")
                    if txt and any(k in txt.lower() for k in ("payment","fee","balance","invoice")):
                        links.append({"text":txt,"href":href,"onclick":onclick,"class":cls,"outer_html":a.evaluate("el=>el.outerHTML")[:1500]})
                except Exception: pass
            tabs={}
            for selector in ("#tab-fee","[id*='fee' i]","[id*='payment' i]"):
                try:
                    loc=page.locator(selector)
                    n=min(loc.count(),20)
                    vals=[]
                    for i in range(n):
                        try:
                            el=loc.nth(i); txt=clean(el.inner_text(timeout=500));
                            if txt: vals.append({"selector":selector,"id":el.get_attribute("id"),"class":el.get_attribute("class"),"text":txt[:5000]})
                        except Exception: pass
                    if vals: tabs[selector]=vals
                except Exception: pass
            # Explicitly activate fee tab anchor if present; read only.
            after_fee=[]
            try:
                fee=page.locator("a[href='#tab-fee']").first
                if fee.count()>0:
                    fee.click(timeout=3000); page.wait_for_timeout(800)
                    try:
                        after_fee.append({"tab_text":clean(page.locator("#tab-fee").inner_text(timeout=2000))[:8000]})
                    except Exception:
                        after_fee.append({"body_lines":lines(page.locator("body").inner_text())})
            except Exception as e:
                after_fee.append({"fee_click_error":f"{type(e).__name__}:{str(e)[:200]}"})
            out.append({
              **item,
              "page_title":page.title(),
              "base_keyword_lines":lines(page.locator("body").inner_text()),
              "fee_payment_balance_links":links,
              "fee_payment_dom_sections":tabs,
              "after_fee_tab":after_fee,
            })
        ctx.close(); b.close()
    # We only call a public outstanding balance proven if the rendered UI explicitly
    # states a numeric amount due/balance, not merely a link/header called Balance.
    numeric_due=[]
    pat=re.compile(r"(?:amount\s+due|balance\s+due|outstanding\s+balance)\s*[:$ ]+\$?([0-9][0-9,]*(?:\.\d{2})?)",re.I)
    for c in out:
        blob=json.dumps(c)
        for m in pat.finditer(blob): numeric_due.append({"case":c["case"],"amount":m.group(1)})
    report={
      "generated_at_utc":datetime.now(timezone.utc).isoformat(),
      "runtime_seconds":round(time.perf_counter()-started,3),
      "cases":out,
      "explicit_numeric_outstanding_balance_found":numeric_due,
      "status":"BALANCE_PROVEN" if numeric_due else "NO_EXPLICIT_OUTSTANDING_BALANCE_FOUND",
      "contract":"CitationAmount remains citation_assessed_amount only. Do not label it amount owed without an explicit public unpaid-balance field."
    }
    (OUT/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8"); print(json.dumps(report,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
