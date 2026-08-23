#!/usr/bin/env python3
"""Inspect row-level CitationAmount semantics for known Accela site visits."""
import json, requests, time
from pathlib import Path
from datetime import datetime, timezone

URL="https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/PM_SiteVisit_Violations/FeatureServer/0/query"
CASES=[
 "ENF-PMNT-26-014474-8",
 "ENF-PMNT-24-017394-74",
 "ENF-PMNT-26-015609-6",
 "ENF-PMNT-26-013339-3",
]
OUT=Path("reports/citation_semantics"); OUT.mkdir(parents=True,exist_ok=True)
started=time.perf_counter(); results=[]
for case in CASES:
    params={
      "where":f"B1_ALT_ID='{case}'",
      "outFields":"B1_ALT_ID,PARCEL_ID,FullAddress,VIOLATION_CODE,GUIDE_ITEM_TEXT,CitationAmount,G6A_G6_STATUS,G6A_G6_COMPL_DD,G6A_G6_STATUS_DD",
      "returnGeometry":"false","f":"json"
    }
    r=requests.get(URL,params=params,timeout=30); r.raise_for_status(); p=r.json()
    rows=[x.get("attributes") or {} for x in p.get("features",[])]
    amounts=[]
    for row in rows:
        try: amounts.append(float(row.get("CitationAmount") or 0))
        except: amounts.append(0.0)
    nonzero=[a for a in amounts if a]
    results.append({
      "child_case":case,
      "row_count":len(rows),
      "naive_row_sum":round(sum(nonzero),2),
      "nonzero_amounts":nonzero,
      "unique_nonzero_amounts":sorted(set(nonzero)),
      "rows":[{
        "violation_code":x.get("VIOLATION_CODE"),
        "citation_amount":x.get("CitationAmount"),
        "status":x.get("G6A_G6_STATUS"),
        "description":x.get("GUIDE_ITEM_TEXT")
      } for x in rows]
    })
report={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"runtime_seconds":round(time.perf_counter()-started,3),"cases":results}
(OUT/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
(OUT/"report.md").write_text("# Citation Semantics Probe\n\n```json\n"+json.dumps(report,indent=2)+"\n```\n",encoding="utf-8")
print(json.dumps(report,indent=2))
