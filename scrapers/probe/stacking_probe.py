#!/usr/bin/env python3
"""Live parcel stacking probe for recent Louisville violation rows."""
from collections import defaultdict
import json, time
from datetime import datetime, timezone
from pathlib import Path
from scrapers.probe import accela_engine_probe_v2 as probe

OUT = Path("reports/stacking_probe")
OUT.mkdir(parents=True, exist_ok=True)
started = time.perf_counter()
features = probe.fetch_recent(2000)
groups = probe.build_groups(features)
by_parcel = defaultdict(list)
for g in groups:
    parcel = g.get("parcel")
    if parcel and g.get("parent_key"):
        by_parcel[parcel].append({
            "parent_case": g["parent_key"],
            "address": g.get("address"),
            "site_visit_rows": g.get("site_visit_rows"),
            "child_cases": g.get("child_cases"),
            "violation_codes": g.get("violation_codes"),
            "citation_total": round(g.get("citation_total", 0.0), 2),
        })
stacks=[]
for parcel, items in by_parcel.items():
    unique={i["parent_case"]:i for i in items}
    if len(unique)>1:
        stacks.append({"parcel":parcel,"open_status_unknown_until_parent_check":True,"distinct_parent_cases":len(unique),"cases":list(unique.values())})
stacks.sort(key=lambda x:x["distinct_parent_cases"], reverse=True)
report={
    "generated_at_utc":datetime.now(timezone.utc).isoformat(),
    "runtime_seconds":round(time.perf_counter()-started,3),
    "features_fetched":len(features),
    "parent_groups":len(groups),
    "parcels_with_multiple_distinct_parent_cases":len(stacks),
    "examples":stacks[:20],
    "status":"PASS" if stacks else "NO_LIVE_STACK_FOUND",
    "note":"This proves parcel-level grouping on live recent feed. Parent OPEN/CLOSED status still comes from the Accela parent extractor before acquisition prioritization."
}
(OUT/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
(OUT/"report.md").write_text("# Live Stacking Probe\n\nStatus: **%s**\n\nLive multi-parent parcels: **%s**\n\n```json\n%s\n```\n"%(report["status"],len(stacks),json.dumps(stacks[:5],indent=2)),encoding="utf-8")
print(json.dumps(report,indent=2))
