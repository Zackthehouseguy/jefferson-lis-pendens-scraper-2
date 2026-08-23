#!/usr/bin/env python3
"""Live parcel stacking + freshness probe for recent Louisville violation rows."""
from collections import defaultdict
import json, time
from datetime import datetime, timezone, date
from pathlib import Path
from zoneinfo import ZoneInfo
from scrapers.probe import accela_engine_probe_v2 as probe

OUT = Path("reports/stacking_probe")
OUT.mkdir(parents=True, exist_ok=True)
started = time.perf_counter()
features = probe.fetch_recent(2000)
groups = probe.build_groups(features)
by_parcel = defaultdict(list)
TARGETS = {
    "ENF-PMNT-26-019301",
    "ENF-PMNT-26-016300",
    "ENF-PMNT-26-013339",
    "ENF-PMNT-26-015609",
    "ENF-PMNT-26-016665",
}
local_today = datetime.now(ZoneInfo("America/New_York")).date()
freshness = []

for g in groups:
    rep = g.get("representative") or {}
    if g.get("parent_key") in TARGETS:
        latest = rep.get("visit_date") or rep.get("status_date")
        age_days = None
        if latest:
            try:
                age_days = (local_today - date.fromisoformat(latest)).days
            except Exception:
                pass
        freshness.append({
            "parent_case": g.get("parent_key"),
            "latest_activity_date": latest,
            "signal_age_days": age_days,
            "address": g.get("address"),
            "parcel": g.get("parcel"),
            "site_visit_rows": g.get("site_visit_rows"),
        })

    parcel = g.get("parcel")
    if parcel and g.get("parent_key"):
        by_parcel[parcel].append({
            "parent_case": g["parent_key"],
            "address": g.get("address"),
            "site_visit_rows": g.get("site_visit_rows"),
            "child_cases": g.get("child_cases"),
            "violation_codes": g.get("violation_codes"),
            "citation_total": round(g.get("citation_total", 0.0), 2),
            "latest_activity_date": (g.get("representative") or {}).get("visit_date"),
        })
stacks=[]
for parcel, items in by_parcel.items():
    unique={i["parent_case"]:i for i in items}
    if len(unique)>1:
        stacks.append({"parcel":parcel,"open_status_unknown_until_parent_check":True,"distinct_parent_cases":len(unique),"cases":list(unique.values())})
stacks.sort(key=lambda x:x["distinct_parent_cases"], reverse=True)
report={
    "generated_at_utc":datetime.now(timezone.utc).isoformat(),
    "local_today_eastern":str(local_today),
    "runtime_seconds":round(time.perf_counter()-started,3),
    "features_fetched":len(features),
    "parent_groups":len(groups),
    "parcels_with_multiple_distinct_parent_cases":len(stacks),
    "examples":stacks[:20],
    "freshness_for_verified_cases":sorted(freshness,key=lambda x:x["parent_case"]),
    "status":"PASS" if stacks else "NO_LIVE_STACK_FOUND",
    "note":"This proves parcel-level grouping and source-derived freshness on the live recent feed. Parent OPEN/CLOSED status comes from Accela before current-distress prioritization."
}
(OUT/"report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
(OUT/"report.md").write_text("# Live Stacking + Freshness Probe\n\nStatus: **%s**\n\nLive multi-parent parcels: **%s**\n\nFreshness:\n```json\n%s\n```\n\nStacks:\n```json\n%s\n```\n"%(report["status"],len(stacks),json.dumps(report["freshness_for_verified_cases"],indent=2),json.dumps(stacks[:5],indent=2)),encoding="utf-8")
print(json.dumps(report,indent=2))
