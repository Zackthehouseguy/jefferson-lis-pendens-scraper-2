#!/usr/bin/env python3
"""Unseen-data full-system extraction stage for TheReaper bench.

Read-only live flow:
ArcGIS newest rows -> child Accela -> JS Related Records -> exact parent href ->
verified OPEN parent -> owner/mailing/parcel/description -> freshness/citation context.

AI classification is intentionally a separate bench stage because GitHub Models
was retired and this repository has no model credential. The output here is then
classified under the strict AI contract and fed into the ranking acceptance run.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright

from scrapers.failure_recovery import RetryPolicy, retry_call
from scrapers.probe import accela_engine_probe_v2 as base
from scrapers.probe import accela_engine_probe_v3 as v4

ET = ZoneInfo("America/New_York")
KNOWN_TUNING_CASES = {
    "ENF-PMNT-26-019301",
    "ENF-PMNT-26-016300",
    "ENF-PMNT-26-013339",
    "ENF-PMNT-26-015609",
    "ENF-PMNT-26-016665",
}


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def clean_location(v: str | None) -> str | None:
    if not v:
        return None
    s = re.sub(r"\s*\*?\s*View Additional Locations.*$", "", clean(v), flags=re.I)
    return s.strip(" *") or None


def owner_mailing_differs(property_address: str | None, mailing: str | None) -> bool:
    if not property_address or not mailing:
        return False
    def key(s: str):
        x = re.sub(r"[^A-Z0-9 ]", " ", s.upper())
        toks = [t for t in x.split() if t]
        return toks[:4]
    return key(property_address) != key(mailing)


def citation_context(features: list[dict]):
    events: dict[str, dict[str, set[float]]] = defaultdict(lambda: defaultdict(set))
    for feat in features:
        r = base.feature_row(feat)
        if not r:
            continue
        amount = float(r.get("citation") or 0)
        if amount > 0:
            events[r["parent_key"]][r["child_case"]].add(amount)
    out = {}
    for parent, child_map in events.items():
        total = 0.0
        count = 0
        ambiguous = []
        for child, vals in child_map.items():
            positive = sorted(v for v in vals if v > 0)
            if len(positive) == 1:
                total += positive[0]
                count += 1
            elif len(positive) > 1:
                ambiguous.append({"child_case": child, "values": positive})
        out[parent] = {
            "citation_event_count": count,
            "citation_assessed_total": round(total, 2),
            "ambiguous_citation_events": ambiguous,
            "outstanding_balance": None,
            "balance_note": "Citation assessed is not treated as current amount owed without an authoritative balance source.",
        }
    return out


def classify_failure(failure: dict) -> BaseException:
    reason = clean(failure.get("reason")).lower()
    transient_terms = ("timeout", "connection", "temporar", "502", "503", "504", "net::err")
    if any(t in reason for t in transient_terms):
        return TimeoutError(reason or "transient_stage_failure")
    return ValueError(reason or "permanent_stage_failure")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-open", type=int, default=8)
    ap.add_argument("--arcgis-limit", type=int, default=1000)
    ap.add_argument("--max-parent-attempts", type=int, default=28)
    ap.add_argument("--out", default="reports/full_system_live")
    args = ap.parse_args()

    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)

    features = base.fetch_recent(args.arcgis_limit)
    groups = base.build_groups(features)
    citations = citation_context(features)

    # Recent-window parcel context. This is not called an OPEN count until each
    # parent has been rendered and verified open.
    parents_by_parcel: dict[str, set[str]] = defaultdict(set)
    for g in groups:
        if g.get("parcel"):
            parents_by_parcel[g["parcel"]].add(g["parent_key"])

    selected = []
    failures = []
    inspected = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1200}, locale="en-US")
        page = ctx.new_page()

        for idx, group in enumerate(groups, 1):
            if len(selected) >= args.target_open or inspected >= args.max_parent_attempts:
                break
            parent_key = group["parent_key"]
            if parent_key in KNOWN_TUNING_CASES:
                continue
            inspected += 1

            child_failure_holder = {"failure": None}
            def child_call():
                resolved, fail = v4.render_child_and_resolve(page, group, out, idx)
                if fail:
                    child_failure_holder["failure"] = fail
                    raise classify_failure(fail)
                return resolved
            try:
                resolved, child_meta = retry_call(
                    child_call,
                    policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.35, max_delay_seconds=1.0),
                    sleep=time.sleep,
                )
            except Exception as e:
                f = child_failure_holder["failure"] or {"stage": "child_resolve", "reason": f"{type(e).__name__}:{e}"}
                failures.append({**f, "recovery": "exhausted_or_permanent"})
                continue

            parent_failure_holder = {"failure": None}
            def parent_call():
                rec, fail = base.browser_parent(page, resolved["parent_case"], resolved["parent_url"], out, idx)
                if fail:
                    parent_failure_holder["failure"] = fail
                    raise classify_failure(fail)
                return rec
            try:
                rec, parent_meta = retry_call(
                    parent_call,
                    policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.35, max_delay_seconds=1.0),
                    sleep=time.sleep,
                )
            except Exception as e:
                f = parent_failure_holder["failure"] or {"stage": "parent_extract", "reason": f"{type(e).__name__}:{e}"}
                failures.append({**f, "parent_case": resolved.get("parent_case"), "recovery": "exhausted_or_permanent"})
                continue

            rec["property_address"] = clean_location(rec.get("property_address"))
            if not rec.get("parcel_id"):
                rec["parcel_id"] = resolved.get("child_parcel") or group.get("parcel")
            rec["source_url"] = resolved["parent_url"]
            rec["child_source_url"] = resolved["child_url"]
            rec["resolved_from_child_case"] = resolved["child_case"]
            rec["inspector_comments"] = resolved.get("inspector_comments") or []
            rec["child_recovery"] = child_meta
            rec["parent_recovery"] = parent_meta
            rec["source_type"] = "code_enforcement"

            status = clean(rec.get("record_status")).lower()
            complete = bool(
                rec.get("case_verified") and status == "open" and rec.get("property_address")
                and rec.get("description_raw") and rec.get("owner_name") and rec.get("parcel_id")
                and rec.get("source_url")
            )
            if not complete:
                failures.append({
                    "stage": "acceptance", "parent_case": resolved.get("parent_case"),
                    "reason": "not_open_or_missing_required_fields",
                    "record_status": rec.get("record_status"),
                })
                continue

            rep = group.get("representative") or {}
            event_date = rep.get("status_date") or rep.get("visit_date")
            same_day = bool(event_date and event_date == now_et.date().isoformat())
            parcel = rec.get("parcel_id")
            cctx = citations.get(parent_key, {
                "citation_event_count": 0, "citation_assessed_total": 0.0,
                "ambiguous_citation_events": [], "outstanding_balance": None,
                "balance_note": "No current outstanding balance inferred.",
            })
            selected.append({
                "case_number": rec.get("case_number"),
                "record_status": rec.get("record_status"),
                "property_address": rec.get("property_address"),
                "description_raw": rec.get("description_raw"),
                "owner_name": rec.get("owner_name"),
                "owner_mailing_address": rec.get("owner_mailing_address"),
                "owner_mailing_differs": owner_mailing_differs(rec.get("property_address"), rec.get("owner_mailing_address")),
                "parcel_id": parcel,
                "source_url": rec.get("source_url"),
                "child_source_url": rec.get("child_source_url"),
                "resolved_from_child_case": rec.get("resolved_from_child_case"),
                "inspector_comments": rec.get("inspector_comments"),
                "event_date": event_date,
                "same_calendar_day_et": same_day,
                "recent_window_distinct_parent_groups_same_parcel": len(parents_by_parcel.get(parcel, set())) if parcel else 0,
                "recent_window_violation_codes": group.get("violation_codes") or [],
                "recent_window_descriptions": group.get("guide_texts") or [],
                "recent_window_occupancies": group.get("occupancies") or [],
                **cctx,
                "recovery": {"child": child_meta, "parent": parent_meta},
            })
            page.wait_for_timeout(250)

        ctx.close(); browser.close()

    status = "PASS" if len(selected) >= args.target_open else ("PARTIAL" if selected else "FAIL")
    report = {
        "status": status,
        "generated_at_utc": now_utc.isoformat(),
        "generated_at_et": now_et.isoformat(),
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "target_open": args.target_open,
        "arcgis_features_fetched": len(features),
        "unique_parent_groups_discovered": len(groups),
        "parent_groups_inspected": inspected,
        "verified_open_unseen_records": selected,
        "failures": failures,
        "known_tuning_cases_excluded": sorted(KNOWN_TUNING_CASES),
        "next_stage": "Strict AI classification of description_raw, then deterministic distress/saturation/freshness/priority ranking.",
    }
    (out / "extract_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
