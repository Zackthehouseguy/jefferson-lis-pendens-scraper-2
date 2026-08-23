#!/usr/bin/env python3
"""Acceptance test for Louisville CitationAmount semantics.

Important: CitationAmount is treated as an ASSESSED EVENT AMOUNT, not proof of
an outstanding/unpaid balance. ArcGIS repeats the same event amount on every
violation row belonging to one B1_ALT_ID/site-visit record, so we dedupe within
the event before summing across distinct citation events.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import requests

URL = "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/PM_SiteVisit_Violations/FeatureServer/0/query"
KNOWN_CASES = [
    "ENF-PMNT-26-014474-8",   # live example: 7 rows all repeating $700
    "ENF-PMNT-24-017394-74", # live example: 8 rows all repeating $700
    "ENF-PMNT-26-015609-6",  # live open-case child: no citation amount
    "ENF-PMNT-26-013339-3",  # live open-case child: no citation amount
]
OUT = Path("reports/citation_semantics")
OUT.mkdir(parents=True, exist_ok=True)


def numeric(v):
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def event_amount(rows: list[dict]) -> dict:
    """Return one safely deduped assessed amount for one child/site-visit event."""
    positive = sorted({numeric(r.get("CitationAmount")) for r in rows if numeric(r.get("CitationAmount")) > 0})
    if not positive:
        return {
            "citation_present": False,
            "assessed_amount": 0.0,
            "ambiguous": False,
            "distinct_positive_values": [],
        }
    if len(positive) == 1:
        return {
            "citation_present": True,
            "assessed_amount": positive[0],
            "ambiguous": False,
            "distinct_positive_values": positive,
        }
    # Never invent a total when one site-visit record exposes conflicting values.
    return {
        "citation_present": True,
        "assessed_amount": None,
        "ambiguous": True,
        "distinct_positive_values": positive,
    }


def query(where: str, count: int = 1000) -> list[dict]:
    params = {
        "where": where,
        "outFields": "B1_ALT_ID,PARCEL_ID,FullAddress,VIOLATION_CODE,GUIDE_ITEM_TEXT,CitationAmount,G6A_G6_STATUS,G6A_G6_COMPL_DD,G6A_G6_STATUS_DD",
        "returnGeometry": "false",
        "resultRecordCount": count,
        "orderByFields": "G6A_G6_COMPL_DD DESC",
        "f": "json",
    }
    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return [f.get("attributes") or {} for f in payload.get("features", [])]


def main() -> int:
    started = time.perf_counter()
    known_results = []
    assertions = []

    for case in KNOWN_CASES:
        rows = query(f"B1_ALT_ID='{case}'")
        event = event_amount(rows)
        raw_positive = [numeric(r.get("CitationAmount")) for r in rows if numeric(r.get("CitationAmount")) > 0]
        result = {
            "child_case": case,
            "row_count": len(rows),
            "naive_row_sum": round(sum(raw_positive), 2),
            "repeated_row_values": raw_positive,
            **event,
        }
        known_results.append(result)

    expected = {
        "ENF-PMNT-26-014474-8": 700.0,
        "ENF-PMNT-24-017394-74": 700.0,
        "ENF-PMNT-26-015609-6": 0.0,
        "ENF-PMNT-26-013339-3": 0.0,
    }
    for r in known_results:
        ok = (not r["ambiguous"]) and r["assessed_amount"] == expected[r["child_case"]]
        assertions.append({
            "name": f"dedupe_{r['child_case']}",
            "passed": ok,
            "expected_assessed_amount": expected[r["child_case"]],
            "actual_assessed_amount": r["assessed_amount"],
        })

    # Broad live sample: prove that we can sum distinct citation EVENTS by parcel
    # without multiplying one event by its number of violation rows.
    sample = query("CitationAmount > 0", count=1000)
    by_event: dict[str, list[dict]] = defaultdict(list)
    for row in sample:
        event_id = str(row.get("B1_ALT_ID") or "").strip()
        if event_id:
            by_event[event_id].append(row)

    events = []
    for event_id, rows in by_event.items():
        ev = event_amount(rows)
        events.append({
            "child_case": event_id,
            "parcel": str(rows[0].get("PARCEL_ID") or "").strip() or None,
            "address": str(rows[0].get("FullAddress") or "").strip() or None,
            "violation_rows": len(rows),
            **ev,
        })

    by_parcel: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        if ev["parcel"]:
            by_parcel[ev["parcel"]].append(ev)

    stacked_citations = []
    for parcel, evs in by_parcel.items():
        unambiguous = [e for e in evs if e["citation_present"] and not e["ambiguous"]]
        if len(unambiguous) < 2:
            continue
        stacked_citations.append({
            "parcel": parcel,
            "distinct_citation_events": len(unambiguous),
            "citation_assessed_total": round(sum(e["assessed_amount"] for e in unambiguous), 2),
            "events": unambiguous,
            "ambiguous_event_count": sum(1 for e in evs if e["ambiguous"]),
        })
    stacked_citations.sort(key=lambda x: (x["distinct_citation_events"], x["citation_assessed_total"]), reverse=True)

    # Synthetic conflict proves our guardrail without depending on a rare live conflict.
    conflict = event_amount([{"CitationAmount": 700}, {"CitationAmount": 1000}])
    assertions.append({
        "name": "conflicting_amounts_flagged_not_summed",
        "passed": conflict["ambiguous"] is True and conflict["assessed_amount"] is None,
        "result": conflict,
    })

    status = "PASS" if all(a["passed"] for a in assertions) else "FAIL"
    report = {
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "known_live_cases": known_results,
        "assertions": assertions,
        "live_positive_rows_sampled": len(sample),
        "distinct_citation_events_sampled": len(events),
        "multi_event_parcel_examples": stacked_citations[:20],
        "semantic_contract": {
            "citation_assessed_total": "Sum one unambiguous CitationAmount per distinct B1_ALT_ID/site-visit event.",
            "outstanding_balance": "UNKNOWN until an authoritative payment/balance source confirms unpaid amount.",
            "ambiguous_event": "If one B1_ALT_ID exposes multiple distinct positive amounts, do not guess; flag for verification.",
        },
    }
    (OUT / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (OUT / "report.md").write_text(
        "# Citation Event Aggregation Acceptance\n\n"
        f"Status: **{status}**\n\n"
        f"Positive live rows sampled: **{len(sample)}**\n\n"
        f"Distinct citation events: **{len(events)}**\n\n"
        f"Multi-event parcel examples: **{len(stacked_citations)}**\n\n"
        "```json\n" + json.dumps(report, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
