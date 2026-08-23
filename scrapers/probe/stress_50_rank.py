#!/usr/bin/env python3
"""50-record full-system ranking acceptance gate.

Consumes the 50-record live extraction report and a strict AI-classification
fixture produced from those exact records. Numeric scoring remains deterministic.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any

from scrapers.decision_layer import (
    distress_score,
    freshness_date,
    priority_score,
    saturation_score,
    validate_ai_classification,
)

ROOT = Path("reports/stress_50_live")
EXTRACT = ROOT / "extract_report.json"
AI_FILE = ROOT / "ai_classifications.json"
OUT_JSON = ROOT / "final_ranked_report.json"
OUT_MD = ROOT / "final_ranked_report.md"
TARGET = 50


def tier(score: int) -> str:
    if score >= 75:
        return "CALL FIRST"
    if score >= 60:
        return "STRONG"
    if score >= 45:
        return "REVIEW"
    return "LOW"


def check(name: str, passed: bool, **details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), **details}


def main() -> int:
    extract = json.loads(EXTRACT.read_text(encoding="utf-8"))
    ai_payload = json.loads(AI_FILE.read_text(encoding="utf-8"))
    records = extract.get("verified_open_unseen_records") or []
    classifications = ai_payload.get("classifications") or {}
    generated_et = extract.get("generated_at_et")
    if not generated_et:
        raise RuntimeError("extract_report_missing_generated_at_et")
    today_et = date.fromisoformat(generated_et[:10])

    cases = [r.get("case_number") for r in records]
    parcel_counts = Counter(r.get("parcel_id") for r in records if r.get("parcel_id"))
    assertions = [
        check("extract_pass", extract.get("status") == "PASS", status=extract.get("status")),
        check("exactly_50_verified_open_records", len(records) == TARGET, count=len(records)),
        check("50_unique_parent_cases", len(set(cases)) == TARGET, unique=len(set(cases))),
        check("exactly_50_ai_classifications", len(classifications) == TARGET, count=len(classifications)),
        check("classification_case_set_matches_extract", set(classifications) == set(cases), missing=sorted(set(cases)-set(classifications)), extra=sorted(set(classifications)-set(cases))),
    ]

    ranked = []
    for rec in records:
        case = rec.get("case_number")
        ai_raw = classifications.get(case)
        try:
            ai = validate_ai_classification(ai_raw or {})
            ai_ok = True
            ai_error = None
        except Exception as exc:
            ai_ok = False
            ai_error = str(exc)
            ai = None
        assertions.append(check(f"ai_contract_{case}", ai_ok, error=ai_error))
        if not ai:
            continue

        required_ok = all(rec.get(k) for k in ("case_number", "property_address", "description_raw", "owner_name", "parcel_id", "source_url"))
        assertions.append(check(f"required_fields_{case}", required_ok))
        assertions.append(check(f"open_{case}", str(rec.get("record_status")).lower() == "open", status=rec.get("record_status")))

        f = freshness_date(rec.get("event_date"), today=today_et)
        open_case_count = parcel_counts.get(rec.get("parcel_id"), 1) if rec.get("parcel_id") else 1
        citation_count = int(rec.get("citation_event_count") or 0)
        d = distress_score(ai, open_case_count=open_case_count, citation_event_count=citation_count, owner_mailing_differs=bool(rec.get("owner_mailing_differs")))
        s = saturation_score(source_type="code_enforcement", freshness_score=f.score, has_free_text_description=bool(rec.get("description_raw")), open_case_count=open_case_count, new_transition_event=False)
        p = priority_score(distress=d, freshness_score=f.score, saturation=s)

        assessed = float(rec.get("citation_assessed_total") or 0.0)
        current_balance = rec.get("outstanding_balance")
        money_label = "CITATION ASSESSED — NOT VERIFIED CURRENT BALANCE" if assessed else "NO ASSESSED CITATION IN CURRENT EXTRACT"
        assertions.extend([
            check(f"freshness_date_precision_{case}", f.date_precision == "date" and f.age_hours is None, freshness=f.__dict__),
            check(f"nonzero_saturation_{case}", 5 <= s <= 100, saturation=s),
            check(f"score_ranges_{case}", all(0 <= x <= 100 for x in (d, s, f.score, p)), distress=d, saturation=s, freshness=f.score, priority=p),
            check(f"no_fake_owed_amount_{case}", not (current_balance is None and "OWED" in money_label), assessed=assessed, current_balance=current_balance, label=money_label),
            check(f"ai_has_no_numeric_scores_{case}", not any(k in ai_raw for k in ("distress_score", "saturation_score", "freshness_score", "priority_score"))),
        ])

        ranked.append({
            "rank": None,
            "priority_score": p,
            "priority_tier": tier(p),
            "distress_score": d,
            "saturation_score": s,
            "freshness_score": f.score,
            "freshness_label": f.label,
            "freshness_precision": f.date_precision,
            "case_number": case,
            "property_address": rec.get("property_address"),
            "owner_name": rec.get("owner_name"),
            "owner_mailing_address": rec.get("owner_mailing_address"),
            "parcel_id": rec.get("parcel_id"),
            "open_cases_in_50_sample_same_parcel": open_case_count,
            "event_date": rec.get("event_date"),
            "ai_distress_level": ai.get("distress_level"),
            "ai_signals": ai.get("signals"),
            "ai_summary": ai.get("summary"),
            "confirmed_facts": ai.get("confirmed_facts"),
            "speculative_claims": ai.get("speculative_claims"),
            "description_raw": rec.get("description_raw"),
            "inspector_comments": rec.get("inspector_comments") or [],
            "citation_event_count": citation_count,
            "citation_assessed_total": assessed,
            "verified_current_outstanding_balance": current_balance,
            "money_label": money_label,
            "source_url": rec.get("source_url"),
            "recovery": rec.get("recovery"),
        })

    ranked.sort(key=lambda x: (x["priority_score"], x["distress_score"], x["freshness_score"]), reverse=True)
    for i, lead in enumerate(ranked, 1):
        lead["rank"] = i

    assertions.append(check("ranked_count_50", len(ranked) == TARGET, count=len(ranked)))
    assertions.append(check("rank_order_descending", all(ranked[i]["priority_score"] >= ranked[i+1]["priority_score"] for i in range(len(ranked)-1))))
    assertions.append(check("no_duplicate_ranked_cases", len({x["case_number"] for x in ranked}) == len(ranked)))

    levels = Counter(x["ai_distress_level"] for x in ranked)
    priority_tiers = Counter(x["priority_tier"] for x in ranked)
    # Sanity check only when both groups exist: HIGH classifications should on
    # average score above LOW/NONE classifications after deterministic math.
    high_scores = [x["distress_score"] for x in ranked if x["ai_distress_level"] == "HIGH"]
    low_scores = [x["distress_score"] for x in ranked if x["ai_distress_level"] in {"LOW", "NONE"}]
    if high_scores and low_scores:
        assertions.append(check("high_distress_average_above_low", mean(high_scores) > mean(low_scores), high_mean=round(mean(high_scores),2), low_mean=round(mean(low_scores),2)))

    failed = [a for a in assertions if not a["passed"]]
    status = "PASS" if not failed and len(ranked) == TARGET else "FAIL"
    report = {
        "status": status,
        "target": TARGET,
        "source_extract_status": extract.get("status"),
        "source_extract_runtime_seconds": extract.get("runtime_seconds"),
        "source_extract_generated_at_et": generated_et,
        "ai_provider_for_bench": ai_payload.get("provider"),
        "ai_contract_version": ai_payload.get("contract_version"),
        "assertions_passed": len(assertions)-len(failed),
        "assertions_failed": len(failed),
        "failed_assertions": failed,
        "distress_level_distribution": dict(levels),
        "priority_tier_distribution": dict(priority_tiers),
        "ranked_live_leads": ranked,
    }
    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    md = [
        "# TheReaper 50-Record Live Stress Acceptance", "",
        f"Status: **{status}**",
        f"Assertions: **{report['assertions_passed']} passed / {report['assertions_failed']} failed**",
        f"Extraction runtime: **{extract.get('runtime_seconds')}s**", "",
        "## Ranked call sheet", "",
    ]
    for x in ranked:
        money = f"${x['citation_assessed_total']:,.0f} assessed; current balance unverified" if x["citation_assessed_total"] else "no assessed citation in current extract"
        md += [
            f"### #{x['rank']} — {x['property_address']}",
            f"- Priority: **{x['priority_score']}/100 — {x['priority_tier']}**",
            f"- Distress: {x['distress_score']}/100 | Saturation: {x['saturation_score']}/100 | Freshness: {x['freshness_score']}/100 ({x['freshness_label']})",
            f"- Case: {x['case_number']}",
            f"- AI: {x['ai_summary']}",
            f"- Signals: {', '.join(x['ai_signals'])}",
            f"- Money: {money}",
            f"- Source: {x['source_url']}", "",
        ]
    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(json.dumps({"status": status, "ranked": len(ranked), "passed": report["assertions_passed"], "failed": report["assertions_failed"], "top10": ranked[:10]}, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
