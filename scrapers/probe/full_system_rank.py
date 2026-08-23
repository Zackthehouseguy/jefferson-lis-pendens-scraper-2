#!/usr/bin/env python3
"""Final pre-Lovable ranking acceptance over unseen live Louisville records.

Consumes:
  reports/full_system_live/extract_report.json
  reports/full_system_live/ai_classifications.json

No source scraping and no model call occur here. This stage proves that verified
live extraction + strict AI semantics + deterministic scoring produce a sane,
grounded acquisition call sheet without inventing competition, timestamps, or
amounts owed.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from scrapers.decision_layer import (
    distress_score,
    freshness_date,
    priority_score,
    saturation_score,
    validate_ai_classification,
)

ROOT = Path("reports/full_system_live")
EXTRACT = ROOT / "extract_report.json"
AI_FILE = ROOT / "ai_classifications.json"
OUT_JSON = ROOT / "final_ranked_report.json"
OUT_MD = ROOT / "final_ranked_report.md"


def tier(score: int) -> str:
    if score >= 75:
        return "CALL FIRST"
    if score >= 60:
        return "STRONG"
    if score >= 45:
        return "REVIEW"
    return "LOW"


def assertion(name: str, passed: bool, **details: Any) -> dict[str, Any]:
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

    parcel_counts = Counter(r.get("parcel_id") for r in records if r.get("parcel_id"))
    ranked: list[dict[str, Any]] = []
    assertions: list[dict[str, Any]] = []

    assertions.append(assertion("live_extract_passed", extract.get("status") == "PASS", status=extract.get("status")))
    assertions.append(assertion("eight_unseen_live_records", len(records) == 8, count=len(records)))
    assertions.append(assertion("eight_ai_classifications", len(classifications) == 8, count=len(classifications)))

    for rec in records:
        case = rec.get("case_number")
        ai_raw = classifications.get(case)
        if ai_raw is None:
            assertions.append(assertion(f"ai_present_{case}", False))
            continue
        try:
            ai = validate_ai_classification(ai_raw)
            assertions.append(assertion(f"ai_contract_{case}", True))
        except Exception as exc:
            assertions.append(assertion(f"ai_contract_{case}", False, error=str(exc)))
            continue

        event_date = rec.get("event_date")
        f = freshness_date(event_date, today=today_et)
        open_case_count = parcel_counts.get(rec.get("parcel_id"), 1) if rec.get("parcel_id") else 1
        citation_event_count = int(rec.get("citation_event_count") or 0)
        d = distress_score(
            ai,
            open_case_count=open_case_count,
            citation_event_count=citation_event_count,
            owner_mailing_differs=bool(rec.get("owner_mailing_differs")),
        )
        # We did not have a prior snapshot proving these were newly-created
        # transition events, so we deliberately do NOT award that bonus.
        s = saturation_score(
            source_type="code_enforcement",
            freshness_score=f.score,
            has_free_text_description=bool(rec.get("description_raw")),
            open_case_count=open_case_count,
            new_transition_event=False,
        )
        p = priority_score(distress=d, freshness_score=f.score, saturation=s)

        current_balance = rec.get("outstanding_balance")
        assessed = float(rec.get("citation_assessed_total") or 0.0)
        lead = {
            "rank": None,
            "priority_score": p,
            "priority_tier": tier(p),
            "distress_score": d,
            "saturation_score": s,
            "saturation_interpretation": "LOWER ESTIMATED COMPETITION" if s <= 30 else ("MODERATE ESTIMATED COMPETITION" if s <= 60 else "HIGHER ESTIMATED COMPETITION"),
            "freshness_score": f.score,
            "freshness_label": f.label,
            "freshness_precision": f.date_precision,
            "freshness_age_hours": f.age_hours,
            "case_number": case,
            "record_status": rec.get("record_status"),
            "property_address": rec.get("property_address"),
            "owner_name": rec.get("owner_name"),
            "owner_mailing_address": rec.get("owner_mailing_address"),
            "possible_absentee_indicator": bool(rec.get("owner_mailing_differs")),
            "parcel_id": rec.get("parcel_id"),
            "verified_open_cases_in_acceptance_set_same_parcel": open_case_count,
            "event_date": event_date,
            "ai_distress_level": ai.get("distress_level"),
            "ai_signals": ai.get("signals"),
            "ai_summary": ai.get("summary"),
            "confirmed_facts": ai.get("confirmed_facts"),
            "speculative_claims": ai.get("speculative_claims"),
            "acquisition_relevant": ai.get("acquisition_relevant"),
            "description_raw": rec.get("description_raw"),
            "inspector_comments": rec.get("inspector_comments") or [],
            "citation_event_count": citation_event_count,
            "citation_assessed_total": assessed,
            "verified_current_outstanding_balance": current_balance,
            "money_label": "CITATION ASSESSED — NOT VERIFIED CURRENT BALANCE" if assessed else "NO ASSESSED CITATION IN CURRENT EXTRACT",
            "source_url": rec.get("source_url"),
            "child_source_url": rec.get("child_source_url"),
            "recovery": rec.get("recovery"),
        }
        ranked.append(lead)

        assertions.extend([
            assertion(f"open_{case}", str(rec.get("record_status")).lower() == "open"),
            assertion(f"source_url_{case}", bool(rec.get("source_url"))),
            assertion(f"date_precision_{case}", f.date_precision == "date" and f.age_hours is None, freshness=f.__dict__),
            assertion(f"saturation_nonzero_{case}", 0 < s <= 100, saturation=s),
            assertion(
                f"no_unverified_owed_amount_{case}",
                not (current_balance is None and "OWED" in lead["money_label"].upper()),
                assessed=assessed,
                current_balance=current_balance,
                label=lead["money_label"],
            ),
        ])

    ranked.sort(key=lambda x: (x["priority_score"], x["distress_score"], x["freshness_score"]), reverse=True)
    for idx, lead in enumerate(ranked, 1):
        lead["rank"] = idx

    by_case = {x["case_number"]: x for x in ranked}
    serious_cases = ["ENF-PMNT-26-018701", "ENF-PMNT-26-005440"]
    grass_admin = by_case.get("ENF-PMNT-26-008705")
    assertions.append(assertion(
        "grass_admin_below_serious_safety_cases",
        bool(grass_admin and all(by_case.get(c) and by_case[c]["priority_score"] > grass_admin["priority_score"] for c in serious_cases)),
        grass_admin_priority=grass_admin.get("priority_score") if grass_admin else None,
        serious_priorities={c: by_case.get(c, {}).get("priority_score") for c in serious_cases},
    ))
    assertions.append(assertion(
        "rank_order_descending",
        all(ranked[i]["priority_score"] >= ranked[i + 1]["priority_score"] for i in range(len(ranked) - 1)),
        order=[(x["case_number"], x["priority_score"]) for x in ranked],
    ))
    assertions.append(assertion(
        "no_zero_competition_claims",
        all(x["saturation_score"] >= 5 and "ZERO" not in x["saturation_interpretation"] for x in ranked),
    ))
    assertions.append(assertion(
        "all_ai_numeric_scores_deterministic",
        all(not any(k in classifications.get(x["case_number"], {}) for k in ("distress_score", "saturation_score", "freshness_score", "priority_score")) for x in ranked),
    ))

    failed = [a for a in assertions if not a["passed"]]
    status = "PASS" if not failed and len(ranked) == 8 else "FAIL"
    report = {
        "status": status,
        "source_extract_status": extract.get("status"),
        "source_extract_generated_at_et": generated_et,
        "source_extract_runtime_seconds": extract.get("runtime_seconds"),
        "ai_provider_for_bench": ai_payload.get("provider"),
        "ai_contract_version": ai_payload.get("contract_version"),
        "model_adapter_status": "BENCH FIXTURE ONLY — production inference adapter not wired yet",
        "score_contract": {
            "distress": "AI semantic evidence + deterministic weights",
            "freshness": "source calendar date; no fabricated event hour",
            "saturation": "heuristic estimate of likely competition, not observed investor-contact count",
            "priority": "60% distress + 25% freshness + 15% inverse saturation",
        },
        "assertions_passed": len(assertions) - len(failed),
        "assertions_failed": len(failed),
        "assertions": assertions,
        "ranked_live_leads": ranked,
    }
    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    md = [
        "# TheReaper Full-System Live Ranking Acceptance",
        "",
        f"Status: **{status}**",
        f"Assertions: **{report['assertions_passed']} passed / {report['assertions_failed']} failed**",
        "",
        "## Ranked live call sheet",
        "",
    ]
    for x in ranked:
        money = f"${x['citation_assessed_total']:,.0f} assessed; current balance unverified" if x["citation_assessed_total"] else "no assessed citation in current extract"
        md += [
            f"### #{x['rank']} — {x['property_address']}",
            f"- Priority: **{x['priority_score']}/100 — {x['priority_tier']}**",
            f"- Distress: {x['distress_score']}/100",
            f"- Saturation estimate: {x['saturation_score']}/100 ({x['saturation_interpretation']})",
            f"- Freshness: {x['freshness_score']}/100 ({x['freshness_label']}, date precision)",
            f"- Case: {x['case_number']}",
            f"- AI: {x['ai_summary']}",
            f"- Signals: {', '.join(x['ai_signals'])}",
            f"- Money: {money}",
            f"- Source: {x['source_url']}",
            "",
        ]
    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
