#!/usr/bin/env python3
"""Bench AI contract, scoring/freshness math, and failure recovery.

The AI fixtures below were classified from live Accela parent descriptions
extracted in the preceding acceptance run. They validate the production
semantic contract without pretending GitHub Actions has a free model API.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapers.decision_layer import (
    validate_ai_classification, freshness, rank_lead, saturation_score,
)
from scrapers.failure_recovery import (
    CircuitBreaker, CircuitOpenError, CircuitState, RetryPolicy, retry_call,
)

OUT = Path("reports/decision_layer_bench")
OUT.mkdir(parents=True, exist_ok=True)
NOW = datetime(2026, 8, 23, 1, 3, tzinfo=timezone.utc)  # 2026-08-22 21:03 ET

LIVE_AI_FIXTURES = [
    {
        "case": "ENF-PMNT-26-019301",
        "address": "3600 GRISSOM WAY LOUISVILLE KY 40229",
        "description_raw": "High grass all year long. I’m sure it has tons of snakes in it by now.",
        "ai": {
            "distress_level": "MEDIUM",
            "signals": ["overgrown_vegetation", "nuisance"],
            "confirmed_facts": ["The complaint reports high grass that has persisted for an extended period."],
            "speculative_claims": ["The complainant speculates that snakes may be present."],
            "summary": "Reported long-running overgrown grass indicates persistent exterior neglect; the snake statement is unverified speculation.",
            "acquisition_relevant": True,
        },
        "owner_mailing_differs": True,
        "open_case_count": 1,
    },
    {
        "case": "ENF-PMNT-26-016300",
        "address": "5508 ALICANTE LN LOUISVILLE KY 40272",
        "description_raw": "Citizen reports the grass on the property is overgrown in the rear and in the front. Citizen reports it is probably taller than 6ft.",
        "ai": {
            "distress_level": "MEDIUM",
            "signals": ["overgrown_vegetation", "nuisance"],
            "confirmed_facts": ["The complaint reports overgrown grass in both the front and rear yards."],
            "speculative_claims": ["The complainant estimates that the grass may be taller than six feet."],
            "summary": "Substantial reported overgrowth suggests meaningful exterior neglect, while the exact height remains an estimate.",
            "acquisition_relevant": True,
        },
        "owner_mailing_differs": False,
        "open_case_count": 1,
    },
    {
        "case": "ENF-PMNT-26-013339",
        "address": "6807 YUMA WAY LOUISVILLE KY 40258",
        "description_raw": "CITIZEN IS REPORTING RESIDENCE HAS ALL KINDS OF TRASH AND AUTO PARTS, ETC IN THE REAR YARD THAT IS CAUSING RODENTS - 20 BUS DAYS",
        "ai": {
            "distress_level": "HIGH",
            "signals": ["trash_or_debris", "infestation", "nuisance"],
            "confirmed_facts": ["The complaint reports trash and auto parts accumulated in the rear yard.", "The complaint reports a rodent problem."],
            "speculative_claims": ["The complaint attributes the rodent problem to the accumulated trash and auto parts, but that causation is not independently verified."],
            "summary": "Reported debris accumulation plus a reported rodent problem creates a strong active-distress signal.",
            "acquisition_relevant": True,
        },
        "owner_mailing_differs": True,
        "open_case_count": 1,
    },
    {
        "case": "ENF-PMNT-26-015609",
        "address": "507 S 19TH ST LOUISVILLE KY 40203",
        "description_raw": "CALLER REPORTING MOST OF THE ELECTRICAL OUTLETS DO NOT WORK. SOME OF THEM BUZZ. THE STOVE TOP WORKS BUT NOT THE OVEN (ONLY THE BROILER). CALLER ASKS TO EXPEDITE THE INSPECTION DUE TO POSSIBLE RISK OF FIRE.",
        "ai": {
            "distress_level": "HIGH",
            "signals": ["electrical", "fire_risk", "habitability", "tenant_issue"],
            "confirmed_facts": ["The complaint reports that most electrical outlets do not work and some buzz.", "The complaint reports that the oven does not operate normally."],
            "speculative_claims": ["The caller reports a possible fire risk; an actual fire hazard has not been independently verified."],
            "summary": "Reported electrical malfunction and appliance problems create a serious habitability concern with a reported possible fire risk.",
            "acquisition_relevant": True,
        },
        "owner_mailing_differs": True,
        "open_case_count": 1,
    },
    {
        "case": "ENF-PMNT-26-016665",
        "address": "1701 W BROADWAY LOUISVILLE KY 40203",
        "description_raw": "Caller rerports overgrown weeds and trees in the back of this property. Also trash in the back of the property.",
        "ai": {
            "distress_level": "MEDIUM",
            "signals": ["overgrown_vegetation", "trash_or_debris", "nuisance"],
            "confirmed_facts": ["The complaint reports overgrown weeds and trees and trash at the rear of the property."],
            "speculative_claims": [],
            "summary": "Reported vegetation overgrowth and trash indicate active exterior neglect.",
            "acquisition_relevant": True,
        },
        "owner_mailing_differs": True,
        "open_case_count": 1,
    },
]


def assertion(name: str, passed: bool, **details):
    return {"name": name, "passed": bool(passed), **details}


def main() -> int:
    results = []

    # AI semantic contract + grounding checks.
    for f in LIVE_AI_FIXTURES:
        try:
            normalized = validate_ai_classification(f["ai"])
            results.append(assertion(f"ai_contract_{f['case']}", True, level=normalized["distress_level"]))
        except Exception as e:
            results.append(assertion(f"ai_contract_{f['case']}", False, error=str(e)))

    grissom = LIVE_AI_FIXTURES[0]["ai"]
    results.append(assertion(
        "grounding_grissom_snakes_speculative",
        any("snake" in s.lower() for s in grissom["speculative_claims"])
        and not any("snake" in s.lower() for s in grissom["confirmed_facts"]),
    ))
    s19 = LIVE_AI_FIXTURES[3]["ai"]
    results.append(assertion(
        "grounding_fire_risk_not_confirmed_fire",
        any("possible fire risk" in s.lower() for s in s19["speculative_claims"])
        and not any("confirmed fire" in s.lower() or "actual fire" in s.lower() for s in s19["confirmed_facts"]),
    ))
    try:
        bad = dict(grissom); bad["distress_score"] = 99
        validate_ai_classification(bad)
        results.append(assertion("ai_cannot_set_numeric_score", False))
    except ValueError:
        results.append(assertion("ai_cannot_set_numeric_score", True))

    # Freshness boundaries, including date-only municipal sources.
    same = freshness(NOW, NOW, same_calendar_day=True)
    results.append(assertion("freshness_same_day_date_precision", same.score == 100 and same.age_hours is None and same.date_precision == "date", actual=same.__dict__))
    f23 = freshness(NOW - timedelta(hours=23), NOW)
    f48 = freshness(NOW - timedelta(hours=48), NOW)
    f5d = freshness(NOW - timedelta(days=5), NOW)
    f20d = freshness(NOW - timedelta(days=20), NOW)
    f45d = freshness(NOW - timedelta(days=45), NOW)
    results += [
        assertion("freshness_23h", f23.score == 95, actual=f23.__dict__),
        assertion("freshness_48h", f48.score == 82, actual=f48.__dict__),
        assertion("freshness_5d", f5d.score == 65, actual=f5d.__dict__),
        assertion("freshness_20d", f20d.score == 42, actual=f20d.__dict__),
        assertion("freshness_45d", f45d.score == 20, actual=f45d.__dict__),
    ]

    # Competition calibration: fresh code-enforcement free-text should be lower
    # saturation than generic/public vendor-style sources.
    code_sat = saturation_score(source_type="code_enforcement", freshness_score=100, has_free_text_description=True, new_transition_event=True)
    lis_sat = saturation_score(source_type="lis_pendens", freshness_score=100, has_free_text_description=False)
    results.append(assertion("fresh_code_lower_saturation_than_lis", code_sat < lis_sat, code=code_sat, lis=lis_sat))

    ranked = []
    for f in LIVE_AI_FIXTURES:
        scores = rank_lead(
            ai=f["ai"], source_type="code_enforcement", event_at=NOW,
            same_calendar_day=True, open_case_count=f["open_case_count"],
            owner_mailing_differs=f["owner_mailing_differs"],
            has_free_text_description=True, new_transition_event=True, now=NOW,
        )
        ranked.append({"case": f["case"], "address": f["address"], "ai": f["ai"], **scores})
    ranked.sort(key=lambda x: x["priority_score"], reverse=True)
    results.append(assertion("electrical_fire_case_ranks_above_grass_case", ranked[0]["case"] == "ENF-PMNT-26-015609", top=ranked[0]["case"], order=[x["case"] for x in ranked]))

    # Retry: two transient failures, third succeeds.
    calls = {"n": 0}
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("forced_timeout")
        return "ok"
    value, meta = retry_call(flaky, policy=RetryPolicy(max_attempts=3, base_delay_seconds=0), sleep=lambda _: None)
    results.append(assertion("retry_recovers_transient_failure", value == "ok" and meta["attempts"] == 3 and meta["recovered"], meta=meta))

    # Permanent errors must fail immediately rather than hammering the source.
    permanent_calls = {"n": 0}
    def permanent():
        permanent_calls["n"] += 1
        raise ValueError("selector_changed")
    try:
        retry_call(permanent, policy=RetryPolicy(max_attempts=4, base_delay_seconds=0), sleep=lambda _: None)
        results.append(assertion("permanent_error_no_retry", False))
    except ValueError:
        results.append(assertion("permanent_error_no_retry", permanent_calls["n"] == 1, attempts=permanent_calls["n"]))

    # Circuit breaker open -> block -> cooldown -> half-open -> success resets.
    fake_time = {"t": 100.0}
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=10, clock=lambda: fake_time["t"])
    for _ in range(3):
        breaker.record_failure()
    opened = breaker.state == CircuitState.OPEN
    blocked = False
    try:
        breaker.allow()
    except CircuitOpenError:
        blocked = True
    fake_time["t"] += 11
    half_open = False
    try:
        breaker.allow(); half_open = breaker.state == CircuitState.HALF_OPEN
    except CircuitOpenError:
        pass
    breaker.record_success()
    results.append(assertion("circuit_breaker_opens_blocks_and_recovers", opened and blocked and half_open and breaker.state == CircuitState.CLOSED))

    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    status = "PASS" if failed == 0 else "FAIL"
    report = {
        "status": status,
        "bench_time_utc": NOW.isoformat(),
        "ai_fixture_provider": "GPT-5.6 Sol current-session bench classification of previously verified live Accela descriptions",
        "ai_fixture_note": "No external model API was called from GitHub Actions; AI semantics are frozen fixtures for contract/scoring acceptance.",
        "assertions_passed": passed,
        "assertions_failed": failed,
        "assertions": results,
        "ranked_live_fixture_leads": ranked,
    }
    (OUT / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
