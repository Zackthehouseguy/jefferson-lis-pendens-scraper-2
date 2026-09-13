import json
from datetime import datetime, timedelta, timezone

from scrapers.probe.reaper_health_check import evaluate


def report(**summary):
    base = {
        "input_candidates": 100,
        "parcel_verified": 95,
        "pva_owner_verified": 95,
        "eligible_sfr": 20,
        "eligible_land": 1,
        "ai_scoring_status": "LIVE",
    }
    base.update(summary)
    return {
        "status": "PASS",
        "generated_at_et": datetime.now(timezone.utc).isoformat(),
        "summary": base,
    }


def test_systemic_parcel_loss_blocks_false_green():
    h = evaluate(report(parcel_verified=0), {"lis_pendens": 0, "wills": 0, "louisville_code_violations": 0, "tax_delinquent": 0})
    assert h["status"] == "BLOCKED"
    assert "systemic_parcel_verification_loss" in h["blockers"]


def test_verified_cache_is_degraded_not_blocked():
    r = report(parcel_live_verified=0, parcel_cache_verified=95)
    r["status"] = "DEGRADED"
    r["summary"]["dependency_health"] = {"status": "DEGRADED", "reason": "lojic_live_unavailable_using_verified_cache"}
    h = evaluate(r, {"lis_pendens": 0, "wills": 0, "louisville_code_violations": 0, "tax_delinquent": 0})
    assert h["status"] == "DEGRADED"
    assert not h["blockers"]


def test_partial_source_failure_degrades_and_all_sources_block():
    partial = evaluate(report(), {"lis_pendens": 1, "wills": 0})
    assert partial["status"] == "DEGRADED"
    all_failed = evaluate(report(), {"lis_pendens": 1, "wills": 2})
    assert all_failed["status"] == "BLOCKED"


def test_stale_report_and_missing_live_ai_block():
    r = report(ai_scoring_status="FALLBACK")
    r["generated_at_et"] = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
    h = evaluate(r, {"code": 0}, max_age_hours=24, require_live_ai=True)
    assert h["status"] == "BLOCKED"
    assert "live_ai_scoring_missing" in h["blockers"]
    assert any(x.startswith("report_stale:") for x in h["blockers"])
