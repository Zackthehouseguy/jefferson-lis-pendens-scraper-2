"""Decision layer for TheReaper code-enforcement leads.

AI classifies evidence into a strict semantic contract. This module owns ALL
numeric math so a model can never invent or silently change scores.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

ALLOWED_LEVELS = {"HIGH", "MEDIUM", "LOW", "NONE"}
ALLOWED_SIGNALS = {
    "vacancy", "abandonment", "structural_damage", "fire_risk",
    "utility_issue", "water_damage", "mold", "unsafe_structure",
    "boarded_or_unsecured", "severe_exterior_deterioration", "habitability",
    "repeated_noncompliance", "owner_unresponsive", "demolition_risk",
    "nuisance", "accumulated_fines", "estate_or_deceased", "tenant_issue",
    "electrical", "infestation", "trash_or_debris", "overgrown_vegetation",
    "mortgage_distress", "tax_delinquent", "probate_or_inherited",
    "multiple_distress_sources",
    "other",
}

LEVEL_BASE = {"HIGH": 52, "MEDIUM": 32, "LOW": 12, "NONE": 0}
SIGNAL_WEIGHTS = {
    "fire_risk": 22,
    "unsafe_structure": 20,
    "structural_damage": 18,
    "electrical": 16,
    "habitability": 14,
    "vacancy": 14,
    "abandonment": 14,
    "demolition_risk": 14,
    "water_damage": 10,
    "mold": 10,
    "utility_issue": 10,
    "infestation": 9,
    "boarded_or_unsecured": 9,
    "severe_exterior_deterioration": 8,
    "repeated_noncompliance": 8,
    "trash_or_debris": 6,
    "owner_unresponsive": 5,
    "nuisance": 4,
    "overgrown_vegetation": 3,
    "tenant_issue": 2,
    "accumulated_fines": 2,
    "estate_or_deceased": 2,
    "mortgage_distress": 16,
    "tax_delinquent": 18,
    "probate_or_inherited": 12,
    "multiple_distress_sources": 10,
    "other": 1,
}


def clamp(n: float, lo: int = 0, hi: int = 100) -> int:
    return max(lo, min(hi, int(round(n))))


def validate_ai_classification(value: dict[str, Any]) -> dict[str, Any]:
    """Strictly validate the model output contract.

    Numeric score fields are intentionally forbidden. The model describes;
    deterministic code scores.
    """
    required = {
        "distress_level", "signals", "confirmed_facts", "speculative_claims",
        "summary", "acquisition_relevant",
    }
    missing = required - set(value)
    if missing:
        raise ValueError(f"ai_contract_missing:{sorted(missing)}")
    forbidden = {"distress_score", "saturation_score", "freshness_score", "priority_score"}
    bad = forbidden & set(value)
    if bad:
        raise ValueError(f"ai_contract_forbidden_numeric_fields:{sorted(bad)}")
    level = str(value["distress_level"]).upper()
    if level not in ALLOWED_LEVELS:
        raise ValueError(f"ai_contract_bad_level:{level}")
    signals = [str(x) for x in value["signals"]]
    unknown = sorted(set(signals) - ALLOWED_SIGNALS)
    if unknown:
        raise ValueError(f"ai_contract_unknown_signals:{unknown}")
    if not isinstance(value["confirmed_facts"], list) or not isinstance(value["speculative_claims"], list):
        raise ValueError("ai_contract_facts_must_be_lists")
    if not isinstance(value["acquisition_relevant"], bool):
        raise ValueError("ai_contract_acquisition_relevant_must_be_bool")
    if not str(value["summary"]).strip():
        raise ValueError("ai_contract_empty_summary")
    out = dict(value)
    out["distress_level"] = level
    out["signals"] = signals
    return out


@dataclass(frozen=True)
class Freshness:
    score: int
    label: str
    age_hours: float | None
    date_precision: str


def freshness(event_at: datetime | None, now: datetime | None = None, *, same_calendar_day: bool = False) -> Freshness:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if event_at is None:
        return Freshness(0, "UNKNOWN", None, "unknown")
    if event_at.tzinfo is None:
        event_at = event_at.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - event_at).total_seconds() / 3600.0)
    if same_calendar_day:
        return Freshness(100, "SAME DAY", None, "date")
    if age_h < 24:
        return Freshness(95, "<24 HOURS", round(age_h, 2), "timestamp")
    days = age_h / 24.0
    if days <= 3:
        return Freshness(82, "1-3 DAYS", round(age_h, 2), "timestamp")
    if days <= 7:
        return Freshness(65, "4-7 DAYS", round(age_h, 2), "timestamp")
    if days <= 30:
        return Freshness(42, "8-30 DAYS", round(age_h, 2), "timestamp")
    if days <= 90:
        return Freshness(20, "31-90 DAYS", round(age_h, 2), "timestamp")
    return Freshness(5, ">90 DAYS", round(age_h, 2), "timestamp")


def freshness_date(event_date: date | str | None, today: date | str | None = None) -> Freshness:
    """Score a source that exposes only calendar-date precision.

    Never invent an hour. age_hours stays None and precision stays `date`.
    """
    if event_date is None:
        return Freshness(0, "UNKNOWN", None, "unknown")
    if isinstance(event_date, str):
        event_date = date.fromisoformat(event_date)
    if today is None:
        today = datetime.now(timezone.utc).date()
    elif isinstance(today, str):
        today = date.fromisoformat(today)
    days = max(0, (today - event_date).days)
    if days == 0:
        return Freshness(100, "SAME DAY", None, "date")
    if days <= 3:
        return Freshness(82, "1-3 DAYS", None, "date")
    if days <= 7:
        return Freshness(65, "4-7 DAYS", None, "date")
    if days <= 30:
        return Freshness(42, "8-30 DAYS", None, "date")
    if days <= 90:
        return Freshness(20, "31-90 DAYS", None, "date")
    return Freshness(5, ">90 DAYS", None, "date")


def distress_score(ai: dict[str, Any], *, open_case_count: int = 1,
                   citation_event_count: int = 0, owner_mailing_differs: bool = False) -> int:
    ai = validate_ai_classification(ai)
    score = LEVEL_BASE[ai["distress_level"]]
    for signal in set(ai["signals"]):
        score += SIGNAL_WEIGHTS.get(signal, 0)
    if open_case_count >= 3:
        score += 12
    elif open_case_count == 2:
        score += 7
    if citation_event_count:
        score += min(8, citation_event_count * 3)
    if owner_mailing_differs:
        score += 3
    if not ai["acquisition_relevant"]:
        score = min(score, 20)
    return clamp(score)


def saturation_score(*, source_type: str, freshness_score: int,
                     has_free_text_description: bool, open_case_count: int = 1,
                     new_transition_event: bool = False) -> int:
    # Bench calibration, not empirical truth. Higher = likely more investor competition.
    # A floor of 5 prevents "low competition" from being presented as proof no one else has it.
    source_base = {
        "code_enforcement": 30,
        "lis_pendens": 68,
        "tax_delinquent": 55,
        "generic_absentee": 78,
        "generic_vacant": 72,
    }.get(source_type, 50)
    score = source_base
    if freshness_score >= 95:
        score -= 8
    elif freshness_score <= 20:
        score += 18
    elif freshness_score <= 42:
        score += 10
    if has_free_text_description:
        score -= 6
    if open_case_count >= 2:
        score -= min(9, 3 * (open_case_count - 1))
    if new_transition_event:
        score -= 8
    return clamp(score, 5, 100)


def priority_score(*, distress: int, freshness_score: int, saturation: int) -> int:
    return clamp(0.60 * distress + 0.25 * freshness_score + 0.15 * (100 - saturation))


def rank_lead(*, ai: dict[str, Any], source_type: str, event_at: datetime | None,
              same_calendar_day: bool = False, open_case_count: int = 1,
              citation_event_count: int = 0, owner_mailing_differs: bool = False,
              has_free_text_description: bool = True, new_transition_event: bool = False,
              now: datetime | None = None) -> dict[str, Any]:
    f = freshness(event_at, now, same_calendar_day=same_calendar_day)
    d = distress_score(ai, open_case_count=open_case_count,
                       citation_event_count=citation_event_count,
                       owner_mailing_differs=owner_mailing_differs)
    s = saturation_score(source_type=source_type, freshness_score=f.score,
                         has_free_text_description=has_free_text_description,
                         open_case_count=open_case_count,
                         new_transition_event=new_transition_event)
    p = priority_score(distress=d, freshness_score=f.score, saturation=s)
    return {
        "distress_score": d,
        "saturation_score": s,
        "freshness_score": f.score,
        "freshness_label": f.label,
        "freshness_age_hours": f.age_hours,
        "freshness_precision": f.date_precision,
        "priority_score": p,
    }
