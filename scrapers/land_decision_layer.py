"""Deterministic decision layer for TheReaper land opportunities.

AI interprets evidence only. Numeric motivation/builder-fit/saturation/freshness
and priority scores are owned by code so the model cannot invent math.
"""
from __future__ import annotations

from typing import Any

from scrapers.decision_layer import clamp, freshness_date

ALLOWED_LEVELS = {"HIGH", "MEDIUM", "LOW", "NONE"}
ALLOWED_SIGNALS = {
    "vacant_lot", "repeat_abatement", "overgrown_vegetation", "trash_or_dumping",
    "demolition_transition", "demolition_order", "condemnation", "boarded_or_unsecured",
    "tax_delinquent", "accumulated_fines", "absentee_owner", "estate_or_deceased",
    "owner_unresponsive", "long_term_neglect", "municipal_cleanup", "dangerous_tree",
    "assemblage_opportunity", "adjacent_same_owner", "other",
}

LEVEL_BASE = {"HIGH": 50, "MEDIUM": 30, "LOW": 12, "NONE": 0}
SIGNAL_WEIGHTS = {
    "demolition_transition": 25,
    "demolition_order": 22,
    "condemnation": 20,
    "tax_delinquent": 18,
    "repeat_abatement": 16,
    "estate_or_deceased": 12,
    "owner_unresponsive": 10,
    "municipal_cleanup": 10,
    "long_term_neglect": 9,
    "absentee_owner": 8,
    "boarded_or_unsecured": 8,
    "trash_or_dumping": 7,
    "accumulated_fines": 6,
    "vacant_lot": 6,
    "dangerous_tree": 4,
    "overgrown_vegetation": 3,
    "adjacent_same_owner": 3,
    "assemblage_opportunity": 2,
    "other": 1,
}

FORBIDDEN_NUMERIC_FIELDS = {
    "motivation_score", "builder_fit_score", "saturation_score",
    "freshness_score", "priority_score",
}


def validate_land_ai(value: dict[str, Any]) -> dict[str, Any]:
    required = {
        "motivation_level", "signals", "confirmed_facts", "speculative_claims",
        "summary", "acquisition_relevant",
    }
    missing = required - set(value)
    if missing:
        raise ValueError(f"land_ai_contract_missing:{sorted(missing)}")
    bad = FORBIDDEN_NUMERIC_FIELDS & set(value)
    if bad:
        raise ValueError(f"land_ai_contract_forbidden_numeric_fields:{sorted(bad)}")
    level = str(value["motivation_level"]).upper()
    if level not in ALLOWED_LEVELS:
        raise ValueError(f"land_ai_contract_bad_level:{level}")
    signals = [str(x) for x in value["signals"]]
    unknown = sorted(set(signals) - ALLOWED_SIGNALS)
    if unknown:
        raise ValueError(f"land_ai_contract_unknown_signals:{unknown}")
    if not isinstance(value["confirmed_facts"], list) or not isinstance(value["speculative_claims"], list):
        raise ValueError("land_ai_contract_facts_must_be_lists")
    if not isinstance(value["acquisition_relevant"], bool):
        raise ValueError("land_ai_contract_acquisition_relevant_must_be_bool")
    if not str(value["summary"]).strip():
        raise ValueError("land_ai_contract_empty_summary")
    out = dict(value)
    out["motivation_level"] = level
    out["signals"] = signals
    return out


def motivation_score(ai: dict[str, Any], *, open_case_count: int = 1,
                     citation_event_count: int = 0,
                     owner_mailing_differs: bool = False) -> int:
    ai = validate_land_ai(ai)
    score = LEVEL_BASE[ai["motivation_level"]]
    for signal in set(ai["signals"]):
        score += SIGNAL_WEIGHTS.get(signal, 0)
    if open_case_count >= 3:
        score += 12
    elif open_case_count == 2:
        score += 7
    if citation_event_count:
        score += min(10, citation_event_count * 4)
    if owner_mailing_differs:
        score += 4
    if not ai["acquisition_relevant"]:
        score = min(score, 20)
    return clamp(score)


def builder_fit_score(*, zoning_type: str | None, zoning_code: str | None,
                      landuse_name: str | None, lot_sqft: float | None,
                      confirmed_vacant_lot: bool, parcel_type: int | None = 0) -> int:
    """Estimate builder usefulness; this is NOT a legal buildability determination."""
    score = 0
    ztype = str(zoning_type or "").upper()
    zcode = str(zoning_code or "").upper()
    landuse = str(landuse_name or "").upper()

    if parcel_type == 0:
        score += 8
    elif parcel_type is not None:
        score -= 25

    if ztype == "RESIDENTIAL":
        score += 35
    elif any(x in ztype for x in ("COMMERCIAL", "INDUSTRIAL", "OFFICE")):
        score -= 25
    elif ztype:
        score += 5

    if "SINGLE FAMILY" in landuse:
        score += 22
    elif "MULTI-FAMILY" in landuse:
        score -= 15
    elif landuse:
        score += 4

    if confirmed_vacant_lot:
        score += 12

    if lot_sqft is not None:
        if 2500 <= lot_sqft <= 15000:
            score += 23
        elif 15000 < lot_sqft <= 43560:
            score += 18
        elif 1500 <= lot_sqft < 2500:
            score += 10
        elif 43560 < lot_sqft <= 217800:
            score += 12
        elif lot_sqft < 1500:
            score -= 15
        else:
            score += 5

    # Common residential zoning codes receive a small confidence bump only.
    if zcode.startswith("R"):
        score += 5
    return clamp(score)


def land_saturation_score(*, freshness_score: int, custom_code_signal: bool,
                          open_case_count: int = 1, demolition_transition: bool = False,
                          tax_signal: bool = False) -> int:
    # Estimate only: higher means likely more generic-investor exposure.
    score = 34
    if freshness_score >= 95:
        score -= 8
    elif freshness_score <= 20:
        score += 18
    elif freshness_score <= 42:
        score += 9
    if custom_code_signal:
        score -= 7
    if open_case_count >= 2:
        score -= min(10, 3 * (open_case_count - 1))
    if demolition_transition:
        score -= 10
    if tax_signal:
        score += 5
    return clamp(score, 5, 100)


def land_priority_score(*, motivation: int, builder_fit: int,
                        freshness_score: int, saturation: int) -> int:
    return clamp(
        0.46 * motivation
        + 0.29 * builder_fit
        + 0.15 * freshness_score
        + 0.10 * (100 - saturation)
    )


def rank_land(*, ai: dict[str, Any], event_date: str | None, today: str,
              zoning_type: str | None, zoning_code: str | None,
              landuse_name: str | None, lot_sqft: float | None,
              confirmed_vacant_lot: bool, parcel_type: int | None = 0,
              open_case_count: int = 1, citation_event_count: int = 0,
              owner_mailing_differs: bool = False,
              custom_code_signal: bool = True,
              demolition_transition: bool = False,
              tax_signal: bool = False) -> dict[str, Any]:
    f = freshness_date(event_date, today=today)
    m = motivation_score(
        ai,
        open_case_count=open_case_count,
        citation_event_count=citation_event_count,
        owner_mailing_differs=owner_mailing_differs,
    )
    b = builder_fit_score(
        zoning_type=zoning_type,
        zoning_code=zoning_code,
        landuse_name=landuse_name,
        lot_sqft=lot_sqft,
        confirmed_vacant_lot=confirmed_vacant_lot,
        parcel_type=parcel_type,
    )
    s = land_saturation_score(
        freshness_score=f.score,
        custom_code_signal=custom_code_signal,
        open_case_count=open_case_count,
        demolition_transition=demolition_transition,
        tax_signal=tax_signal,
    )
    p = land_priority_score(
        motivation=m, builder_fit=b, freshness_score=f.score, saturation=s
    )
    return {
        "motivation_score": m,
        "builder_fit_score": b,
        "saturation_score": s,
        "freshness_score": f.score,
        "freshness_label": f.label,
        "freshness_precision": f.date_precision,
        "priority_score": p,
    }
