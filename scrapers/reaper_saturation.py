"""Transparent public-source exposure heuristic for Reaper leads.

This score is deliberately *not* represented as observed investor competition.
It estimates how widely a lead's source pattern is likely to appear in common
public-record lead lists.  Higher means greater estimated exposure.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from scrapers.decision_layer import clamp

METHOD = "PUBLIC_SOURCE_EXPOSURE_HEURISTIC_V2 — not observed competition"

SOURCE_BASE = {
    "louisville_landbank": 85,
    "generic_absentee": 78,
    "generic_vacant": 72,
    "lis_pendens": 68,
    "tax_delinquent": 55,
    "wills": 42,
    "louisville_code_violations": 30,
}

CUSTOM_EVIDENCE_TERMS = (
    "FOUNDATION", "STRUCTURAL", "COLLAPSE", "SEWAGE", "MOLD", "FIRE",
    "NO WATER", "NO ELECTRIC", "TERMINATED UTIL", "DEMOL", "CONDEMN",
    "BOARD", "UNSAFE", "INFEST", "ROOF",
)


def public_source_exposure_score(
    *,
    sources: Iterable[str],
    freshness_score: int,
    evidence: Iterable[dict[str, Any]] = (),
    owner_mailing_differs: bool = False,
    open_case_count: int = 1,
) -> tuple[int, list[str]]:
    """Return a deterministic score plus an audit trail of every adjustment."""
    normalized_sources = sorted({str(source or "").strip().lower() for source in sources if str(source or "").strip()})
    bases = [SOURCE_BASE.get(source, 50) for source in normalized_sources] or [50]
    score = sum(bases) / len(bases)
    factors = [
        "source_base=" + ",".join(
            f"{source}:{SOURCE_BASE.get(source, 50)}" for source in (normalized_sources or ["unknown"])
        ),
        f"source_base_average={round(score)}",
    ]

    if len(normalized_sources) > 1:
        adjustment = min(18, 8 * (len(normalized_sources) - 1))
        score += adjustment
        factors.append(f"multi_source_public_visibility=+{adjustment}")

    if freshness_score >= 95:
        score -= 12
        factors.append("same_day_or_under_24h=-12")
    elif freshness_score >= 82:
        score -= 6
        factors.append("one_to_three_days=-6")
    elif freshness_score <= 20:
        score += 18
        factors.append("older_than_30_days=+18")
    elif freshness_score <= 42:
        score += 10
        factors.append("eight_to_30_days=+10")

    details = " ".join(
        str(item.get("details") or "").strip()
        for item in evidence
        if isinstance(item, dict) and str(item.get("details") or "").strip()
    )
    if len(details) >= 600:
        score -= 10
        factors.append("detail_rich_source_narrative=-10")
    elif len(details) >= 200:
        score -= 6
        factors.append("substantive_source_narrative=-6")
    elif len(details) >= 80:
        score -= 3
        factors.append("source_narrative=-3")

    custom_hits = sorted({term for term in CUSTOM_EVIDENCE_TERMS if term in details.upper()})
    if len(custom_hits) >= 3:
        score -= 7
        factors.append("multiple_specific_distress_terms=-7")
    elif custom_hits:
        score -= 4
        factors.append("specific_distress_term=-4")

    if owner_mailing_differs:
        score += 8
        factors.append("mailing_differs_common_absentee_filter=+8")

    if open_case_count >= 2:
        adjustment = min(9, 3 * (open_case_count - 1))
        score += adjustment
        factors.append(f"repeat_public_case_visibility=+{adjustment}")

    final = clamp(score, 5, 100)
    factors.append(f"bounded_score={final}")
    return final, factors
