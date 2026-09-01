#!/usr/bin/env python3
"""Run live semantic AI classification and deterministic production scoring.

The model may classify evidence, but it may not return numeric scores.  This
stage is atomic and fail-closed: every prequalified property must receive a
valid classification before any report is written.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from scrapers.decision_layer import (
    distress_score,
    freshness_date,
    priority_score,
    validate_ai_classification,
)
from scrapers.land_decision_layer import (
    builder_fit_score,
    land_priority_score,
    motivation_score,
    validate_land_ai,
)
from scrapers.probe.reaper_bulk_qualify import norm_addr, render_md
from scrapers.reaper_saturation import METHOD as SATURATION_METHOD
from scrapers.reaper_saturation import public_source_exposure_score

ET = ZoneInfo("America/New_York")
CONTRACT_VERSION = "reaper-live-ai-v1"
DEFAULT_MODEL = "gpt-5.6"
SCORING_REJECTIONS = {
    "priority_below_production_threshold",
    "distress_below_production_threshold",
    "motivation_below_production_threshold",
    "builder_fit_below_production_threshold",
    "ai_classification_missing",
}

HouseSignal = Literal[
    "vacancy", "abandonment", "structural_damage", "fire_risk",
    "utility_issue", "water_damage", "mold", "unsafe_structure",
    "boarded_or_unsecured", "severe_exterior_deterioration", "habitability",
    "repeated_noncompliance", "owner_unresponsive", "demolition_risk",
    "nuisance", "accumulated_fines", "estate_or_deceased", "tenant_issue",
    "electrical", "infestation", "trash_or_debris", "overgrown_vegetation",
    "mortgage_distress", "tax_delinquent", "probate_or_inherited",
    "multiple_distress_sources", "other",
]
LandSignal = Literal[
    "vacant_lot", "repeat_abatement", "overgrown_vegetation", "trash_or_dumping",
    "demolition_transition", "demolition_order", "condemnation", "boarded_or_unsecured",
    "tax_delinquent", "accumulated_fines", "absentee_owner", "estate_or_deceased",
    "owner_unresponsive", "long_term_neglect", "municipal_cleanup", "dangerous_tree",
    "assemblage_opportunity", "adjacent_same_owner", "other",
]
Level = Literal["HIGH", "MEDIUM", "LOW", "NONE"]


class HouseClassification(BaseModel):
    model_config = ConfigDict(extra="forbid")
    property_key: str
    distress_level: Level
    signals: list[HouseSignal]
    confirmed_facts: list[str]
    speculative_claims: list[str]
    summary: str
    acquisition_relevant: bool


class HouseBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    classifications: list[HouseClassification]


class LandClassification(BaseModel):
    model_config = ConfigDict(extra="forbid")
    property_key: str
    motivation_level: Level
    signals: list[LandSignal]
    confirmed_facts: list[str]
    speculative_claims: list[str]
    summary: str
    acquisition_relevant: bool


class LandBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    classifications: list[LandClassification]


SYSTEM_PROMPT = """You classify public-record evidence for a real-estate acquisition workflow.
Return exactly one classification for every required property_key and no others.
Use only the supplied evidence. Never invent a fact, event, amount, status, condition, or owner motive.
Numeric scores are forbidden; deterministic code calculates every score after classification.
Treat complaint text as a reported allegation, not as proof the physical condition is true.
A lis pendens is legal/property distress, not automatically a foreclosure or a mortgage default.
A will filing is not automatically an active probate case and does not prove willingness to sell.
A tax-list appearance is a published tax-delinquency signal only for the stated source/date.
HIGH means multiple or severe current acquisition-relevant signals; MEDIUM means one material current signal;
LOW means limited/indirect evidence; NONE means the supplied evidence is not acquisition-relevant.
Confirmed facts must describe what the source reports or records. Put unsupported inferences in speculative_claims.
Keep the summary concise and explicitly grounded in the named source evidence."""


def model_key(row: dict[str, Any]) -> str:
    parcel = re.sub(r"[^A-Z0-9]", "", str(row.get("parcel_id") or "").upper())
    if parcel:
        return f"JEFFERSON_KY::PARCEL::{parcel}"
    key = str(row.get("property_key") or "").strip()
    if key:
        return key
    address = norm_addr(row.get("source_property_address") or row.get("property_address"))
    if not address:
        raise ValueError("scoring_target_missing_property_identity")
    return f"JEFFERSON_KY::ADDRESS::{address}"


def is_scoring_target(row: dict[str, Any]) -> bool:
    if row.get("candidate_type") not in {"SFR", "LAND"}:
        return False
    if row.get("qualification_status") == "ERROR":
        return False
    core_rejections = [reason for reason in (row.get("rejection_reasons") or []) if reason not in SCORING_REJECTIONS]
    return not core_rejections


def _evidence_packet(row: dict[str, Any]) -> dict[str, Any]:
    evidence = []
    for item in row.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        evidence.append({
            "source": item.get("source"),
            "signal_date": item.get("signal_date"),
            "status": item.get("status"),
            "amount": item.get("amount"),
            "party_or_owner": item.get("party_or_owner"),
            "details": str(item.get("details") or "")[:4000],
            "source_url": item.get("source_url"),
        })
    return {
        "property_key": model_key(row),
        "candidate_type": row.get("candidate_type"),
        "property_address": row.get("source_property_address") or row.get("property_address"),
        "parcel_id": row.get("parcel_id"),
        "current_pva_owner": row.get("pva_owner"),
        "owner_mailing_address": row.get("pva_mailing_address"),
        "landuse_name": row.get("landuse_name"),
        "lot_sqft": row.get("lot_sqft"),
        "sources": row.get("sources") or [],
        "evidence": evidence,
    }


def _validate_exact_keys(items: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    expected = {model_key(row) for row in rows}
    keys = [str(item.get("property_key") or "") for item in items]
    if len(keys) != len(set(keys)):
        raise ValueError("ai_response_duplicate_property_keys")
    actual = set(keys)
    if actual != expected:
        raise ValueError(
            f"ai_response_key_mismatch:missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )
    return {str(item["property_key"]): item for item in items}


def _api_classify_batch(rows: list[dict[str, Any]], lane: str, model: str, api_key: str) -> dict[str, dict[str, Any]]:
    from openai import OpenAI

    response_type = HouseBatch if lane == "SFR" else LandBatch
    payload = {
        "lane": "single-family distress" if lane == "SFR" else "vacant-land motivation",
        "required_property_keys": [model_key(row) for row in rows],
        "candidates": [_evidence_packet(row) for row in rows],
    }
    last_error: Exception | None = None
    for _attempt in range(2):
        try:
            client = OpenAI(api_key=api_key, max_retries=3, timeout=120.0)
            response = client.responses.parse(
                model=model,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                text_format=response_type,
                store=False,
            )
            parsed = response.output_parsed
            if parsed is None:
                raise RuntimeError("openai_structured_output_missing")
            data = parsed.model_dump()
            return _validate_exact_keys(data["classifications"], rows)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"openai_classification_failed:{type(last_error).__name__}:{last_error}")


def classify_live(
    rows: list[dict[str, Any]], *, model: str, api_key: str, batch_size: int, workers: int
) -> dict[str, dict[str, Any]]:
    batches: list[tuple[str, list[dict[str, Any]]]] = []
    for lane in ("SFR", "LAND"):
        lane_rows = [row for row in rows if row.get("candidate_type") == lane]
        for start in range(0, len(lane_rows), max(1, batch_size)):
            batches.append((lane, lane_rows[start:start + max(1, batch_size)]))
    output: dict[str, dict[str, Any]] = {}
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(_api_classify_batch, batch, lane, model, api_key): (lane, batch)
            for lane, batch in batches
        }
        for future in cf.as_completed(futures):
            result = future.result()
            overlap = set(output) & set(result)
            if overlap:
                raise RuntimeError(f"duplicate_ai_results_across_batches:{sorted(overlap)}")
            output.update(result)
    return output


def load_fixture_classifications(path: Path, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw = data.get("classifications", data)
    if isinstance(raw, dict):
        items = []
        for key, value in raw.items():
            item = dict(value)
            item.setdefault("property_key", key)
            items.append(item)
    elif isinstance(raw, list):
        items = raw
    else:
        raise ValueError("fixture_classifications_must_be_mapping_or_list")
    return _validate_exact_keys(items, rows)


def _parse_signal_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _latest_date(row: dict[str, Any]) -> date | None:
    dates = [_parse_signal_date(item.get("signal_date")) for item in (row.get("evidence") or []) if isinstance(item, dict)]
    return max((item for item in dates if item), default=None)


def _owner_mailing_differs(row: dict[str, Any]) -> bool:
    situs = norm_addr(row.get("pva_situs_address") or row.get("source_property_address"))
    mailing = norm_addr(row.get("pva_mailing_address"))
    return bool(situs and mailing and situs != mailing)


def _open_case_count(row: dict[str, Any]) -> int:
    code_rows = [item for item in (row.get("evidence") or []) if item.get("source") == "louisville_code_violations"]
    return max(1, len(code_rows))


def _citation_event_count(row: dict[str, Any]) -> int:
    return sum(
        1 for item in (row.get("evidence") or [])
        if item.get("source") == "louisville_code_violations" and item.get("amount") not in (None, "", 0, 0.0)
    )


def _priority_tier(score: int) -> str:
    if score >= 75:
        return "CALL FIRST"
    if score >= 60:
        return "STRONG"
    if score >= 45:
        return "REVIEW"
    return "LOW"


def _address_fields(address: str) -> dict[str, str | None]:
    match = re.search(r",\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$", address, re.I)
    return {
        "city": match.group(1).strip() if match else "Louisville",
        "state": match.group(2).upper() if match else "KY",
        "zip": match.group(3) if match else None,
    }


def _presentation_fields(row: dict[str, Any]) -> dict[str, Any]:
    evidence = [item for item in (row.get("evidence") or []) if isinstance(item, dict)]
    details = [str(item.get("details") or "").strip() for item in evidence if str(item.get("details") or "").strip()]
    source_urls = [str(item.get("source_url") or "").strip() for item in evidence if str(item.get("source_url") or "").strip()]
    address = str(row.get("source_property_address") or row.get("property_address") or "").strip()
    citation_amounts = []
    tax_amounts = []
    for item in evidence:
        try:
            amount = float(item.get("amount"))
        except (TypeError, ValueError):
            continue
        if item.get("source") == "louisville_code_violations":
            citation_amounts.append(amount)
        elif item.get("source") == "tax_delinquent":
            tax_amounts.append(amount)
    return {
        "property_address": address,
        **_address_fields(address),
        "owner_name": row.get("pva_owner"),
        "owner_mailing_address": row.get("pva_mailing_address"),
        "source_url": source_urls[0] if source_urls else row.get("pva_url"),
        "description_raw": " ".join(details)[:12000],
        "inspector_comments": [
            str(item.get("details") or "").strip()
            for item in evidence
            if item.get("source") == "louisville_code_violations" and str(item.get("details") or "").strip()
        ],
        "citation_event_count": len(citation_amounts),
        "citation_assessed_total": round(sum(citation_amounts), 2),
        "tax_delinquent_verified": True if "tax_delinquent" in (row.get("sources") or []) else None,
        "tax_bill_total": max(tax_amounts) if tax_amounts else None,
        "property_type": row.get("candidate_type"),
    }


def score_row(
    row: dict[str, Any], classification: dict[str, Any], *, today: date,
    scoring_status: str, model: str, scored_at: str,
) -> dict[str, Any]:
    scored = dict(row)
    classification = dict(classification)
    key = classification.pop("property_key", None)
    if key != model_key(row):
        raise ValueError(f"classification_key_drift:{key}:{model_key(row)}")

    owner_differs = _owner_mailing_differs(row)
    open_cases = _open_case_count(row)
    citations = _citation_event_count(row)
    latest = _latest_date(row)
    fresh = freshness_date(latest, today=today)
    saturation, saturation_factors = public_source_exposure_score(
        sources=row.get("sources") or [],
        freshness_score=fresh.score,
        evidence=row.get("evidence") or [],
        owner_mailing_differs=owner_differs,
        open_case_count=open_cases,
    )

    if row.get("candidate_type") == "SFR":
        ai = validate_ai_classification(classification)
        primary = distress_score(
            ai,
            open_case_count=open_cases,
            citation_event_count=citations,
            owner_mailing_differs=owner_differs,
        )
        priority = priority_score(distress=primary, freshness_score=fresh.score, saturation=saturation)
        score_fields = {
            "ai_distress_level": ai["distress_level"],
            "distress_score": primary,
            "priority_formula": "round(0.60*distress_score + 0.25*freshness_score + 0.15*(100-saturation_score))",
            "priority_components": {
                "distress_score": primary,
                "freshness_score": fresh.score,
                "saturation_score": saturation,
            },
        }
        qualified = primary >= 50 and priority >= 60
        new_rejections = []
        if primary < 50:
            new_rejections.append("distress_below_production_threshold")
    else:
        ai = validate_land_ai(classification)
        primary = motivation_score(
            ai,
            open_case_count=open_cases,
            citation_event_count=citations,
            owner_mailing_differs=owner_differs,
        )
        builder = builder_fit_score(
            zoning_type=row.get("zoning_type"),
            zoning_code=row.get("zoning_code"),
            landuse_name=row.get("landuse_name"),
            lot_sqft=row.get("lot_sqft"),
            confirmed_vacant_lot=bool(row.get("vacant_lot_context")),
            parcel_type=row.get("parcel_type"),
        )
        priority = land_priority_score(
            motivation=primary,
            builder_fit=builder,
            freshness_score=fresh.score,
            saturation=saturation,
        )
        score_fields = {
            "ai_motivation_level": ai["motivation_level"],
            "motivation_score": primary,
            "builder_fit_score": builder,
            "priority_formula": "round(0.46*motivation_score + 0.29*builder_fit_score + 0.15*freshness_score + 0.10*(100-saturation_score))",
            "priority_components": {
                "motivation_score": primary,
                "builder_fit_score": builder,
                "freshness_score": fresh.score,
                "saturation_score": saturation,
            },
        }
        qualified = primary >= 50 and builder >= 50 and priority >= 60
        new_rejections = []
        if primary < 50:
            new_rejections.append("motivation_below_production_threshold")
        if builder < 50:
            new_rejections.append("builder_fit_below_production_threshold")

    if priority < 60:
        new_rejections.append("priority_below_production_threshold")
    prior_rejections = [reason for reason in (row.get("rejection_reasons") or []) if reason not in SCORING_REJECTIONS]
    scored.update(_presentation_fields(row))
    scored.update(score_fields)
    scored.update({
        "property_key": model_key(row),
        "ai_signals": ai["signals"],
        "confirmed_facts": ai["confirmed_facts"],
        "speculative_claims": ai["speculative_claims"],
        "ai_summary": ai["summary"],
        "ai_acquisition_relevant": ai["acquisition_relevant"],
        "ai_scoring_status": scoring_status,
        "ai_provider": "OpenAI Responses API" if scoring_status == "LIVE" else "TEST FIXTURE",
        "ai_model": model,
        "ai_contract_version": CONTRACT_VERSION,
        "ai_scored_at_et": scored_at,
        "deterministic_score_inputs": {
            "ai_level": ai.get("distress_level") or ai.get("motivation_level"),
            "unique_ai_signals": sorted(set(ai["signals"])),
            "open_case_count": open_cases,
            "citation_event_count": citations,
            "owner_mailing_differs": owner_differs,
            "acquisition_relevant": ai["acquisition_relevant"],
        },
        "open_case_count": open_cases,
        "owner_mailing_differs": owner_differs,
        "event_date": latest.isoformat() if latest else None,
        "latest_activity_date": latest.isoformat() if latest else None,
        "freshness_score": fresh.score,
        "freshness_label": fresh.label,
        "freshness_precision": fresh.date_precision,
        "saturation_score": saturation,
        "saturation_method": SATURATION_METHOD,
        "saturation_factors": saturation_factors,
        "priority_score": priority,
        "priority_tier": _priority_tier(priority),
        "qualification_status": "ELIGIBLE" if qualified and not prior_rejections else "REJECTED",
        "rejection_reasons": prior_rejections + new_rejections,
    })
    return scored


def score_report(
    report: dict[str, Any], classifications: dict[str, dict[str, Any]], *,
    scoring_status: str, model: str, now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc).astimezone(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    targets = [row for row in (report.get("all_results") or []) if is_scoring_target(row)]
    expected = {model_key(row) for row in targets}
    if set(classifications) != expected:
        raise ValueError(
            f"classification_set_mismatch:missing={sorted(expected - set(classifications))}:"
            f"extra={sorted(set(classifications) - expected)}"
        )

    scored_by_key = {
        model_key(row): score_row(
            row,
            classifications[model_key(row)],
            today=now.date(),
            scoring_status=scoring_status,
            model=model,
            scored_at=now.isoformat(),
        )
        for row in targets
    }
    results = [scored_by_key.get(model_key(row), row) if is_scoring_target(row) else row for row in (report.get("all_results") or [])]
    results.sort(
        key=lambda row: (
            row.get("qualification_status") == "ELIGIBLE",
            int(row.get("priority_score") or 0),
            int(row.get("distress_score") or row.get("motivation_score") or 0),
        ),
        reverse=True,
    )
    eligible_sfr = [row for row in results if row.get("qualification_status") == "ELIGIBLE" and row.get("candidate_type") == "SFR"]
    eligible_land = [row for row in results if row.get("qualification_status") == "ELIGIBLE" and row.get("candidate_type") == "LAND"]
    for lane_rows in (eligible_sfr, eligible_land):
        for rank, row in enumerate(lane_rows, 1):
            row["rank"] = rank

    summary = dict(report.get("summary") or {})
    summary.update({
        "eligible_sfr": len(eligible_sfr),
        "eligible_land": len(eligible_land),
        "ai_scoring_status": scoring_status,
        "ai_model": model,
        "ai_targets": len(targets),
        "ai_classified": len(scored_by_key),
        "ai_unclassified": len(targets) - len(scored_by_key),
        "saturation_method": SATURATION_METHOD,
        "saturation_distinct_scores": len({row.get("saturation_score") for row in scored_by_key.values()}),
        "ai_level_distribution": dict(Counter(
            row.get("ai_distress_level") or row.get("ai_motivation_level") for row in scored_by_key.values()
        )),
    })
    notes = list(report.get("notes") or [])
    notes.extend([
        "Every prequalified property was classified through the live structured-output AI contract before deterministic scoring.",
        "The model returns semantic classifications only; numeric scores are calculated by version-controlled code.",
        "Each scored row retains deterministic_score_inputs, priority_components, priority_formula, saturation_factors, and the AI contract/model metadata needed to reproduce the result.",
        f"Saturation uses {SATURATION_METHOD}; it does not claim measured investor contacts or competition.",
        "No prior bench fixture or stale classification is accepted by the production allocator.",
    ])
    return {
        **report,
        "status": "PASS",
        "generated_at_et": now.isoformat(),
        "ai_scored_at_et": now.isoformat(),
        "ai_provider": "OpenAI Responses API" if scoring_status == "LIVE" else "TEST FIXTURE",
        "ai_model": model,
        "ai_contract_version": CONTRACT_VERSION,
        "summary": summary,
        "eligible_sfr": eligible_sfr,
        "eligible_land": eligible_land,
        "all_results": results,
        "notes": notes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify and score every prequalified Reaper property.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--md", required=True)
    parser.add_argument("--model", default=os.getenv("REAPER_AI_MODEL") or DEFAULT_MODEL)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--fixture-classifications", help="Tests only; never configured in the production workflow.")
    args = parser.parse_args()

    report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    targets = [row for row in (report.get("all_results") or []) if is_scoring_target(row)]
    if args.fixture_classifications:
        classifications = load_fixture_classifications(Path(args.fixture_classifications), targets)
        scoring_status = "TEST_FIXTURE"
    elif targets:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is required for live Reaper AI scoring; refusing stale or fixture fallback"
            )
        classifications = classify_live(
            targets,
            model=args.model,
            api_key=api_key,
            batch_size=max(1, args.batch_size),
            workers=max(1, args.workers),
        )
        scoring_status = "LIVE"
    else:
        classifications = {}
        scoring_status = "LIVE"

    scored = score_report(report, classifications, scoring_status=scoring_status, model=args.model)
    output = Path(args.output)
    markdown = Path(args.md)
    output.parent.mkdir(parents=True, exist_ok=True)
    markdown.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(scored, indent=2, ensure_ascii=False), encoding="utf-8")
    markdown.write_text(render_md(scored), encoding="utf-8")
    print(json.dumps(scored["summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
