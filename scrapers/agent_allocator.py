#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def norm_address(v: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", clean(v).upper())


def property_key(row: dict[str, Any]) -> str:
    parcel = clean(row.get("parcel_id")).upper()
    if parcel:
        return f"JEFFERSON_KY::PARCEL::{parcel}"
    addr = norm_address(row.get("property_address"))
    if not addr:
        raise ValueError("missing_property_identity")
    return f"JEFFERSON_KY::ADDRESS::{addr}"


def material_revision(row: dict[str, Any]) -> str:
    """Fingerprint one material source/event row, excluding presentation-only fields."""
    material = {
        "case_number": row.get("case_number"),
        "event_date": row.get("event_date") or row.get("latest_activity_date"),
        "open_case_count": row.get("open_case_count") or row.get("open_cases_in_50_sample_same_parcel") or row.get("recent_window_distinct_parent_groups_same_parcel"),
        "citation_assessed_total": row.get("citation_assessed_total"),
        "tax_delinquent_verified": row.get("tax_delinquent_verified"),
        "tax_bill_total": row.get("tax_bill_total"),
        "demolition_verified": row.get("demolition_verified"),
        "possible_structure_to_lot_transition": row.get("possible_structure_to_lot_transition"),
        "description_raw": clean(row.get("description_raw")),
        "inspector_comments": [clean(v) for v in (row.get("inspector_comments") or [])],
        "source_url": clean(row.get("source_url")),
    }
    raw = json.dumps(material, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def property_material_revision(rows: list[dict[str, Any]]) -> str:
    """Fingerprint the complete material event set known for one property.

    This prevents two cases already present on the same parcel from consuming two
    agent slots or falsely looking like a same-run reactivation. A genuinely new
    case/event changes the set and therefore changes the property revision.
    """
    revisions = sorted(set(material_revision(row) for row in rows))
    raw = json.dumps(revisions, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def qualify_house(row: dict[str, Any]) -> bool:
    try:
        return (
            clean(row.get("ai_scoring_status")).upper() == "LIVE"
            and clean(row.get("ai_contract_version")) == "reaper-live-ai-v1"
            and int(row.get("priority_score") or 0) >= 60
            and int(row.get("distress_score") or 0) >= 50
            and clean(row.get("landuse_name")).upper() == "SINGLE FAMILY"
            and bool(row.get("lojic_parcel_verified", True))
        )
    except Exception:
        return False


def qualify_land(row: dict[str, Any]) -> bool:
    try:
        site = clean(row.get("occupancy") or row.get("site_status") or "vacant lot").upper()
        land_context = (
            "VACANT" in site
            or bool(row.get("vacant_lot_context"))
            or "VACANT LOT" in clean(row.get("recent_window_occupancies")).upper()
            or clean(row.get("property_type") or row.get("candidate_type")).upper() == "LAND"
        )
        return (
            clean(row.get("ai_scoring_status")).upper() == "LIVE"
            and clean(row.get("ai_contract_version")) == "reaper-live-ai-v1"
            and int(row.get("priority_score") or 0) >= 60
            and int(row.get("motivation_score") or 0) >= 50
            and int(row.get("builder_fit_score") or 0) >= 50
            and land_context
            and bool(row.get("lojic_parcel_verified", True))
        )
    except Exception:
        return False


def load_rows(path: Path, kind: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        rows = data
    elif kind == "house":
        rows = data.get("houses") or data.get("ranked_live_leads") or data.get("qualified_houses") or data.get("eligible_sfr") or []
    else:
        rows = data.get("land") or data.get("ranked_land") or data.get("qualified_land") or data.get("eligible_land") or []
    return [r for r in rows if isinstance(r, dict)]


def row_sort_key(r: dict[str, Any]) -> tuple[int, int, int]:
    return (
        int(r.get("priority_score") or r.get("reaper_priority_score") or 0),
        int(r.get("distress_score") or r.get("motivation_score") or r.get("source_score") or 0),
        -int(r.get("saturation_score") or 100),
    )


def sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=row_sort_key, reverse=True)


def grouped_candidates(
    rows: list[dict[str, Any]], kind: str
) -> tuple[list[tuple[dict[str, Any], str, str, int]], dict[str, int]]:
    """Return one best representative per property plus a parcel-level revision."""
    qualifier = qualify_house if kind == "house" else qualify_land
    groups: dict[str, list[dict[str, Any]]] = {}
    stats = {"seen": 0, "failed_quality": 0, "duplicate_property_rows_collapsed": 0}

    for row in sort_rows(rows):
        stats["seen"] += 1
        if not qualifier(row):
            stats["failed_quality"] += 1
            continue
        try:
            pkey = property_key(row)
        except ValueError:
            stats["failed_quality"] += 1
            continue
        groups.setdefault(pkey, []).append(row)

    candidates: list[tuple[dict[str, Any], str, str, int]] = []
    for pkey, event_rows in groups.items():
        representative = sort_rows(event_rows)[0]
        revision = property_material_revision(event_rows)
        candidates.append((representative, pkey, revision, len(event_rows)))
        stats["duplicate_property_rows_collapsed"] += max(0, len(event_rows) - 1)

    candidates.sort(key=lambda item: row_sort_key(item[0]), reverse=True)
    return candidates, stats


def allocate_kind(
    *,
    rows: list[dict[str, Any]],
    kind: str,
    agent_id: str,
    limit: int,
    state: dict[str, Any],
    now: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    selected: list[dict[str, Any]] = []
    candidates, prep_stats = grouped_candidates(rows, kind)
    stats = {
        **prep_stats,
        "qualified_unique_properties": len(candidates),
        "assigned_other": 0,
        "unchanged_self": 0,
        "dnc_or_closed": 0,
        "new_assignments": 0,
        "reactivations": 0,
    }
    properties = state.setdefault("properties", {})

    for row, pkey, revision, event_count in candidates:
        if len(selected) >= limit:
            break
        existing = properties.get(pkey)

        if existing:
            status = clean(existing.get("status") or "active").lower()
            if status in {"dnc", "closed"}:
                stats["dnc_or_closed"] += 1
                continue
            assigned_to = clean(existing.get("assigned_to"))
            if status != "released" and assigned_to and assigned_to != agent_id:
                stats["assigned_other"] += 1
                continue
            if (
                status != "released"
                and assigned_to == agent_id
                and existing.get("last_material_revision") == revision
            ):
                stats["unchanged_self"] += 1
                continue

        is_reactivation = bool(
            existing
            and existing.get("assigned_to") == agent_id
            and existing.get("last_material_revision") != revision
            and clean(existing.get("status") or "active").lower() != "released"
        )
        if existing and clean(existing.get("status") or "active").lower() == "released":
            first_assigned_at = now
            react_count = 0
        else:
            first_assigned_at = (existing or {}).get("first_assigned_at") or now
            react_count = int((existing or {}).get("reactivation_count") or 0)

        properties[pkey] = {
            "assigned_to": agent_id,
            "lead_type": kind,
            "status": "active",
            "first_assigned_at": first_assigned_at,
            "last_delivered_at": now,
            "last_material_revision": revision,
            "reactivation_count": react_count + (1 if is_reactivation else 0),
            "parcel_id": clean(row.get("parcel_id")) or None,
            "property_address": clean(row.get("property_address") or row.get("source_property_address")) or None,
        }

        delivered = dict(row)
        delivered["agent_id"] = agent_id
        delivered["property_key"] = pkey
        delivered["material_revision"] = revision
        delivered["property_event_rows_in_pool"] = event_count
        delivered["assignment_status"] = "REACTIVATED" if is_reactivation else "NEW"
        selected.append(delivered)
        if is_reactivation:
            stats["reactivations"] += 1
        else:
            stats["new_assignments"] += 1

    return selected, stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Allocate unique sticky TheReaper leads to one agent.")
    ap.add_argument("--agent-id", required=True)
    ap.add_argument("--houses-file", required=True)
    ap.add_argument("--land-file", required=True)
    ap.add_argument("--state-file", default="state/lead_assignments.json")
    ap.add_argument("--house-limit", type=int, default=25)
    ap.add_argument("--land-limit", type=int, default=25)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    state_path = Path(args.state_file)
    state = (
        json.loads(state_path.read_text(encoding="utf-8"))
        if state_path.exists()
        else {"schema_version": 1, "properties": {}}
    )
    now = datetime.now(timezone.utc).isoformat()

    houses, house_stats = allocate_kind(
        rows=load_rows(Path(args.houses_file), "house"),
        kind="house",
        agent_id=args.agent_id,
        limit=max(0, args.house_limit),
        state=state,
        now=now,
    )
    land, land_stats = allocate_kind(
        rows=load_rows(Path(args.land_file), "land"),
        kind="land",
        agent_id=args.agent_id,
        limit=max(0, args.land_limit),
        state=state,
        now=now,
    )

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    output = {
        "status": "PASS",
        "agent_id": args.agent_id,
        "generated_at": now,
        "houses": houses,
        "land": land,
        "summary": {
            "houses_delivered": len(houses),
            "land_delivered": len(land),
            "total_delivered": len(houses) + len(land),
            "house_stats": house_stats,
            "land_stats": land_stats,
        },
    }
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(output["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
