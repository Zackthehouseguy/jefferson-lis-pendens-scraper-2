#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from scrapers.probe import accela_engine_probe_v2 as base
from scrapers.probe import full_system_live_extract as legacy

ENTITY_TERMS = (
    " LLC", "L.L.C", " INC", " CORPORATION", " CORP", " COMPANY", " CO ",
    " TRUST", " ESTATE", " HEIRS", " DEVISEES", " CHURCH", " MINISTR",
    " FOUNDATION", " HOLDINGS", " PROPERTIES", " ASSETS", " INVESTMENTS",
    " DEVELOPMENT", " REALTY", " ASSOCIATION", " PARTNERSHIP", " LLP", " LP ",
)


def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def is_individual_owner(name: str | None, mailing: str | None = None) -> bool:
    name_u = f" {clean(name).upper()} "
    mailing_u = f" {clean(mailing).upper()} "
    combined = name_u + " " + mailing_u
    if not clean(name):
        return False
    if any(term in combined for term in ENTITY_TERMS):
        return False
    if any(tok in name_u for tok in (" DBA ", " D/B/A ", " C/O ")):
        return False
    return True


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def active_assigned_parcels(path: Path) -> set[str]:
    data = load_json(path, {"properties": {}})
    out: set[str] = set()
    for rec in (data.get("properties") or {}).values():
        if clean(rec.get("status") or "active").lower() in {"released"}:
            continue
        parcel = clean(rec.get("parcel_id")).upper()
        if parcel:
            out.add(parcel)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Fresh-only individual-owner wrapper for the live extractor.")
    ap.add_argument("--target-individual", type=int, default=8)
    ap.add_argument("--raw-target", type=int, default=20)
    ap.add_argument("--arcgis-limit", type=int, default=1000)
    ap.add_argument("--max-parent-attempts", type=int, default=60)
    ap.add_argument("--out", default="reports/full_system_live")
    ap.add_argument("--seen-state", default="state/full_system_seen.json")
    ap.add_argument("--assignment-state", default="state/lead_assignments.json")
    args = ap.parse_args()

    out_dir = Path(args.out)
    seen_path = Path(args.seen_state)
    assigned_path = Path(args.assignment_state)
    seen = load_json(seen_path, {"schema_version": 1, "cases": {}, "parcels": {}})
    seen.setdefault("schema_version", 1)
    seen_cases = {clean(k).upper() for k in (seen.get("cases") or {})}
    seen_parcels = {clean(k).upper() for k in (seen.get("parcels") or {})}
    assigned_parcels = active_assigned_parcels(assigned_path)

    original_build_groups = base.build_groups
    filter_stats = {"seen_case_skipped": 0, "seen_parcel_skipped": 0, "assigned_property_skipped": 0}

    def fresh_build_groups(features):
        groups = original_build_groups(features)
        fresh = []
        for g in groups:
            case = clean(g.get("parent_key")).upper()
            parcel = clean(g.get("parcel")).upper()
            if case and case in seen_cases:
                filter_stats["seen_case_skipped"] += 1
                continue
            if parcel and parcel in seen_parcels:
                filter_stats["seen_parcel_skipped"] += 1
                continue
            if parcel and parcel in assigned_parcels:
                filter_stats["assigned_property_skipped"] += 1
                continue
            fresh.append(g)
        return fresh

    base.build_groups = fresh_build_groups
    old_argv = sys.argv[:]
    try:
        sys.argv = [
            old_argv[0],
            "--target-open", str(max(args.raw_target, args.target_individual)),
            "--arcgis-limit", str(args.arcgis_limit),
            "--max-parent-attempts", str(args.max_parent_attempts),
            "--out", str(out_dir),
        ]
        legacy.main()
    finally:
        sys.argv = old_argv
        base.build_groups = original_build_groups

    report_path = out_dir / "extract_report.json"
    report = load_json(report_path, {})
    raw_records = [r for r in (report.get("verified_open_unseen_records") or []) if isinstance(r, dict)]

    now = datetime.now(timezone.utc).isoformat()
    cases_state = seen.setdefault("cases", {})
    parcels_state = seen.setdefault("parcels", {})
    entity_skipped = 0
    individual_records = []
    duplicate_property_skipped = 0
    delivered_parcels: set[str] = set()

    for row in raw_records:
        case = clean(row.get("case_number")).upper()
        parcel = clean(row.get("parcel_id")).upper()
        if case:
            cases_state.setdefault(case, {"parcel_id": parcel or None, "first_seen_at": now})
        if parcel:
            parcels_state.setdefault(parcel, {"first_seen_at": now})

        if not is_individual_owner(row.get("owner_name"), row.get("owner_mailing_address")):
            entity_skipped += 1
            continue
        if parcel and parcel in delivered_parcels:
            duplicate_property_skipped += 1
            continue
        if parcel:
            delivered_parcels.add(parcel)
        individual_records.append(row)
        if len(individual_records) >= args.target_individual:
            break

    seen_path.parent.mkdir(parents=True, exist_ok=True)
    seen_path.write_text(json.dumps(seen, indent=2, ensure_ascii=False), encoding="utf-8")

    report["raw_fresh_records_examined"] = len(raw_records)
    report["verified_open_unseen_records"] = individual_records
    report["fresh_individual_records"] = individual_records
    report["fresh_individual_count"] = len(individual_records)
    report["entity_owner_skipped"] = entity_skipped
    report["duplicate_property_skipped"] = duplicate_property_skipped
    report["persistent_freshness_filter"] = {
        **filter_stats,
        "seen_cases_before_run": len(seen_cases),
        "seen_parcels_before_run": len(seen_parcels),
        "active_assigned_parcels_before_run": len(assigned_parcels),
    }
    report["status"] = (
        "PASS" if len(individual_records) >= args.target_individual
        else "PARTIAL" if individual_records
        else "EMPTY"
    )
    report["shortfall_note"] = None if len(individual_records) >= args.target_individual else (
        f"Only {len(individual_records)} fresh individual-owned properties were found after excluding previously surfaced, assigned, duplicate, and entity-owned records."
    )
    report["next_stage"] = "Rank only these fresh individual-owner records. Do not recycle prior surfaced properties."
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps({
        "status": report["status"],
        "fresh_individual_count": len(individual_records),
        "raw_fresh_records_examined": len(raw_records),
        "entity_owner_skipped": entity_skipped,
        **filter_stats,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
