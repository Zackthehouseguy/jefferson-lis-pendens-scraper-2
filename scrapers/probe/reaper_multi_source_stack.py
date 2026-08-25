#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

ENTITY_TERMS = (
    " LLC", " L.L.C", " INC", " CORPORATION", " CORP", " COMPANY", " CO ",
    " TRUST", " ESTATE", " HEIRS", " DEVISEES", " CHURCH", " MINISTRY",
    " FOUNDATION", " HOLDINGS", " PROPERTIES", " ASSETS", " INVESTMENTS",
    " DEVELOPMENT", " REALTY", " ASSOCIATION", " PARTNERSHIP", " LLP", " LP ",
    " AUTHORITY", " METRO", " COUNTY", " CITY OF ", " COMMONWEALTH",
)

SOURCE_FILES = {
    "lis_pendens": "lis_pendens_results.csv",
    "wills": "wills_results.csv",
    "louisville_code_violations": "louisville_code_violations_results.csv",
    "tax_delinquent": "jefferson_tax_delinquent_results.csv",
    "louisville_landbank": "louisville_landbank_results.csv",
}


def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def norm_addr(v) -> str:
    s = clean(v).upper()
    if not s or s in {"ADDRESS NOT FOUND", "N/A", "NONE"}:
        return ""
    replacements = {
        " STREET": " ST", " AVENUE": " AVE", " ROAD": " RD", " DRIVE": " DR",
        " LANE": " LN", " COURT": " CT", " BOULEVARD": " BLVD", " PLACE": " PL",
        " TERRACE": " TER", " HIGHWAY": " HWY", " PARKWAY": " PKWY",
    }
    for a, b in replacements.items():
        s = s.replace(a, b)
    s = re.sub(r"\bLOUISVILLE\b.*$", "", s).strip()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_money(v) -> float | None:
    s = re.sub(r"[^0-9.]", "", clean(v))
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def likely_individual(name: str) -> bool | None:
    n = clean(name)
    if not n:
        return None
    u = f" {n.upper()} "
    if any(term in u for term in ENTITY_TERMS):
        return False
    if any(x in u for x in (" BANK ", " MORTGAGE ", " FINANCIAL ", " CREDIT UNION ", " SERVICING ")):
        return False
    return True


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def parse_source(source: str, rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        if source in {"lis_pendens", "wills", "louisville_landbank"}:
            addr = clean(r.get("Property Address"))
            party = clean(r.get("Defendants/Parties"))
            date = clean(r.get("Date")) or None
            url = clean(r.get("PDF Link"))
            notes = clean(r.get("Notes"))
            parcel = ""
            amount = None
            status = ""
            details = notes
        elif source == "louisville_code_violations":
            addr = clean(r.get("Property Address"))
            party = clean(r.get("Parties"))
            date = clean(r.get("Filing Date")) or None
            url = clean(r.get("Source Link") or r.get("PDF Link"))
            notes = clean(r.get("Notes"))
            parcel = clean(r.get("Parcel"))
            amount = parse_money(r.get("Citation Total"))
            status = clean(r.get("Status"))
            details = " | ".join(x for x in [clean(r.get("Distress Signals")), clean(r.get("Violation Codes")), notes] if x)
        elif source == "tax_delinquent":
            addr = clean(r.get("Property Address"))
            party = clean(r.get("Parties"))
            date = clean(r.get("Filing Date")) or None
            url = clean(r.get("Source Link"))
            notes = clean(r.get("Notes"))
            parcel = clean(r.get("Parcel ID"))
            amount = parse_money(r.get("Amount Due"))
            status = clean(r.get("Status"))
            details = " | ".join(x for x in [f"Tax year {clean(r.get('Tax Year'))}" if clean(r.get('Tax Year')) else "", f"Amount due {clean(r.get('Amount Due'))}" if clean(r.get('Amount Due')) else "", notes] if x)
        else:
            continue

        if not addr and not parcel:
            # Wills often lack a property locator; preserve counts upstream but
            # do not pretend an unlocated estate is a property lead.
            continue

        out.append({
            "source": source,
            "signal_date": date,
            "property_address": addr or None,
            "normalized_address": norm_addr(addr),
            "parcel_id": parcel or None,
            "party_or_owner": party or None,
            "individual_party_likely": likely_individual(party),
            "status": status or None,
            "amount": amount,
            "details": details or None,
            "source_url": url or None,
            "raw_priority": clean(r.get("Priority")) if source == "louisville_code_violations" else None,
            "raw_distress_score": int(float(r.get("Distress Score"))) if source == "louisville_code_violations" and clean(r.get("Distress Score")) else None,
        })
    return out


def load_state(path: Path) -> tuple[set[str], set[str]]:
    if not path.exists():
        return set(), set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set(), set()
    parcels, addrs = set(), set()
    for rec in (data.get("properties") or {}).values():
        status = clean(rec.get("status") or "active").lower()
        if status in {"released"}:
            continue
        p = clean(rec.get("parcel_id")).upper()
        a = norm_addr(rec.get("property_address"))
        if p:
            parcels.add(p)
        if a:
            addrs.add(a)
    return parcels, addrs


def load_seen(path: Path) -> tuple[set[str], set[str]]:
    if not path.exists():
        return set(), set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set(), set()
    parcels = {clean(k).upper() for k in (data.get("parcels") or {}) if clean(k)}
    return parcels, set()


def property_score(evidence: list[dict]) -> tuple[int, list[str]]:
    sources = {e["source"] for e in evidence}
    score = 0
    reasons: list[str] = []

    if "lis_pendens" in sources:
        score += 48
        reasons.append("fresh/recent lis pendens signal")
    if "tax_delinquent" in sources:
        score += 24
        vals = [e.get("amount") or 0 for e in evidence if e["source"] == "tax_delinquent"]
        mx = max(vals or [0])
        if mx >= 10000:
            score += 16
            reasons.append(f"large published delinquent-tax balance (${mx:,.0f})")
        elif mx >= 5000:
            score += 10
            reasons.append(f"meaningful published delinquent-tax balance (${mx:,.0f})")
        else:
            reasons.append("published delinquent-tax listing")
    if "wills" in sources:
        score += 18
        reasons.append("will/probate-related filing signal")
    if "louisville_landbank" in sources:
        score += 10
        reasons.append("landbank inventory signal")
    if "louisville_code_violations" in sources:
        code_rows = [e for e in evidence if e["source"] == "louisville_code_violations"]
        raw = max([e.get("raw_distress_score") or 0 for e in code_rows] or [0])
        score += min(34, max(18, round(raw * 0.34)))
        text = " ".join(clean(e.get("details")).upper() for e in code_rows)
        severe_terms = ("CONDEMN", "UNSAFE", "STRUCTURAL", "FOUNDATION", "ABANDON", "VACANT", "TERMINATED UTIL", "FIRE", "NO WATER", "NO ELECTRIC", "ROOF")
        severe_hits = sorted({t for t in severe_terms if t in text})
        if severe_hits:
            score += 16
            reasons.append("severe active code-enforcement indicators: " + ", ".join(severe_hits[:4]).lower())
        else:
            reasons.append("active code-enforcement distress")

    if len(sources) >= 3:
        score += 32
        reasons.append(f"{len(sources)} independent distress/source types stacked")
    elif len(sources) == 2:
        score += 24
        reasons.append("2 independent distress/source types stacked")

    return min(100, score), reasons


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="reaper_live_sources")
    ap.add_argument("--out", default="reports/reaper_multi_source_live/stacked.json")
    ap.add_argument("--assignments", default="state/lead_assignments.json")
    ap.add_argument("--seen", default="state/full_system_seen.json")
    ap.add_argument("--start-date", default=None)
    ap.add_argument("--end-date", default=None)
    ap.add_argument("--top", type=int, default=150)
    args = ap.parse_args()

    root = Path(args.root)
    source_counts = {}
    source_status = {}
    all_evidence: list[dict] = []

    for source, fname in SOURCE_FILES.items():
        path = root / source / fname
        rows = read_csv(path)
        source_counts[source] = len(rows)
        source_status[source] = "OK" if path.exists() else "MISSING_OR_FAILED"
        all_evidence.extend(parse_source(source, rows))

    assigned_parcels, assigned_addrs = load_state(Path(args.assignments))
    seen_parcels, _ = load_seen(Path(args.seen))

    groups: dict[str, list[dict]] = {}
    for e in all_evidence:
        parcel = clean(e.get("parcel_id")).upper()
        addr = e.get("normalized_address") or ""
        key = f"PARCEL::{parcel}" if parcel else (f"ADDR::{addr}" if addr else "")
        if not key:
            continue
        groups.setdefault(key, []).append(e)

    # Address-merge parcel groups with address-only groups (e.g. lis pendens + tax/code).
    addr_to_parcel_key = {}
    for key, rows in groups.items():
        if key.startswith("PARCEL::"):
            for r in rows:
                if r.get("normalized_address"):
                    addr_to_parcel_key.setdefault(r["normalized_address"], key)
    for key in list(groups):
        if not key.startswith("ADDR::"):
            continue
        addr = key.split("::", 1)[1]
        target = addr_to_parcel_key.get(addr)
        if target and target != key:
            groups[target].extend(groups.pop(key))

    properties = []
    for key, evidence in groups.items():
        sources = sorted({e["source"] for e in evidence})
        score, reasons = property_score(evidence)
        parcels = [clean(e.get("parcel_id")).upper() for e in evidence if clean(e.get("parcel_id"))]
        parcel = parcels[0] if parcels else None
        addresses = [clean(e.get("property_address")) for e in evidence if clean(e.get("property_address"))]
        address = addresses[0] if addresses else None
        norm = norm_addr(address)
        names = []
        for e in evidence:
            n = clean(e.get("party_or_owner"))
            if n and n not in names:
                names.append(n)
        individual_flags = [e.get("individual_party_likely") for e in evidence if e.get("individual_party_likely") is not None]
        individual_likely = True if individual_flags and all(individual_flags) else (False if False in individual_flags else None)
        previously_assigned = bool((parcel and parcel in assigned_parcels) or (norm and norm in assigned_addrs))
        previously_seen_code = bool(parcel and parcel in seen_parcels)
        fresh_unworked = not previously_assigned
        properties.append({
            "property_key": key,
            "property_address": address,
            "parcel_id": parcel,
            "parties_or_owners": names,
            "individual_owner_or_party_likely": individual_likely,
            "sources": sources,
            "signal_count": len(sources),
            "motivation_score": score,
            "motivation_class": "HIGH" if score >= 70 else ("MEDIUM" if score >= 50 else "LOW"),
            "score_reasons": reasons,
            "previously_assigned": previously_assigned,
            "previously_seen_in_code_queue": previously_seen_code,
            "fresh_unworked_candidate": fresh_unworked,
            "evidence": evidence,
        })

    properties.sort(key=lambda r: (r["motivation_score"], r["signal_count"]), reverse=True)
    fresh_individual = [r for r in properties if r["fresh_unworked_candidate"] and r["individual_owner_or_party_likely"] is True]
    high = [r for r in fresh_individual if r["motivation_score"] >= 70]
    medium = [r for r in fresh_individual if 50 <= r["motivation_score"] < 70]

    report = {
        "status": "PASS" if any(v == "OK" for v in source_status.values()) else "FAIL",
        "generated_at_et": datetime.now(timezone.utc).astimezone(ET).isoformat(),
        "query_window": {"start_date": args.start_date, "end_date": args.end_date},
        "source_status": source_status,
        "source_record_counts": source_counts,
        "properties_after_stacking": len(properties),
        "fresh_individual_candidates": len(fresh_individual),
        "fresh_individual_high_count": len(high),
        "fresh_individual_medium_count": len(medium),
        "top_fresh_individual": fresh_individual[: args.top],
        "top_all": properties[: min(args.top, 100)],
        "scoring_note": "Deterministic pre-score for triage. ChatGPT should review evidence and produce final AI motivation ranking; no unsupported signal may be added.",
        "tax_note": "Published delinquent-tax list can contain bills paid after publication; verify current status before outreach.",
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("status", "source_record_counts", "properties_after_stacking", "fresh_individual_candidates", "fresh_individual_high_count", "fresh_individual_medium_count")}, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
