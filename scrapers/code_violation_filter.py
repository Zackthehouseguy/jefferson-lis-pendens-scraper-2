"""
Lead-quality filtering, scoring, and property-level dedupe for code-violation
sources (currently Louisville Metro PM_SiteVisit_Violations).

Code-violation feeds are violation-level, so the same property typically
appears in many rows for a single distressed condition. We want one
distressed-property *lead* per property, with combined notes and a distress
score that explains why the property is flagged.

Pure functions only — no I/O, no network. The Louisville scraper feeds raw
ArcGIS feature attributes in and gets back a list of grouped lead rows
shaped for the canonical 5-column CSV (plus structured `_instrument_number`
and `_filing_date_iso` extras for the ingest sidecar).
"""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Signal taxonomy
# ---------------------------------------------------------------------------

# Themes are the buckets we use both for scoring and for the human-readable
# "Reasons" string in Notes. Each theme has:
#   - keywords: substrings matched against GUIDE_ITEM_TEXT (case-insensitive)
#   - codes:    VIOLATION_CODE prefixes that always count for this theme
#   - weight:   distress points contributed when present on a property
#
# Order matters for "Reasons" rendering — we iterate THEMES in declaration
# order so the most distress-relevant themes appear first.
THEMES: list[dict[str, Any]] = [
    {
        "name": "vacant/abandoned",
        "keywords": [
            "vacant", "abandoned", "unoccupied", "boarded", "boarding",
            "condemned", "condemnation",
        ],
        "codes": [],
        "weight": 4,
    },
    {
        "name": "structural/foundation",
        "keywords": [
            "structural", "foundation", "collapse", "unsafe", "unstable",
            "load bearing", "load-bearing",
        ],
        "codes": ["X19", "I33"],
        "weight": 3,
    },
    {
        "name": "exterior",
        "keywords": [
            "exterior", "siding", "stucco", "masonry", "brick", "paint peeling",
            "deteriorated",
        ],
        "codes": ["X19"],
        "weight": 2,
    },
    {
        "name": "roof/gutters",
        "keywords": [
            "roof", "gutter", "down spout", "downspout", "soffit", "fascia",
            "eave",
        ],
        "codes": ["X50"],
        "weight": 2,
    },
    {
        "name": "porch/stairs",
        "keywords": [
            "porch", "stairs", "stairway", "stairwell", "handrail", "railing",
            "balcony", "deck",
        ],
        "codes": ["X40"],
        "weight": 2,
    },
    {
        "name": "windows/doors",
        "keywords": ["window", "door", "screen"],
        "codes": [],
        "weight": 1,
    },
    {
        "name": "demolition",
        "keywords": ["demolition", "demolish", "raze"],
        "codes": ["X94"],
        "weight": 5,
    },
    {
        "name": "cleaning/weeds/rubbish",
        "keywords": [
            "cleaning", "rubbish", "garbage", "weeds", "plant growth", "litter",
            "junk", "debris", "trash", "overgrown", "overgrowth",
        ],
        "codes": ["02A"],
        "weight": 2,
    },
    {
        "name": "graffiti/defacement",
        "keywords": ["graffiti", "defacement", "defaced"],
        "codes": ["080A"],
        "weight": 1,
    },
    {
        "name": "abandoned/illegal vehicle",
        "keywords": [
            "abandoned vehicle", "illegally parked", "illegal vehicle",
            "inoperable vehicle", "inoperative vehicle", "junk vehicle",
        ],
        "codes": ["05A"],
        "weight": 2,
    },
    {
        "name": "accessory/fence",
        "keywords": [
            "fence", "gate", "retaining wall", "accessory structure",
            "shed",
        ],
        "codes": ["X78"],
        "weight": 1,
    },
    {
        "name": "drainage/stagnant water",
        "keywords": [
            "drainage", "stagnant water", "standing water", "ponding",
            "flooding",
        ],
        "codes": ["X47"],
        "weight": 2,
    },
    {
        "name": "dead/dangerous tree",
        "keywords": [
            "dead tree", "dangerous tree", "hazardous tree", "fallen tree",
        ],
        "codes": ["X90"],
        "weight": 1,
    },
    {
        "name": "infestation/vermin",
        "keywords": [
            "infestation", "rats", "rodent", "vermin", "roach", "bed bug",
            "pest",
        ],
        "codes": ["I17"],
        "weight": 2,
    },
    {
        "name": "sewage/plumbing/water",
        "keywords": [
            "sewage", "sewer", "plumbing", "water leak", "potable water",
            "no water",
        ],
        "codes": [],
        "weight": 2,
    },
    {
        "name": "electric",
        "keywords": ["electric", "electrical", "wiring", "no power"],
        "codes": [],
        "weight": 2,
    },
    {
        "name": "heating",
        "keywords": ["heating", "heat", "furnace", "no heat"],
        "codes": [],
        "weight": 2,
    },
    {
        "name": "public nuisance/hazard",
        "keywords": ["public nuisance", "hazard", "hazardous"],
        "codes": ["X48", "I18"],
        "weight": 2,
    },
]

# Occupancy statuses that are themselves a strong distress signal.
HIGH_SIGNAL_OCCUPANCY = {
    "VACANT", "VACANT STRUCTURE", "VACANT LOT",
    "ABANDONED", "ABANDONED STRUCTURE", "ABANDONED LOT",
    "CONDEMNED",
}
OCCUPANCY_THEME_NAME = "occupancy: vacant/abandoned/condemned"
OCCUPANCY_WEIGHT = 4

# Codes that on their own are explicitly low-signal (administrative,
# bookkeeping). They never *create* a lead but do not suppress one if a
# property also has high-signal items.
LOW_SIGNAL_CODES = {"R01", "X69"}

# Minimum distress score for a property to be considered a lead by default.
MIN_DEFAULT_SCORE = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")


def normalize_address(addr: str | None) -> str:
    """Normalize an address for grouping: uppercase, collapse whitespace,
    strip punctuation that varies between rows for the same property."""
    if not addr:
        return ""
    s = addr.upper()
    s = s.replace(",", " ")
    s = s.replace(".", " ")
    s = _WS_RE.sub(" ", s).strip()
    return s


def _code_prefix_match(code: str, prefixes: Iterable[str]) -> bool:
    code = (code or "").strip().upper()
    if not code:
        return False
    for p in prefixes:
        if code.startswith(p.upper()):
            return True
    return False


def _theme_hits_for_row(code: str, description: str) -> list[str]:
    """Return the theme names this row contributes to."""
    desc_lower = (description or "").lower()
    hits: list[str] = []
    for theme in THEMES:
        if _code_prefix_match(code, theme["codes"]):
            hits.append(theme["name"])
            continue
        for kw in theme["keywords"]:
            if kw in desc_lower:
                hits.append(theme["name"])
                break
    return hits


def _theme_weight(name: str) -> int:
    for theme in THEMES:
        if theme["name"] == name:
            return int(theme["weight"])
    return 0


def grouping_key(parcel_id: str | None, full_addr: str | None,
                 partial_addr: str | None) -> str:
    """Pick the best stable grouping key for a property.

    Prefer normalized FullAddress + PARCEL_ID. Fall back to whichever piece
    is available. Two rows with the same key collapse to one lead.
    """
    addr = normalize_address(full_addr or partial_addr)
    parcel = (parcel_id or "").strip().upper()
    if addr and parcel:
        return f"{addr}::{parcel}"
    if parcel:
        return f"PARCEL::{parcel}"
    if addr:
        return f"ADDR::{addr}"
    return ""


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _empty_group(key: str) -> dict[str, Any]:
    return {
        "key": key,
        "rows": [],
        "themes": OrderedDict(),     # theme_name -> True (preserves first-seen order)
        "case_ids": OrderedDict(),
        "violation_codes": OrderedDict(),
        "statuses": OrderedDict(),
        "occupancy": OrderedDict(),
        "citation_total": 0.0,
        "citation_seen": False,
        "latest_date": None,         # YYYY-MM-DD
        "earliest_date": None,
        "parcel": "",
        "address_full": "",
        "address_partial": "",
        "row_count": 0,
        "low_signal_only": True,     # cleared once a high-signal row is added
    }


def _ingest_row(group: dict[str, Any], row: dict[str, Any]) -> None:
    """Fold a single transformed Louisville row into its group."""
    code = (row.get("violation_code") or "").strip()
    description = row.get("description") or ""
    occupancy = (row.get("occupancy") or "").strip().upper()
    status = (row.get("status") or "").strip()
    case_id = (row.get("alt_id") or "").strip()
    parcel = (row.get("parcel") or "").strip()
    full_addr = (row.get("full_address") or "").strip()
    partial_addr = (row.get("partial_address") or "").strip()
    citation = row.get("citation_amount")
    compl_date = row.get("compl_date")  # YYYY-MM-DD or None

    group["row_count"] += 1
    group["rows"].append(row)
    if case_id:
        group["case_ids"][case_id] = True
    if code:
        group["violation_codes"][code.upper()] = True
    if status:
        group["statuses"][status] = True
    if occupancy:
        group["occupancy"][occupancy] = True
    if not group["parcel"] and parcel:
        group["parcel"] = parcel
    if not group["address_full"] and full_addr:
        group["address_full"] = full_addr
    if not group["address_partial"] and partial_addr:
        group["address_partial"] = partial_addr

    if citation not in (None, "", 0, 0.0):
        try:
            group["citation_total"] += float(citation)
            group["citation_seen"] = True
        except (TypeError, ValueError):
            pass

    if compl_date:
        if not group["latest_date"] or compl_date > group["latest_date"]:
            group["latest_date"] = compl_date
        if not group["earliest_date"] or compl_date < group["earliest_date"]:
            group["earliest_date"] = compl_date

    # Theme hits from description + code
    for theme_name in _theme_hits_for_row(code, description):
        group["themes"][theme_name] = True
    if occupancy in HIGH_SIGNAL_OCCUPANCY:
        group["themes"][OCCUPANCY_THEME_NAME] = True

    # Low-signal tracking: a row is low-signal if its code is in
    # LOW_SIGNAL_CODES and it contributed no themes (no description hits).
    is_low_signal_row = False
    if code.upper() in LOW_SIGNAL_CODES and not _theme_hits_for_row(code, description):
        if occupancy not in HIGH_SIGNAL_OCCUPANCY:
            is_low_signal_row = True
    if not is_low_signal_row:
        group["low_signal_only"] = False


def _score_group(group: dict[str, Any]) -> int:
    score = 0
    for theme_name in group["themes"]:
        if theme_name == OCCUPANCY_THEME_NAME:
            score += OCCUPANCY_WEIGHT
        else:
            score += _theme_weight(theme_name)
    # Multiple distinct violation codes on the same property compound distress.
    distinct_codes = len(group["violation_codes"])
    if distinct_codes >= 3:
        score += 2
    elif distinct_codes == 2:
        score += 1
    # Citation issued (any non-zero amount) is a real-money signal.
    if group["citation_seen"]:
        score += 1
    return score


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def group_and_score_rows(
    rows: Iterable[dict[str, Any]],
    *,
    include_low_signal: bool = False,
    min_score: int = MIN_DEFAULT_SCORE,
) -> list[dict[str, Any]]:
    """Group violation-level rows by property and produce one lead per group.

    Each input row is a dict with the following keys (all optional except
    where noted; see Louisville scraper's `_extract_row` for the producer):
        alt_id, full_address, partial_address, parcel,
        compl_date (YYYY-MM-DD or None), status, status_date,
        description, violation_code, citation_amount, occupancy

    Returns a list of lead dicts shaped for the canonical CSV plus the
    structured extras used by upload_results' sidecar:
        {
            "Date": "YYYY-MM-DD",
            "Defendants/Parties": "...",
            "Property Address": "...",
            "PDF Link": "...",
            "Notes": "Distress score: ...; Reasons: ...; ...",
            "_instrument_number": "LOU_CODE::<key>::<latest_date>",
            "_filing_date_iso": "YYYY-MM-DD",
            "_distress_score": int,
            "_violation_row_count": int,
        }
    """
    groups: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    skipped_no_key = 0
    for row in rows:
        key = grouping_key(
            row.get("parcel"), row.get("full_address"), row.get("partial_address")
        )
        if not key:
            # No usable identity — drop. These are nearly always test/garbage rows.
            skipped_no_key += 1
            continue
        if key not in groups:
            groups[key] = _empty_group(key)
        _ingest_row(groups[key], row)

    leads: list[dict[str, Any]] = []
    for group in groups.values():
        score = _score_group(group)
        group["_score"] = score
        if not include_low_signal:
            if group["low_signal_only"]:
                continue
            if score < min_score:
                continue
        leads.append(_render_lead(group))
    return leads


def _render_lead(group: dict[str, Any]) -> dict[str, Any]:
    address = group["address_full"] or group["address_partial"] or "Address not found"
    statuses = list(group["statuses"].keys())
    case_ids = list(group["case_ids"].keys())
    codes = list(group["violation_codes"].keys())
    occ_list = list(group["occupancy"].keys())

    parties_bits = ["LMG Codes & Regulations"]
    if statuses:
        parties_bits.append("/".join(statuses))
    parties = " - ".join(parties_bits)

    reasons = list(group["themes"].keys())
    note_bits: list[str] = [f"Distress score: {group['_score']}"]
    if reasons:
        note_bits.append(f"Reasons: {', '.join(reasons)}")
    if occ_list:
        note_bits.append(f"Occupancy: {', '.join(occ_list)}")
    if statuses:
        note_bits.append(f"Statuses: {', '.join(statuses)}")
    if codes:
        note_bits.append(f"Violation codes: {', '.join(codes)}")
    if group["citation_seen"]:
        # Render whole dollars when the total has no fractional cents.
        amount = group["citation_total"]
        if abs(amount - round(amount)) < 0.005:
            note_bits.append(f"Citation amount: ${int(round(amount))}")
        else:
            note_bits.append(f"Citation amount: ${amount:.2f}")
    if group["row_count"]:
        note_bits.append(f"Violation rows: {group['row_count']}")
    if group["earliest_date"] and group["latest_date"]:
        if group["earliest_date"] == group["latest_date"]:
            note_bits.append(f"Date: {group['latest_date']}")
        else:
            note_bits.append(
                f"Dates: {group['earliest_date']} to {group['latest_date']}"
            )
    if case_ids:
        note_bits.append(f"Case IDs: {', '.join(case_ids)}")
    if group["parcel"]:
        note_bits.append(f"Parcel: {group['parcel']}")

    instrument_seed_key = (
        group["parcel"]
        or normalize_address(group["address_full"] or group["address_partial"])
        or group["key"]
    )
    latest = group["latest_date"] or ""
    instrument_number = f"LOU_CODE::{instrument_seed_key}::{latest}"

    return {
        "Date": group["latest_date"] or "",
        "Defendants/Parties": parties,
        "Property Address": address,
        # PDF Link is filled in by the caller (it knows SOURCE_URL).
        "PDF Link": "",
        "Notes": "; ".join(note_bits),
        "_instrument_number": instrument_number,
        "_filing_date_iso": group["latest_date"],
        "_distress_score": group["_score"],
        "_violation_row_count": group["row_count"],
    }
