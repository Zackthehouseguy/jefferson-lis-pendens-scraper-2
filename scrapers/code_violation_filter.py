"""
Lead-quality filtering, scoring, dedupe, and lead-brief rendering for
code-violation sources (currently Louisville Metro PM_SiteVisit_Violations).

Code-violation feeds are violation-level, so the same property typically
appears in many rows for a single distressed condition. We want one
distressed-property *lead* per property, with a concise human-readable
brief and a distress score that explains why the property is flagged.

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
# Status taxonomy — open vs closed and priority weighting
# ---------------------------------------------------------------------------

# Statuses that mean the case is resolved / no longer actionable. Cases that
# are *only* in these statuses are excluded by default.
CLOSED_STATUSES = {"CLOSED"}

# Statuses that signal active enforcement escalation — strongest active leads.
HIGH_STATUSES = {"EMERGENCY REFERRAL", "CITATION REFERRAL", "CITATION"}

# Statuses that indicate an open case still in inspection / notice phase.
MEDIUM_STATUSES = {"VIOLATION NOTICE", "HOLD", "OPEN"}

# Per-status weight added to the distress score when present on the group.
STATUS_WEIGHTS = {
    "EMERGENCY REFERRAL": 5,
    "CITATION REFERRAL": 4,
    "CITATION": 3,
    "VIOLATION NOTICE": 1,
    "HOLD": 1,
    "OPEN": 0,
    "CLOSED": 0,
}


def _normalize_status(status: str | None) -> str:
    return (status or "").strip().upper()


def is_closed_status(status: str | None) -> bool:
    return _normalize_status(status) in CLOSED_STATUSES


def is_open_status(status: str | None) -> bool:
    s = _normalize_status(status)
    return bool(s) and s not in CLOSED_STATUSES


def status_priority_weight(status: str | None) -> int:
    return STATUS_WEIGHTS.get(_normalize_status(status), 0)


# ---------------------------------------------------------------------------
# Signal taxonomy — distress themes
# ---------------------------------------------------------------------------

# Each theme contributes to scoring and to the human-readable "Distress
# signals" string in Notes. Order here is the order they appear in Notes.
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
        "codes": ["I33"],
        "weight": 3,
    },
    {
        "name": "exterior/foundation",
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
        "name": "trash/weeds",
        "keywords": [
            "cleaning", "rubbish", "garbage", "weeds", "plant growth", "litter",
            "junk", "debris", "trash", "overgrown", "overgrowth",
        ],
        "codes": ["02A"],
        "weight": 2,
    },
    {
        "name": "graffiti",
        "keywords": ["graffiti", "defacement", "defaced"],
        "codes": ["080A"],
        "weight": 1,
    },
    {
        "name": "abandoned vehicle",
        "keywords": [
            "abandoned vehicle", "illegally parked", "illegal vehicle",
            "inoperable vehicle", "inoperative vehicle", "junk vehicle",
        ],
        "codes": ["05A"],
        "weight": 2,
    },
    {
        "name": "fence/accessory",
        "keywords": [
            "fence", "gate", "retaining wall", "accessory structure",
            "shed",
        ],
        "codes": ["X78"],
        "weight": 1,
    },
    {
        "name": "drainage",
        "keywords": [
            "drainage", "stagnant water", "standing water", "ponding",
            "flooding",
        ],
        "codes": ["X47"],
        "weight": 2,
    },
    {
        "name": "dangerous tree",
        "keywords": [
            "dead tree", "dangerous tree", "hazardous tree", "fallen tree",
        ],
        "codes": ["X90"],
        "weight": 1,
    },
    {
        "name": "infestation",
        "keywords": [
            "infestation", "rats", "rodent", "vermin", "roach", "bed bug",
            "pest",
        ],
        "codes": ["I17"],
        "weight": 2,
    },
    {
        "name": "sewage/plumbing",
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
        "name": "public hazard",
        "keywords": ["public nuisance", "hazard", "hazardous"],
        "codes": ["X48", "I18"],
        "weight": 2,
    },
]

# Strong-distress themes — at least one must be present (or the property
# must have a citation/referral status) for the property to qualify.
STRONG_THEMES = {
    "vacant/abandoned",
    "structural/foundation",
    "exterior/foundation",
    "roof/gutters",
    "porch/stairs",
    "demolition",
    "drainage",
    "infestation",
    "sewage/plumbing",
    "electric",
    "heating",
    "public hazard",
    "trash/weeds",
    "abandoned vehicle",
}

# Short labels shown next to each violation code in Notes (avoids dumping
# raw GUIDE_ITEM_TEXT). First matching prefix wins.
CODE_LABELS: list[tuple[str, str]] = [
    ("02A", "Cleaning"),
    ("05A", "Abandoned Vehicle"),
    ("080A", "Graffiti"),
    ("I17", "Infestation"),
    ("I18", "Public Hazard"),
    ("I33", "Structural"),
    ("X19", "Exterior/Foundation"),
    ("X40", "Porch/Stairs"),
    ("X47", "Drainage"),
    ("X48", "Public Hazard"),
    ("X50", "Roof/Gutters"),
    ("X69", "Address Numbers"),
    ("X78", "Fence/Accessory"),
    ("X90", "Tree"),
    ("X94", "Demolition"),
    ("R01", "Rental Reg."),
]

HIGH_SIGNAL_OCCUPANCY = {
    "VACANT", "VACANT STRUCTURE", "VACANT LOT",
    "ABANDONED", "ABANDONED STRUCTURE", "ABANDONED LOT",
    "CONDEMNED",
}
OCCUPANCY_THEME_NAME = "vacant/abandoned"
OCCUPANCY_WEIGHT = 4

# Codes that on their own are administrative and never *create* a lead.
LOW_SIGNAL_CODES = {"R01", "X69"}

# Default minimum distress score for a property to qualify as a lead.
# Tuned upward (was 3) so we surface meaningful leads, not single-violation
# paperwork. Combined with the open-status + strong-theme requirement below.
MIN_DEFAULT_SCORE = 5

# Truncation limits for the rendered lead brief.
MAX_VIOLATIONS_IN_NOTE = 6
MAX_CASE_IDS_IN_NOTE = 4

# Priority bands derived from score + status + occupancy.
PRIORITY_HIGH = "HIGH"
PRIORITY_MEDIUM = "MEDIUM"
PRIORITY_LOW = "LOW"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")


def normalize_address(addr: str | None) -> str:
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


def _label_for_code(code: str) -> str:
    code = (code or "").strip().upper()
    for prefix, label in CODE_LABELS:
        if code.startswith(prefix.upper()):
            return f"{code} {label}"
    return code


def grouping_key(parcel_id: str | None, full_addr: str | None,
                 partial_addr: str | None) -> str:
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
        "themes": OrderedDict(),
        "case_ids": OrderedDict(),
        "violation_codes": OrderedDict(),
        "statuses": OrderedDict(),
        "occupancy": OrderedDict(),
        "citation_total": 0.0,
        "citation_seen": False,
        "latest_date": None,
        "earliest_date": None,
        "parcel": "",
        "address_full": "",
        "address_partial": "",
        "row_count": 0,
        "low_signal_only": True,
        "has_open_status": False,
        "has_closed_status": False,
        "has_high_status": False,
    }


def _ingest_row(group: dict[str, Any], row: dict[str, Any]) -> None:
    code = (row.get("violation_code") or "").strip()
    description = row.get("description") or ""
    occupancy = (row.get("occupancy") or "").strip().upper()
    status = (row.get("status") or "").strip()
    case_id = (row.get("alt_id") or "").strip()
    parcel = (row.get("parcel") or "").strip()
    full_addr = (row.get("full_address") or "").strip()
    partial_addr = (row.get("partial_address") or "").strip()
    citation = row.get("citation_amount")
    compl_date = row.get("compl_date")

    group["row_count"] += 1
    group["rows"].append(row)
    if case_id:
        group["case_ids"][case_id] = True
    if code:
        group["violation_codes"][code.upper()] = True
    if status:
        group["statuses"][status] = True
        norm = _normalize_status(status)
        if norm in CLOSED_STATUSES:
            group["has_closed_status"] = True
        else:
            group["has_open_status"] = True
            if norm in HIGH_STATUSES:
                group["has_high_status"] = True
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

    for theme_name in _theme_hits_for_row(code, description):
        group["themes"][theme_name] = True
    if occupancy in HIGH_SIGNAL_OCCUPANCY:
        group["themes"][OCCUPANCY_THEME_NAME] = True

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
    distinct_codes = len(group["violation_codes"])
    if distinct_codes >= 3:
        score += 2
    elif distinct_codes == 2:
        score += 1
    if group["citation_seen"]:
        score += 1
    best_status_weight = 0
    for s in group["statuses"]:
        best_status_weight = max(best_status_weight, status_priority_weight(s))
    score += best_status_weight
    return score


def _group_priority(group: dict[str, Any], score: int) -> str:
    has_strong_theme = any(t in STRONG_THEMES for t in group["themes"])
    has_vacant_occupancy = any(
        occ in HIGH_SIGNAL_OCCUPANCY for occ in group["occupancy"]
    )
    if group["has_high_status"] and (has_strong_theme or has_vacant_occupancy):
        return PRIORITY_HIGH
    if score >= 10:
        return PRIORITY_HIGH
    if score >= 6:
        return PRIORITY_MEDIUM
    return PRIORITY_LOW


_PRIORITY_RANK = {PRIORITY_HIGH: 3, PRIORITY_MEDIUM: 2, PRIORITY_LOW: 1}


def _group_qualifies(
    group: dict[str, Any],
    score: int,
    *,
    include_low_signal: bool,
    include_closed: bool,
    min_score: int,
) -> bool:
    if include_low_signal:
        return True

    if group["low_signal_only"]:
        return False
    if not include_closed and not group["has_open_status"]:
        return False

    has_strong_theme = any(t in STRONG_THEMES for t in group["themes"])
    has_any_theme = bool(group["themes"])
    has_high_status = group["has_high_status"]

    # Two-part rule for the strong-theme gate:
    #   (a) citation/referral status + any distress theme, OR
    #   (b) open status + strong distress theme.
    # When include_closed is on, also accept closed-only groups that have a
    # strong distress theme — the user has explicitly opted in to historical
    # cases.
    citation_or_high_with_theme = has_high_status and has_any_theme
    open_with_strong_theme = group["has_open_status"] and has_strong_theme
    closed_with_strong_theme = (
        include_closed and not group["has_open_status"] and has_strong_theme
    )

    if not (citation_or_high_with_theme or open_with_strong_theme
            or closed_with_strong_theme):
        return False
    if score < min_score:
        return False
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def group_and_score_rows(
    rows: Iterable[dict[str, Any]],
    *,
    include_low_signal: bool = False,
    include_closed: bool = False,
    min_score: int = MIN_DEFAULT_SCORE,
) -> list[dict[str, Any]]:
    """Group violation-level rows by property and produce one lead per group.

    Output is sorted by (priority desc, score desc, latest_date desc).
    """
    groups: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
    for row in rows:
        key = grouping_key(
            row.get("parcel"), row.get("full_address"), row.get("partial_address")
        )
        if not key:
            continue
        if key not in groups:
            groups[key] = _empty_group(key)
        _ingest_row(groups[key], row)

    qualified: list[dict[str, Any]] = []
    for group in groups.values():
        score = _score_group(group)
        group["_score"] = score
        group["_priority"] = _group_priority(group, score)
        if not _group_qualifies(
            group,
            score,
            include_low_signal=include_low_signal,
            include_closed=include_closed,
            min_score=min_score,
        ):
            continue
        qualified.append(group)

    qualified.sort(
        key=lambda g: (
            _PRIORITY_RANK.get(g["_priority"], 0),
            g["_score"],
            g["latest_date"] or "",
        ),
        reverse=True,
    )
    return [_render_lead(g) for g in qualified]


def _format_citation_amount(amount: float) -> str:
    if abs(amount - round(amount)) < 0.005:
        return f"${int(round(amount))}"
    return f"${amount:.2f}"


def _truncate_list(items: list[str], limit: int) -> str:
    if len(items) <= limit:
        return ", ".join(items)
    head = ", ".join(items[:limit])
    return f"{head} +{len(items) - limit} more"


def _render_lead(group: dict[str, Any]) -> dict[str, Any]:
    address = group["address_full"] or group["address_partial"] or "Address not found"
    statuses = list(group["statuses"].keys())
    case_ids = list(group["case_ids"].keys())
    codes = list(group["violation_codes"].keys())
    occ_list = list(group["occupancy"].keys())
    themes = list(group["themes"].keys())
    priority = group["_priority"]
    score = group["_score"]

    headline_status = ""
    for status in statuses:
        if _normalize_status(status) in HIGH_STATUSES:
            headline_status = status
            break
    if not headline_status and statuses:
        headline_status = statuses[0]
    parties_bits = ["LMG Codes & Regulations"]
    if headline_status:
        parties_bits.append(headline_status)
    parties = " - ".join(parties_bits)

    note_bits: list[str] = [f"Priority: {priority}", f"Distress score: {score}"]
    if statuses:
        note_bits.append(f"Status: {'; '.join(statuses)}")
    if occ_list:
        note_bits.append(f"Occupancy: {', '.join(occ_list)}")
    if themes:
        note_bits.append(f"Distress signals: {'; '.join(themes)}")
    if codes:
        labels = [_label_for_code(c) for c in codes]
        note_bits.append(f"Violations: {_truncate_list(labels, MAX_VIOLATIONS_IN_NOTE)}")
    if group["citation_seen"]:
        note_bits.append(
            f"Citation amount: {_format_citation_amount(group['citation_total'])}"
        )
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
        note_bits.append(f"Case IDs: {_truncate_list(case_ids, MAX_CASE_IDS_IN_NOTE)}")
    if group["parcel"]:
        note_bits.append(f"Parcel: {group['parcel']}")

    instrument_seed_key = (
        group["parcel"]
        or normalize_address(group["address_full"] or group["address_partial"])
        or group["key"]
    )
    latest = group["latest_date"] or ""
    instrument_number = f"LOU_CODE::{instrument_seed_key}::{latest}"

    citation_str = (
        _format_citation_amount(group["citation_total"])
        if group["citation_seen"]
        else ""
    )
    status_str = "; ".join(statuses)
    occupancy_str = ", ".join(occ_list)
    distress_signals_str = "; ".join(themes)
    violations_str = (
        _truncate_list([_label_for_code(c) for c in codes], MAX_VIOLATIONS_IN_NOTE)
        if codes
        else ""
    )
    case_ids_str = (
        _truncate_list(case_ids, MAX_CASE_IDS_IN_NOTE) if case_ids else ""
    )

    return {
        # Canonical 5-column shape (kept for back-compat with Jefferson-shape consumers).
        "Date": group["latest_date"] or "",
        "Defendants/Parties": parties,
        "Property Address": address,
        "PDF Link": "",
        "Notes": " | ".join(note_bits),
        # Structured extras for the Louisville-specific CSV + JSON sidecar.
        "_instrument_number": instrument_number,
        "_filing_date_iso": group["latest_date"],
        "_distress_score": score,
        "_priority": priority,
        "_violation_row_count": group["row_count"],
        "_status": status_str,
        "_occupancy": occupancy_str,
        "_distress_signals": distress_signals_str,
        "_violation_codes": violations_str,
        "_citation_total": citation_str,
        "_case_ids": case_ids_str,
        "_parcel": group["parcel"],
    }
