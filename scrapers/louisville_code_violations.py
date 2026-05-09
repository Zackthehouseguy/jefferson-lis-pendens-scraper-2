#!/usr/bin/env python3
"""
Louisville KY code violations scraper.

Queries the official Louisville Property Maintenance ArcGIS FeatureServer:
    https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/PM_SiteVisit_Violations/FeatureServer/0

Produces records compatible with the Lovable ingest endpoint, using:
  - instrument_number: B1_ALT_ID (the public alt-id of the inspection)
  - filing_date:       G6A_G6_COMPL_DD (compliance/inspection date) as YYYY-MM-DD
  - parties:           agency / status string ("LMG Codes & Regs - <STATUS>")
  - property_address:  FullAddress (falls back to PartialAddress)
  - pdf_link:          source dataset URL (no per-record PDF available)
  - notes:             violation code, citation amount, parcel id, description, status, etc.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

# Local imports work both as `python -m scrapers.louisville_code_violations`
# and as `python scrapers/louisville_code_violations.py`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scrapers.common import parse_date_input, write_canonical_csv  # noqa: E402
    from scrapers.code_violation_filter import (  # noqa: E402
        MIN_DEFAULT_SCORE,
        group_and_score_rows,
    )
else:
    from .common import parse_date_input, write_canonical_csv
    from .code_violation_filter import MIN_DEFAULT_SCORE, group_and_score_rows


SOURCE_NAME = "Louisville Metro Code Violations (PM_SiteVisit_Violations)"
SOURCE_URL = (
    "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/"
    "PM_SiteVisit_Violations/FeatureServer/0"
)
QUERY_URL = SOURCE_URL + "/query"
DATE_FIELD = "G6A_G6_COMPL_DD"

OUT_FIELDS = ",".join(
    [
        "B1_ALT_ID",
        "FullAddress",
        "PartialAddress",
        "PARCEL_ID",
        DATE_FIELD,
        "G6A_G6_STATUS",
        "G6A_G6_STATUS_DD",
        "GUIDE_ITEM_TEXT",
        "VIOLATION_CODE",
        "CitationAmount",
        "OccupancyStatus",
        "Longitude",
        "Latitude",
    ]
)


def _esri_timestamp(dt: datetime) -> str:
    """ESRI date literal: TIMESTAMP 'YYYY-MM-DD HH:MM:SS'."""
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def build_where_clause(start: datetime, end: datetime) -> str:
    """ArcGIS where clause for an inclusive [start, end] day range on DATE_FIELD.

    end is widened to 23:59:59 so the final day is included. Records with
    a NULL compliance date are excluded.
    """
    end_eod = end.replace(hour=23, minute=59, second=59, microsecond=0)
    return (
        f"{DATE_FIELD} >= {_esri_timestamp(start)} "
        f"AND {DATE_FIELD} <= {_esri_timestamp(end_eod)}"
    )


def epoch_ms_to_iso_date(value: Any) -> str | None:
    """ArcGIS Date attributes come back as epoch milliseconds (UTC). Return YYYY-MM-DD."""
    if value in (None, "", 0):
        # 0 is technically 1970-01-01; treat as missing for our purposes.
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return None


def _fetch_page(
    session: requests.Session,
    where: str,
    offset: int,
    page_size: int,
) -> dict:
    params = {
        "where": where,
        "outFields": OUT_FIELDS,
        "f": "json",
        "returnGeometry": "false",
        "orderByFields": f"{DATE_FIELD} ASC",
        "resultOffset": offset,
        "resultRecordCount": page_size,
    }
    resp = session.get(QUERY_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"ArcGIS error: {payload['error']}")
    return payload


def fetch_violations(
    start: datetime,
    end: datetime,
    page_size: int = 1000,
    sleep: float = 0.25,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch all violation features in [start, end] via paginated ArcGIS query."""
    session = session or requests.Session()
    where = build_where_clause(start, end)
    all_features: list[dict] = []
    offset = 0
    while True:
        payload = _fetch_page(session, where, offset, page_size)
        features = payload.get("features", [])
        if not features:
            break
        all_features.extend(features)
        if not payload.get("exceededTransferLimit") and len(features) < page_size:
            break
        offset += len(features)
        time.sleep(sleep)
    return all_features


def transform_feature(feature: dict) -> dict:
    """Map an ArcGIS feature into the canonical 5-column scraper schema.

    Also returns a few extra structured fields used by upload_results for
    the ingest payload (instrument_number, filing_date already-ISO).
    """
    attrs = feature.get("attributes", {}) or {}
    alt_id = (attrs.get("B1_ALT_ID") or "").strip()
    full_addr = (attrs.get("FullAddress") or "").strip()
    partial_addr = (attrs.get("PartialAddress") or "").strip()
    address = full_addr or partial_addr or "Address not found"

    compl_date = epoch_ms_to_iso_date(attrs.get(DATE_FIELD))
    status = (attrs.get("G6A_G6_STATUS") or "").strip()
    status_date = epoch_ms_to_iso_date(attrs.get("G6A_G6_STATUS_DD"))
    code = (attrs.get("VIOLATION_CODE") or "").strip()
    description = (attrs.get("GUIDE_ITEM_TEXT") or "").strip()
    citation = attrs.get("CitationAmount")
    parcel = (attrs.get("PARCEL_ID") or "").strip()
    occupancy = (attrs.get("OccupancyStatus") or "").strip()

    parties_bits = ["LMG Codes & Regulations"]
    if status:
        parties_bits.append(status)
    parties = " - ".join(parties_bits)

    note_bits: list[str] = []
    if code:
        note_bits.append(f"Violation code: {code}")
    if description:
        note_bits.append(f"Description: {description}")
    if status:
        note_bits.append(f"Status: {status}")
    if status_date:
        note_bits.append(f"Status date: {status_date}")
    if citation not in (None, "", 0, 0.0):
        note_bits.append(f"Citation amount: ${citation}")
    if parcel:
        note_bits.append(f"Parcel: {parcel}")
    if occupancy:
        note_bits.append(f"Occupancy: {occupancy}")
    note_bits.append(f"Source: {SOURCE_NAME}")

    return {
        "Date": compl_date or "",
        "Defendants/Parties": parties,
        "Property Address": address,
        "PDF Link": SOURCE_URL,
        "Notes": "; ".join(note_bits),
        # Extras used by upload_results for canonical record fields:
        "_instrument_number": alt_id,
        "_filing_date_iso": compl_date,
    }


def transform_features(features: Iterable[dict]) -> list[dict]:
    return [transform_feature(f) for f in features]


def _extract_row(feature: dict) -> dict:
    """Pull just the fields the grouping/scoring layer needs from one feature."""
    attrs = feature.get("attributes", {}) or {}
    return {
        "alt_id": (attrs.get("B1_ALT_ID") or "").strip(),
        "full_address": (attrs.get("FullAddress") or "").strip(),
        "partial_address": (attrs.get("PartialAddress") or "").strip(),
        "parcel": (attrs.get("PARCEL_ID") or "").strip(),
        "compl_date": epoch_ms_to_iso_date(attrs.get(DATE_FIELD)),
        "status": (attrs.get("G6A_G6_STATUS") or "").strip(),
        "status_date": epoch_ms_to_iso_date(attrs.get("G6A_G6_STATUS_DD")),
        "description": (attrs.get("GUIDE_ITEM_TEXT") or "").strip(),
        "violation_code": (attrs.get("VIOLATION_CODE") or "").strip(),
        "citation_amount": attrs.get("CitationAmount"),
        "occupancy": (attrs.get("OccupancyStatus") or "").strip(),
    }


def build_distressed_leads(
    features: Iterable[dict],
    *,
    include_low_signal: bool = False,
    include_closed: bool = False,
    min_score: int = MIN_DEFAULT_SCORE,
) -> list[dict]:
    """Group violation-level features into one lead per distressed property.

    Each returned dict is shaped for the canonical 5-column CSV plus the
    `_instrument_number` / `_filing_date_iso` extras the ingest sidecar
    expects. PDF Link is set to the public dataset URL since there is no
    per-property PDF. Output is sorted by priority/score desc, latest date desc.
    """
    rows = [_extract_row(f) for f in features]
    leads = group_and_score_rows(
        rows,
        include_low_signal=include_low_signal,
        include_closed=include_closed,
        min_score=min_score,
    )
    for lead in leads:
        lead["PDF Link"] = SOURCE_URL
        lead["Notes"] = lead["Notes"] + f" | Source: {SOURCE_NAME}"
    return leads


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Louisville Metro code violations from ArcGIS FeatureServer."
    )
    parser.add_argument("--start-date", required=True, help="MM/DD/YYYY or YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="MM/DD/YYYY or YYYY-MM-DD.")
    parser.add_argument("--output-dir", default=".", help="Directory for CSV + log.")
    parser.add_argument("--csv-name", default="louisville_code_violations_results.csv")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument(
        "--include-low-signal-code-violations",
        dest="include_low_signal",
        action="store_true",
        help="Include low-signal/administrative-only violations (rental "
             "registration, street-number-only, etc.). Default: high-signal only.",
    )
    parser.add_argument(
        "--include-closed-code-violations",
        dest="include_closed",
        action="store_true",
        help="Include leads whose statuses are all CLOSED. Default: open / "
             "active-enforcement leads only.",
    )
    parser.add_argument(
        "--min-distress-score",
        type=int,
        default=MIN_DEFAULT_SCORE,
        help=f"Minimum distress score for a lead (default {MIN_DEFAULT_SCORE}). "
             "Ignored when --include-low-signal-code-violations is set.",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Emit one row per violation (legacy/debug). Default: dedupe by property.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "action_log.txt"
    csv_path = output_dir / args.csv_name

    def log(level: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {level}: {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    log_path.write_text("", encoding="utf-8")

    try:
        start = parse_date_input(args.start_date)
        end = parse_date_input(args.end_date)
        if start > end:
            raise ValueError("start-date must be on or before end-date")

        log("ACTION", f"Querying {SOURCE_URL}")
        log(
            "ACTION",
            f"Date range {start.strftime('%Y-%m-%d')}..{end.strftime('%Y-%m-%d')} on {DATE_FIELD}",
        )
        features = fetch_violations(
            start, end, page_size=args.page_size, sleep=args.sleep
        )
        log("RESULT", f"Fetched {len(features)} features from ArcGIS")

        if args.no_dedupe:
            rows = transform_features(features)
            log("ACTION", "Dedupe disabled; emitting one row per violation.")
        else:
            rows = build_distressed_leads(
                features,
                include_low_signal=args.include_low_signal,
                include_closed=args.include_closed,
                min_score=args.min_distress_score,
            )
            log(
                "RESULT",
                f"Grouped {len(features)} violation rows into {len(rows)} "
                f"distressed-property leads (include_low_signal="
                f"{args.include_low_signal}, include_closed={args.include_closed}, "
                f"min_score={args.min_distress_score}).",
            )
        write_canonical_csv(rows, csv_path)
        log("RESULT", f"Wrote {len(rows)} rows to {csv_path}")

        # Drop a small JSON sidecar so ingest can keep the structured fields
        # (instrument_number, ISO filing_date) without re-parsing the CSV.
        sidecar = output_dir / "louisville_code_violations_records.json"
        sidecar.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        log("RESULT", f"Wrote structured sidecar: {sidecar}")
        return 0
    except Exception as exc:  # pragma: no cover - top-level guard
        log("ERROR", f"Fatal scraper error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
