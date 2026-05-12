"""Shared helpers for the property-signal scrapers."""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable


CANONICAL_COLUMNS = [
    "Date",
    "Defendants/Parties",
    "Property Address",
    "PDF Link",
    "Notes",
]


def parse_date_input(value: str) -> datetime:
    """Parse start/end dates given as MM/DD/YYYY or YYYY-MM-DD."""
    value = (value or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date must be MM/DD/YYYY or YYYY-MM-DD, got: {value!r}")


def write_canonical_csv(rows: Iterable[dict], output_csv: Path) -> int:
    """Write rows in the canonical 5-column shape consumed by upload_results.py."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in CANONICAL_COLUMNS})
            count += 1
    return count


# Louisville code-violation CSV column order. The first 8 columns are the
# user-facing scannable fields (Filing Date, Distress Score, Status, Property
# Address, Occupancy, Parties, PDF Link, Distress Signals), in this exact
# order. The remaining columns provide additional useful detail without
# dropping any data carried in the structured sidecar JSON.
LOUISVILLE_CSV_COLUMNS = [
    "Filing Date",
    "Distress Score",
    "Status",
    "Property Address",
    "Occupancy",
    "Parties",
    "PDF Link",
    "Distress Signals",
    "Priority",
    "Violation Codes",
    "Citation Total",
    "Violation Rows",
    "Case IDs",
    "Parcel",
    "Source Link",
    "Instrument Number",
    "Notes",
]


def lead_to_louisville_row(lead: dict) -> dict:
    """Project a structured Louisville lead dict onto the Louisville CSV columns.

    Tolerates both the grouped/scored leads from code_violation_filter and the
    legacy per-violation transform output (which only carries the canonical
    5-column fields plus _instrument_number / _filing_date_iso).
    """
    return {
        "Filing Date": lead.get("_filing_date_iso") or lead.get("Date", "") or "",
        "Distress Score": lead.get("_distress_score", ""),
        "Status": lead.get("_status", ""),
        "Property Address": lead.get("Property Address", ""),
        "Occupancy": lead.get("_occupancy", ""),
        "Parties": lead.get("Defendants/Parties", ""),
        "PDF Link": lead.get("PDF Link", ""),
        "Distress Signals": lead.get("_distress_signals", ""),
        "Priority": lead.get("_priority", ""),
        "Violation Codes": lead.get("_violation_codes", ""),
        "Citation Total": lead.get("_citation_total", ""),
        "Violation Rows": lead.get("_violation_row_count", ""),
        "Case IDs": lead.get("_case_ids", ""),
        "Parcel": lead.get("_parcel", ""),
        "Source Link": lead.get("_source_link", lead.get("PDF Link", "")),
        "Instrument Number": lead.get("_instrument_number", ""),
        "Notes": lead.get("Notes", ""),
    }


def write_louisville_csv(rows: Iterable[dict], output_csv: Path) -> int:
    """Write rows in the Louisville-specific column order.

    The required leading columns are, in this exact order:
      Filing Date, Distress Score, Status, Property Address, Occupancy,
      Parties, PDF Link, Distress Signals
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOUISVILLE_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            mapped = lead_to_louisville_row(row)
            writer.writerow(
                {col: mapped.get(col, "") for col in LOUISVILLE_CSV_COLUMNS}
            )
            count += 1
    return count


# Jefferson tax-delinquent CSV column order. Leading columns mirror what
# the operator specified for Lovable + Google Sheets readability. Filing
# Date is intentionally retained as the first column for parity with the
# other property-signal CSVs even though the source has no per-record
# filing date (it is always blank for this source).
TAX_DELINQUENT_CSV_COLUMNS = [
    "Filing Date",
    "Tax Year",
    "Amount Due",
    "Status",
    "Property Address",
    "Parcel ID",
    "Parties",
    "Source Link",
    "Notes",
]


def write_tax_delinquent_csv(rows: Iterable[dict], output_csv: Path) -> int:
    """Write rows in the Jefferson tax-delinquent column order.

    Required leading columns (in order):
      Filing Date, Tax Year, Amount Due, Status, Property Address,
      Parcel ID, Parties, Source Link, Notes
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TAX_DELINQUENT_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {col: row.get(col, "") for col in TAX_DELINQUENT_CSV_COLUMNS}
            )
            count += 1
    return count
