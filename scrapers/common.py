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
