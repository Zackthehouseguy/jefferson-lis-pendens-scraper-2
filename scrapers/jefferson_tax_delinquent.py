#!/usr/bin/env python3
"""
Jefferson County KY tax-delinquent property scraper.

Official source: Jefferson County Clerk publishes the annual delinquent
property listing as PDFs at
https://www.jeffersoncountyclerk.org/jefferson-county-delinquent-property-listing/
Each PDF (Real Estate A-J, Real Estate K-Z, Personal Property) contains the
canonical, machine-readable table:

    ParcelID | Name (taxpayer/owner) | Property Address | Account Balance

We download those PDFs, parse them with PyMuPDF using position-based block
clustering (the documents have no embedded table structure but the columns
are pixel-aligned), and emit one canonical record per row.

Important constraints (per the operator):
  - The Clerk's delinquent listing carries NO per-row filing date, NO
    bill number, and NO tax_year field — those fields do not exist on
    the published source. We leave them blank and surface that fact via
    `notes` rather than fabricating values. The list-level "tax year"
    (e.g. "2024 listing published 2025-06-12") IS available from the
    PDF metadata and is recorded once per row in the `tax_year`/`Notes`
    columns.
  - The listing landing page warns that some bills may have been paid
    since the list was generated. We propagate that warning into Notes
    so downstream lead-creation can mark records as needing verification.

CCLIX / CCLIX+ and Jefferson Deeds are referenced from the Clerk's site as
the place to look up the underlying delinquent-tax DOCUMENT for individual
parcels (from June 2017 forward). They are NOT a bulk data source — they
require interactive lookup per parcel — so we keep them as a per-record
`document_lookup_url` pointer rather than scraping them in this run.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus

import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scrapers.common import (  # noqa: E402
        parse_date_input,
        write_tax_delinquent_csv,
    )
else:
    from .common import parse_date_input, write_tax_delinquent_csv


SOURCE_NAME = "Jefferson County Clerk — Delinquent Property Listing"
LISTING_PAGE = (
    "https://www.jeffersoncountyclerk.org/jefferson-county-delinquent-property-listing/"
)

# Known machine-readable PDF artifacts. These are the URLs the Clerk
# publishes on the listing page for the current cycle. They are
# explicitly enumerated rather than scraped from the landing page so the
# scraper fails loudly when the Clerk renames or rotates them, instead
# of silently uploading stale data.
KNOWN_PDFS: list[dict] = [
    {
        "url": "https://jeffersoncountyclerk.org/wp-content/uploads/2025/06/Real_Estate_2025_A-J.pdf",
        "kind": "real_estate",
        "tax_year": "2024",
        "list_published_date": "2025-06-01",
    },
    {
        "url": "https://jeffersoncountyclerk.org/wp-content/uploads/2025/06/Real_Estate_2025_K-Z.pdf",
        "kind": "real_estate",
        "tax_year": "2024",
        "list_published_date": "2025-06-01",
    },
    {
        "url": "https://jeffersoncountyclerk.org/wp-content/uploads/2025/06/Personal_Property_2025.pdf",
        "kind": "personal_property",
        "tax_year": "2024",
        "list_published_date": "2025-06-01",
    },
]

# CCLIX+ document lookup URL template. CCLIX+ is the Clerk's online land
# records app; the path below opens a search for documents linked to a
# specific parcel and is suitable as a per-record "go look up the
# delinquent tax document" pointer. CCLIX+ requires interactive search;
# we surface this URL so Lovable users can click through to the source
# document on a case-by-case basis.
CCLIX_PARCEL_LOOKUP = "https://cclix.us/?parcel={parcel}"

# Y-axis tolerance (in PDF points) for clustering spans into a single row.
ROW_Y_TOLERANCE = 5.0

# Heuristic: a Jefferson Co parcel id leads each delinquent-tax row. Real
# estate parcels are 14 alphanumeric chars (digit lead); personal property
# parcels are 7-9 digit account numbers. We accept either so the same
# parser handles both PDFs and we still reject header rows like "ParcelID".
PARCEL_RE = re.compile(r"^[0-9](?:[0-9A-Z]{13}|[0-9]{6,8})$")

# Account balance looks like "$ 1,234.56" — sometimes the dollar sign and
# the number are emitted as two separate spans by PyMuPDF.
AMOUNT_RE = re.compile(r"^[\d,]+\.\d{2}$")


@dataclass
class TaxDelinquentRecord:
    """One row from the published delinquent property listing."""

    parcel_id: str
    taxpayer_name: str
    property_address: str
    amount_due: str  # human string e.g. "$1,234.56"; never fabricated
    amount_due_value: float | None  # parsed numeric, None if parse failed
    tax_year: str
    list_published_date: str
    source_pdf_url: str
    document_lookup_url: str
    kind: str  # real_estate | personal_property
    page_number: int

    def to_canonical_row(self) -> dict:
        """Return the dict consumed by write_tax_delinquent_csv.

        Field rules (NEVER FABRICATE):
          - filing_date is intentionally left blank because the source
            does not carry one. We DO carry list_published_date as a
            separate column.
          - status is always "Delinquent" because that is exactly what
            the source asserts about every record on the listing.
          - parties = the taxpayer/owner name from the listing.
          - source_record_id = stable composite of parcel + tax_year so
            the same parcel on different cycles dedupes correctly.
        """
        source_record_id = f"{self.parcel_id}-{self.tax_year}"
        notes_bits: list[str] = []
        notes_bits.append(f"Source: {SOURCE_NAME}")
        notes_bits.append(f"List type: {self.kind}")
        notes_bits.append(f"Tax year: {self.tax_year}")
        notes_bits.append(f"List published: {self.list_published_date}")
        notes_bits.append(
            "Verification: some bills on the published list may have been "
            "paid since publication — verify status before contact."
        )
        if not self.property_address:
            notes_bits.append(
                "Missing source field: property_address (not present on PDF row)."
            )
        if self.amount_due_value is None and not self.amount_due:
            notes_bits.append(
                "Missing source field: amount_due (could not parse from PDF row)."
            )

        return {
            "Filing Date": "",  # Source has no per-record filing date
            "Tax Year": self.tax_year,
            "Amount Due": self.amount_due,
            "Status": "Delinquent",
            "Property Address": self.property_address or "Address not found",
            "Parcel ID": self.parcel_id,
            "Parties": self.taxpayer_name,
            "Source Link": self.source_pdf_url,
            "Notes": " | ".join(notes_bits),
            # Extras consumed by upload_results sidecar path
            "_instrument_number": source_record_id,
            "_source_record_id": source_record_id,
            "_filing_date_iso": None,
            "_list_published_date": self.list_published_date,
            "_amount_due_value": self.amount_due_value,
            "_bill_number": "",  # Not present on source
            "_document_url": self.document_lookup_url,
            "_kind": self.kind,
            "_source_pdf_url": self.source_pdf_url,
        }


def _download_pdf(url: str, dest: Path, session: requests.Session | None = None) -> Path:
    session = session or requests.Session()
    resp = session.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                fh.write(chunk)
    return dest


def _page_rows(page) -> list[list[tuple[float, str]]]:
    """Cluster text spans on one page into rows sorted by x-position.

    PyMuPDF (`fitz`) gives us per-span coordinates. We group spans whose
    y-positions are within ROW_Y_TOLERANCE of each other into the same
    row, then sort each row's spans left-to-right. This is robust to the
    Clerk's PDF emitting the dollar sign as a separate span from the
    numeric amount.
    """
    blocks = page.get_text("dict")["blocks"]
    spans: list[tuple[float, float, str]] = []
    for b in blocks:
        if b.get("type") != 0:
            continue
        for line in b["lines"]:
            for s in line["spans"]:
                txt = (s.get("text") or "").strip()
                if not txt:
                    continue
                spans.append((s["bbox"][1], s["bbox"][0], txt))
    spans.sort()
    rows: list[list[tuple[float, str]]] = []
    current: list[tuple[float, str]] = []
    last_y: float | None = None
    for y, x, txt in spans:
        if last_y is None or (y - last_y) > ROW_Y_TOLERANCE:
            if current:
                current.sort()
                rows.append(current)
            current = []
        current.append((x, txt))
        last_y = y
    if current:
        current.sort()
        rows.append(current)
    return rows


def _normalize_amount(parts: list[str]) -> tuple[str, float | None]:
    """Stitch the "$" / number spans back into a normalized amount string.

    Returns (human_string, numeric_value_or_none). Never returns a
    fabricated value — if no numeric component is present, returns
    ("", None).
    """
    nums = [p for p in parts if AMOUNT_RE.match(p)]
    if not nums:
        return "", None
    number = nums[-1]  # last numeric span on the row IS the balance
    try:
        value = float(number.replace(",", ""))
    except ValueError:
        return f"${number}", None
    return f"${number}", value


def _parse_row(
    spans: list[tuple[float, str]],
) -> tuple[str, str, str, str, float | None] | None:
    """Turn one clustered row into (parcel, name, address, amount_str, amount_value)."""
    texts = [t for _x, t in spans]
    if not texts:
        return None
    parcel = texts[0].strip()
    if not PARCEL_RE.match(parcel):
        return None
    # Identify the amount fragments at the right side of the row.
    amount_fragments: list[str] = []
    body: list[str] = []
    for t in texts[1:]:
        if AMOUNT_RE.match(t) or t.strip() == "$" or t.strip().startswith("$"):
            amount_fragments.append(t.strip())
        else:
            body.append(t.strip())
    amount_str, amount_value = _normalize_amount(amount_fragments)
    if not body:
        return parcel, "", "", amount_str, amount_value
    # Name is the first body span; remaining body spans form the address.
    name = body[0]
    address = " ".join(body[1:]).strip()
    return parcel, name, address, amount_str, amount_value


def parse_pdf(
    pdf_path: Path,
    *,
    tax_year: str,
    list_published_date: str,
    source_pdf_url: str,
    kind: str,
) -> list[TaxDelinquentRecord]:
    """Parse a downloaded delinquent-listing PDF into structured records."""
    import fitz  # PyMuPDF; imported lazily so tests that stub parsing can skip it.

    records: list[TaxDelinquentRecord] = []
    with fitz.open(pdf_path) as doc:
        for page_index in range(doc.page_count):
            page = doc[page_index]
            for row_spans in _page_rows(page):
                parsed = _parse_row(row_spans)
                if not parsed:
                    continue
                parcel, name, address, amount_str, amount_value = parsed
                if name.upper() in {"NAME"} or parcel.upper() in {"PARCELID"}:
                    continue  # header row
                doc_url = CCLIX_PARCEL_LOOKUP.format(parcel=quote_plus(parcel))
                records.append(
                    TaxDelinquentRecord(
                        parcel_id=parcel,
                        taxpayer_name=name,
                        property_address=address,
                        amount_due=amount_str,
                        amount_due_value=amount_value,
                        tax_year=tax_year,
                        list_published_date=list_published_date,
                        source_pdf_url=source_pdf_url,
                        document_lookup_url=doc_url,
                        kind=kind,
                        page_number=page_index + 1,
                    )
                )
    return records


def _date_in_range(
    date_str: str | None, start: datetime | None, end: datetime | None
) -> bool:
    if not date_str:
        return True
    if start is None and end is None:
        return True
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return True
    if start and dt < start:
        return False
    if end and dt > end.replace(hour=23, minute=59, second=59):
        return False
    return True


def fetch_records(
    *,
    start: datetime | None,
    end: datetime | None,
    cache_dir: Path,
    session: requests.Session | None = None,
    pdfs: Iterable[dict] = KNOWN_PDFS,
    log=None,
) -> list[TaxDelinquentRecord]:
    """Download + parse all delinquent listing PDFs covering the date range.

    A PDF is "in range" when its list_published_date falls within
    [start, end]. The Clerk publishes annually; if the requested range
    doesn't cover ANY known publication, this returns []. That is the
    correct behaviour — there are no per-day delinquent rows.
    """
    session = session or requests.Session()
    out: list[TaxDelinquentRecord] = []
    selected = [p for p in pdfs if _date_in_range(p.get("list_published_date"), start, end)]
    if log:
        log(
            "ACTION",
            f"{len(selected)} of {len(list(pdfs))} known delinquent PDFs fall in "
            f"requested date range.",
        )
    for spec in selected:
        url = spec["url"]
        fname = url.rsplit("/", 1)[-1]
        dest = cache_dir / fname
        if log:
            log("ACTION", f"Downloading {url}")
        _download_pdf(url, dest, session=session)
        if log:
            log("ACTION", f"Parsing {dest.name}")
        rows = parse_pdf(
            dest,
            tax_year=spec["tax_year"],
            list_published_date=spec["list_published_date"],
            source_pdf_url=url,
            kind=spec["kind"],
        )
        if log:
            log("RESULT", f"Parsed {len(rows)} rows from {dest.name}")
        out.extend(rows)
    return out


def records_to_rows(records: Iterable[TaxDelinquentRecord]) -> list[dict]:
    return [r.to_canonical_row() for r in records]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Jefferson County KY tax-delinquent property listing."
    )
    parser.add_argument("--start-date", required=True, help="MM/DD/YYYY or YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="MM/DD/YYYY or YYYY-MM-DD.")
    parser.add_argument("--output-dir", default=".", help="Directory for CSV + log.")
    parser.add_argument(
        "--csv-name", default="jefferson_tax_delinquent_results.csv"
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Optional directory for downloaded PDFs. Defaults to <output-dir>/pdf_cache.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if zero records are produced (rather than writing "
             "an empty CSV).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "action_log.txt"
    csv_path = output_dir / args.csv_name
    cache_dir = Path(args.cache_dir).resolve() if args.cache_dir else output_dir / "pdf_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    def log(level: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {level}: {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    if not log_path.exists():
        log_path.write_text("", encoding="utf-8")

    try:
        start = parse_date_input(args.start_date)
        end = parse_date_input(args.end_date)
        if start > end:
            raise ValueError("start-date must be on or before end-date")

        log("ACTION", f"Source landing page: {LISTING_PAGE}")
        log(
            "ACTION",
            f"Requested range {start.strftime('%Y-%m-%d')}..{end.strftime('%Y-%m-%d')}",
        )

        records = fetch_records(
            start=start,
            end=end,
            cache_dir=cache_dir,
            log=log,
        )
        log("RESULT", f"Total parsed: {len(records)} delinquent rows")

        if not records:
            log(
                "WARNING",
                "No delinquent tax records produced. This is correct only if "
                "the requested date range falls outside the JCC publication "
                "schedule (annual). If you expected records, verify the date "
                "range and the KNOWN_PDFS list in jefferson_tax_delinquent.py.",
            )
            if args.strict:
                return 2

        rows = records_to_rows(records)
        # Validate no fabricated fields slipped through. Each row should
        # have a non-empty parcel id; addresses/amounts may be blank.
        invalid = [r for r in rows if not r.get("Parcel ID")]
        if invalid:
            raise RuntimeError(
                f"{len(invalid)} parsed rows have no parcel id — refusing to "
                "upload guessed data. Inspect pdf_cache/ and adjust parser."
            )

        write_tax_delinquent_csv(rows, csv_path)
        log("RESULT", f"Wrote {len(rows)} rows to {csv_path}")

        sidecar = output_dir / "jefferson_tax_delinquent_records.json"
        sidecar.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
        log("RESULT", f"Wrote structured sidecar: {sidecar}")
        return 0
    except Exception as exc:  # pragma: no cover - top-level guard
        log("ERROR", f"Fatal scraper error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
