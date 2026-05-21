#!/usr/bin/env python3
"""
Louisville Metro Landbank inventory scraper.

Official source: the Louisville Metro Open Data / LOJIC ArcGIS Hub item
"Louisville Metro KY - Property Available for Purchase" (item id
047c3ff02bb9404f8965d30e6171baa1). The published artifact is a Microsoft
Excel binary file (Sales_Inventory_(1).xls) covering three property pools:

    1  Landbank Authority
    2  Urban Renewal and Community Development Agency
    3  Metro-owned (limited)

The item exposes NO FeatureServer / REST layer — `url` is null and
`type == "Microsoft Excel"` in the item metadata. The only way to get the
canonical rows is to download the `.../items/<id>/data` binary and parse
the `.xls` workbook.

The workbook has two sheets:

    Data Dictionary    — column legend
    Sales Inventory    — the actual property rows (~400 rows as of last pull)

Sales Inventory columns (verified against the live download):

    OWNER, ST , LOC, DIR , STREET NAME, TYPE , BLK,  LOT, SUB, PARCELID,
    WIDTH, DEPTH, IMP , ZONE, CENSUSTRACT, PVA TOTAL VALUE, "CD, 2012",
    ZIP CODE , NHOOD , STATUS, SOURCE DEED BKxPG, DATE OF DEED, RESERVEE,
    NOTATION, LAND Value, IMP value, DATE RECEIVED

Parsing approach:

    The repo does NOT pin a binary-XLS reader (no xlrd / olefile in
    requirements.txt). Rather than introduce a new Python dependency for
    one source, we rely on `libreoffice --headless --convert-to csv` to
    translate the workbook into a per-sheet CSV. LibreOffice is already
    available on the GitHub Actions ubuntu-latest runner.

    If LibreOffice is not installed, the scraper exits non-zero with a
    clear remediation message rather than silently emitting nothing.

Output:

    Standalone mode (`python scrapers/louisville_landbank.py --out FILE`):
        Writes the canonical Lovable record list (one JSON array, schema
        per docs/SCRAPER_SPEC.md §3.2).

    Dispatcher mode (`python -m scrapers.louisville_landbank --output-dir
    DIR --csv-name NAME.csv`): writes both a CSV in the canonical
    5-column shape and the JSON sidecar that upload_results consumes.

No field is fabricated. Missing source values produce JSON `null`, not
empty strings or "N/A".
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scrapers.common import write_canonical_csv  # noqa: E402
else:
    from .common import write_canonical_csv


SOURCE_SLUG = "louisville_landbank"
SOURCE_NAME = "Louisville Metro KY - Property Available for Purchase"
SIGNAL_TYPE = "landbank_inventory"

ARC_ITEM_ID = "047c3ff02bb9404f8965d30e6171baa1"
ARC_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ARC_ITEM_ID}"
DATA_URL = f"{ARC_ITEM_URL}/data"
HUB_PAGE = (
    "https://louisville-metro-opendata-lojic.hub.arcgis.com/documents/"
    "LOJIC::louisville-metro-ky-property-available-for-purchase"
)
USER_AGENT = "LovablePropertySignalBot/1.0 (+contact: ops@lovable.example)"

OWNER_CODE_MAP = {
    "1": "Louisville Metro Landbank Authority",
    "2": "Louisville Metro Urban Renewal and Community Development Agency",
    "3": "Louisville Metro (Metro-owned)",
}

INVENTORY_SHEET_CANDIDATES = (
    "sales inventory",
    "inventory",
)


@dataclass
class LandbankRecord:
    """One landbank inventory row, normalized to the Lovable contract."""

    parcel_id: str | None
    property_address: str | None
    owner_name: str | None
    signal_date: str | None
    case_number: str | None
    source_url: str
    raw: dict

    def to_canonical(self) -> dict:
        return {
            "source": SOURCE_SLUG,
            "source_url": self.source_url,
            "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "parcel_id": self.parcel_id,
            "property_address": self.property_address,
            "owner_name": self.owner_name,
            "owner_mailing_address": None,
            "signal_type": SIGNAL_TYPE,
            "signal_date": self.signal_date,
            "amount_owed": None,
            "case_number": self.case_number,
            "raw": self.raw,
        }

    def to_csv_row(self) -> dict:
        """Project the record onto the canonical 5-column CSV used by ingest."""
        note_bits = [f"Source: {SOURCE_NAME}"]
        status = (self.raw.get("STATUS") or "").strip()
        if status:
            note_bits.append(f"Status: {status}")
        pva = (self.raw.get("PVA TOTAL VALUE") or "").strip()
        if pva:
            note_bits.append(f"PVA total value: {pva}")
        zone = (self.raw.get("ZONE") or "").strip()
        if zone:
            note_bits.append(f"Zone: {zone}")
        notation = (self.raw.get("NOTATION") or "").strip()
        if notation:
            note_bits.append(f"Notation: {notation}")
        if self.parcel_id:
            note_bits.append(f"Parcel: {self.parcel_id}")
        note_bits.append(f"Signal: {SIGNAL_TYPE}")
        return {
            "Date": self.signal_date or "",
            "Defendants/Parties": self.owner_name or "",
            "Property Address": self.property_address or "Address not found",
            "PDF Link": self.source_url,
            "Notes": "; ".join(note_bits),
            "_instrument_number": self.parcel_id or "",
            "_filing_date_iso": self.signal_date,
            "_source_link": self.source_url,
        }


def _log(stream, level: str, message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {level}: [{SOURCE_SLUG}] {message}", file=stream, flush=True)


def download_xls(
    dest: Path,
    *,
    session: requests.Session | None = None,
    url: str = DATA_URL,
    retries: int = 5,
    backoff: float = 1.5,
) -> Path:
    """Download the landbank workbook to `dest`. Retries with backoff on 5xx/network."""
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=120, stream=True)
            if resp.status_code >= 500 or resp.status_code == 429:
                resp.raise_for_status()
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
            return dest
        except (requests.RequestException, OSError) as exc:
            last_exc = exc
            if attempt == retries:
                break
            sleep_for = backoff ** attempt
            time.sleep(sleep_for)
    raise RuntimeError(
        f"Failed to download landbank workbook from {url} after {retries} attempts: {last_exc}"
    )


def _find_libreoffice() -> str:
    for candidate in ("libreoffice", "soffice"):
        path = shutil.which(candidate)
        if path:
            return path
    raise RuntimeError(
        "LibreOffice is required to parse the landbank .xls workbook but was "
        "not found on PATH. Install `libreoffice-calc` (the GitHub Actions "
        "ubuntu-latest runner already ships with it) or pre-convert the file "
        "and pass --csv-path to skip the conversion step."
    )


def convert_xls_to_csvs(xls_path: Path, out_dir: Path) -> list[Path]:
    """Convert every sheet of an .xls workbook to CSV via LibreOffice headless."""
    out_dir.mkdir(parents=True, exist_ok=True)
    bin_path = _find_libreoffice()
    # The trailing "-1" picks "all sheets" so we get one CSV per sheet rather
    # than just the first. Filter options:
    #   44 = comma, 34 = double-quote, 76 = UTF-8, 1 = first-row, blank lang,
    #   0 = cell format, false flags, -1 = all sheets.
    cmd = [
        bin_path,
        "--headless",
        "--convert-to",
        "csv:Text - txt - csv (StarCalc):44,34,76,1,,0,false,true,false,false,false,-1",
        str(xls_path),
        "--outdir",
        str(out_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if proc.returncode != 0:
        raise RuntimeError(
            f"LibreOffice conversion failed (exit {proc.returncode}): "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
    csvs = sorted(out_dir.glob("*.csv"))
    if not csvs:
        raise RuntimeError(
            f"LibreOffice produced no CSV output from {xls_path}. stderr: "
            f"{proc.stderr.strip() or '(empty)'}"
        )
    return csvs


def _pick_inventory_csv(csvs: Iterable[Path]) -> Path:
    """Choose the CSV file that holds the Sales Inventory sheet."""
    csvs = list(csvs)
    # Match on sheet name slug in the filename emitted by LibreOffice.
    for path in csvs:
        lower = path.stem.lower()
        for needle in INVENTORY_SHEET_CANDIDATES:
            if needle in lower:
                return path
    # Fallback: pick the largest CSV (the dictionary sheet is much smaller).
    if csvs:
        return max(csvs, key=lambda p: p.stat().st_size)
    raise RuntimeError("No CSV files emitted from XLS conversion.")


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _normalize_header(name: str) -> str:
    return " ".join(name.split()).strip().upper()


def _resolve_columns(fieldnames: list[str]) -> dict[str, str]:
    """Map normalized header names back to whatever LibreOffice emitted.

    LibreOffice preserves the source spreadsheet's trailing spaces and
    odd capitalization, so we normalize once and key by uppercase to
    avoid scattering header-matching brittleness through the row loop.
    """
    return {_normalize_header(name): name for name in fieldnames if name}


def _build_address(row: dict, cols: dict[str, str]) -> str | None:
    """Assemble a street address from the source's component columns.

    The Sales Inventory sheet keeps the address in pieces. Never invent
    components — if street number / name is missing, return None and
    let `property_address` be null in the canonical record.
    """
    def g(label: str) -> str:
        col = cols.get(label)
        return _clean(row.get(col, "")) if col else ""

    st_num = g("ST #") or g("ST")
    loc = g("LOC")
    direction = g("DIR")
    name = g("STREET NAME")
    st_type = g("TYPE")
    zip_code = g("ZIP CODE") or g("ZIP CODE 2012") or g("ZIP")

    if not st_num and not name:
        return None

    parts: list[str] = []
    if st_num:
        parts.append(st_num)
    if loc:
        parts.append(loc)
    if direction:
        parts.append(direction)
    if name:
        parts.append(name)
    if st_type:
        parts.append(st_type)
    address = " ".join(parts).strip()
    if not address:
        return None
    suffix_bits = ["LOUISVILLE", "KY"]
    if zip_code:
        suffix_bits.append(zip_code)
    return f"{address}, {' '.join(suffix_bits)}"


def _parse_owner(row: dict, cols: dict[str, str]) -> str | None:
    col = cols.get("OWNER")
    if not col:
        return None
    code = _clean(row.get(col, ""))
    if not code:
        return None
    return OWNER_CODE_MAP.get(code, f"Louisville Metro (owner code {code})")


def _parse_signal_date(row: dict, cols: dict[str, str]) -> str | None:
    """Pull the most recent date the source provides.

    Preference order: DATE RECEIVED (acquired into inventory) -> DATE OF DEED.
    """
    for label in ("DATE RECEIVED", "DATE OF DEED"):
        col = cols.get(label)
        if not col:
            continue
        raw = _clean(row.get(col, ""))
        if not raw:
            continue
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _parse_case_number(row: dict, cols: dict[str, str]) -> str | None:
    col = cols.get("SOURCE DEED BKXPG") or cols.get("SOURCE DEED BK X PG")
    if not col:
        return None
    val = _clean(row.get(col, ""))
    return val or None


def parse_inventory_csv(
    csv_path: Path,
    *,
    source_url: str = HUB_PAGE,
    limit: int | None = None,
    log=None,
) -> list[LandbankRecord]:
    """Parse the Sales Inventory CSV (already extracted from the .xls)."""
    records: list[LandbankRecord] = []
    skipped = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        cols = _resolve_columns(list(reader.fieldnames or []))
        if not cols:
            raise RuntimeError(
                f"No header row found in {csv_path}; the workbook layout may "
                "have changed. Inspect the converted CSV manually."
            )
        parcel_col = cols.get("PARCELID")
        for row in reader:
            try:
                parcel = _clean(row.get(parcel_col, "")) if parcel_col else ""
                address = _build_address(row, cols)
                owner = _parse_owner(row, cols)
                signal_date = _parse_signal_date(row, cols)
                case_number = _parse_case_number(row, cols)
                if not parcel and not address and not owner:
                    # Documentation rows / blank trailing rows.
                    continue
                raw = {k: _clean(v) for k, v in row.items() if k}
                records.append(
                    LandbankRecord(
                        parcel_id=parcel or None,
                        property_address=address,
                        owner_name=owner,
                        signal_date=signal_date,
                        case_number=case_number,
                        source_url=source_url,
                        raw=raw,
                    )
                )
                if limit is not None and len(records) >= limit:
                    break
            except Exception as exc:  # Row-level errors are skipped, not fatal.
                skipped += 1
                if log:
                    log("WARNING", f"Skipping malformed row: {exc!r}")
    if log:
        log("RESULT", f"Parsed {len(records)} inventory rows (skipped {skipped}).")
    return records


def fetch_records(
    *,
    cache_dir: Path,
    session: requests.Session | None = None,
    xls_path: Path | None = None,
    csv_path: Path | None = None,
    limit: int | None = None,
    log=None,
) -> list[LandbankRecord]:
    """End-to-end: download the XLS (if needed), convert, and parse."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    if csv_path is None:
        if xls_path is None:
            xls_path = cache_dir / "Sales_Inventory.xls"
            if log:
                log("ACTION", f"Downloading {DATA_URL}")
            download_xls(xls_path, session=session)
            if log:
                log("ACTION", f"Saved workbook to {xls_path}")
        else:
            if log:
                log("ACTION", f"Using cached workbook at {xls_path}")
        if log:
            log("ACTION", "Converting .xls to CSV via LibreOffice headless")
        csvs = convert_xls_to_csvs(xls_path, cache_dir / "csv")
        csv_path = _pick_inventory_csv(csvs)
        if log:
            log("ACTION", f"Selected inventory sheet: {csv_path.name}")
    else:
        if log:
            log("ACTION", f"Using pre-converted CSV at {csv_path}")
    return parse_inventory_csv(csv_path, limit=limit, log=log)


def records_to_canonical(records: Iterable[LandbankRecord]) -> list[dict]:
    return [r.to_canonical() for r in records]


def records_to_csv_rows(records: Iterable[LandbankRecord]) -> list[dict]:
    return [r.to_csv_row() for r in records]


def _make_logger(log_path: Path | None):
    def log(level: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {level}: {message}"
        # Spec §5: progress/errors go to stderr (stdout is reserved for JSON
        # in standalone mode).
        print(line, file=sys.stderr, flush=True)
        if log_path is not None:
            try:
                with log_path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError:
                pass

    return log


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Louisville Metro Landbank inventory (.xls workbook)."
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Standalone-mode output: writes a JSON array of canonical records here.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Dispatcher-mode output dir: writes <csv-name> + JSON sidecar inside.",
    )
    parser.add_argument(
        "--csv-name",
        default="louisville_landbank_results.csv",
        help="CSV filename inside --output-dir (dispatcher mode).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Accepted for dispatcher parity; landbank inventory is a static snapshot "
             "and does not filter by date.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Accepted for dispatcher parity; ignored (see --start-date).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap on records emitted (for smoke tests). Applied after normalization.",
    )
    parser.add_argument(
        "--xls-path",
        default=None,
        help="Use a local .xls instead of downloading. Useful for tests / CI smoke runs.",
    )
    parser.add_argument(
        "--csv-path",
        default=None,
        help="Use a pre-extracted Sales Inventory CSV; skips download AND conversion. "
             "Useful when LibreOffice is unavailable on the runner.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Directory for the downloaded .xls + per-sheet CSVs. Defaults to a temp dir.",
    )
    args = parser.parse_args(argv)

    if not args.out and not args.output_dir:
        parser.error("Either --out (standalone mode) or --output-dir (dispatcher mode) is required.")

    output_dir = Path(args.output_dir).resolve() if args.output_dir else None
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
    log_path = (output_dir / "action_log.txt") if output_dir else None
    if log_path is not None and not log_path.exists():
        log_path.write_text("", encoding="utf-8")
    log = _make_logger(log_path)

    cache_dir = (
        Path(args.cache_dir).resolve()
        if args.cache_dir
        else (output_dir / "landbank_cache" if output_dir else Path(tempfile.mkdtemp(prefix="landbank-")))
    )

    try:
        log("ACTION", f"Source landing page: {HUB_PAGE}")
        log(
            "ACTION",
            "Landbank inventory is a static snapshot (no date filtering on source). "
            f"start={args.start_date} end={args.end_date} are accepted for dispatcher parity.",
        )
        records = fetch_records(
            cache_dir=cache_dir,
            xls_path=Path(args.xls_path).resolve() if args.xls_path else None,
            csv_path=Path(args.csv_path).resolve() if args.csv_path else None,
            limit=args.limit,
            log=log,
        )
        log("RESULT", f"Total normalized landbank records: {len(records)}")

        canonical = records_to_canonical(records)

        if args.out:
            out_path = Path(args.out).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(canonical, indent=2), encoding="utf-8")
            log("RESULT", f"Wrote {len(canonical)} canonical records to {out_path}")

        if output_dir is not None:
            csv_path = output_dir / args.csv_name
            csv_rows = records_to_csv_rows(records)
            write_canonical_csv(csv_rows, csv_path)
            log("RESULT", f"Wrote {len(csv_rows)} CSV rows to {csv_path}")
            sidecar = output_dir / "louisville_landbank_records.json"
            sidecar.write_text(json.dumps(canonical, indent=2), encoding="utf-8")
            log("RESULT", f"Wrote structured sidecar: {sidecar}")
        return 0
    except Exception as exc:
        log("ERROR", f"Fatal scraper error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
