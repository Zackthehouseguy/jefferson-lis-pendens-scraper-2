#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import mimetypes
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests


# ---------------------------------------------------------------------------
# Schema-aware CSV parsing
# ---------------------------------------------------------------------------

JEFFERSON_SCHEMAS = {"jefferson_deeds"}
LOUISVILLE_SCHEMA = "louisville_code_violations"
INDIANAPOLIS_SCHEMA = "indianapolis_code_violations"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_date(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def extract_instrument_number(pdf_link: str, fallback_seed: str) -> str:
    parsed = urlparse(pdf_link or "")
    img_values = parse_qs(parsed.query).get("img", [])
    if img_values:
        try:
            decoded = base64.b64decode(img_values[0] + "==").decode("utf-8", errors="ignore")
            match = re.search(r"(\d{10})\.(?:tif|pdf|png|jpg)", decoded, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        except Exception:
            pass
    match = re.search(r"(\d{10})", pdf_link or "")
    if match:
        return match.group(1)
    return hashlib.sha1(fallback_seed.encode("utf-8")).hexdigest()[:16]


def extract_pva_link(notes: str) -> str | None:
    match = re.search(r"https://jeffersonpva\.ky\.gov/[^\s;,\"]+", notes or "")
    return match.group(0) if match else None


def _row_to_jefferson_record(row: dict, run_id: str) -> dict:
    parties = row.get("Defendants/Parties", "").strip()
    pdf_link = row.get("PDF Link", "").strip()
    notes = row.get("Notes", "").strip()
    seed = "|".join([row.get("Date", ""), parties, row.get("Property Address", ""), pdf_link])
    return {
        "run_id": run_id,
        "filing_date": iso_date(row.get("Date", "")),
        "instrument_number": extract_instrument_number(pdf_link, seed),
        "parties": parties,
        "property_address": row.get("Property Address", "").strip(),
        "pdf_link": pdf_link,
        "notes": notes,
        "pva_verification_link": extract_pva_link(notes),
    }


def _row_to_simple_record(
    row: dict,
    run_id: str,
    sidecar_lookup: dict[str, dict] | None = None,
) -> dict:
    parties = row.get("Defendants/Parties", "").strip()
    pdf_link = row.get("PDF Link", "").strip()
    notes = row.get("Notes", "").strip()
    address = row.get("Property Address", "").strip()
    raw_date = row.get("Date", "").strip()

    instrument_number: str | None = None
    filing_date: str | None = iso_date(raw_date)

    # Prefer the structured sidecar emitted by the source scraper when present.
    if sidecar_lookup is not None:
        key = "|".join([raw_date, parties, address, pdf_link, notes])
        sidecar = sidecar_lookup.get(key)
        if sidecar:
            instrument_number = (sidecar.get("_instrument_number") or "").strip() or None
            filing_date = sidecar.get("_filing_date_iso") or filing_date

    if not instrument_number:
        seed = "|".join([raw_date, parties, address, pdf_link, notes])
        instrument_number = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    return {
        "run_id": run_id,
        "filing_date": filing_date,
        "instrument_number": instrument_number,
        "parties": parties,
        "property_address": address,
        "pdf_link": pdf_link,
        "notes": notes,
        "pva_verification_link": None,
    }


def _build_sidecar_lookup(sidecar_path: Path) -> dict[str, dict]:
    if not sidecar_path.exists():
        return {}
    try:
        items = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    lookup: dict[str, dict] = {}
    for item in items or []:
        key = "|".join(
            [
                item.get("Date", ""),
                item.get("Defendants/Parties", ""),
                item.get("Property Address", ""),
                item.get("PDF Link", ""),
                item.get("Notes", ""),
            ]
        )
        lookup[key] = item
    return lookup


def read_records(csv_path: Path, run_id: str, schema: str, sidecar_path: Path | None = None) -> list[dict]:
    records: list[dict] = []
    if not csv_path.exists():
        return records
    sidecar_lookup = _build_sidecar_lookup(sidecar_path) if sidecar_path else {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if schema in JEFFERSON_SCHEMAS:
                records.append(_row_to_jefferson_record(row, run_id))
            else:
                records.append(_row_to_simple_record(row, run_id, sidecar_lookup))
    return records


def encode_file(path: Path) -> dict | None:
    if not path.exists():
        return None
    return {
        "filename": path.name,
        "content_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
        "base64": base64.b64encode(path.read_bytes()).decode("ascii"),
        "size": path.stat().st_size,
    }


def post_ingest(ingest_url: str, ingest_token: str, payload: dict) -> None:
    response = requests.post(
        ingest_url,
        headers={
            "Authorization": f"Bearer {ingest_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()


def _resolve_meta(output_dir: Path, source_type_arg: str | None) -> dict:
    """Pick CSV filename + schema from source_meta.json, falling back to lis_pendens."""
    meta_path = output_dir / "source_meta.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    source_type = source_type_arg or "lis_pendens"
    fallback = {
        "lis_pendens": {
            "source_type": "lis_pendens",
            "label": "Jefferson Lis Pendens",
            "csv_name": "lis_pendens_results.csv",
            "schema": "jefferson_deeds",
        },
        "wills": {
            "source_type": "wills",
            "label": "Jefferson Wills",
            "csv_name": "wills_results.csv",
            "schema": "jefferson_deeds",
        },
        "louisville_code_violations": {
            "source_type": "louisville_code_violations",
            "label": "Louisville Code Violations",
            "csv_name": "louisville_code_violations_results.csv",
            "schema": LOUISVILLE_SCHEMA,
        },
        "indianapolis_code_violations": {
            "source_type": "indianapolis_code_violations",
            "label": "Indianapolis Code Violations",
            "csv_name": "indianapolis_code_violations_results.csv",
            "schema": INDIANAPOLIS_SCHEMA,
        },
    }
    return fallback.get(source_type, fallback["lis_pendens"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload scraper results to Lovable ingest endpoint.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingest-url", required=True)
    parser.add_argument("--ingest-token", required=True)
    parser.add_argument("--output-dir", default="scraper_output")
    parser.add_argument("--status", choices=["completed", "failed"], required=True)
    parser.add_argument("--error-message", default="")
    parser.add_argument(
        "--source-type",
        default=None,
        help="Optional override; otherwise read from source_meta.json or defaults to lis_pendens.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    meta = _resolve_meta(output_dir, args.source_type)
    csv_path = output_dir / meta["csv_name"]
    log_path = output_dir / "action_log.txt"
    validation_path = output_dir / "validation_report.txt"
    sidecar_path = None
    if meta["schema"] == LOUISVILLE_SCHEMA:
        sidecar_path = output_dir / "louisville_code_violations_records.json"

    records = read_records(csv_path, args.run_id, meta["schema"], sidecar_path)

    addresses_found = sum(
        1 for r in records
        if r.get("property_address") and r.get("property_address") != "Address not found"
    )
    failures = sum(
        1 for r in records if r.get("property_address") == "Address not found"
    )

    files: dict[str, dict] = {}
    candidate_files = [
        ("csv", csv_path),
        ("action_log", log_path),
        ("validation_report", validation_path),
    ]
    if sidecar_path is not None:
        candidate_files.append(("source_records_json", sidecar_path))
    for key, path in candidate_files:
        encoded = encode_file(path)
        if encoded:
            files[key] = encoded

    payload = {
        "action": "finalize_results",
        "type": "scraper_results",
        "run_id": args.run_id,
        "status": args.status,
        "error_message": args.error_message or None,
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "github_repository": os.environ.get("GITHUB_REPOSITORY"),
        "timestamp": utc_now(),
        "source_type": meta["source_type"],
        "source_label": meta["label"],
        "source_schema": meta["schema"],
        "summary": {
            "total_records": len(records),
            "addresses_found": addresses_found,
            "failures": failures,
            "source_type": meta["source_type"],
        },
        "records": records,
        "files": files,
    }
    post_ingest(args.ingest_url, args.ingest_token, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
