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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_date(value: str) -> str | None:
    value = (value or "").strip()
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


def read_records(csv_path: Path, run_id: str) -> list[dict]:
    records: list[dict] = []
    if not csv_path.exists():
        return records
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            parties = row.get("Defendants/Parties", "").strip()
            pdf_link = row.get("PDF Link", "").strip()
            notes = row.get("Notes", "").strip()
            seed = "|".join([row.get("Date", ""), parties, row.get("Property Address", ""), pdf_link])
            records.append(
                {
                    "run_id": run_id,
                    "filing_date": iso_date(row.get("Date", "")),
                    "instrument_number": extract_instrument_number(pdf_link, seed),
                    "parties": parties,
                    "property_address": row.get("Property Address", "").strip(),
                    "pdf_link": pdf_link,
                    "notes": notes,
                    "pva_verification_link": extract_pva_link(notes),
                }
            )
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload scraper results to Lovable ingest endpoint.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingest-url", required=True)
    parser.add_argument("--ingest-token", required=True)
    parser.add_argument("--output-dir", default="scraper_output")
    parser.add_argument("--status", choices=["completed", "failed"], required=True)
    parser.add_argument("--error-message", default="")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    csv_path = output_dir / "lis_pendens_results.csv"
    log_path = output_dir / "action_log.txt"
    validation_path = output_dir / "validation_report.txt"

    records = read_records(csv_path, args.run_id)
    addresses_found = sum(1 for r in records if r.get("property_address") and r.get("property_address") != "Address not found")
    failures = sum(1 for r in records if r.get("property_address") == "Address not found")

    files = {}
    for key, path in [
        ("csv", csv_path),
        ("action_log", log_path),
        ("validation_report", validation_path),
    ]:
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
        "summary": {
            "total_records": len(records),
            "addresses_found": addresses_found,
            "failures": failures,
        },
        "records": records,
        "files": files,
    }
    post_ingest(args.ingest_url, args.ingest_token, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
