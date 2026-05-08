#!/usr/bin/env python3
"""
Sync scraper outputs from GitHub Actions back to Supabase.

Required environment variables:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY

Expected tables/bucket:
  scraper_runs
  lis_pendens_records
  scraper-artifacts
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import mimetypes
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required GitHub secret/environment variable: {name}")
    return value.rstrip("/") if name == "SUPABASE_URL" else value


class SupabaseClient:
    def __init__(self) -> None:
        self.url = env_required("SUPABASE_URL")
        self.key = env_required("SUPABASE_SERVICE_ROLE_KEY")
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }

    def patch_run(self, run_id: str, payload: dict) -> None:
        endpoint = f"{self.url}/rest/v1/scraper_runs?id=eq.{run_id}"
        response = requests.patch(endpoint, headers={**self.headers, "Prefer": "return=minimal"}, json=payload, timeout=30)
        response.raise_for_status()

    def upsert_records(self, records: list[dict]) -> None:
        if not records:
            return
        endpoint = f"{self.url}/rest/v1/lis_pendens_records?on_conflict=run_id,instrument_number"
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates,return=minimal"}
        response = requests.post(endpoint, headers=headers, json=records, timeout=60)
        response.raise_for_status()

    def upload_file(self, bucket: str, storage_path: str, local_path: Path) -> str:
        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
        endpoint = f"{self.url}/storage/v1/object/{bucket}/{storage_path}"
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": content_type,
            "x-upsert": "true",
        }
        response = requests.post(endpoint, headers=headers, data=local_path.read_bytes(), timeout=60)
        response.raise_for_status()
        return storage_path


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default="scraper_output")
    parser.add_argument("--mark-running", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--failed", action="store_true")
    args = parser.parse_args()

    client = SupabaseClient()
    output_dir = Path(args.output_dir)

    if args.mark_running:
        client.patch_run(args.run_id, {"status": "running", "started_at": datetime.utcnow().isoformat() + "Z"})
        return 0

    if not args.finalize:
        raise RuntimeError("Pass either --mark-running or --finalize")

    csv_path = output_dir / "lis_pendens_results.csv"
    log_path = output_dir / "action_log.txt"
    validation_path = output_dir / "validation_report.txt"

    storage_updates: dict[str, str] = {}
    for local_path, column_name in [
        (csv_path, "csv_storage_path"),
        (log_path, "action_log_storage_path"),
        (validation_path, "validation_report_storage_path"),
    ]:
        if local_path.exists():
            storage_path = f"runs/{args.run_id}/{local_path.name}"
            storage_updates[column_name] = client.upload_file("scraper-artifacts", storage_path, local_path)

    records: list[dict] = []
    if csv_path.exists():
        records = read_records(csv_path, args.run_id)
        client.upsert_records(records)

    addresses_found = sum(1 for r in records if r.get("property_address") and r.get("property_address") != "Address not found")
    failures = sum(1 for r in records if r.get("property_address") == "Address not found")
    status = "failed" if args.failed or not csv_path.exists() else "completed"
    error_message = None if status == "completed" else "GitHub scraper failed or CSV was not created. Check action log/artifacts."

    client.patch_run(
        args.run_id,
        {
            "status": status,
            "total_records": len(records),
            "addresses_found": addresses_found,
            "failures": failures,
            "finished_at": datetime.utcnow().isoformat() + "Z",
            "error_message": error_message,
            **storage_updates,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
