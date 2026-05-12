#!/usr/bin/env python3
"""
Multi-source dispatcher.

Picks the right scraper based on --source-type and forwards the relevant
flags. This is the single entry point invoked by .github/workflows/run-scraper.yml.

Supported source types:
  - lis_pendens                  Jefferson County KY Lis Pendens (default)
  - wills                        Jefferson County KY Wills
  - louisville_code_violations   Louisville Metro PM_SiteVisit_Violations
  - indianapolis_code_violations Indianapolis Accela Enforcement (scaffold)

The dispatcher writes a small `source_meta.json` file into --output-dir so
the downstream upload_results.py step can choose the correct CSV name and
schema mapping without re-parsing CLI args.
"""
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent

# Source-type registry. Each entry says:
#   csv_name       — the CSV file the scraper writes inside --output-dir
#   schema         — how upload_results should map CSV rows to ingest records
#   build_command  — function returning argv for that source
SOURCES = {
    "lis_pendens": {
        "csv_name": "lis_pendens_results.csv",
        "schema": "jefferson_deeds",
        "label": "Jefferson Lis Pendens",
    },
    "wills": {
        "csv_name": "wills_results.csv",
        "schema": "jefferson_deeds",
        "label": "Jefferson Wills",
    },
    "louisville_code_violations": {
        "csv_name": "louisville_code_violations_results.csv",
        "schema": "louisville_code_violations",
        "label": "Louisville Code Violations",
    },
    "indianapolis_code_violations": {
        "csv_name": "indianapolis_code_violations_results.csv",
        "schema": "indianapolis_code_violations",
        "label": "Indianapolis Code Violations",
    },
    "tax_delinquent": {
        "csv_name": "jefferson_tax_delinquent_results.csv",
        "schema": "jefferson_tax_delinquent",
        "label": "Jefferson Tax Delinquent",
    },
}


def _jefferson_command(args: argparse.Namespace, source_type: str) -> list[str]:
    if source_type == "lis_pendens":
        instrument_code = "LP "
        instrument_label = "LIS PENDENS"
    elif source_type == "wills":
        # Jefferson Deeds uses "WIL" (no trailing space) as the itype1 value
        # for the WILL instrument type, verified against the live insttype.php
        # dropdown. The earlier "WI " value matched nothing.
        instrument_code = "WIL"
        instrument_label = "WILLS"
    else:
        raise ValueError(f"Unsupported Jefferson source_type: {source_type}")

    cmd = [
        sys.executable,
        str(REPO_ROOT / "jefferson_lis_pendens_scraper.py"),
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--output-dir", args.output_dir,
        "--search-mode", args.search_mode,
        "--instrument-code", instrument_code,
        "--instrument-label", instrument_label,
        "--csv-name", SOURCES[source_type]["csv_name"],
    ]
    if args.resume:
        cmd.append("--resume")
    if args.pva_cross_check and source_type == "lis_pendens":
        # PVA cross-check is Lis-Pendens-specific; skip for Wills.
        cmd.append("--pva-cross-check")
    if source_type != "lis_pendens":
        # The benchmark validator hardcodes the Matthew Martin / Malcolm Rd
        # Lis Pendens fixtures; skip for other Jefferson types.
        cmd.append("--skip-validation")
    if source_type == "wills":
        # Wills filings are not deeds; addresses are usually absent. Tag the
        # Notes column so downstream consumers can distinguish Wills records
        # from Lis Pendens, and always retain the legal-description field
        # since it is often the only locator we have for the estate.
        cmd.extend(["--source-tag", "Source: WILLS"])
        cmd.append("--always-include-legal-desc")
        # Emit the Wills smart-field CSV shape (Decedent / Date of Death /
        # Surviving Spouse / Beneficiary / Complexity Flag up front).
        cmd.append("--wills-csv-format")
    return cmd


def _louisville_command(args: argparse.Namespace) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "scrapers.louisville_code_violations",
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--output-dir", args.output_dir,
        "--csv-name", SOURCES["louisville_code_violations"]["csv_name"],
    ]
    if getattr(args, "include_low_signal_code_violations", False):
        cmd.append("--include-low-signal-code-violations")
    if getattr(args, "include_closed_code_violations", False):
        cmd.append("--include-closed-code-violations")
    return cmd


def _indianapolis_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scrapers.indianapolis_code_violations",
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--output-dir", args.output_dir,
        "--csv-name", SOURCES["indianapolis_code_violations"]["csv_name"],
    ]


def _tax_delinquent_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scrapers.jefferson_tax_delinquent",
        "--start-date", args.start_date,
        "--end-date", args.end_date,
        "--output-dir", args.output_dir,
        "--csv-name", SOURCES["tax_delinquent"]["csv_name"],
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Property-signal scraper dispatcher.")
    parser.add_argument(
        "--source-type",
        default="lis_pendens",
        choices=sorted(SOURCES.keys()),
        help="Which source to scrape. Defaults to lis_pendens for backward compatibility.",
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", default="scraper_output")
    parser.add_argument("--search-mode", default="auto")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--pva-cross-check", action="store_true")
    parser.add_argument(
        "--include-low-signal-code-violations",
        dest="include_low_signal_code_violations",
        action="store_true",
        help="Code-violation sources only: include low-signal/administrative "
             "violations (rental registration, street-number-only, etc.). "
             "Default: high-signal distressed-property leads only.",
    )
    parser.add_argument(
        "--include-closed-code-violations",
        dest="include_closed_code_violations",
        action="store_true",
        help="Code-violation sources only: include closed/resolved cases. "
             "Default: open / active-enforcement leads only.",
    )
    args = parser.parse_args()

    source_type = args.source_type
    info = SOURCES[source_type]

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Persist metadata for upload_results.
    meta = {
        "source_type": source_type,
        "label": info["label"],
        "csv_name": info["csv_name"],
        "schema": info["schema"],
        "start_date": args.start_date,
        "end_date": args.end_date,
    }
    (output_dir / "source_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if source_type in ("lis_pendens", "wills"):
        cmd = _jefferson_command(args, source_type)
    elif source_type == "louisville_code_violations":
        cmd = _louisville_command(args)
    elif source_type == "indianapolis_code_violations":
        cmd = _indianapolis_command(args)
    elif source_type == "tax_delinquent":
        cmd = _tax_delinquent_command(args)
    else:  # pragma: no cover - argparse already restricts choices
        raise ValueError(f"Unknown source_type: {source_type}")

    print(f"[dispatcher] Running: {shlex.join(cmd)}", flush=True)
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


if __name__ == "__main__":
    sys.exit(main())
