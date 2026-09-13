#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_time(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def read_source_codes(path):
    codes = {}
    p = Path(path)
    if not p.exists():
        return codes
    for line in p.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, raw = line.split("=", 1)
        try:
            codes[key.strip()] = int(raw.strip())
        except ValueError:
            codes[key.strip()] = -1
    return codes


def evaluate(report, source_codes, max_age_hours=24.0, require_live_ai=False):
    blockers, warnings = [], []
    summary = report.get("summary") or {}
    inputs = int(summary.get("input_candidates") or 0)
    parcels = int(summary.get("parcel_verified") or 0)
    pva = int(summary.get("pva_owner_verified") or 0)

    failed_sources = sorted(k for k, v in source_codes.items() if v != 0)
    if not source_codes:
        blockers.append("source_exit_codes_missing")
    elif failed_sources and len(failed_sources) == len(source_codes):
        blockers.append("all_sources_failed")
    elif failed_sources:
        warnings.append("partial_source_failure:" + ",".join(failed_sources))

    dep = summary.get("dependency_health") or {}
    if report.get("status") == "BLOCKED" or dep.get("status") == "BLOCKED":
        blockers.append(str(dep.get("reason") or "qualification_report_blocked"))
    elif report.get("status") == "DEGRADED" or dep.get("status") == "DEGRADED":
        warnings.append(str(dep.get("reason") or "qualification_degraded"))

    if inputs >= 20 and parcels == 0:
        blockers.append("systemic_parcel_verification_loss")
    if inputs >= 20 and pva == 0:
        blockers.append("systemic_pva_verification_loss")

    generated = parse_time(report.get("generated_at_et"))
    age_hours = None
    if not generated:
        blockers.append("report_timestamp_missing")
    else:
        age_hours = max(0.0, (datetime.now(timezone.utc) - generated.astimezone(timezone.utc)).total_seconds() / 3600)
        if age_hours > max_age_hours:
            blockers.append(f"report_stale:{age_hours:.1f}h")

    if require_live_ai and summary.get("ai_scoring_status") != "LIVE":
        blockers.append("live_ai_scoring_missing")

    blockers = list(dict.fromkeys(blockers))
    warnings = list(dict.fromkeys(warnings))
    status = "BLOCKED" if blockers else ("DEGRADED" if warnings else "PASS")
    return {
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "report_age_hours": None if age_hours is None else round(age_hours, 2),
        "source_exit_codes": source_codes,
        "summary": {
            "input_candidates": inputs,
            "parcel_verified": parcels,
            "parcel_live_verified": int(summary.get("parcel_live_verified") or 0),
            "parcel_cache_verified": int(summary.get("parcel_cache_verified") or 0),
            "pva_owner_verified": pva,
            "eligible_sfr": int(summary.get("eligible_sfr") or 0),
            "eligible_land": int(summary.get("eligible_land") or 0),
            "ai_scoring_status": summary.get("ai_scoring_status"),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--source-exit-codes", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-age-hours", type=float, default=24.0)
    ap.add_argument("--require-live-ai", action="store_true")
    args = ap.parse_args()

    try:
        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    except Exception as exc:
        result = {
            "status": "BLOCKED",
            "blockers": [f"qualification_report_unreadable:{type(exc).__name__}"],
            "warnings": [],
            "source_exit_codes": read_source_codes(args.source_exit_codes),
            "summary": {},
        }
    else:
        result = evaluate(report, read_source_codes(args.source_exit_codes), args.max_age_hours, args.require_live_ai)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"## Reaper health: {result['status']}\n\n")
            f.write(f"- Blockers: {', '.join(result.get('blockers') or []) or 'none'}\n")
            f.write(f"- Warnings: {', '.join(result.get('warnings') or []) or 'none'}\n")
            for k, v in (result.get("summary") or {}).items():
                f.write(f"- {k}: {v}\n")
    return 2 if result["status"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
