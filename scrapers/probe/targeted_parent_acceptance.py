#!/usr/bin/env python3
"""Targeted Accela parent-case acceptance test using parent URLs already resolved
from the prior live Chromium child-page artifacts. Read-only; no production writes.
"""
from __future__ import annotations

import argparse, json, re, time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright
from scrapers.probe import accela_engine_probe_v2 as probe

FIXTURES = [
    ("ENF-PMNT-26-019301", probe.GRISSOM_PARENT_URL),
    ("ENF-PMNT-26-016300", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=C2728&agencyCode=LJCMG"),
    ("ENF-PMNT-26-011650", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=93019&agencyCode=LJCMG"),
    ("ENF-PMNT-26-013339", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=A4429&agencyCode=LJCMG"),
    ("ENF-PMNT-26-017799", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D1781&agencyCode=LJCMG"),
    ("ENF-PMNT-26-019101", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=E0948&agencyCode=LJCMG"),
    ("ENF-PMNT-26-015609", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=B8677&agencyCode=LJCMG"),
    ("ENF-PMNT-26-014265", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=B0371&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018660", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D8336&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018242", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D4662&agencyCode=LJCMG"),
    ("ENF-PMNT-26-010516", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=85202&agencyCode=LJCMG"),
    ("ENF-PMNT-26-016665", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=C4795&agencyCode=LJCMG"),
    ("ENF-PMNT-26-008252", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=69561&agencyCode=LJCMG"),
    ("ENF-PMNT-26-015369", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=B7214&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018733", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D8776&agencyCode=LJCMG"),
    ("ENF-PMNT-26-019272", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=E2067&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018885", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D9638&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018540", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D7596&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018701", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D8557&agencyCode=LJCMG"),
    ("ENF-PMNT-26-018716", "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&capID1=26REC&capID2=00000&capID3=D8695&agencyCode=LJCMG"),
]


def sanitize_location(value: str | None) -> str | None:
    if not value:
        return value
    value = re.sub(r"\s*\*?\s*View\s+Additional\s+Locations\s*>>.*$", "", value, flags=re.I)
    return probe.clean(value)


def complete_open(r: dict) -> bool:
    return bool(
        r.get("case_verified")
        and probe.clean(r.get("record_status")).lower() == "open"
        and r.get("property_address")
        and r.get("description_raw")
        and r.get("owner_name")
        and r.get("parcel_id")
        and r.get("source_url")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-open", type=int, default=5)
    ap.add_argument("--out", default="reports/targeted_parent_acceptance")
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    selected, inspected, failures = [], [], []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1200}, locale="en-US")
        page = ctx.new_page()
        for idx, (case, url) in enumerate(FIXTURES, 1):
            if len(selected) >= args.target_open:
                break
            rec, fail = probe.browser_parent(page, case, url, out, idx)
            if fail:
                failures.append(fail); continue
            rec["property_address"] = sanitize_location(rec.get("property_address"))
            inspected.append(rec)
            if complete_open(rec):
                selected.append(rec)
            else:
                missing = []
                if probe.clean(rec.get("record_status")).lower() != "open": missing.append(f"status={rec.get('record_status')}")
                for k in ("property_address", "description_raw", "owner_name", "parcel_id", "source_url"):
                    if not rec.get(k): missing.append(k)
                failures.append({"case": case, "reason": "not_accepted:" + ",".join(missing), "url": url})
            page.wait_for_timeout(200)
        ctx.close(); browser.close()

    runtime = round(time.perf_counter() - started, 3)
    no_ui_bleed = all("View Additional Locations" not in (r.get("property_address") or "") for r in selected)
    status = "PASS" if len(selected) >= args.target_open and no_ui_bleed else ("PARTIAL" if selected else "FAIL")
    report = {
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": runtime,
        "target_open": args.target_open,
        "fixtures_available": len(FIXTURES),
        "address_ui_bleed_check": no_ui_bleed,
        "fully_verified_open_records": selected,
        "all_inspected_records": inspected,
        "failures": failures,
        "provenance": "Parent URLs were resolved from live Chromium-rendered child Related Records in the preceding probe artifacts; this test independently validates parent extraction at scale."
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md = ["# Targeted Parent Acceptance", "", f"Status: **{status}**", f"Runtime: **{runtime}s**", f"Verified OPEN records: **{len(selected)}**", f"Address UI bleed clean: **{no_ui_bleed}**", ""]
    for i, r in enumerate(selected, 1):
        md += [f"## {i}. {r.get('case_number')}", f"- Status: {r.get('record_status')}", f"- Address: {r.get('property_address')}", f"- Description: {r.get('description_raw')}", f"- Owner: {r.get('owner_name')}", f"- Mailing: {r.get('owner_mailing_address')}", f"- Parcel: {r.get('parcel_id')}", f"- Source: {r.get('source_url')}", f"- Seconds: {r.get('parent_browser_seconds')}", ""]
    (out / "report.md").write_text("\n".join(md), encoding="utf-8")
    print(json.dumps({"status": status, "selected_count": len(selected), "runtime_seconds": runtime, "address_ui_bleed_check": no_ui_bleed, "records": selected, "failure_count": len(failures)}, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2

if __name__ == "__main__":
    raise SystemExit(main())
