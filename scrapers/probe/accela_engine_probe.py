#!/usr/bin/env python3
"""Isolated Jefferson County Accela extraction bench test.

This probe does NOT write to TheReaper, Supabase, Lovable, or production ingest.
It discovers recent Louisville code-enforcement records from the official ArcGIS
feed, constructs the exact public Accela detail URLs using B1_PER_ID1/2/3, then
uses real Chromium (Playwright) to render JavaScript and extract public fields.

Outputs JSON + Markdown + failure diagnostics under reports/accela_engine/.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ARCGIS_QUERY = (
    "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/"
    "PM_SiteVisit_Violations/FeatureServer/0/query"
)
ACCELA_BASE = "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx"

GRISSOM = {
    "case_number": "ENF-PMNT-26-019301",
    "cap1": "26REC",
    "cap2": "00000",
    "cap3": "E2186",
    "arcgis_address": "3600 GRISSOM WAY, LOUISVILLE KY 40229",
    "arcgis_parcel": None,
    "arcgis_description": None,
    "arcgis_status": None,
    "fixture": True,
}

FIELDS = [
    "B1_PER_ID1", "B1_PER_ID2", "B1_PER_ID3", "B1_ALT_ID",
    "FullAddress", "PartialAddress", "PARCEL_ID",
    "G6A_G6_STATUS", "G6A_G6_COMPL_DD", "G6A_G6_STATUS_DD",
    "GUIDE_ITEM_TEXT", "VIOLATION_CODE", "CitationAmount",
    "OccupancyStatus",
]


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip().strip("*").strip()


def detail_url(cap1: str, cap2: str, cap3: str) -> str:
    q = {
        "Module": "Enforcement",
        "TabName": "Enforcement",
        "capID1": cap1,
        "capID2": cap2,
        "capID3": cap3,
        "agencyCode": "LJCMG",
        "IsToShowInspection": "",
    }
    return ACCELA_BASE + "?" + urlencode(q)


def fetch_recent_features(limit: int = 500) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": ",".join(FIELDS),
        "returnGeometry": "false",
        "orderByFields": "G6A_G6_COMPL_DD DESC",
        "resultRecordCount": str(limit),
        "f": "json",
    }
    r = requests.get(ARCGIS_QUERY, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    if payload.get("error"):
        raise RuntimeError(f"ArcGIS error: {payload['error']}")
    return payload.get("features", [])


def epoch_date(v: Any) -> str | None:
    if not v:
        return None
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return None


def candidate_from_feature(feature: dict) -> dict | None:
    a = feature.get("attributes") or {}
    cap1, cap2, cap3 = (clean(a.get(k)) for k in ("B1_PER_ID1", "B1_PER_ID2", "B1_PER_ID3"))
    case = clean(a.get("B1_ALT_ID"))
    if not (cap1 and cap2 and cap3 and case):
        return None
    return {
        "case_number": case,
        "cap1": cap1,
        "cap2": cap2,
        "cap3": cap3,
        "arcgis_address": clean(a.get("FullAddress") or a.get("PartialAddress")),
        "arcgis_parcel": clean(a.get("PARCEL_ID")) or None,
        "arcgis_description": clean(a.get("GUIDE_ITEM_TEXT")) or None,
        "arcgis_status": clean(a.get("G6A_G6_STATUS")) or None,
        "arcgis_date": epoch_date(a.get("G6A_G6_COMPL_DD")),
        "arcgis_status_date": epoch_date(a.get("G6A_G6_STATUS_DD")),
        "violation_code": clean(a.get("VIOLATION_CODE")) or None,
        "citation_amount": a.get("CitationAmount"),
        "occupancy": clean(a.get("OccupancyStatus")) or None,
        "fixture": False,
    }


def discover_candidates(features: list[dict], max_candidates: int) -> list[dict]:
    # Grissom is always first regression fixture. Then use newest unique Accela cases.
    out = [dict(GRISSOM)]
    seen = {(GRISSOM["cap1"], GRISSOM["cap2"], GRISSOM["cap3"])}
    # ArcGIS is violation-level, so a case may repeat. Merge structured rows by case.
    merged: dict[tuple[str, str, str], dict] = {}
    for f in features:
        c = candidate_from_feature(f)
        if not c:
            continue
        key = (c["cap1"], c["cap2"], c["cap3"])
        if key in seen:
            continue
        if key not in merged:
            c["arcgis_descriptions"] = []
            c["violation_codes"] = []
            c["citation_values"] = []
            merged[key] = c
        m = merged[key]
        if c.get("arcgis_description") and c["arcgis_description"] not in m["arcgis_descriptions"]:
            m["arcgis_descriptions"].append(c["arcgis_description"])
        if c.get("violation_code") and c["violation_code"] not in m["violation_codes"]:
            m["violation_codes"].append(c["violation_code"])
        try:
            val = float(c.get("citation_amount") or 0)
            if val:
                m["citation_values"].append(val)
        except Exception:
            pass
    out.extend(list(merged.values())[: max(0, max_candidates - 1)])
    return out


def body_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def first_match(patterns: list[str], text: str, flags=re.I | re.S) -> str | None:
    for p in patterns:
        m = re.search(p, text, flags)
        if m:
            return clean(m.group(1))
    return None


def parse_detail_text(text: str, candidate: dict) -> dict:
    case = first_match([r"(ENF-PMNT-\d{2}-\d+)", r"Record\s*(?:Number|#)\s*:?\s*([^\n]+)"], text)
    status = first_match([r"Record\s*Status\s*:\s*([^\n]+)"], text)

    # Accela mobile/desktop layouts both expose these labels in DOM text.
    description = first_match([
        r"Description\s*:\s*(.*?)\s*Owner\s*:",
        r"Description\s*:\s*(.*?)\s*More Details",
    ], text)
    if description:
        description = re.sub(r"\s+", " ", description).strip()

    owner_block = first_match([
        r"Owner\s*:\s*(.*?)\s*More Details",
        r"Owner\s*:\s*(.*?)\s*Parcel(?:\s+Number)?\s*:",
    ], text)
    owner_name = None
    owner_mailing = None
    if owner_block:
        # Re-split from original-ish normalized content. Name is first logical line/segment;
        # addresses usually begin with a number, PO BOX, or C/O.
        bits = [clean(x) for x in re.split(r"[\r\n]+", owner_block) if clean(x)]
        if len(bits) <= 1:
            # innerText normalization can collapse lines; infer break before street number.
            m = re.match(r"^(.*?)(\s+(?:\d{1,6}\s+|PO BOX\s+|P O BOX\s+|C/O\s+).*)$", owner_block, re.I)
            if m:
                owner_name, owner_mailing = clean(m.group(1)), clean(m.group(2))
            else:
                owner_name = clean(owner_block)
        else:
            owner_name = bits[0]
            owner_mailing = ", ".join(bits[1:]) or None

    location = first_match([
        r"Location\s+(.*?)\s+Record Details",
        r"Location\s*:\s*(.*?)\s+Record Details",
    ], text)
    if location:
        location = re.sub(r"\s+", " ", location).strip().strip("*").strip()

    parcel = first_match([
        r"Parcel\s*(?:Number|No\.?|#|ID)?\s*:\s*([^\n]+)",
        r"Parcel\s*(?:Number|No\.?|#|ID)?\s+([A-Z0-9._-]{4,})",
    ], text)
    if parcel:
        parcel = parcel.split(" ")[0].strip("* ,;") if len(parcel) > 40 else parcel.strip("* ,;")

    return {
        "case_number_extracted": case,
        "record_status": status,
        "property_address_extracted": location,
        "description_raw": description,
        "owner_name": owner_name,
        "owner_mailing_address": owner_mailing,
        "parcel_id_extracted": parcel,
        "case_number_expected": candidate.get("case_number"),
    }


def save_failure(out_dir: Path, idx: int, candidate: dict, page, reason: str, text: str) -> dict:
    stem = f"failure_{idx:02d}_{re.sub(r'[^A-Za-z0-9_-]+', '_', candidate.get('case_number') or 'unknown')}"
    screenshot = out_dir / f"{stem}.png"
    html_file = out_dir / f"{stem}.html"
    text_file = out_dir / f"{stem}.txt"
    try:
        page.screenshot(path=str(screenshot), full_page=True)
    except Exception:
        screenshot = None
    try:
        html_file.write_text(page.content(), encoding="utf-8", errors="ignore")
    except Exception:
        html_file = None
    try:
        text_file.write_text(text or "", encoding="utf-8", errors="ignore")
    except Exception:
        text_file = None
    return {
        "case_number": candidate.get("case_number"),
        "reason": reason,
        "url": detail_url(candidate["cap1"], candidate["cap2"], candidate["cap3"]),
        "screenshot": str(screenshot) if screenshot else None,
        "html": str(html_file) if html_file else None,
        "text": str(text_file) if text_file else None,
    }


def extract_candidate(page, candidate: dict, out_dir: Path, idx: int) -> tuple[dict | None, dict | None]:
    url = detail_url(candidate["cap1"], candidate["cap2"], candidate["cap3"])
    started = time.perf_counter()
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
        http_status = response.status if response else None
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(800)
        text = body_text(page)
        low = text.lower()
        if any(x in low for x in ["captcha", "access denied", "verify you are human", "cloudflare"]):
            return None, save_failure(out_dir, idx, candidate, page, "challenge_or_access_block", text)

        # More Details may contain parcel and additional stable identifiers.
        try:
            md = page.get_by_text(re.compile(r"More Details", re.I)).first
            if md.count() > 0:
                md.click(timeout=2500)
                page.wait_for_timeout(500)
                text = body_text(page)
        except Exception:
            pass

        parsed = parse_detail_text(text, candidate)
        elapsed = round(time.perf_counter() - started, 3)
        parsed.update({
            "source_url": url,
            "final_url": page.url,
            "http_status": http_status,
            "elapsed_seconds": elapsed,
            "page_title": page.title(),
            "arcgis_address": candidate.get("arcgis_address"),
            "arcgis_parcel": candidate.get("arcgis_parcel"),
            "arcgis_descriptions": candidate.get("arcgis_descriptions") or ([candidate.get("arcgis_description")] if candidate.get("arcgis_description") else []),
            "arcgis_status": candidate.get("arcgis_status"),
            "arcgis_date": candidate.get("arcgis_date"),
            "violation_codes": candidate.get("violation_codes") or ([candidate.get("violation_code")] if candidate.get("violation_code") else []),
            "citation_total_in_recent_feed": round(sum(candidate.get("citation_values") or []), 2),
            "occupancy": candidate.get("occupancy"),
            "fixture": bool(candidate.get("fixture")),
        })
        parsed["case_number_verified"] = bool(
            parsed.get("case_number_extracted") and
            parsed.get("case_number_expected") and
            parsed["case_number_extracted"].upper() == parsed["case_number_expected"].upper()
        )
        parsed["is_open"] = clean(parsed.get("record_status")).lower() == "open"
        parsed["detail_description_extracted"] = bool(parsed.get("description_raw"))
        parsed["owner_extracted"] = bool(parsed.get("owner_name"))
        parsed["parcel_available"] = bool(parsed.get("parcel_id_extracted") or parsed.get("arcgis_parcel"))
        parsed["property_key"] = parsed.get("parcel_id_extracted") or parsed.get("arcgis_parcel") or parsed.get("property_address_extracted") or parsed.get("arcgis_address")

        # Require actual rendered Accela proof, not just ArcGIS fallback.
        if not parsed.get("record_status"):
            return None, save_failure(out_dir, idx, candidate, page, "record_status_not_extracted", text)
        if not parsed.get("case_number_verified"):
            return None, save_failure(out_dir, idx, candidate, page, "case_number_not_verified", text)
        return parsed, None
    except PlaywrightTimeoutError:
        text = body_text(page)
        return None, save_failure(out_dir, idx, candidate, page, "timeout", text)
    except Exception as e:
        text = body_text(page)
        f = save_failure(out_dir, idx, candidate, page, f"exception:{type(e).__name__}:{str(e)[:160]}", text)
        return None, f


def heuristic_signals(desc: str | None) -> dict:
    """Non-AI prefilter only. Final AI classification is intentionally done outside runner."""
    t = (desc or "").lower()
    mapping = {
        "vacancy_or_abandonment": ["vacant", "abandon", "unoccupied", "no one lives"],
        "unsecured": ["unsecured", "open door", "broken window", "boarded"],
        "structural": ["structural", "roof", "collapse", "foundation", "unsafe structure"],
        "fire_damage": ["fire damage", "burned", "burnt"],
        "utilities": ["no water", "no electric", "utilities off", "utility shut"],
        "severe_neglect": ["all year", "overgrown", "trash", "debris", "deteriorat"],
        "demolition": ["demol", "condemn"],
    }
    hits = [name for name, terms in mapping.items() if any(term in t for term in terms)]
    return {"prefilter_signals": hits, "prefilter_hit": bool(hits)}


def write_markdown(report: dict, path: Path) -> None:
    lines = [
        "# Accela Engine Probe",
        "",
        f"- Status: **{report['status']}**",
        f"- Open records successfully extracted: **{len(report['selected_open_records'])}**",
        f"- Candidates attempted: **{report['candidates_attempted']}**",
        f"- Runtime: **{report['runtime_seconds']}s**",
        f"- Failures: **{len(report['failures'])}**",
        "",
        "## Selected open records",
        "",
    ]
    for i, r in enumerate(report["selected_open_records"], 1):
        lines += [
            f"### {i}. {r.get('case_number_extracted')}",
            f"- Status: {r.get('record_status')}",
            f"- Address: {r.get('property_address_extracted') or r.get('arcgis_address')}",
            f"- Description: {r.get('description_raw') or 'NOT EXTRACTED'}",
            f"- Owner: {r.get('owner_name') or 'NOT EXTRACTED'}",
            f"- Owner mailing: {r.get('owner_mailing_address') or 'NOT EXTRACTED'}",
            f"- Parcel: {r.get('parcel_id_extracted') or r.get('arcgis_parcel') or 'NOT EXTRACTED'}",
            f"- Exact source URL: {r.get('source_url')}",
            f"- Seconds: {r.get('elapsed_seconds')}",
            f"- Prefilter signals: {', '.join(r.get('prefilter_signals') or []) or 'none'}",
            "",
        ]
    if report.get("failures"):
        lines += ["## Failure log", ""]
        for f in report["failures"]:
            lines.append(f"- {f.get('case_number')}: `{f.get('reason')}`")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-open", type=int, default=5)
    ap.add_argument("--max-candidates", type=int, default=35)
    ap.add_argument("--out", default="reports/accela_engine")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    failures: list[dict] = []
    all_extracted: list[dict] = []
    selected: list[dict] = []

    try:
        features = fetch_recent_features(500)
        candidates = discover_candidates(features, args.max_candidates)
        arcgis_error = None
    except Exception as e:
        features = []
        candidates = [dict(GRISSOM)]
        arcgis_error = f"{type(e).__name__}: {e}"

    # Recent-feed context used only for stacking proof; production will query parcel history directly.
    parcel_case_counts: dict[str, set[str]] = defaultdict(set)
    parcel_row_counts: Counter[str] = Counter()
    for f in features:
        c = candidate_from_feature(f)
        if not c or not c.get("arcgis_parcel"):
            continue
        p = c["arcgis_parcel"]
        parcel_row_counts[p] += 1
        parcel_case_counts[p].add(c["case_number"])

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 1200},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        for idx, candidate in enumerate(candidates, 1):
            if len(selected) >= args.target_open:
                break
            rec, fail = extract_candidate(page, candidate, out_dir, idx)
            if fail:
                failures.append(fail)
                continue
            if not rec:
                continue
            all_extracted.append(rec)
            if rec.get("is_open") and rec.get("detail_description_extracted"):
                pkey = rec.get("parcel_id_extracted") or rec.get("arcgis_parcel")
                rec.update(heuristic_signals(rec.get("description_raw")))
                rec["recent_feed_rows_same_parcel"] = parcel_row_counts.get(pkey, 0) if pkey else 0
                rec["recent_feed_unique_cases_same_parcel"] = len(parcel_case_counts.get(pkey, set())) if pkey else 0
                selected.append(rec)
            # Respectful, low-rate bench test. No stealth/CAPTCHA bypassing.
            page.wait_for_timeout(350)
        context.close()
        browser.close()

    grissom = next((r for r in all_extracted if r.get("fixture")), None)
    regression = {
        "case_number": bool(grissom and grissom.get("case_number_extracted") == "ENF-PMNT-26-019301"),
        "status_open": bool(grissom and grissom.get("record_status", "").lower() == "open"),
        "address": bool(grissom and "3600 GRISSOM WAY" in (grissom.get("property_address_extracted") or grissom.get("arcgis_address") or "").upper()),
        "description": bool(grissom and "HIGH GRASS ALL YEAR LONG" in (grissom.get("description_raw") or "").upper()),
        "owner": bool(grissom and "ABDELRAZEQ IBRAHEEM A" in (grissom.get("owner_name") or "").upper()),
    }

    runtime = round(time.perf_counter() - start, 3)
    if len(selected) >= args.target_open and all(regression.values()):
        status = "PASS"
    elif selected:
        status = "PARTIAL"
    else:
        status = "FAIL"

    report = {
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": runtime,
        "target_open_records": args.target_open,
        "candidates_available": len(candidates),
        "candidates_attempted": len(all_extracted) + len(failures),
        "arcgis_discovery_error": arcgis_error,
        "grissom_regression": regression,
        "selected_open_records": selected,
        "all_successfully_rendered_records": all_extracted,
        "failures": failures,
        "notes": [
            "This is an isolated read-only bench test; no production ingestion occurs.",
            "Chromium executes Accela JavaScript; no CAPTCHA bypass, stealth plugin, proxy rotation, or anti-bot circumvention is used.",
            "The keyword prefilter is NOT the AI classification. AI classification is performed from description_raw after this report is returned for review.",
            "recent_feed_* stacking fields are proof-of-concept counts within the fetched recent ArcGIS window, not lifetime case totals.",
        ],
    }
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    write_markdown(report, out_dir / "report.md")

    print("ACCELA_ENGINE_PROBE_BEGIN")
    print(json.dumps({
        "status": status,
        "runtime_seconds": runtime,
        "selected_count": len(selected),
        "attempted": report["candidates_attempted"],
        "failure_count": len(failures),
        "grissom_regression": regression,
        "records": [
            {
                "case": r.get("case_number_extracted"),
                "status": r.get("record_status"),
                "address": r.get("property_address_extracted") or r.get("arcgis_address"),
                "description": r.get("description_raw"),
                "owner": r.get("owner_name"),
                "owner_mailing": r.get("owner_mailing_address"),
                "parcel": r.get("parcel_id_extracted") or r.get("arcgis_parcel"),
                "source_url": r.get("source_url"),
                "seconds": r.get("elapsed_seconds"),
                "recent_same_parcel_cases": r.get("recent_feed_unique_cases_same_parcel"),
            }
            for r in selected
        ],
        "failures": [{"case": f.get("case_number"), "reason": f.get("reason")} for f in failures],
    }, indent=2, ensure_ascii=False))
    print("ACCELA_ENGINE_PROBE_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
