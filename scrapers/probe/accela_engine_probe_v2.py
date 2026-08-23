#!/usr/bin/env python3
"""Accela engine probe v2: resolve site-visit rows to parent PM cases.

Read-only bench test. No production ingest, DB writes, Lovable, or CRM changes.

Flow:
  Louisville ArcGIS PM_SiteVisit_Violations
    -> newest unique site-visit child records
    -> cheap HTTP fetch of child Accela page
    -> parse Related Records for exact parent Property Maintenance Case URL
    -> Chromium-render parent case
    -> extract status/address/description/owner/mailing/parcel/source URL
    -> preserve child inspector comments + structured violation/citation context
    -> group by parcel to prove stacking
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ARCGIS_QUERY = (
    "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/"
    "PM_SiteVisit_Violations/FeatureServer/0/query"
)
ACCELA_BASE = "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx"
ACCELA_ROOT = "https://aca-prod.accela.com/LJCMG/"
GRISSOM_PARENT_URL = (
    "https://aca-prod.accela.com/LJCMG/Cap/CapDetail.aspx?Module=Enforcement&TabName=Enforcement"
    "&capID1=26REC&capID2=00000&capID3=E2186&agencyCode=LJCMG&IsToShowInspection="
)

FIELDS = [
    "B1_PER_ID1", "B1_PER_ID2", "B1_PER_ID3", "B1_ALT_ID",
    "FullAddress", "PartialAddress", "PARCEL_ID",
    "G6A_G6_STATUS", "G6A_G6_COMPL_DD", "G6A_G6_STATUS_DD",
    "GUIDE_ITEM_TEXT", "VIOLATION_CODE", "CitationAmount", "OccupancyStatus",
]


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip().strip("*").strip()


def child_url(cap1: str, cap2: str, cap3: str) -> str:
    q = {
        "Module": "Enforcement", "TabName": "Enforcement",
        "capID1": cap1, "capID2": cap2, "capID3": cap3,
        "agencyCode": "LJCMG", "IsToShowInspection": "",
    }
    return ACCELA_BASE + "?" + urlencode(q)


def parent_key(case_id: str) -> str | None:
    m = re.match(r"^(ENF-PMNT-\d{2}-\d+)(?:-\d+)?$", clean(case_id), re.I)
    return m.group(1).upper() if m else None


def epoch_date(v: Any) -> str | None:
    if not v:
        return None
    try:
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return None


def fetch_recent(limit: int) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": ",".join(FIELDS),
        "returnGeometry": "false",
        "orderByFields": "G6A_G6_COMPL_DD DESC",
        "resultRecordCount": str(limit),
        "f": "json",
    }
    r = requests.get(ARCGIS_QUERY, params=params, timeout=45)
    r.raise_for_status()
    p = r.json()
    if p.get("error"):
        raise RuntimeError(p["error"])
    return p.get("features", [])


def attrs(feature: dict) -> dict:
    return feature.get("attributes") or {}


def feature_row(feature: dict) -> dict | None:
    a = attrs(feature)
    case = clean(a.get("B1_ALT_ID"))
    pkey = parent_key(case)
    cap1, cap2, cap3 = [clean(a.get(x)) for x in ("B1_PER_ID1", "B1_PER_ID2", "B1_PER_ID3")]
    if not (case and pkey and cap1 and cap2 and cap3):
        return None
    citation = 0.0
    try:
        citation = float(a.get("CitationAmount") or 0)
    except Exception:
        pass
    return {
        "child_case": case,
        "parent_key": pkey,
        "cap1": cap1, "cap2": cap2, "cap3": cap3,
        "child_url": child_url(cap1, cap2, cap3),
        "address": clean(a.get("FullAddress") or a.get("PartialAddress")) or None,
        "parcel": clean(a.get("PARCEL_ID")) or None,
        "visit_status": clean(a.get("G6A_G6_STATUS")) or None,
        "visit_date": epoch_date(a.get("G6A_G6_COMPL_DD")),
        "status_date": epoch_date(a.get("G6A_G6_STATUS_DD")),
        "guide_text": clean(a.get("GUIDE_ITEM_TEXT")) or None,
        "violation_code": clean(a.get("VIOLATION_CODE")) or None,
        "citation": citation,
        "occupancy": clean(a.get("OccupancyStatus")) or None,
    }


def build_groups(features: list[dict]) -> list[dict]:
    groups: dict[str, dict] = {}
    for f in features:
        r = feature_row(f)
        if not r:
            continue
        k = r["parent_key"]
        if k not in groups:
            groups[k] = {
                "parent_key": k,
                "representative": r,
                "site_visit_rows": 0,
                "child_cases": [],
                "violation_codes": [],
                "guide_texts": [],
                "citation_total": 0.0,
                "parcel": r.get("parcel"),
                "address": r.get("address"),
                "occupancies": [],
            }
        g = groups[k]
        g["site_visit_rows"] += 1
        if r["child_case"] not in g["child_cases"]:
            g["child_cases"].append(r["child_case"])
        if r.get("violation_code") and r["violation_code"] not in g["violation_codes"]:
            g["violation_codes"].append(r["violation_code"])
        if r.get("guide_text") and r["guide_text"] not in g["guide_texts"]:
            g["guide_texts"].append(r["guide_text"])
        if r.get("occupancy") and r["occupancy"] not in g["occupancies"]:
            g["occupancies"].append(r["occupancy"])
        g["citation_total"] += r.get("citation") or 0
        if not g.get("parcel") and r.get("parcel"):
            g["parcel"] = r["parcel"]
    return list(groups.values())


def parse_parcel(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Parcel\s+Number\s*:\s*([A-Z0-9._-]+)", text, re.I)
    return clean(m.group(1)) if m else None


def parse_inspector_comments(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    vals: list[str] = []
    for label in soup.find_all(string=re.compile(r"Inspector\s+Comment\s*:", re.I)):
        parent = label.parent
        container = parent.parent if parent else None
        if container:
            txt = clean(container.get_text(" ", strip=True))
            txt = re.sub(r"^Inspector\s+Comment\s*:\s*", "", txt, flags=re.I)
            if txt and txt.lower() != "inspector comment:" and txt not in vals:
                vals.append(txt)
    # More precise fallback for Accela's two-column MoreDetail blocks.
    if not vals:
        pat = re.compile(
            r"Inspector Comment:</span></div><div[^>]*><span[^>]*>(.*?)</span>",
            re.I | re.S,
        )
        for raw in pat.findall(html):
            txt = clean(BeautifulSoup(raw, "html.parser").get_text(" ", strip=True))
            if txt and txt not in vals:
                vals.append(txt)
    return vals


def resolve_parent_from_html(html: str, expected_parent: str) -> tuple[str | None, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for cap in soup.find_all("caption"):
        if "related records" in clean(cap.get_text()).lower():
            table = cap.find_parent("table")
            break
    if table:
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            row_text = clean(tr.get_text(" ", strip=True))
            if "Property Maintenance Case" not in row_text:
                continue
            rec = None
            m = re.search(r"ENF-PMNT-\d{2}-\d+", row_text, re.I)
            if m:
                rec = m.group(0).upper()
            a = tr.find("a", href=re.compile(r"CapDetail\.aspx", re.I))
            if a and rec:
                return rec, urljoin(ACCELA_ROOT, a.get("href"))
    # Last-resort exact-parent regex over raw HTML.
    m = re.search(
        rf"{re.escape(expected_parent)}.*?href=\"([^\"]*CapDetail\.aspx\?[^\"]+)\"",
        html,
        re.I | re.S,
    )
    if m:
        return expected_parent, urljoin(ACCELA_ROOT, m.group(1).replace("&amp;", "&"))
    return None, None


def http_resolve_parent(session: requests.Session, group: dict) -> dict:
    rep = group["representative"]
    t0 = time.perf_counter()
    try:
        r = session.get(rep["child_url"], timeout=25)
        status = r.status_code
        r.raise_for_status()
        html = r.text
        parent_case, parent_url = resolve_parent_from_html(html, group["parent_key"])
        return {
            "ok": bool(parent_case and parent_url),
            "parent_case": parent_case,
            "parent_url": parent_url,
            "child_http_status": status,
            "child_http_seconds": round(time.perf_counter() - t0, 3),
            "child_parcel_html": parse_parcel(html),
            "inspector_comments": parse_inspector_comments(html),
            "reason": None if parent_url else "parent_link_not_found_in_child_html",
        }
    except Exception as e:
        return {
            "ok": False,
            "parent_case": None,
            "parent_url": None,
            "child_http_status": None,
            "child_http_seconds": round(time.perf_counter() - t0, 3),
            "child_parcel_html": None,
            "inspector_comments": [],
            "reason": f"child_http_{type(e).__name__}:{str(e)[:140]}",
        }


def first_match(patterns: list[str], text: str) -> str | None:
    for p in patterns:
        m = re.search(p, text, re.I | re.S)
        if m:
            return clean(m.group(1))
    return None


def parse_parent_text(text: str, html: str, expected_case: str) -> dict:
    case = first_match([r"Record\s+(ENF-PMNT-\d{2}-\d+)\s*:", r"(ENF-PMNT-\d{2}-\d+)"], text)
    status = first_match([r"Record\s*Status\s*:\s*([^\n]+)"], text)
    record_type = first_match([rf"{re.escape(case or expected_case)}\s*:\s*([^\n]+)"], text)
    description = first_match([
        r"Description\s*:\s*(.*?)\s*Owner\s*:",
        r"Description\s*:\s*(.*?)\s*More Details",
    ], text)
    owner_block = first_match([
        r"Owner\s*:\s*(.*?)\s*More Details",
        r"Owner\s*:\s*(.*?)\s*Parcel Information",
    ], text)
    owner_name = None
    owner_mailing = None
    if owner_block:
        lines = [clean(x) for x in owner_block.splitlines() if clean(x)]
        if len(lines) >= 2:
            owner_name = lines[0]
            owner_mailing = " ".join(lines[1:])
        else:
            m = re.match(r"^(.*?)(\s+(?:\d{1,6}\s+|PO BOX\s+|P O BOX\s+|C/O\s+).*)$", owner_block, re.I)
            if m:
                owner_name, owner_mailing = clean(m.group(1)), clean(m.group(2))
            else:
                owner_name = owner_block
    location = first_match([r"Location\s+(.*?)\s+Record Details", r"Location\s*:\s*(.*?)\s+Record Details"], text)
    parcel = parse_parcel(html)
    return {
        "case_number": case,
        "record_type": record_type,
        "record_status": status,
        "property_address": location,
        "description_raw": description,
        "owner_name": owner_name,
        "owner_mailing_address": owner_mailing,
        "parcel_id": parcel,
        "case_verified": bool(case and case.upper() == expected_case.upper()),
    }


def browser_parent(page, parent_case: str, parent_url: str, out_dir: Path, idx: int) -> tuple[dict | None, dict | None]:
    t0 = time.perf_counter()
    try:
        resp = page.goto(parent_url, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=7000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(400)
        text = page.locator("body").inner_text(timeout=5000)
        html = page.content()
        low = text.lower()
        if any(x in low for x in ("captcha", "verify you are human", "access denied", "cloudflare")):
            raise RuntimeError("challenge_or_access_block")
        parsed = parse_parent_text(text, html, parent_case)
        parsed.update({
            "source_url": parent_url,
            "final_url": page.url,
            "http_status": resp.status if resp else None,
            "parent_browser_seconds": round(time.perf_counter() - t0, 3),
            "page_title": page.title(),
        })
        if not parsed.get("case_verified"):
            reason = "parent_case_not_verified"
        elif clean(parsed.get("record_type")).lower() != "property maintenance case":
            reason = "not_parent_property_maintenance_case"
        elif not parsed.get("record_status"):
            reason = "parent_status_missing"
        else:
            return parsed, None
        stem = out_dir / f"failure_parent_{idx:02d}_{parent_case}"
        Path(str(stem) + ".html").write_text(html, encoding="utf-8", errors="ignore")
        Path(str(stem) + ".txt").write_text(text, encoding="utf-8", errors="ignore")
        try:
            page.screenshot(path=str(stem) + ".png", full_page=True)
        except Exception:
            pass
        return None, {"case": parent_case, "reason": reason, "url": parent_url}
    except Exception as e:
        try:
            text = page.locator("body").inner_text(timeout=2000)
            html = page.content()
        except Exception:
            text, html = "", ""
        stem = out_dir / f"failure_parent_{idx:02d}_{parent_case}"
        if html:
            Path(str(stem) + ".html").write_text(html, encoding="utf-8", errors="ignore")
        if text:
            Path(str(stem) + ".txt").write_text(text, encoding="utf-8", errors="ignore")
        return None, {"case": parent_case, "reason": f"parent_browser_{type(e).__name__}:{str(e)[:140]}", "url": parent_url}


def heuristic(desc: str | None, comments: list[str]) -> dict:
    t = " ".join([desc or ""] + comments).lower()
    mapping = {
        "vacancy_or_abandonment": ["vacant", "abandon", "unoccupied", "no one lives"],
        "unsecured": ["unsecured", "open door", "broken window", "boarded"],
        "structural": ["structural", "collapse", "foundation", "unsafe", "roof"],
        "fire_damage": ["fire damage", "burned", "burnt"],
        "utilities": ["no water", "no electric", "utilities off", "utility shut"],
        "persistent_neglect": ["all year", "overgrown", "trash", "debris", "deteriorat", "high grass"],
        "demolition": ["demol", "condemn"],
    }
    hits = [k for k, terms in mapping.items() if any(term in t for term in terms)]
    return {"prefilter_signals": hits}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-open", type=int, default=5)
    ap.add_argument("--arcgis-limit", type=int, default=500)
    ap.add_argument("--max-parent-attempts", type=int, default=25)
    ap.add_argument("--out", default="reports/accela_engine_v2")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()

    features = fetch_recent(args.arcgis_limit)
    groups = build_groups(features)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 compatible; TheReaper read-only validation probe"})

    # Always validate the user-supplied Grissom parent first, then discovered parents.
    parent_queue = [{
        "group": {
            "parent_key": "ENF-PMNT-26-019301", "site_visit_rows": 0,
            "child_cases": [], "violation_codes": [], "guide_texts": [],
            "citation_total": 0.0, "parcel": None,
            "address": "3600 GRISSOM WAY, LOUISVILLE KY 40229", "occupancies": [],
        },
        "resolved": {
            "ok": True, "parent_case": "ENF-PMNT-26-019301", "parent_url": GRISSOM_PARENT_URL,
            "child_http_status": None, "child_http_seconds": 0.0,
            "child_parcel_html": None, "inspector_comments": [], "reason": None,
        },
        "fixture": True,
    }]

    resolve_failures = []
    seen_parent = {"ENF-PMNT-26-019301"}
    http_resolve_total = 0.0
    for g in groups:
        if len(parent_queue) >= args.max_parent_attempts:
            break
        if g["parent_key"] in seen_parent:
            continue
        rr = http_resolve_parent(session, g)
        http_resolve_total += rr.get("child_http_seconds") or 0
        if not rr.get("ok"):
            resolve_failures.append({"case": g["parent_key"], "reason": rr.get("reason"), "child_url": g["representative"]["child_url"]})
            continue
        pc = rr["parent_case"]
        if pc in seen_parent:
            continue
        seen_parent.add(pc)
        parent_queue.append({"group": g, "resolved": rr, "fixture": False})

    selected = []
    parent_failures = []
    rendered = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1200}, locale="en-US")
        page = ctx.new_page()
        for idx, item in enumerate(parent_queue, 1):
            if len(selected) >= args.target_open:
                break
            g, rr = item["group"], item["resolved"]
            rec, fail = browser_parent(page, rr["parent_case"], rr["parent_url"], out, idx)
            if fail:
                parent_failures.append(fail)
                continue
            rec.update({
                "fixture": item["fixture"],
                "resolved_from_child_case": g["representative"].get("child_case") if g.get("representative") else None,
                "child_source_url": g["representative"].get("child_url") if g.get("representative") else None,
                "child_http_seconds": rr.get("child_http_seconds"),
                "inspector_comments": rr.get("inspector_comments") or [],
                "structured_arcgis_parcel": g.get("parcel"),
                "structured_arcgis_address": g.get("address"),
                "site_visit_rows_in_recent_window": g.get("site_visit_rows", 0),
                "child_cases_in_recent_window": g.get("child_cases", []),
                "violation_codes_in_recent_window": g.get("violation_codes", []),
                "violation_descriptions_in_recent_window": g.get("guide_texts", []),
                "citation_total_in_recent_window": round(g.get("citation_total", 0.0), 2),
                "occupancies_in_recent_window": g.get("occupancies", []),
            })
            if not rec.get("parcel_id"):
                rec["parcel_id"] = rr.get("child_parcel_html") or g.get("parcel")
            rec.update(heuristic(rec.get("description_raw"), rec.get("inspector_comments") or []))
            rendered.append(rec)
            if clean(rec.get("record_status")).lower() == "open" and rec.get("description_raw"):
                selected.append(rec)
            page.wait_for_timeout(250)
        ctx.close()
        browser.close()

    # Property-level stacking across selected records by parcel/address.
    by_property: dict[str, list[str]] = defaultdict(list)
    for r in selected:
        key = r.get("parcel_id") or clean(r.get("property_address") or r.get("structured_arcgis_address"))
        if key:
            by_property[key].append(r["case_number"])
    for r in selected:
        key = r.get("parcel_id") or clean(r.get("property_address") or r.get("structured_arcgis_address"))
        r["selected_open_parent_cases_same_property"] = len(set(by_property.get(key, []))) if key else 1

    grissom = next((x for x in rendered if x.get("fixture")), None)
    regression = {
        "case_number": bool(grissom and grissom.get("case_number") == "ENF-PMNT-26-019301"),
        "status_open": bool(grissom and clean(grissom.get("record_status")).lower() == "open"),
        "address": bool(grissom and "3600 GRISSOM WAY" in clean(grissom.get("property_address")).upper()),
        "description": bool(grissom and "HIGH GRASS ALL YEAR LONG" in clean(grissom.get("description_raw")).upper()),
        "owner": bool(grissom and "ABDELRAZEQ IBRAHEEM A" in clean(grissom.get("owner_name")).upper()),
        "parcel": bool(grissom and grissom.get("parcel_id")),
    }
    runtime = round(time.perf_counter() - started, 3)
    status = "PASS" if len(selected) >= args.target_open and all(regression.values()) else ("PARTIAL" if selected else "FAIL")
    report = {
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": runtime,
        "target_open_records": args.target_open,
        "arcgis_features_fetched": len(features),
        "unique_parent_groups_discovered": len(groups),
        "parent_links_resolved": len(parent_queue),
        "child_http_resolution_total_seconds": round(http_resolve_total, 3),
        "parents_rendered": len(rendered),
        "selected_open_records": selected,
        "grissom_regression": regression,
        "resolve_failures": resolve_failures,
        "parent_failures": parent_failures,
        "notes": [
            "Read-only bench test; no production ingest occurs.",
            "ArcGIS child/site-visit records are used for discovery and violation context; Related Records resolves the exact parent Property Maintenance Case URL.",
            "Cheap HTTP is used for child-page parent-link resolution; Chromium is reserved for parent-case validation/extraction.",
            "No CAPTCHA solver, stealth plugin, proxy rotation, or anti-bot circumvention is used.",
            "AI distress classification is performed after extraction in ChatGPT; prefilter_signals are deterministic keywords only.",
        ],
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md = [
        "# Accela Engine Probe v2", "", f"Status: **{status}**", f"Runtime: **{runtime}s**",
        f"Open parent cases extracted: **{len(selected)}**", "",
    ]
    for i, r in enumerate(selected, 1):
        md += [
            f"## {i}. {r.get('case_number')}",
            f"- Status: {r.get('record_status')}",
            f"- Address: {r.get('property_address') or r.get('structured_arcgis_address')}",
            f"- Description: {r.get('description_raw')}",
            f"- Owner: {r.get('owner_name')}",
            f"- Owner mailing: {r.get('owner_mailing_address')}",
            f"- Parcel: {r.get('parcel_id')}",
            f"- Parent URL: {r.get('source_url')}",
            f"- Inspector comments: {' | '.join(r.get('inspector_comments') or []) or 'none'}",
            f"- Recent violation rows: {r.get('site_visit_rows_in_recent_window')}",
            f"- Recent citations total: ${r.get('citation_total_in_recent_window')}",
            f"- Parent browser seconds: {r.get('parent_browser_seconds')}", "",
        ]
    (out / "report.md").write_text("\n".join(md), encoding="utf-8")
    print(json.dumps({
        "status": status, "runtime_seconds": runtime, "selected_count": len(selected),
        "grissom_regression": regression,
        "records": [{
            "case": r.get("case_number"), "status": r.get("record_status"),
            "address": r.get("property_address") or r.get("structured_arcgis_address"),
            "description": r.get("description_raw"), "owner": r.get("owner_name"),
            "owner_mailing": r.get("owner_mailing_address"), "parcel": r.get("parcel_id"),
            "parent_url": r.get("source_url"), "inspector_comments": r.get("inspector_comments"),
            "site_visit_rows": r.get("site_visit_rows_in_recent_window"),
            "citation_total": r.get("citation_total_in_recent_window"),
            "seconds": r.get("parent_browser_seconds"),
        } for r in selected],
        "resolve_failure_count": len(resolve_failures), "parent_failure_count": len(parent_failures),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
