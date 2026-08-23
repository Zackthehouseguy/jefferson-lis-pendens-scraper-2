#!/usr/bin/env python3
"""Accela engine probe v4: Chromium child -> parent resolution.

Read-only bench test. No production ingest, DB writes, Lovable, or CRM changes.

Flow:
  Louisville ArcGIS PM_SiteVisit_Violations
    -> newest unique parent groups
    -> real Chromium opens child/site-visit record
    -> wait for JS Related Records tree
    -> resolve exact parent Property Maintenance Case URL
    -> extract child parcel + inspector comments
    -> same Chromium opens parent case
    -> verify OPEN + address + description + owner + parcel + source URL
    -> stop after target number of fully verified open parent cases
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from scrapers.probe import accela_engine_probe_v2 as probe


def clean(v):
    return probe.clean(v)


def resolve_parent_from_rendered(html: str, child_url: str, expected_parent: str):
    soup = BeautifulSoup(html, "html.parser")
    for cap in soup.find_all("caption"):
        if "related records" not in clean(cap.get_text()).lower():
            continue
        table = cap.find_parent("table")
        if not table:
            continue
        for tr in table.find_all("tr"):
            row_text = clean(tr.get_text(" ", strip=True))
            if "Property Maintenance Case" not in row_text:
                continue
            m = re.search(r"ENF-PMNT-\d{2}-\d+", row_text, re.I)
            if not m:
                continue
            parent_case = m.group(0).upper()
            if expected_parent and parent_case != expected_parent.upper():
                continue
            a = tr.find("a", href=re.compile(r"CapDetail\.aspx", re.I))
            if a and a.get("href"):
                return parent_case, urljoin(child_url, a.get("href"))
    # raw fallback when the table was injected but BeautifulSoup structure is odd
    m = re.search(
        rf"{re.escape(expected_parent)}.*?Property Maintenance Case.*?href=\"([^\"]*CapDetail\.aspx\?[^\"]+)\"",
        html,
        re.I | re.S,
    )
    if m:
        return expected_parent.upper(), urljoin(child_url, m.group(1).replace("&amp;", "&"))
    return None, None


def render_child_and_resolve(page, group: dict, out: Path, idx: int):
    rep = group["representative"]
    child_url = rep["child_url"]
    expected = group["parent_key"]
    t0 = time.perf_counter()
    try:
        resp = page.goto(child_url, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=7000)
        except PlaywrightTimeoutError:
            pass

        # Accela usually fills Related Records asynchronously. Give it a short
        # bounded wait, and explicitly invoke the public page function if needed.
        found = False
        for attempt in range(2):
            try:
                page.get_by_text(expected, exact=True).first.wait_for(state="attached", timeout=4500)
                found = True
                break
            except Exception:
                try:
                    page.evaluate("""
                        () => {
                          if (typeof ExpandRelatedPermitSection === 'function') {
                            ExpandRelatedPermitSection(true);
                            return true;
                          }
                          return false;
                        }
                    """)
                except Exception:
                    pass
                page.wait_for_timeout(900)

        html = page.content()
        text = page.locator("body").inner_text(timeout=5000)
        low = text.lower()
        if any(x in low for x in ("captcha", "verify you are human", "access denied", "cloudflare")):
            raise RuntimeError("challenge_or_access_block")

        parent_case, parent_url = resolve_parent_from_rendered(html, child_url, expected)
        elapsed = round(time.perf_counter() - t0, 3)
        if not (parent_case and parent_url):
            stem = out / f"child_fail_{idx:02d}_{rep['child_case']}"
            Path(str(stem) + ".html").write_text(html, encoding="utf-8", errors="ignore")
            Path(str(stem) + ".txt").write_text(text, encoding="utf-8", errors="ignore")
            try:
                page.screenshot(path=str(stem) + ".png", full_page=True)
            except Exception:
                pass
            return None, {
                "stage": "child_resolve",
                "child_case": rep["child_case"],
                "expected_parent": expected,
                "reason": "parent_link_not_found_after_js",
                "http_status": resp.status if resp else None,
                "seconds": elapsed,
                "child_url": child_url,
                "related_text_seen": found,
            }

        return {
            "parent_case": parent_case,
            "parent_url": parent_url,
            "child_case": rep["child_case"],
            "child_url": child_url,
            "child_http_status": resp.status if resp else None,
            "child_browser_seconds": elapsed,
            "child_parcel": probe.parse_parcel(html),
            "inspector_comments": probe.parse_inspector_comments(html),
        }, None
    except Exception as e:
        return None, {
            "stage": "child_resolve",
            "child_case": rep.get("child_case"),
            "expected_parent": expected,
            "reason": f"{type(e).__name__}:{str(e)[:180]}",
            "seconds": round(time.perf_counter() - t0, 3),
            "child_url": child_url,
        }


def complete_enough(rec: dict) -> bool:
    return bool(
        rec.get("case_verified")
        and clean(rec.get("record_status")).lower() == "open"
        and rec.get("property_address")
        and rec.get("description_raw")
        and rec.get("owner_name")
        and rec.get("parcel_id")
        and rec.get("source_url")
    )


def add_context(rec: dict, group: dict, resolved: dict, fixture=False):
    rec.update({
        "fixture": fixture,
        "resolved_from_child_case": resolved.get("child_case") if resolved else None,
        "child_source_url": resolved.get("child_url") if resolved else None,
        "child_browser_seconds": resolved.get("child_browser_seconds", 0.0) if resolved else 0.0,
        "inspector_comments": resolved.get("inspector_comments", []) if resolved else [],
        "structured_arcgis_parcel": group.get("parcel"),
        "structured_arcgis_address": group.get("address"),
        "site_visit_rows_in_recent_window": group.get("site_visit_rows", 0),
        "child_cases_in_recent_window": group.get("child_cases", []),
        "violation_codes_in_recent_window": group.get("violation_codes", []),
        "violation_descriptions_in_recent_window": group.get("guide_texts", []),
        "citation_total_in_recent_window": round(group.get("citation_total", 0.0), 2),
        "occupancies_in_recent_window": group.get("occupancies", []),
    })
    if not rec.get("parcel_id"):
        rec["parcel_id"] = (resolved or {}).get("child_parcel") or group.get("parcel")
    rec.update(probe.heuristic(rec.get("description_raw"), rec.get("inspector_comments") or []))
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-open", type=int, default=5)
    ap.add_argument("--arcgis-limit", type=int, default=500)
    ap.add_argument("--max-parent-attempts", type=int, default=15)
    ap.add_argument("--out", default="reports/accela_engine_v4")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()

    features = probe.fetch_recent(args.arcgis_limit)
    groups = probe.build_groups(features)
    selected = []
    rendered = []
    failures = []
    attempted_parent_keys = set()

    grissom_group = {
        "parent_key": "ENF-PMNT-26-019301",
        "site_visit_rows": 0,
        "child_cases": [],
        "violation_codes": [],
        "guide_texts": [],
        "citation_total": 0.0,
        "parcel": None,
        "address": "3600 GRISSOM WAY, LOUISVILLE KY 40229",
        "occupancies": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1200}, locale="en-US")
        page = ctx.new_page()

        # 1) Regression fixture first.
        rec, fail = probe.browser_parent(page, "ENF-PMNT-26-019301", probe.GRISSOM_PARENT_URL, out, 0)
        if fail:
            failures.append({"stage": "grissom_parent", **fail})
        else:
            rec = add_context(rec, grissom_group, {}, fixture=True)
            rendered.append(rec)
            if complete_enough(rec):
                selected.append(rec)
        attempted_parent_keys.add("ENF-PMNT-26-019301")

        # 2) Real recent child records -> JS Related Records -> parent case.
        parent_attempts = 0
        for idx, group in enumerate(groups, 1):
            if len(selected) >= args.target_open:
                break
            if parent_attempts >= args.max_parent_attempts:
                break
            if group["parent_key"] in attempted_parent_keys:
                continue
            attempted_parent_keys.add(group["parent_key"])
            parent_attempts += 1

            resolved, child_fail = render_child_and_resolve(page, group, out, idx)
            if child_fail:
                failures.append(child_fail)
                continue

            parent_case = resolved["parent_case"]
            parent_url = resolved["parent_url"]
            parent_rec, parent_fail = probe.browser_parent(page, parent_case, parent_url, out, idx)
            if parent_fail:
                failures.append({"stage": "parent_extract", **parent_fail})
                continue
            parent_rec = add_context(parent_rec, group, resolved, fixture=False)
            rendered.append(parent_rec)
            if complete_enough(parent_rec):
                selected.append(parent_rec)
            elif clean(parent_rec.get("record_status")).lower() != "open":
                failures.append({
                    "stage": "acceptance",
                    "case": parent_case,
                    "reason": f"parent_not_open:{parent_rec.get('record_status')}",
                    "url": parent_url,
                })
            else:
                missing = [k for k in ("property_address", "description_raw", "owner_name", "parcel_id", "source_url") if not parent_rec.get(k)]
                failures.append({
                    "stage": "acceptance",
                    "case": parent_case,
                    "reason": "missing_required_fields:" + ",".join(missing),
                    "url": parent_url,
                })
            page.wait_for_timeout(250)

        ctx.close()
        browser.close()

    # Property stacking proof across selected output.
    by_property = defaultdict(list)
    for r in selected:
        key = r.get("parcel_id") or clean(r.get("property_address"))
        by_property[key].append(r["case_number"])
    for r in selected:
        key = r.get("parcel_id") or clean(r.get("property_address"))
        r["selected_open_parent_cases_same_property"] = len(set(by_property.get(key, [])))

    grissom = next((r for r in rendered if r.get("fixture")), None)
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
        "parent_groups_attempted": len(attempted_parent_keys),
        "parents_rendered": len(rendered),
        "selected_open_records": selected,
        "grissom_regression": regression,
        "failures": failures,
        "notes": [
            "Read-only bench test; no production ingest occurs.",
            "Chromium is used for child and parent pages because Accela Related Records is JS-populated.",
            "The exact parent href is preserved; URLs are not guessed from the parent case number.",
            "No CAPTCHA solver, stealth plugin, proxy rotation, or anti-bot circumvention is used.",
            "A record only counts toward PASS when parent is OPEN and case/address/description/owner/parcel/source URL are present.",
        ],
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    md = ["# Accela Engine Probe v4", "", f"Status: **{status}**", f"Runtime: **{runtime}s**", f"Fully verified OPEN parent cases: **{len(selected)}**", ""]
    for i, r in enumerate(selected, 1):
        md += [
            f"## {i}. {r.get('case_number')}",
            f"- Status: {r.get('record_status')}",
            f"- Address: {r.get('property_address')}",
            f"- Description: {r.get('description_raw')}",
            f"- Owner: {r.get('owner_name')}",
            f"- Owner mailing: {r.get('owner_mailing_address')}",
            f"- Parcel: {r.get('parcel_id')}",
            f"- Parent URL: {r.get('source_url')}",
            f"- Child URL: {r.get('child_source_url') or 'fixture'}",
            f"- Inspector comments: {' | '.join(r.get('inspector_comments') or []) or 'none'}",
            f"- Recent violation rows: {r.get('site_visit_rows_in_recent_window')}",
            f"- Recent citations total: ${r.get('citation_total_in_recent_window')}",
            f"- Child browser seconds: {r.get('child_browser_seconds')}",
            f"- Parent browser seconds: {r.get('parent_browser_seconds')}",
            "",
        ]
    if failures:
        md += ["## Failure log", ""]
        for f in failures:
            md.append(f"- {f.get('stage')}: {f.get('case') or f.get('child_case') or f.get('expected_parent')} — {f.get('reason')}")
    (out / "report.md").write_text("\n".join(md), encoding="utf-8")

    print(json.dumps({
        "status": status,
        "runtime_seconds": runtime,
        "selected_count": len(selected),
        "grissom_regression": regression,
        "records": [{
            "case": r.get("case_number"),
            "status": r.get("record_status"),
            "address": r.get("property_address"),
            "description": r.get("description_raw"),
            "owner": r.get("owner_name"),
            "owner_mailing": r.get("owner_mailing_address"),
            "parcel": r.get("parcel_id"),
            "parent_url": r.get("source_url"),
            "inspector_comments": r.get("inspector_comments"),
            "site_visit_rows": r.get("site_visit_rows_in_recent_window"),
            "citation_total": r.get("citation_total_in_recent_window"),
            "child_seconds": r.get("child_browser_seconds"),
            "parent_seconds": r.get("parent_browser_seconds"),
        } for r in selected],
        "failure_count": len(failures),
    }, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
