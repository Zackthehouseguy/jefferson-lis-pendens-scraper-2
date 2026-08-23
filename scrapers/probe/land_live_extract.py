#!/usr/bin/env python3
"""Read-only live land extraction bench for TheReaper.

Flow:
Louisville PM site-visit rows -> current Vacant Lot candidates -> Accela child JS
-> exact OPEN parent -> owner/mailing/parcel -> LOJIC parcel geometry/area ->
zoning + land-use enrichment -> evidence packet for land AI scoring.

Landbank is intentionally excluded. A vacant/condemned STRUCTURE is not emitted
as land unless the source says Vacant Lot; structure cases are kept in a
separate demolition-transition watch list.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright

from scrapers.failure_recovery import RetryPolicy, retry_call
from scrapers.probe import accela_engine_probe_v2 as base
from scrapers.probe import accela_engine_probe_v3 as browser_child
from scrapers.probe.full_system_live_extract import citation_context, classify_failure, clean_location, owner_mailing_differs

ET = ZoneInfo("America/New_York")
PVA_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataPVA/MapServer/1/query"
ZONING_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/15/query"
LANDUSE_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/6/query"


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def fetch_pm_rows(total: int, page_size: int = 1000) -> list[dict]:
    out: list[dict] = []
    offset = 0
    while len(out) < total:
        take = min(page_size, total - len(out))
        params = {
            "where": "1=1",
            "outFields": ",".join(base.FIELDS),
            "returnGeometry": "false",
            "orderByFields": "G6A_G6_COMPL_DD DESC",
            "resultOffset": str(offset),
            "resultRecordCount": str(take),
            "f": "json",
        }
        r = requests.get(base.ARCGIS_QUERY, params=params, timeout=45)
        r.raise_for_status()
        payload = r.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        rows = payload.get("features") or []
        out.extend(rows)
        if len(rows) < take:
            break
        offset += len(rows)
    return out


def parcel_enrichment(parcel_id: str) -> tuple[dict, list[dict]]:
    failures: list[dict] = []
    result = {
        "parcel_type": None,
        "lot_sqft": None,
        "lot_acres": None,
        "pin": None,
        "zoning_code": None,
        "zoning_name": None,
        "zoning_type": None,
        "landuse_name": None,
        "lojic_parcel_verified": False,
    }
    try:
        params = {
            "where": f"PARCELID='{parcel_id.replace(chr(39), chr(39)*2)}'",
            "outFields": "PARCELID,PARCEL_TYPE,PIN,SHAPE.AREA",
            "returnGeometry": "true",
            "outSR": "2246",
            "f": "json",
        }
        r = requests.get(PVA_QUERY, params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        feats = payload.get("features") or []
        if not feats:
            failures.append({"source": "lojic_parcel", "reason": "parcel_not_found"})
            return result, failures
        feat = feats[0]
        a = feat.get("attributes") or {}
        geom = feat.get("geometry")
        sqft = a.get("SHAPE.AREA")
        if sqft is not None:
            try:
                sqft = float(sqft)
            except Exception:
                sqft = None
        result.update({
            "parcel_type": a.get("PARCEL_TYPE"),
            "lot_sqft": round(sqft, 1) if sqft is not None else None,
            "lot_acres": round(sqft / 43560.0, 4) if sqft is not None else None,
            "pin": a.get("PIN"),
            "lojic_parcel_verified": True,
        })
        if geom:
            for name, url, fields in (
                ("zoning", ZONING_QUERY, "ZONING_CODE,ZONING_NAME,ZONING_TYPE"),
                ("landuse", LANDUSE_QUERY, "LANDUSE_NAME"),
            ):
                try:
                    q = {
                        "where": "1=1",
                        "geometry": json.dumps(geom, separators=(",", ":")),
                        "geometryType": "esriGeometryPolygon",
                        "inSR": "2246",
                        "spatialRel": "esriSpatialRelIntersects",
                        "outFields": fields,
                        "returnGeometry": "false",
                        "f": "json",
                    }
                    rr = requests.get(url, params=q, timeout=20)
                    rr.raise_for_status()
                    pp = rr.json()
                    if pp.get("error"):
                        raise RuntimeError(pp["error"])
                    ff = pp.get("features") or []
                    if ff:
                        aa = ff[0].get("attributes") or {}
                        if name == "zoning":
                            result["zoning_code"] = clean(aa.get("ZONING_CODE")) or None
                            result["zoning_name"] = clean(aa.get("ZONING_NAME")) or None
                            result["zoning_type"] = clean(aa.get("ZONING_TYPE")) or None
                        else:
                            result["landuse_name"] = clean(aa.get("LANDUSE_NAME")) or None
                    else:
                        failures.append({"source": f"lojic_{name}", "reason": "no_intersection"})
                except Exception as exc:
                    failures.append({"source": f"lojic_{name}", "reason": f"{type(exc).__name__}:{exc}"})
    except Exception as exc:
        failures.append({"source": "lojic_parcel", "reason": f"{type(exc).__name__}:{exc}"})
    return result, failures


def parse_address_parts(address: str | None) -> dict:
    s = clean(address)
    m = re.search(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b", s, re.I)
    state = m.group(1).upper() if m else None
    zip_code = m.group(2) if m else None
    city = "LOUISVILLE" if "LOUISVILLE" in s.upper() else None
    return {"city": city, "state": state, "zip": zip_code}


def latest_by_parcel(groups: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for g in groups:
        if g.get("parcel"):
            out[g["parcel"]].append(g)
    return out


def is_vacant_lot_group(g: dict) -> bool:
    return any(clean(x).lower() == "vacant lot" for x in (g.get("occupancies") or []))


def is_demolition_watch(g: dict) -> bool:
    occ = " ".join(g.get("occupancies") or []).lower()
    codes = {clean(x).upper() for x in (g.get("violation_codes") or [])}
    text = " ".join(g.get("guide_texts") or []).lower()
    return (
        "vacant structure" in occ
        and ("C01" in codes or "condemn" in text or "demol" in text or "boarding" in text)
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", type=int, default=10)
    ap.add_argument("--pm-limit", type=int, default=4000)
    ap.add_argument("--max-attempts", type=int, default=60)
    ap.add_argument("--out", default="reports/land_live")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)

    features = fetch_pm_rows(args.pm_limit)
    groups = base.build_groups(features)
    cctx = citation_context(features)
    parcel_groups = latest_by_parcel(groups)

    land_groups = [g for g in groups if is_vacant_lot_group(g) and g.get("parcel")]
    demo_watch = [g for g in groups if is_demolition_watch(g) and g.get("parcel")]

    # Newest first. build_groups retains first/newest source row as representative.
    def event_key(g):
        r = g.get("representative") or {}
        return r.get("status_date") or r.get("visit_date") or ""
    land_groups.sort(key=event_key, reverse=True)
    demo_watch.sort(key=event_key, reverse=True)

    selected: list[dict] = []
    failures: list[dict] = []
    inspected = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 1200}, locale="en-US")
        page = ctx.new_page()
        for idx, group in enumerate(land_groups, 1):
            if len(selected) >= args.target or inspected >= args.max_attempts:
                break
            inspected += 1
            child_fail = {"value": None}
            def get_child():
                resolved, fail = browser_child.render_child_and_resolve(page, group, out_dir, idx)
                if fail:
                    child_fail["value"] = fail
                    raise classify_failure(fail)
                return resolved
            try:
                resolved, child_meta = retry_call(
                    get_child,
                    policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.35, max_delay_seconds=1.0),
                    sleep=time.sleep,
                )
            except Exception as exc:
                failures.append({"stage": "child", "parcel_id": group.get("parcel"), "reason": str(child_fail["value"] or exc)})
                continue

            parent_fail = {"value": None}
            def get_parent():
                rec, fail = base.browser_parent(page, resolved["parent_case"], resolved["parent_url"], out_dir, idx)
                if fail:
                    parent_fail["value"] = fail
                    raise classify_failure(fail)
                return rec
            try:
                rec, parent_meta = retry_call(
                    get_parent,
                    policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.35, max_delay_seconds=1.0),
                    sleep=time.sleep,
                )
            except Exception as exc:
                failures.append({"stage": "parent", "parcel_id": group.get("parcel"), "reason": str(parent_fail["value"] or exc)})
                continue

            rec["property_address"] = clean_location(rec.get("property_address"))
            if not rec.get("parcel_id"):
                rec["parcel_id"] = resolved.get("child_parcel") or group.get("parcel")
            if clean(rec.get("record_status")).lower() != "open":
                failures.append({"stage": "acceptance", "parcel_id": rec.get("parcel_id"), "reason": "parent_not_open", "status": rec.get("record_status")})
                continue
            parcel_id = rec.get("parcel_id")
            if not parcel_id:
                failures.append({"stage": "acceptance", "reason": "missing_parcel"})
                continue

            enrich, enrich_fail = parcel_enrichment(parcel_id)
            failures.extend({"stage": "enrichment", "parcel_id": parcel_id, **x} for x in enrich_fail)
            rep = group.get("representative") or {}
            event_date = rep.get("status_date") or rep.get("visit_date")
            all_pg = parcel_groups.get(parcel_id, [])
            all_occ = sorted({x for pg in all_pg for x in (pg.get("occupancies") or []) if x})
            has_structure_history = any(clean(x).lower() == "vacant structure" for x in all_occ)
            possible_transition = bool(has_structure_history and any(clean(x).lower() == "vacant lot" for x in all_occ))
            citations = cctx.get(group["parent_key"], {
                "citation_event_count": 0,
                "citation_assessed_total": 0.0,
                "ambiguous_citation_events": [],
                "outstanding_balance": None,
                "balance_note": "No current outstanding balance inferred.",
            })
            address = rec.get("property_address") or group.get("address")
            selected.append({
                "property_type": "LAND",
                "confirmed_vacant_lot": True,
                "demolition_verified": False,
                "possible_structure_to_lot_transition": possible_transition,
                "transition_note": (
                    "Source history contains both Vacant Structure and Vacant Lot occupancy; demolition/removal is NOT independently verified."
                    if possible_transition else None
                ),
                "case_number": rec.get("case_number"),
                "record_status": rec.get("record_status"),
                "property_address": address,
                **parse_address_parts(address),
                "description_raw": rec.get("description_raw"),
                "inspector_comments": resolved.get("inspector_comments") or [],
                "owner_name": rec.get("owner_name"),
                "owner_mailing_address": rec.get("owner_mailing_address"),
                "owner_mailing_differs": owner_mailing_differs(address, rec.get("owner_mailing_address")),
                "parcel_id": parcel_id,
                "source_url": resolved.get("parent_url"),
                "child_source_url": resolved.get("child_url"),
                "event_date": event_date,
                "open_case_groups_in_source_window_same_parcel": len(all_pg),
                "source_window_occupancies": all_occ,
                "violation_codes": group.get("violation_codes") or [],
                "violation_descriptions": group.get("guide_texts") or [],
                **citations,
                **enrich,
                "tax_delinquent_verified": None,
                "tax_note": "Tax adapter not used in this extraction run; do not infer tax delinquency.",
                "recovery": {"child": child_meta, "parent": parent_meta},
            })
            page.wait_for_timeout(225)
        ctx.close(); browser.close()

    demo_preview = []
    for g in demo_watch[:50]:
        r = g.get("representative") or {}
        demo_preview.append({
            "parent_case": g.get("parent_key"),
            "parcel_id": g.get("parcel"),
            "address": g.get("address"),
            "occupancies": g.get("occupancies") or [],
            "violation_codes": g.get("violation_codes") or [],
            "event_date": r.get("status_date") or r.get("visit_date"),
            "status": "WATCH_ONLY_NOT_VERIFIED_LAND",
        })

    status = "PASS" if len(selected) >= args.target else ("PARTIAL" if selected else "FAIL")
    report = {
        "status": status,
        "generated_at_utc": now_utc.isoformat(),
        "generated_at_et": now_et.isoformat(),
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "pm_features_fetched": len(features),
        "parent_groups_discovered": len(groups),
        "vacant_lot_parent_groups": len(land_groups),
        "demolition_transition_watch_groups": len(demo_watch),
        "target_verified_land": args.target,
        "parent_groups_inspected": inspected,
        "verified_land_records": selected,
        "demolition_watch_preview": demo_preview,
        "failures": failures,
        "guardrails": {
            "landbank_excluded": True,
            "vacant_structure_is_not_land": True,
            "demolition_not_inferred_from_vacancy": True,
            "citation_assessed_not_current_balance": True,
            "builder_fit_is_estimate_not_buildability": True,
        },
    }
    (out_dir / "extract_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
