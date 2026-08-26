#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

ET = ZoneInfo("America/New_York")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
ADDR_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataAddresses/MapServer/0/query"
PARCEL_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataPVA/MapServer/1/query"
LANDUSE_QUERY = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/6/query"
PVA_LISTINGS = "https://jeffersonpva.ky.gov/property-search/property-listings/"
ENTITY_TERMS = (
    " LLC", " L.L.C", " INC", " CORPORATION", " CORP", " COMPANY", " TRUST",
    " ESTATE", " HEIRS", " DEVISEES", " CHURCH", " MINISTRY", " FOUNDATION",
    " HOLDINGS", " PROPERTIES", " ASSETS", " INVESTMENTS", " DEVELOPMENT",
    " REALTY", " ASSOCIATION", " PARTNERSHIP", " LLP", " AUTHORITY", " METRO",
    " COUNTY", " CITY OF ", " COMMONWEALTH", " BANK", " MORTGAGE", " FINANCIAL",
    " CREDIT UNION", " SERVICING", " BUREAU", " DEPARTMENT", " CABINET",
    " SECRETARY", " UNITED STATES", " U.S. BANK", " USA ", " HOA", " SCHOOL",
    " UNIVERSITY", " HOSPITAL", " MINISTRIES", " TEMPLE",
)
SEVERE_TERMS = (
    "CONDEMN", "UNSAFE", "STRUCTURAL", "FOUNDATION", "ABANDON", "VACANT STRUCTURE",
    "TERMINATED UTIL", "NO WATER", "NO ELECTRIC", "FIRE", "ROOF", "DEMOL",
    "COLLAPSE", "SEWAGE", "BOARD",
)
CREDITOR_TERMS = (
    "BANK", "MORTGAGE", "FINANCIAL", "CREDIT", "SERVIC", "ADJUSTMENT BUREAU",
    "REVENUE", "TAX", "SYNCHRONY", "CAPITAL ONE", "MERS", "U.S. BANK", "US BANK",
)
_thread = threading.local()


def session() -> requests.Session:
    s = getattr(_thread, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=1)
        s.mount("https://", adapter)
        _thread.session = s
    return s


def clean(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def norm_addr(v) -> str:
    s = clean(v).upper()
    s = re.sub(r"\bLOUISVILLE\b.*$", "", s).strip()
    for a, b in {
        " STREET": " ST", " AVENUE": " AVE", " ROAD": " RD", " DRIVE": " DR",
        " LANE": " LN", " COURT": " CT", " BOULEVARD": " BLVD", " PLACE": " PL",
        " TERRACE": " TER", " HIGHWAY": " HWY", " PARKWAY": " PKWY", " CIRCLE": " CIR",
    }.items():
        s = s.replace(a, b)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def addr_parts(address: str) -> tuple[str | None, str]:
    a = norm_addr(address)
    m = re.match(r"^(\d+[A-Z]?)\s+(.+)$", a)
    return (m.group(1), m.group(2)) if m else (None, a)


def is_individual_owner(name: str | None) -> bool:
    n = clean(name).upper()
    if not n:
        return False
    padded = f" {n} "
    if any(t in padded for t in ENTITY_TERMS):
        return False
    words = [x for x in re.findall(r"[A-Z][A-Z'-]+", n) if len(x) > 1]
    return len(words) >= 2


def parse_date(v) -> date | None:
    s = clean(v)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def latest_signal_date(row: dict) -> date | None:
    dates = [parse_date(e.get("signal_date")) for e in (row.get("evidence") or [])]
    dates = [d for d in dates if d]
    return max(dates) if dates else None


def resolve_parcel(address: str) -> tuple[str | None, str | None]:
    house, street = addr_parts(address)
    if not house or not street:
        return None, "unparseable_address"
    street_core = re.sub(r"\b(ST|AVE|RD|DR|LN|CT|BLVD|WAY|PL|TER|TRL|HWY|PKWY|CIR)\b", "", street).strip()
    if not street_core:
        return None, "street_missing"
    where = f"UPPER(STRNAME) LIKE '%{street_core.replace(chr(39), chr(39)*2)}%'"
    if house.isdigit():
        where += f" AND HOUSENO = {int(house)}"
    try:
        r = session().get(ADDR_QUERY, params={
            "where": where,
            "outFields": "ADDRESS,HOUSENO,STRNAME,PARCELID,LRSN",
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": 10,
        }, timeout=15)
        r.raise_for_status()
        feats = (r.json() or {}).get("features") or []
        if not feats:
            return None, "address_not_found"
        expected = norm_addr(address)
        best = None
        for f in feats:
            a = f.get("attributes") or {}
            candidate = norm_addr(a.get("ADDRESS"))
            if candidate == expected:
                best = a
                break
            if house and str(a.get("HOUSENO") or "").strip().upper() == house.upper():
                best = a
        best = best or (feats[0].get("attributes") or {})
        pid = clean(best.get("PARCELID")).upper()
        return (pid or None), None if pid else "parcel_blank"
    except Exception as exc:
        return None, f"lojic_address:{type(exc).__name__}"


def parcel_enrichment(parcel_id: str) -> tuple[dict, list[str]]:
    out = {
        "lojic_parcel_verified": False,
        "parcel_type": None,
        "pin": None,
        "lot_sqft": None,
        "lot_acres": None,
        "landuse_name": None,
    }
    errs: list[str] = []
    try:
        r = session().get(PARCEL_QUERY, params={
            "where": f"PARCELID='{parcel_id.replace(chr(39), chr(39)*2)}'",
            "outFields": "PARCELID,PARCEL_TYPE,PIN,SHAPE.AREA",
            "returnGeometry": "true",
            "outSR": "2246",
            "f": "json",
            "resultRecordCount": 3,
        }, timeout=15)
        r.raise_for_status()
        feats = (r.json() or {}).get("features") or []
        if not feats:
            return out, ["parcel_not_found"]
        feat = feats[0]
        a = feat.get("attributes") or {}
        area = a.get("SHAPE.AREA")
        try:
            area = float(area) if area is not None else None
        except Exception:
            area = None
        out.update({
            "lojic_parcel_verified": True,
            "parcel_type": a.get("PARCEL_TYPE"),
            "pin": clean(a.get("PIN")) or None,
            "lot_sqft": round(area, 1) if area is not None else None,
            "lot_acres": round(area / 43560.0, 4) if area is not None else None,
        })
        geom = feat.get("geometry")
        if geom:
            try:
                rr = session().post(LANDUSE_QUERY, data={
                    "where": "1=1",
                    "geometry": json.dumps(geom, separators=(",", ":")),
                    "geometryType": "esriGeometryPolygon",
                    "inSR": "2246",
                    "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "LANDUSE_NAME",
                    "returnGeometry": "false",
                    "f": "json",
                }, timeout=15)
                rr.raise_for_status()
                ff = (rr.json() or {}).get("features") or []
                if ff:
                    out["landuse_name"] = clean((ff[0].get("attributes") or {}).get("LANDUSE_NAME")).upper() or None
                else:
                    errs.append("landuse_no_intersection")
            except Exception as exc:
                errs.append(f"landuse:{type(exc).__name__}")
        else:
            errs.append("parcel_geometry_missing")
    except Exception as exc:
        errs.append(f"parcel:{type(exc).__name__}")
    return out, errs


def pva_lookup(parcel_id: str, expected_address: str) -> tuple[dict, str | None]:
    out = {
        "pva_verified": False,
        "pva_owner": None,
        "pva_parcel_id": None,
        "pva_assessed_value": None,
        "pva_acres": None,
        "pva_mailing_address": None,
        "pva_situs_address": None,
        "pva_url": None,
    }
    url = f"{PVA_LISTINGS}?psfldParcelId={quote(parcel_id)}&propertySearchFormButton=Search&searchType=ParcelSearch"
    out["pva_url"] = url
    try:
        r = session().get(url, timeout=20, allow_redirects=True)
        r.raise_for_status()
        html = r.text or ""
        if len(html) < 500:
            return out, "pva_empty"
        soup = BeautifulSoup(html, "html.parser")
        cells = [clean(x) for x in soup.get_text("|", strip=True).split("|") if clean(x)]
        for key, lab in {
            "pva_owner": "Owner",
            "pva_parcel_id": "Parcel ID",
            "pva_assessed_value": "Assessed Value",
            "pva_acres": "Acres",
            "pva_mailing_address": "Mailing Address",
        }.items():
            for i, cell in enumerate(cells[:-1]):
                if cell.lower() == lab.lower():
                    val = clean(cells[i + 1])
                    if val and val.lower() != lab.lower():
                        out[key] = val
                    break
        expected_house, _ = addr_parts(expected_address)
        if expected_house:
            for cell in cells:
                if cell.upper().startswith(expected_house.upper() + " "):
                    out["pva_situs_address"] = cell
                    break
        page_pid = re.sub(r"[^A-Z0-9]", "", clean(out.get("pva_parcel_id")).upper())
        wanted_pid = re.sub(r"[^A-Z0-9]", "", parcel_id.upper())
        owner = clean(out.get("pva_owner"))
        out["pva_verified"] = bool(owner and (not page_pid or page_pid == wanted_pid))
        return out, None if out["pva_verified"] else "pva_owner_or_parcel_unverified"
    except Exception as exc:
        return out, f"pva:{type(exc).__name__}"


def vacant_lot_context(row: dict) -> bool:
    text = " ".join(
        clean(x) for x in [
            row.get("property_address"),
            *row.get("score_reasons", []),
            *[
                " ".join(clean(e.get(k)) for k in ("details", "status", "party_or_owner"))
                for e in (row.get("evidence") or [])
            ],
        ]
    ).upper()
    return (
        "VACANT LOT" in text
        or "LAND BANK" in text
        or "LANDBANK" in text
        or "PROPERTY AVAILABLE FOR PURCHASE" in text
    )


def market_status_zillow(address: str) -> dict:
    result = {
        "market_status": "UNKNOWN",
        "market_source": "Zillow exact-address live check",
        "market_url": None,
        "market_checked_at_et": datetime.now(timezone.utc).astimezone(ET).isoformat(),
        "market_reason": None,
    }
    slug = re.sub(r"[^A-Za-z0-9]+", "-", clean(address)).strip("-")
    url = f"https://www.zillow.com/homes/{quote(slug)}_rb/"
    try:
        r = session().get(url, timeout=20, allow_redirects=True)
        result["market_url"] = r.url
        if r.status_code in (403, 429):
            result["market_reason"] = f"http_{r.status_code}"
            return result
        r.raise_for_status()
        html = r.text or ""
        low = html.lower()
        expected = norm_addr(address)
        title = ""
        m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
        if m:
            title = BeautifulSoup(m.group(1), "html.parser").get_text(" ", strip=True)
        address_match = bool(expected and expected in norm_addr(title))
        if not address_match:
            house, street = addr_parts(address)
            probe = f"{house} {street}" if house else expected
            address_match = bool(probe and probe in norm_addr(html[:250000]))
        if not address_match:
            result["market_reason"] = "exact_address_not_confirmed"
            return result
        statuses = set(re.findall(r'"homeStatus"\s*:\s*"([A-Z_]+)"', html))
        if statuses & {"FOR_SALE", "COMING_SOON"}:
            result["market_status"] = "ACTIVE"
            result["market_reason"] = "zillow_homeStatus_for_sale"
        elif statuses & {"PENDING", "UNDER_CONTRACT"}:
            result["market_status"] = "PENDING"
            result["market_reason"] = "zillow_homeStatus_pending"
        elif (
            "currently not for sale" in low
            or ">off market<" in low
            or '"homeStatus":"OTHER"' in html
            or '"homeStatus":"SOLD"' in html
        ):
            result["market_status"] = "OFF_MARKET"
            result["market_reason"] = "zillow_explicit_off_market"
        else:
            result["market_reason"] = "status_not_explicit"
    except Exception as exc:
        result["market_reason"] = f"zillow:{type(exc).__name__}"
    return result


def assignment_indexes(path: Path) -> tuple[dict[str, dict], dict[str, dict]]:
    pidx, aidx = {}, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return pidx, aidx
    for rec in (data.get("properties") or {}).values():
        if clean(rec.get("status") or "active").lower() == "released":
            continue
        pid = re.sub(r"[^A-Z0-9]", "", clean(rec.get("parcel_id")).upper())
        ad = norm_addr(rec.get("property_address"))
        if pid:
            pidx[pid] = rec
        if ad:
            aidx[ad] = rec
    return pidx, aidx


def delivery_index(path: Path) -> dict[str, dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}
    out = {}
    for key, rec in (data.get("properties") or {}).items():
        pid = clean(key).split("|")[-1].upper()
        if pid:
            out[pid] = rec
    return out


def seen_parcels(path: Path) -> dict[str, dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}
    return {clean(k).upper(): v for k, v in (data.get("parcels") or {}).items() if clean(k)}


def freshness_state(row: dict, parcel: str, assigned_p: dict, assigned_a: dict,
                    delivered: dict, seen: dict) -> tuple[str, list[str]]:
    latest = latest_signal_date(row)
    assignment = assigned_p.get(parcel) or assigned_a.get(norm_addr(row.get("property_address")))
    delivery = delivered.get(parcel)
    if assignment:
        agent = clean(assignment.get("assigned_to"))
        if agent and agent.lower() != "zack":
            return "OTHER_AGENT", [f"assigned_to_{agent}"]
        prior = parse_date((delivery or {}).get("last_event_date"))
        if latest and prior and latest > prior:
            return "REACTIVATED", [f"new_signal_{latest.isoformat()}_after_{prior.isoformat()}"]
        return "PREVIOUSLY_ASSIGNED", ["active_assignment_exists"]
    if delivery:
        prior = parse_date(delivery.get("last_event_date"))
        if latest and prior and latest > prior:
            return "REACTIVATED", [f"new_signal_{latest.isoformat()}_after_{prior.isoformat()}"]
        return "PREVIOUSLY_DELIVERED", ["delivery_ledger_match"]
    if parcel in seen:
        sources = set(row.get("sources") or [])
        if sources == {"louisville_code_violations"}:
            return "SEEN_CODE_ONLY", ["previous_code_queue_seen"]
        return "FRESH", ["parcel_seen_in_code_queue_but_new_noncode_signal_present"]
    return "FRESH", []


def priority_score(row: dict, pva_ok: bool, individual: bool, current_type: str | None) -> tuple[int, list[str]]:
    sources = set(row.get("sources") or [])
    evidence = row.get("evidence") or []
    score = 0
    why: list[str] = []
    if "lis_pendens" in sources:
        score += 45
        why.append("fresh lis pendens")
        party_text = " ".join(clean(e.get("party_or_owner")).upper() for e in evidence if e.get("source") == "lis_pendens")
        creditor_hits = sum(1 for t in CREDITOR_TERMS if t in party_text)
        if creditor_hits >= 2:
            score += 8
            why.append("multiple creditor/lender parties")
        elif creditor_hits == 1:
            score += 4
            why.append("creditor/lender party")
    if "tax_delinquent" in sources:
        score += 30
        vals = [float(e.get("amount") or 0) for e in evidence if e.get("source") == "tax_delinquent"]
        mx = max(vals or [0])
        if mx >= 10000:
            score += 15
            why.append("large published delinquent-tax amount")
        elif mx >= 5000:
            score += 8
            why.append("meaningful published delinquent-tax amount")
        else:
            why.append("published delinquent-tax signal")
    if "wills" in sources:
        score += 25
        why.append("will/probate-related filing")
    if "louisville_code_violations" in sources:
        raws = [int(e.get("raw_distress_score") or 0) for e in evidence if e.get("source") == "louisville_code_violations"]
        raw = max(raws or [0])
        score += 18 + min(17, round(raw * 0.17))
        text = " ".join(clean(e.get("details")).upper() for e in evidence if e.get("source") == "louisville_code_violations")
        severe = sorted({t for t in SEVERE_TERMS if t in text})
        if severe:
            score += 12
            why.append("severe code distress")
        else:
            why.append("code-enforcement distress")
    if "louisville_landbank" in sources:
        score += 8
        why.append("landbank/vacant inventory signal")
    if len(sources) >= 3:
        score += 25
        why.append(f"{len(sources)} independent signals stacked")
    elif len(sources) == 2:
        score += 18
        why.append("2 independent signals stacked")
    latest = latest_signal_date(row)
    if latest:
        age = (datetime.now(timezone.utc).astimezone(ET).date() - latest).days
        if age <= 1:
            score += 12
            why.append("signal age <=1 day")
        elif age <= 3:
            score += 8
            why.append("signal age <=3 days")
        elif age <= 7:
            score += 4
            why.append("signal age <=7 days")
    if pva_ok and individual:
        score += 5
        why.append("current PVA owner verified individual")
    if current_type == "SFR":
        score += 3
        why.append("LOJIC exact single-family land use")
    return min(100, score), why


def qualify_one(row: dict, assigned_p: dict, assigned_a: dict, delivered: dict, seen: dict) -> dict:
    result = {
        "property_key": row.get("property_key"),
        "source_property_address": row.get("property_address"),
        "source_parcel_id": row.get("parcel_id"),
        "sources": row.get("sources") or [],
        "signal_count": row.get("signal_count"),
        "evidence": row.get("evidence") or [],
        "source_score": row.get("motivation_score"),
        "source_score_reasons": row.get("score_reasons") or [],
        "qualification_errors": [],
        "rejection_reasons": [],
    }
    parcel = re.sub(r"[^A-Z0-9]", "", clean(row.get("parcel_id")).upper())
    if not parcel:
        parcel, err = resolve_parcel(clean(row.get("property_address")))
        if err:
            result["qualification_errors"].append(err)
    result["parcel_id"] = parcel or None
    if not parcel:
        result["qualification_status"] = "REJECTED"
        result["rejection_reasons"].append("parcel_unverified")
        return result

    enrich, errs = parcel_enrichment(parcel)
    result.update(enrich)
    result["qualification_errors"].extend(errs)
    pva, err = pva_lookup(parcel, clean(row.get("property_address")))
    result.update(pva)
    if err:
        result["qualification_errors"].append(err)
    current_owner = clean(result.get("pva_owner"))
    individual = is_individual_owner(current_owner)
    result["current_owner_individual"] = individual

    landuse = clean(result.get("landuse_name")).upper()
    vac_ctx = vacant_lot_context(row)
    result["vacant_lot_context"] = vac_ctx
    if landuse == "SINGLE FAMILY":
        candidate_type = "SFR"
    elif vac_ctx:
        candidate_type = "LAND"
    else:
        candidate_type = None
    result["candidate_type"] = candidate_type

    fresh_state, fresh_reasons = freshness_state(row, parcel, assigned_p, assigned_a, delivered, seen)
    result["freshness_state"] = fresh_state
    result["freshness_reasons"] = fresh_reasons

    if not result.get("lojic_parcel_verified"):
        result["rejection_reasons"].append("lojic_parcel_unverified")
    if not result.get("pva_verified"):
        result["rejection_reasons"].append("current_pva_owner_unverified")
    if not individual:
        result["rejection_reasons"].append("current_owner_not_verified_individual")
    if not candidate_type:
        result["rejection_reasons"].append("property_type_not_target")
    if fresh_state in {"OTHER_AGENT", "PREVIOUSLY_ASSIGNED", "PREVIOUSLY_DELIVERED", "SEEN_CODE_ONLY"}:
        result["rejection_reasons"].append(f"freshness_{fresh_state.lower()}")

    score, why = priority_score(row, bool(result.get("pva_verified")), individual, candidate_type)
    result["reaper_priority_score"] = score
    result["priority_reasons"] = why

    if not result["rejection_reasons"]:
        result.update(market_status_zillow(clean(row.get("property_address"))))
        if result.get("market_status") in {"ACTIVE", "PENDING"}:
            result["rejection_reasons"].append(f"market_{result['market_status'].lower()}")
        elif result.get("market_status") != "OFF_MARKET":
            result["rejection_reasons"].append("market_status_unverified")
    else:
        result.update({
            "market_status": "NOT_CHECKED",
            "market_source": None,
            "market_url": None,
            "market_checked_at_et": None,
            "market_reason": "failed_pre_market_gate",
        })

    if not result["rejection_reasons"]:
        if candidate_type == "SFR":
            eligible = score >= 60
        else:
            result["builder_fit_score"] = 50
            eligible = score >= 60
        if eligible:
            result["qualification_status"] = "ELIGIBLE"
        else:
            result["qualification_status"] = "REJECTED"
            result["rejection_reasons"].append("priority_below_production_threshold")
    else:
        result["qualification_status"] = "REJECTED"
    return result


def render_md(report: dict) -> str:
    lines = [
        "# Reaper Bulk Qualification", "",
        f"Generated: {report['generated_at_et']}",
        f"Input candidates: {report['summary']['input_candidates']}",
        f"Eligible SFR: {report['summary']['eligible_sfr']}",
        f"Eligible land: {report['summary']['eligible_land']}",
        f"Market unknown: {report['summary']['market_unknown']}", "",
        "## Eligible SFR", "",
    ]
    for i, r in enumerate(report["eligible_sfr"], 1):
        lines += [
            f"{i}. **{r.get('source_property_address')}** — {r.get('pva_owner')}",
            f"   - Score: {r.get('reaper_priority_score')} | Parcel: {r.get('parcel_id')} | Land use: {r.get('landuse_name')} | Market: {r.get('market_status')}",
            f"   - Sources: {', '.join(r.get('sources') or [])}",
        ]
    lines += ["", "## Eligible Land", ""]
    for i, r in enumerate(report["eligible_land"], 1):
        lines += [
            f"{i}. **{r.get('source_property_address')}** — {r.get('pva_owner')}",
            f"   - Score: {r.get('reaper_priority_score')} | Builder fit: {r.get('builder_fit_score')} | Parcel: {r.get('parcel_id')} | Market: {r.get('market_status')}",
            f"   - Sources: {', '.join(r.get('sources') or [])}",
        ]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stacked", default="reports/reaper_multi_source_live/stacked.json")
    ap.add_argument("--assignments", default="state/lead_assignments.json")
    ap.add_argument("--delivery-ledger", default="data/reaper_delivery_ledger.json")
    ap.add_argument("--seen", default="state/full_system_seen.json")
    ap.add_argument("--out", default="reports/reaper_multi_source_live/bulk_qualified.json")
    ap.add_argument("--md", default="reports/reaper_multi_source_live/bulk_qualified.md")
    ap.add_argument("--workers", type=int, default=10)
    args = ap.parse_args()

    stacked = json.loads(Path(args.stacked).read_text(encoding="utf-8"))
    rows = stacked.get("top_all") or stacked.get("top_fresh_individual") or []
    assigned_p, assigned_a = assignment_indexes(Path(args.assignments))
    delivered = delivery_index(Path(args.delivery_ledger))
    seen = seen_parcels(Path(args.seen))

    print(f"[bulk] qualifying {len(rows)} stacked properties with {args.workers} workers", flush=True)
    started = time.perf_counter()
    results: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = [ex.submit(qualify_one, r, assigned_p, assigned_a, delivered, seen) for r in rows]
        for i, fut in enumerate(cf.as_completed(futures), 1):
            try:
                results.append(fut.result())
            except Exception as exc:
                results.append({
                    "qualification_status": "ERROR",
                    "qualification_errors": [f"worker:{type(exc).__name__}:{exc}"],
                    "rejection_reasons": ["worker_error"],
                    "reaper_priority_score": 0,
                })
            if i % 50 == 0 or i == len(futures):
                print(f"[bulk] {i}/{len(futures)} complete", flush=True)

    results.sort(key=lambda r: (r.get("qualification_status") == "ELIGIBLE", r.get("reaper_priority_score") or 0), reverse=True)
    eligible_sfr = [r for r in results if r.get("qualification_status") == "ELIGIBLE" and r.get("candidate_type") == "SFR"]
    eligible_land = [r for r in results if r.get("qualification_status") == "ELIGIBLE" and r.get("candidate_type") == "LAND"]
    summary = {
        "input_candidates": len(rows),
        "parcel_verified": sum(bool(r.get("lojic_parcel_verified")) for r in results),
        "pva_owner_verified": sum(bool(r.get("pva_verified")) for r in results),
        "current_owner_individual": sum(bool(r.get("current_owner_individual")) for r in results),
        "exact_single_family": sum(r.get("landuse_name") == "SINGLE FAMILY" for r in results),
        "vacant_lot_context": sum(bool(r.get("vacant_lot_context")) for r in results),
        "off_market_verified": sum(r.get("market_status") == "OFF_MARKET" for r in results),
        "active_or_pending_rejected": sum(r.get("market_status") in {"ACTIVE", "PENDING"} for r in results),
        "market_unknown": sum(r.get("market_status") == "UNKNOWN" for r in results),
        "fresh": sum(r.get("freshness_state") == "FRESH" for r in results),
        "reactivated": sum(r.get("freshness_state") == "REACTIVATED" for r in results),
        "previously_assigned_or_delivered": sum(r.get("freshness_state") in {"PREVIOUSLY_ASSIGNED", "PREVIOUSLY_DELIVERED", "OTHER_AGENT"} for r in results),
        "seen_code_only": sum(r.get("freshness_state") == "SEEN_CODE_ONLY" for r in results),
        "eligible_sfr": len(eligible_sfr),
        "eligible_land": len(eligible_land),
        "errors": sum(r.get("qualification_status") == "ERROR" for r in results),
    }
    report = {
        "status": "PASS",
        "generated_at_et": datetime.now(timezone.utc).astimezone(ET).isoformat(),
        "source_generated_at_et": stacked.get("generated_at_et"),
        "query_window": stacked.get("query_window"),
        "summary": summary,
        "eligible_sfr": eligible_sfr,
        "eligible_land": eligible_land,
        "all_results": results,
        "notes": [
            "Current ownership is accepted only when the public Jefferson PVA page returns an owner tied to the resolved parcel.",
            "SFR requires LOJIC landuse_name exactly SINGLE FAMILY.",
            "Land requires confirmed vacant-lot/landbank context plus a verified individual current owner.",
            "Off-market status is accepted only when the live exact-address Zillow response explicitly indicates off-market/not currently for sale; blocked or ambiguous responses remain UNKNOWN and are rejected pending manual review.",
            "Lis pendens is treated as litigation/distress, not automatically as mortgage foreclosure.",
            "Citation amounts are assessed citation events, not claimed current balances.",
        ],
        "runtime_seconds": round(time.perf_counter() - started, 2),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    Path(args.md).write_text(render_md(report), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
