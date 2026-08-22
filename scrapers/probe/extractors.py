"""PVA / GIS source extractors for the read-only source probe.

Read-only. No writes to any county system. Each extractor takes a normalized
address dict and returns a dict of discovered fields plus diagnostics.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Optional
from urllib.parse import quote_plus

import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Fields we hope to recover from any PVA/GIS source.
TARGET_FIELDS = [
    "owner_name",
    "mailing_address",
    "parcel_id",
    "assessed_value",
    "land_value",
    "improvement_value",
    "acreage",
    "year_built",
    "beds",
    "baths",
    "sqft",
    "legal_description",
    "deed_book_page",
    "latitude",
    "longitude",
]


@dataclass
class ProbeResult:
    county: str
    source: str
    address: str
    ok: bool = False
    http_status: Optional[int] = None
    blocked_reason: Optional[str] = None
    error: Optional[str] = None
    elapsed_ms: int = 0
    fetcher: str = "http"
    fields: dict = field(default_factory=dict)

    @property
    def coverage(self) -> float:
        found = sum(1 for f in TARGET_FIELDS if self.fields.get(f) not in (None, "", []))
        return round(found / len(TARGET_FIELDS) * 100, 1)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["coverage_pct"] = self.coverage
        return d


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _detect_block(text: str, status: int) -> Optional[str]:
    low = (text or "")[:20000].lower()
    if "challenges.cloudflare.com" in low or "cf-turnstile" in low or "turnstile" in low:
        return "cloudflare_turnstile"
    if "just a moment" in low or "checking your browser" in low:
        return "cloudflare_interstitial"
    if "captcha" in low or "recaptcha" in low:
        return "captcha"
    if status in (403, 429):
        return f"http_{status}"
    return None


def _num(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    m = re.search(r"-?[\d,]+(?:\.\d+)?", str(v))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Jefferson County (Louisville) — LOJIC / Jefferson PVA ArcGIS services
# --------------------------------------------------------------------------

LOJIC_PARCEL_LAYER = (
    "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/"
    "LOJIC_Parcels_Jefferson_County_KY/FeatureServer/0/query"
)
# Fallback: Louisville Metro open-data parcel service
LOJIC_ALT_LAYER = (
    "https://maps.lojic.org/arcgis/rest/services/LOJIC_PublicSafety/"
    "Parcels/MapServer/0/query"
)


def _arcgis_query(sess: requests.Session, url: str, where: str, timeout: int = 30) -> tuple[Optional[dict], int, str]:
    params = {
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": "25",
    }
    r = sess.get(url, params=params, timeout=timeout)
    try:
        return r.json(), r.status_code, r.text
    except Exception:
        return None, r.status_code, r.text


def _split_address(address: str) -> tuple[Optional[str], str]:
    m = re.match(r"^\s*(\d+)\s+(.+)$", address.strip())
    if m:
        return m.group(1), m.group(2).strip()
    return None, address.strip()


def _esc(v: str) -> str:
    return v.replace("'", "''")


def probe_jefferson(address: str, city: str, sess: requests.Session) -> ProbeResult:
    res = ProbeResult(county="jefferson", source="LOJIC ArcGIS Parcels", address=address)
    t0 = time.time()
    house_no, street = _split_address(address)
    try:
        attempts = [
            f"UPPER(PROPERTY_ADDRESS) LIKE '%{_esc(address.upper())}%'",
            f"UPPER(ADDRESS) LIKE '%{_esc(address.upper())}%'",
        ]
        # Street-name fallback: match street, then filter by house number client-side.
        street_core = re.sub(r"\b(ln|lane|rd|road|st|street|ave|avenue|dr|drive|ct|court|blvd|way|pl|place|cir|circle|ter|trl)\b\.?", "", street, flags=re.I).strip()
        if street_core:
            attempts.append(f"UPPER(PROPERTY_ADDRESS) LIKE '%{_esc(street_core.upper())}%'")
            attempts.append(f"UPPER(ADDRESS) LIKE '%{_esc(street_core.upper())}%'")

        feats: list = []
        status = None
        for url in (LOJIC_PARCEL_LAYER, LOJIC_ALT_LAYER):
            for where in attempts:
                data, status, raw = _arcgis_query(sess, url, where)
                res.http_status = status
                blocked = _detect_block(raw, status)
                if blocked:
                    res.blocked_reason = blocked
                    continue
                if data and data.get("features"):
                    feats = data["features"]
                    res.source = f"ArcGIS {url.split('/services/')[-1].split('/')[0]}"
                    break
            if feats:
                break

        if not feats:
            res.error = res.error or "no matching parcel features"
            res.elapsed_ms = int((time.time() - t0) * 1000)
            return res

        # Filter by house number when we have one.
        chosen = None
        if house_no:
            for f in feats:
                a = f.get("attributes", {})
                blob = " ".join(str(v) for v in a.values() if v)
                if re.search(rf"\b{re.escape(house_no)}\b", blob):
                    chosen = f
                    break
        chosen = chosen or feats[0]
        a = {k.upper(): v for k, v in (chosen.get("attributes") or {}).items()}

        def pick(*keys):
            for k in keys:
                if a.get(k) not in (None, "", " "):
                    return a[k]
            return None

        res.fields = {
            "owner_name": pick("OWNER", "OWNER_NAME", "OWNERNAME", "OWNER1"),
            "mailing_address": pick("MAILING_ADDRESS", "MAIL_ADDR", "MAILADDR", "OWNER_ADDRESS"),
            "parcel_id": pick("PARCELID", "PARCEL_ID", "PVA_PARCEL_ID", "LRSN", "GISID"),
            "assessed_value": _num(pick("TOTAL_VALUE", "TOTALVALUE", "ASSESSED_VALUE", "TOT_VAL")),
            "land_value": _num(pick("LAND_VALUE", "LANDVALUE", "LAND_VAL")),
            "improvement_value": _num(pick("IMPROVEMENT_VALUE", "IMPVALUE", "IMP_VAL", "BLDG_VALUE")),
            "acreage": _num(pick("ACRES", "ACREAGE", "DEED_ACRES", "GIS_ACRES")),
            "year_built": _num(pick("YEAR_BUILT", "YEARBUILT", "YR_BLT")),
            "beds": _num(pick("BEDROOMS", "BEDS", "NUM_BEDS")),
            "baths": _num(pick("BATHROOMS", "BATHS", "NUM_BATHS", "FULL_BATHS")),
            "sqft": _num(pick("SQUARE_FEET", "SQFT", "TOTAL_SQFT", "FIN_SQFT", "BLDG_SQFT")),
            "legal_description": pick("LEGAL_DESCRIPTION", "LEGAL", "LEGALDESC"),
            "deed_book_page": pick("DEED_BOOK_PAGE", "DEEDBOOK", "DEED_BK_PG"),
        }
        geom = chosen.get("geometry") or {}
        if "x" in geom and "y" in geom:
            res.fields["longitude"], res.fields["latitude"] = geom["x"], geom["y"]
        elif geom.get("rings"):
            ring = geom["rings"][0]
            res.fields["longitude"] = round(sum(p[0] for p in ring) / len(ring), 6)
            res.fields["latitude"] = round(sum(p[1] for p in ring) / len(ring), 6)

        res.fields = {k: v for k, v in res.fields.items() if v not in (None, "")}
        res.ok = bool(res.fields)
    except Exception as exc:  # noqa: BLE001
        res.error = f"{type(exc).__name__}: {exc}"
    res.elapsed_ms = int((time.time() - t0) * 1000)
    return res


# --------------------------------------------------------------------------
# qPublic / Schneider counties (Hardin, Bullitt, Nelson, Spencer, Washington)
# --------------------------------------------------------------------------

QPUBLIC = {
    "hardin": "ky/hardin",
    "bullitt": "ky/bullitt",
    "nelson": "ky/nelson",
    "spencer": "ky/spencer",
    "washington": "ky/washington",
}


def probe_qpublic(county: str, address: str, city: str, sess: requests.Session,
                  browser_fetch: Optional[Callable[[str], tuple[int, str]]] = None) -> ProbeResult:
    slug = QPUBLIC[county]
    res = ProbeResult(county=county, source=f"qPublic {slug}", address=address)
    t0 = time.time()
    url = (
        f"https://qpublic.schneidercorp.com/Application.aspx?"
        f"AppID=0&LayerID=0&PageTypeID=2&KeyValue={quote_plus(address)}"
    )
    base = f"https://qpublic.schneidercorp.com/Application.aspx?App={quote_plus(slug)}"
    try:
        r = sess.get(base, timeout=30)
        res.http_status = r.status_code
        blocked = _detect_block(r.text, r.status_code)
        if blocked and browser_fetch:
            res.fetcher = "browser"
            status, html = browser_fetch(base)
            res.http_status = status
            blocked = _detect_block(html, status)
            body = html
        else:
            body = r.text
        if blocked:
            res.blocked_reason = blocked
            res.error = f"blocked by {blocked}"
            res.elapsed_ms = int((time.time() - t0) * 1000)
            return res

        # Not blocked — attempt a very light extraction of labelled values.
        pairs = dict(
            (m.group(1).strip().lower(), re.sub(r"<[^>]+>", "", m.group(2)).strip())
            for m in re.finditer(r">([A-Za-z /]{3,40})</\w+>\s*<\w[^>]*>([^<]{1,120})<", body)
        )

        def g(*names):
            for n in names:
                for k, v in pairs.items():
                    if n in k and v:
                        return v
            return None

        res.fields = {
            "owner_name": g("owner name", "owner"),
            "mailing_address": g("mailing address"),
            "parcel_id": g("parcel", "pin"),
            "assessed_value": _num(g("total value", "assessed")),
            "land_value": _num(g("land value")),
            "improvement_value": _num(g("improvement")),
            "acreage": _num(g("acres", "acreage")),
            "year_built": _num(g("year built")),
            "beds": _num(g("bedroom")),
            "baths": _num(g("bathroom", "baths")),
            "sqft": _num(g("square", "sq ft")),
            "legal_description": g("legal"),
            "deed_book_page": g("deed book", "book/page"),
        }
        res.fields = {k: v for k, v in res.fields.items() if v not in (None, "")}
        res.ok = bool(res.fields)
        if not res.ok:
            res.error = "reachable but no fields parsed (search flow requires interactive session)"
    except Exception as exc:  # noqa: BLE001
        res.error = f"{type(exc).__name__}: {exc}"
    res.elapsed_ms = int((time.time() - t0) * 1000)
    return res


def probe(county: str, address: str, city: str,
          browser_fetch: Optional[Callable[[str], tuple[int, str]]] = None) -> ProbeResult:
    sess = _session()
    if county == "jefferson":
        return probe_jefferson(address, city, sess)
    if county in QPUBLIC:
        return probe_qpublic(county, address, city, sess, browser_fetch)
    r = ProbeResult(county=county, source="unknown", address=address)
    r.error = f"no extractor registered for county {county!r}"
    return r
