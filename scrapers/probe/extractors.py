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

# Jefferson County KY Parcels (LOJIC OpenDataPVA) - authoritative PVA layer.
LOJIC_PARCEL_LAYER = (
    "https://gis.lojic.org/maps/rest/services/LojicSolutions/"
    "OpenDataPVA/MapServer/1/query"
)
# Secondary PVA layer on the same service (assessment / improvement detail).
LOJIC_ALT_LAYER = (
    "https://gis.lojic.org/maps/rest/services/LojicSolutions/"
    "OpenDataPVA/MapServer/0/query"
)


def _arcgis_query(sess: requests.Session, url: str, where: str, timeout: int = 90) -> tuple[Optional[dict], int, str]:
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


LOJIC_SERVICE = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataPVA/MapServer"
JEFFERSON_DIAG: list = []


def _layer_catalog(sess: requests.Session) -> list[dict]:
    """Discover layers + their field names on the LOJIC PVA service."""
    out = []
    try:
        r = sess.get(LOJIC_SERVICE, params={"f": "json"}, timeout=60)
        root = r.json()
    except Exception as exc:  # noqa: BLE001
        JEFFERSON_DIAG.append({"step": "service_root", "error": str(exc)})
        return out
    JEFFERSON_DIAG.append({"step": "service_root", "layers": [l.get("name") for l in root.get("layers", [])]})
    for lyr in root.get("layers", []):
        lid = lyr.get("id")
        try:
            lr = sess.get(f"{LOJIC_SERVICE}/{lid}", params={"f": "json"}, timeout=60).json()
        except Exception as exc:  # noqa: BLE001
            JEFFERSON_DIAG.append({"step": "layer", "id": lid, "error": str(exc)})
            continue
        fields = [f.get("name") for f in (lr.get("fields") or [])]
        JEFFERSON_DIAG.append({"step": "layer", "id": lid, "name": lyr.get("name"), "fields": fields})
        out.append({"id": lid, "name": lyr.get("name") or "", "fields": fields})
    return out


_CATALOG_CACHE: list = []


ADDR_LAYER = "https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataAddresses/MapServer/0/query"
PVA_SEARCH = "https://jeffersonpva.ky.gov/property-search/"


def probe_jefferson(address: str, city: str, sess: requests.Session,
                    browser_fetch: Optional[Callable[[str], tuple[int, str]]] = None) -> ProbeResult:
    res = ProbeResult(county="jefferson", source="LOJIC OpenDataAddresses + Jefferson PVA", address=address)
    t0 = time.time()
    house_no, street = _split_address(address)
    fields: dict = {}
    try:
        try:
            # 1) Address point layer -> parcel id, LRSN, normalized address, coords
            terms = [address.upper(), f"{house_no} {street}".upper().strip()]
            feats = []
            for term in terms:
                if not term:
                    continue
                where = f"UPPER(ADDRESS) LIKE '%{_esc(term)}%'"
                data, status, raw = _arcgis_query(sess, ADDR_LAYER, where, timeout=60)
                res.http_status = status
                JEFFERSON_DIAG.append({"step": "addr_query", "term": term, "status": status,
                                       "count": len((data or {}).get("features", []))})
                if data and data.get("features"):
                    feats = data["features"]
                    break
            if not feats and street:
                where = (f"UPPER(STRNAME) LIKE '%{_esc(re.sub(r'[^A-Za-z ]', '', street).strip().upper())}%'"
                         + (f" AND HOUSENO = '{_esc(house_no)}'" if house_no else ""))
                data, status, raw = _arcgis_query(sess, ADDR_LAYER, where, timeout=60)
                res.http_status = status
                JEFFERSON_DIAG.append({"step": "addr_query_parts", "where": where, "status": status,
                                       "count": len((data or {}).get("features", []))})
                feats = (data or {}).get("features", [])

            if feats:
                a = {k.upper(): v for k, v in (feats[0].get("attributes") or {}).items()}
                fields["situs_address"] = a.get("ADDRESS")
                fields["zip"] = a.get("ZIPCODE")
                fields["parcel_id"] = a.get("PARCELID")
                fields["lrsn"] = a.get("LRSN")
                g = feats[0].get("geometry") or {}
                if "x" in g and "y" in g:
                    fields["longitude"], fields["latitude"] = g["x"], g["y"]
            else:
                res.error = "address not found in LOJIC address points"
        except Exception as exc:  # noqa: BLE001
            res.error = f"LOJIC unreachable: {type(exc).__name__}: {exc}"
            JEFFERSON_DIAG.append({"step": "gis", "error": str(exc)})

        # 2) Jefferson PVA public search for owner / assessment
        pid = fields.get("parcel_id")
        pva_url = f"{PVA_SEARCH}?searchtype=parcel&search={pid}" if pid else f"{PVA_SEARCH}?search={address.replace(' ', '+')}"
        html = ""
        try:
            r = sess.get(pva_url, timeout=60)
            html, pstatus = r.text, r.status_code
        except Exception as exc:  # noqa: BLE001
            html, pstatus = "", 0
            JEFFERSON_DIAG.append({"step": "pva_http", "error": str(exc)})
        blocked = _detect_block(html, pstatus)
        if (blocked or len(html) < 2000) and browser_fetch:
            res.browser_fallback_used = True
            try:
                pstatus, html = browser_fetch(pva_url)
                blocked = _detect_block(html, pstatus)
            except Exception as exc:  # noqa: BLE001
                JEFFERSON_DIAG.append({"step": "pva_browser", "error": str(exc)})
        JEFFERSON_DIAG.append({"step": "pva_page", "url": pva_url, "status": pstatus,
                               "len": len(html), "blocked": blocked,
                               "excerpt": re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))[:1500]})
        if blocked:
            res.blocked_reason = blocked
        else:
            text = re.sub(r"\s+", " ", re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I))
            text = re.sub(r"<[^>]+>", " | ", text)
            def grab(label, pattern=r"([^|]{2,80})"):
                m = re.search(label + r"\s*\|*\s*" + pattern, text, re.I)
                return m.group(1).strip(" |") if m else None
            for key, label in [("owner_name", r"Owner(?: Name)?"),
                               ("mailing_address", r"Mailing Address"),
                               ("assessed_value", r"(?:Total )?Assess(?:ed|ment)(?: Value)?"),
                               ("market_value", r"Market Value"),
                               ("year_built", r"Year Built"),
                               ("sqft", r"(?:Total )?(?:Living|Finished) Area|Square Feet"),
                               ("acreage", r"Acre(?:s|age)"),
                               ("last_sale_date", r"(?:Last )?Sale Date"),
                               ("last_sale_price", r"(?:Last )?Sale Price"),
                               ("legal_description", r"Legal Description")]:
                v = grab(label)
                if v:
                    fields[key] = v
        res.fields = {k: v for k, v in fields.items() if v not in (None, "", " ")}
        res.ok = bool(res.fields)
        if res.ok:
            res.error = None
    except Exception as exc:  # noqa: BLE001
        res.error = f"{type(exc).__name__}: {exc}"
    res.elapsed_ms = int((time.time() - t0) * 1000)
    return res


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
        return probe_jefferson(address, city, sess, browser_fetch)
    if county in QPUBLIC:
        return probe_qpublic(county, address, city, sess, browser_fetch)
    r = ProbeResult(county=county, source="unknown", address=address)
    r.error = f"no extractor registered for county {county!r}"
    return r
