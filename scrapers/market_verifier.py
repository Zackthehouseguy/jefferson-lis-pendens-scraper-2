from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup

PORTALS = {
    "zillow": "zillow.com",
    "realtor": "realtor.com",
    "redfin": "redfin.com",
    "homes": "homes.com",
}

ACTIVE_TERMS = (
    "for sale",
    "coming soon",
    "pending",
    "contingent",
    "active listing",
    "listed for",
    "auction",
    "for sale by owner",
    "fsbo",
)

OFF_MARKET_TERMS = (
    "off market",
    "not currently for sale",
    "not for sale",
    "sold",
    "last sold",
    "property is not currently for sale",
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


@dataclass
class Evidence:
    portal: str
    source: str
    url: str | None
    verdict: str
    text: str
    opened: bool = False
    http_status: int | None = None


def norm_text(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def address_tokens(address: str) -> list[str]:
    n = norm_text(address)
    # Ignore generic locality/state words; retain street number/name and ZIP.
    stop = {"louisville", "ky", "kentucky", "usa"}
    return [t for t in n.split() if t not in stop and len(t) > 1]


def looks_like_same_property(address: str, text: str) -> bool:
    toks = address_tokens(address)
    if not toks:
        return False
    txt = norm_text(text)
    number = toks[0] if toks and toks[0].isdigit() else None
    if number and number not in txt.split():
        return False
    meaningful = [t for t in toks[1:] if not t.isdigit() and len(t) >= 3]
    if not meaningful:
        return bool(number)
    # Require at least one street-name token in addition to the street number.
    return any(t in txt for t in meaningful[:3])


def classify_text(text: str) -> str:
    n = norm_text(text)
    # Active wins over off-market to avoid an old SOLD/OFF MARKET phrase masking
    # a newer live listing on the same page/snippet.
    if any(term in n for term in ACTIVE_TERMS):
        return "ACTIVE_MARKETED"
    if any(term in n for term in OFF_MARKET_TERMS):
        return "OFF_MARKET"
    return "UNKNOWN"


def unwrap_search_url(href: str) -> str:
    if not href:
        return href
    if href.startswith("//"):
        href = "https:" + href
    try:
        p = urlparse(href)
        if "duckduckgo.com" in p.netloc:
            q = parse_qs(p.query)
            if "uddg" in q and q["uddg"]:
                return unquote(q["uddg"][0])
    except Exception:
        pass
    return href


def get(session: requests.Session, url: str, timeout: int = 15) -> requests.Response | None:
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        return r
    except requests.RequestException:
        return None


def ddg_results(session: requests.Session, query: str) -> list[tuple[str, str]]:
    url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query)
    r = get(session, url)
    if not r or r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    rows: list[tuple[str, str]] = []
    for result in soup.select(".result")[:8]:
        a = result.select_one("a.result__a")
        if not a:
            continue
        href = unwrap_search_url(a.get("href") or "")
        snippet = result.select_one(".result__snippet")
        text = " ".join(
            x for x in [a.get_text(" ", strip=True), snippet.get_text(" ", strip=True) if snippet else ""] if x
        )
        rows.append((href, text))
    return rows


def bing_results(session: requests.Session, query: str) -> list[tuple[str, str]]:
    url = "https://www.bing.com/search?q=" + quote_plus(query)
    r = get(session, url)
    if not r or r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    rows: list[tuple[str, str]] = []
    for result in soup.select("li.b_algo")[:8]:
        a = result.select_one("h2 a")
        if not a:
            continue
        p = result.select_one("p")
        text = " ".join(
            x for x in [a.get_text(" ", strip=True), p.get_text(" ", strip=True) if p else ""] if x
        )
        rows.append((a.get("href") or "", text))
    return rows


def page_text(session: requests.Session, url: str) -> tuple[str, int | None, bool]:
    r = get(session, url)
    if not r:
        return "", None, False
    status = r.status_code
    if status != 200:
        return "", status, False
    ctype = (r.headers.get("content-type") or "").lower()
    if "html" not in ctype and ctype:
        return "", status, False
    soup = BeautifulSoup(r.text, "html.parser")
    parts = []
    if soup.title:
        parts.append(soup.title.get_text(" ", strip=True))
    for key in ("description", "og:description", "twitter:description"):
        tag = soup.find("meta", attrs={"name": key}) or soup.find("meta", attrs={"property": key})
        if tag and tag.get("content"):
            parts.append(tag.get("content"))
    body = soup.get_text(" ", strip=True)
    if body:
        parts.append(body[:25000])
    return " ".join(parts), status, True


def verify_address(address: str, delay: float = 0.8) -> dict:
    checked_at = dt.datetime.now(dt.timezone.utc).isoformat()
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    evidence: list[Evidence] = []
    seen_urls: set[str] = set()

    for portal, domain in PORTALS.items():
        query = f'"{address}" site:{domain}'
        results = ddg_results(session, query)
        if not results:
            results = bing_results(session, query)
        for href, snippet in results[:4]:
            if domain not in (urlparse(href).netloc or "").lower():
                continue
            combined = f"{href} {snippet}"
            if not looks_like_same_property(address, combined):
                continue
            verdict = classify_text(snippet)
            evidence.append(Evidence(portal, "search", href, verdict, snippet[:1200]))
            if href and href not in seen_urls:
                seen_urls.add(href)
                text, status, opened = page_text(session, href)
                if opened and looks_like_same_property(address, f"{href} {text}"):
                    evidence.append(
                        Evidence(portal, "property_page", href, classify_text(text), text[:2000], True, status)
                    )
                elif status is not None:
                    evidence.append(Evidence(portal, "property_page", href, "UNKNOWN", "", False, status))
            break
        time.sleep(delay)

    active = [e for e in evidence if e.verdict == "ACTIVE_MARKETED"]
    off = [e for e in evidence if e.verdict == "OFF_MARKET"]
    off_portals = {e.portal for e in off}
    direct_opened = any(e.source == "property_page" and e.opened for e in evidence)

    if active:
        status = "PUBLICLY MARKETED - DO NOT DELIVER"
        reason = "At least one current portal/search signal indicates an active public sale status."
    elif len(off_portals) >= 2 and direct_opened:
        status = "OFF-MARKET VERIFIED"
        reason = "At least two independent portal sources indicate off-market/sold/not-for-sale and at least one property page opened successfully; no active-sale signal was found."
    else:
        status = "MARKET STATUS UNVERIFIED"
        reason = "Insufficient independent current portal evidence to prove off-market status."

    return {
        "market_status": status,
        "market_status_checked_at": checked_at,
        "market_status_reason": reason,
        "market_sources_checked": list(PORTALS.values()),
        "market_verification": {
            "method": "MULTI_PORTAL_SEARCH_PLUS_DIRECT_PAGE_V1",
            "minimum_off_market_portals": 2,
            "requires_direct_property_page_open": True,
            "active_signal_count": len(active),
            "off_market_signal_count": len(off),
            "off_market_portals": sorted(off_portals),
            "direct_property_page_opened": direct_opened,
            "evidence": [asdict(e) for e in evidence],
        },
    }


def iter_leads(batch: dict) -> Iterable[dict]:
    for key in ("houses", "land"):
        for lead in batch.get(key, []) or []:
            yield lead


def main() -> int:
    ap = argparse.ArgumentParser(description="Conservative live listing-status verifier for Reaper agent batches")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", help="Defaults to overwriting --input")
    ap.add_argument("--delay", type=float, default=0.8)
    args = ap.parse_args()

    src = Path(args.input)
    out = Path(args.output) if args.output else src
    batch = json.loads(src.read_text(encoding="utf-8"))

    counts = {"OFF-MARKET VERIFIED": 0, "PUBLICLY MARKETED - DO NOT DELIVER": 0, "MARKET STATUS UNVERIFIED": 0}
    for lead in iter_leads(batch):
        address = (
            lead.get("property_address")
            or lead.get("source_property_address")
            or lead.get("address")
        )
        if not address:
            result = {
                "market_status": "MARKET STATUS UNVERIFIED",
                "market_status_checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "market_status_reason": "Missing property address.",
                "market_sources_checked": [],
                "market_verification": {"method": "MULTI_PORTAL_SEARCH_PLUS_DIRECT_PAGE_V1", "evidence": []},
            }
        else:
            result = verify_address(str(address), delay=args.delay)
        lead.update(result)
        counts[result["market_status"]] = counts.get(result["market_status"], 0) + 1
        print(f"{address or '[missing address]'} -> {result['market_status']}")

    batch["market_verification_summary"] = {
        "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "counts": counts,
        "hard_stop": "Only OFF-MARKET VERIFIED may proceed to CRM",
    }
    out.write_text(json.dumps(batch, indent=2), encoding="utf-8")
    print(json.dumps(batch["market_verification_summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
