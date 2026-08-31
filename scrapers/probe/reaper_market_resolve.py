#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

ET = ZoneInfo("America/New_York")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
CORE_SOURCES = (
    ("Homes.com / Metro Search IDX", "homes.com"),
    ("Redfin", "redfin.com"),
    ("Realtor.com", "realtor.com"),
    ("Auction.com", "auction.com"),
    ("Zillow", "zillow.com"),
)
LAND_SOURCES = (
    ("Land.com", "land.com"),
    ("LandWatch", "landwatch.com"),
    ("LandSearch", "landsearch.com"),
)
MARKET_REASONS = {
    "market_status_unverified", "market_active", "market_pending",
    "priority_below_production_threshold", "distress_below_production_threshold",
    "motivation_below_production_threshold", "builder_fit_below_production_threshold",
    "distress_score_unverified", "motivation_score_unverified",
}
ACTIVE_STRUCTURED = {
    "ACTIVE", "FOR_SALE", "FORSALE", "COMING_SOON", "COMINGSOON", "FSBO", "AUCTION",
}
PENDING_STRUCTURED = {
    "PENDING", "CONTINGENT", "UNDER_CONTRACT", "UNDERCONTRACT", "ACCEPTING_BACKUPS",
}
OFF_STRUCTURED = {
    "OFF_MARKET", "OFFMARKET", "SOLD", "WITHDRAWN", "EXPIRED", "CANCELED", "CANCELLED",
    "OTHER", "NOT_FOR_SALE", "NOTFORSALE",
}
_thread = threading.local()


def session() -> requests.Session:
    s = getattr(_thread, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        adapter = requests.adapters.HTTPAdapter(pool_connections=12, pool_maxsize=12, max_retries=1)
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


def address_signature(address: str) -> tuple[str | None, list[str]]:
    n = norm_addr(address)
    m = re.match(r"^(\d+[A-Z]?)\s+(.+)$", n)
    if not m:
        return None, []
    house = m.group(1)
    stop = {"ST", "AVE", "RD", "DR", "LN", "CT", "BLVD", "WAY", "PL", "TER", "TRL", "HWY", "PKWY", "CIR"}
    tokens = [t for t in m.group(2).split() if t not in stop and len(t) > 1]
    return house, tokens


def exact_address_match(text: str, address: str) -> bool:
    house, tokens = address_signature(address)
    if not house or not tokens:
        return False
    hay = norm_addr(text)
    if not re.search(rf"\b{re.escape(house)}\b", hay):
        return False
    required = 1 if len(tokens) == 1 else 2
    return sum(bool(re.search(rf"\b{re.escape(t)}\b", hay)) for t in tokens) >= required


def _structured_statuses(html: str) -> set[str]:
    statuses: set[str] = set()
    patterns = (
        r'"homeStatus"\s*:\s*"([A-Za-z_ ]+)"',
        r'"listingStatus"\s*:\s*"([A-Za-z_ ]+)"',
        r'"mlsStatus"\s*:\s*"([A-Za-z_ ]+)"',
        r'"listing_status"\s*:\s*"([A-Za-z_ ]+)"',
    )
    for pat in patterns:
        for value in re.findall(pat, html[:750000], re.I):
            statuses.add(re.sub(r"[^A-Z]+", "_", value.upper()).strip("_"))
    return statuses


def parse_direct_page(source: str, domain: str, url: str, html: str, address: str, http_status: int = 200) -> dict:
    out = {
        "source": source,
        "domain": domain,
        "page_url": url,
        "http_status": http_status,
        "direct_page": True,
        "exact_address_confirmed": False,
        "status": "UNKNOWN",
        "reason": None,
    }
    if http_status in (403, 429):
        out["reason"] = f"http_{http_status}"
        return out
    if http_status >= 400:
        out["reason"] = f"http_{http_status}"
        return out
    soup = BeautifulSoup(html or "", "html.parser")
    title = clean(soup.title.get_text(" ", strip=True) if soup.title else "")
    visible = clean(soup.get_text(" ", strip=True))
    probe = f"{title} {visible[:16000]}"
    if not exact_address_match(probe, address):
        out["reason"] = "exact_address_not_confirmed"
        return out
    out["exact_address_confirmed"] = True
    early = f"{title} {visible[:10000]}".upper()
    statuses = _structured_statuses(html or "")

    if statuses & ACTIVE_STRUCTURED:
        out.update(status="ACTIVE", reason="structured_active_listing_status")
        return out
    if statuses & PENDING_STRUCTURED:
        out.update(status="PENDING", reason="structured_pending_listing_status")
        return out

    active_phrases = (
        "COMING SOON", "FOR SALE BY OWNER", "FOR SALE", "ACTIVE LISTING",
        "AUCTION DATE", "CURRENT BID", "LIVE AUCTION", "FORECLOSURE AUCTION",
    )
    pending_phrases = ("UNDER CONTRACT", "PENDING", "CONTINGENT")
    off_phrases = (
        "NOT LISTED FOR SALE", "CURRENTLY NOT FOR SALE", "THIS HOME IS NOT FOR SALE",
        "OFF MARKET", "LISTING REMOVED", "WITHDRAWN", "LISTING EXPIRED",
        "CANCELLED LISTING", "CANCELED LISTING",
    )

    # Source-specific strong positive signals get precedence over historical sale text.
    if domain == "auction.com" and any(p in early for p in ("AUCTION DATE", "CURRENT BID", "LIVE AUCTION", "FORECLOSURE AUCTION")):
        out.update(status="ACTIVE", reason="current_auction_marketing")
        return out
    if any(p in early for p in pending_phrases):
        out.update(status="PENDING", reason="explicit_pending_or_contingent")
        return out
    if any(p in early for p in active_phrases):
        # Avoid interpreting Homes.com's explicit NOT LISTED banner as FOR SALE because of navigation text.
        if "NOT LISTED FOR SALE" not in early:
            out.update(status="ACTIVE", reason="explicit_current_sale_marketing")
            return out
    if any(p in early for p in off_phrases):
        out.update(status="OFF_MARKET", reason="explicit_off_market_status")
        return out
    if statuses & OFF_STRUCTURED:
        out.update(status="OFF_MARKET", reason="structured_off_market_status")
        return out

    out["reason"] = "status_not_explicit"
    return out


def _decode_ddg(href: str) -> str:
    try:
        parsed = urlparse(href)
        q = parse_qs(parsed.query)
        if q.get("uddg"):
            return unquote(q["uddg"][0])
    except Exception:
        pass
    return href


def _domain_match(url: str, domain: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host == domain or host.endswith("." + domain)
    except Exception:
        return False


def search_domain(address: str, domain: str) -> tuple[list[str], dict]:
    query = f'"{clean(address)}" site:{domain}'
    attempts = [
        ("DuckDuckGo", "https://html.duckduckgo.com/html/", {"q": query}),
        ("Bing", "https://www.bing.com/search", {"q": query}),
    ]
    last = {
        "search_engine": None,
        "search_url": None,
        "search_http_status": None,
        "search_completed": False,
        "search_reason": "search_not_attempted",
    }
    for engine, endpoint, params in attempts:
        try:
            r = session().get(endpoint, params=params, timeout=15, allow_redirects=True)
            last.update(search_engine=engine, search_url=r.url, search_http_status=r.status_code)
            if r.status_code in (403, 429) or r.status_code >= 500:
                last["search_reason"] = f"http_{r.status_code}"
                continue
            r.raise_for_status()
            soup = BeautifulSoup(r.text or "", "html.parser")
            links: list[str] = []
            selectors = ["a.result__a"] if engine == "DuckDuckGo" else ["li.b_algo h2 a"]
            for sel in selectors:
                for a in soup.select(sel):
                    href = clean(a.get("href"))
                    if not href:
                        continue
                    href = _decode_ddg(href)
                    if href.startswith("http") and _domain_match(href, domain) and href not in links:
                        links.append(href)
            last.update(search_completed=True, search_reason="ok")
            return links[:4], last
        except Exception as exc:
            last["search_reason"] = f"search:{type(exc).__name__}"
    return [], last


def check_source(address: str, source: str, domain: str) -> dict:
    links, meta = search_domain(address, domain)
    check = {
        "source": source,
        "domain": domain,
        **meta,
        "status": "UNKNOWN",
        "reason": "no_exact_property_page_found" if meta.get("search_completed") else meta.get("search_reason"),
        "page_url": None,
        "http_status": None,
        "direct_page": False,
        "exact_address_confirmed": False,
    }
    for url in links:
        try:
            r = session().get(url, timeout=18, allow_redirects=True)
            parsed = parse_direct_page(source, domain, r.url, r.text or "", address, r.status_code)
            if parsed.get("exact_address_confirmed") or parsed.get("status") in {"ACTIVE", "PENDING", "OFF_MARKET"}:
                parsed.update({k: v for k, v in meta.items() if k not in parsed})
                return parsed
            check.update(
                page_url=r.url,
                http_status=r.status_code,
                direct_page=True,
                reason=parsed.get("reason") or check["reason"],
            )
        except Exception as exc:
            check.update(page_url=url, direct_page=True, reason=f"page:{type(exc).__name__}")
    return check


def prior_market_check(row: dict) -> dict | None:
    status = clean(row.get("market_status")).upper()
    if status not in {"ACTIVE", "PENDING", "OFF_MARKET", "UNKNOWN"}:
        return None
    reason = clean(row.get("market_reason")) or "prior_reaper_market_check"
    url = row.get("market_url")
    direct = bool(url and status in {"ACTIVE", "PENDING", "OFF_MARKET"})
    return {
        "source": clean(row.get("market_source")) or "Prior Reaper market check",
        "domain": "zillow.com" if "zillow" in clean(row.get("market_source")).lower() else None,
        "search_engine": None,
        "search_url": None,
        "search_http_status": None,
        "search_completed": bool(direct),
        "status": status,
        "reason": reason,
        "page_url": url,
        "http_status": None,
        "direct_page": direct,
        "exact_address_confirmed": direct,
    }


def decide_market_status(checks: list[dict]) -> tuple[str, str, str | None]:
    positives = [c for c in checks if c.get("status") in {"ACTIVE", "PENDING"} and c.get("exact_address_confirmed")]
    if positives:
        # Pending/contingent is still public marketing and therefore excluded just like active.
        winner = next((c for c in positives if c.get("status") == "ACTIVE"), positives[0])
        return winner["status"], f"public_marketing_found:{winner.get('source')}", winner.get("page_url")

    off = [c for c in checks if c.get("status") == "OFF_MARKET" and c.get("exact_address_confirmed")]
    off_domains = {c.get("domain") or c.get("source") for c in off}
    completed_domains = {
        c.get("domain") or c.get("source") for c in checks
        if c.get("search_completed") or (c.get("direct_page") and c.get("exact_address_confirmed"))
    }

    if len(off_domains) >= 2:
        return "OFF_MARKET", "two_independent_explicit_off_market_sources", off[0].get("page_url")
    if len(off_domains) >= 1 and len(completed_domains) >= 3:
        return "OFF_MARKET", "explicit_off_market_plus_multi_source_clear_search", off[0].get("page_url")
    return "UNKNOWN", "insufficient_independent_evidence_for_off_market", off[0].get("page_url") if off else None


def resolve_market(row: dict) -> dict:
    address = clean(row.get("source_property_address"))
    ctype = clean(row.get("candidate_type")).upper()
    checked_at = datetime.now(timezone.utc).astimezone(ET).isoformat()
    checks: list[dict] = []
    prior = prior_market_check(row)
    if prior:
        checks.append(prior)

    sources = list(CORE_SOURCES)
    if ctype == "LAND":
        sources.extend(LAND_SOURCES)
    # If the prior Zillow attempt already failed or succeeded, do not waste a second identical request.
    if prior and prior.get("domain") == "zillow.com":
        sources = [x for x in sources if x[1] != "zillow.com"]

    with cf.ThreadPoolExecutor(max_workers=min(8, max(1, len(sources)))) as ex:
        futures = {ex.submit(check_source, address, source, domain): (source, domain) for source, domain in sources}
        for fut in cf.as_completed(futures):
            source, domain = futures[fut]
            try:
                checks.append(fut.result())
            except Exception as exc:
                checks.append({
                    "source": source, "domain": domain, "status": "UNKNOWN",
                    "reason": f"worker:{type(exc).__name__}", "search_completed": False,
                    "direct_page": False, "exact_address_confirmed": False,
                })

    status, reason, supporting_url = decide_market_status(checks)
    return {
        "market_status": status,
        "market_source": "Reaper multi-source exact-address resolver",
        "market_url": supporting_url,
        "market_checked_at_et": checked_at,
        "market_status_checked_at": checked_at,
        "market_reason": reason,
        "market_checks": sorted(checks, key=lambda c: clean(c.get("source"))),
        "market_status_sources_checked": sorted({clean(c.get("source")) for c in checks if clean(c.get("source"))}),
    }


def _num(v) -> float | None:
    try:
        return float(v) if v is not None and clean(v) != "" else None
    except Exception:
        return None


def quality_gate(row: dict) -> list[str]:
    reasons: list[str] = []
    ctype = clean(row.get("candidate_type")).upper()
    priority = _num(row.get("reaper_priority_score"))
    source_score = _num(row.get("source_score"))
    if priority is None or priority < 60:
        reasons.append("priority_below_production_threshold")
    if ctype == "SFR":
        row["distress_score"] = source_score
        if source_score is None:
            reasons.append("distress_score_unverified")
        elif source_score < 50:
            reasons.append("distress_below_production_threshold")
    elif ctype == "LAND":
        row["motivation_score"] = source_score
        builder = _num(row.get("builder_fit_score"))
        if builder is None:
            builder = 50.0
            row["builder_fit_score"] = 50
        if source_score is None:
            reasons.append("motivation_score_unverified")
        elif source_score < 50:
            reasons.append("motivation_below_production_threshold")
        if builder < 50:
            reasons.append("builder_fit_below_production_threshold")
    return reasons


def can_resolve(row: dict) -> bool:
    if clean(row.get("candidate_type")).upper() not in {"SFR", "LAND"}:
        return False
    if not row.get("lojic_parcel_verified") or not row.get("pva_verified") or not row.get("current_owner_individual"):
        return False
    if clean(row.get("freshness_state")).upper() not in {"FRESH", "REACTIVATED"}:
        return False
    reasons = set(row.get("rejection_reasons") or [])
    non_market = reasons - MARKET_REASONS
    return not non_market


def requalify(row: dict) -> dict:
    reasons = [r for r in (row.get("rejection_reasons") or []) if r not in MARKET_REASONS]
    quality = quality_gate(row)
    for r in quality:
        if r not in reasons:
            reasons.append(r)

    if quality:
        row["rejection_reasons"] = reasons
        row["qualification_status"] = "REJECTED"
        return row

    row.update(resolve_market(row))
    status = clean(row.get("market_status")).upper()
    if status == "ACTIVE":
        reasons.append("market_active")
    elif status == "PENDING":
        reasons.append("market_pending")
    elif status != "OFF_MARKET":
        reasons.append("market_status_unverified")

    row["rejection_reasons"] = list(dict.fromkeys(reasons))
    row["qualification_status"] = "ELIGIBLE" if not row["rejection_reasons"] else "REJECTED"
    return row


def render_md(report: dict) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Reaper Bulk Qualification — Multi-Source Market Gate", "",
        f"Generated: {report.get('generated_at_et')}",
        f"Input candidates: {s.get('input_candidates', 0)}",
        f"Eligible SFR: {s.get('eligible_sfr', 0)}",
        f"Eligible land: {s.get('eligible_land', 0)}",
        f"Off-market verified: {s.get('off_market_verified', 0)}",
        f"Market unknown: {s.get('market_unknown', 0)}", "",
        "## Eligible SFR", "",
    ]
    for i, r in enumerate(report.get("eligible_sfr") or [], 1):
        lines += [
            f"{i}. **{r.get('source_property_address')}** — {r.get('pva_owner')}",
            f"   - Priority: {r.get('reaper_priority_score')} | Distress: {r.get('distress_score')} | Parcel: {r.get('parcel_id')} | Market: {r.get('market_status')}",
            f"   - Market sources: {', '.join(r.get('market_status_sources_checked') or [])}",
        ]
    lines += ["", "## Eligible Land", ""]
    for i, r in enumerate(report.get("eligible_land") or [], 1):
        lines += [
            f"{i}. **{r.get('source_property_address')}** — {r.get('pva_owner')}",
            f"   - Priority: {r.get('reaper_priority_score')} | Motivation: {r.get('motivation_score')} | Builder fit: {r.get('builder_fit_score')} | Market: {r.get('market_status')}",
            f"   - Market sources: {', '.join(r.get('market_status_sources_checked') or [])}",
        ]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="reports/reaper_multi_source_live/bulk_qualified.json")
    ap.add_argument("--output", default="reports/reaper_multi_source_live/bulk_qualified.json")
    ap.add_argument("--md", default="reports/reaper_multi_source_live/bulk_qualified.md")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    rows = list(report.get("all_results") or [])
    targets = [i for i, row in enumerate(rows) if can_resolve(row)]
    print(f"[market] candidates eligible for strict market resolution={len(targets)}", flush=True)

    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        future_map = {ex.submit(requalify, rows[i]): i for i in targets}
        for n, fut in enumerate(cf.as_completed(future_map), 1):
            i = future_map[fut]
            try:
                rows[i] = fut.result()
            except Exception as exc:
                rows[i]["market_status"] = "UNKNOWN"
                rows[i]["market_reason"] = f"resolver:{type(exc).__name__}"
                rows[i]["market_status_checked_at"] = datetime.now(timezone.utc).astimezone(ET).isoformat()
                rr = [r for r in (rows[i].get("rejection_reasons") or []) if r not in MARKET_REASONS]
                rr.append("market_status_unverified")
                rows[i]["rejection_reasons"] = list(dict.fromkeys(rr))
                rows[i]["qualification_status"] = "REJECTED"
            if n % 10 == 0 or n == len(targets):
                print(f"[market] {n}/{len(targets)} resolved", flush=True)

    # Never allow an old single-source eligibility result to bypass the new resolver.
    for i, row in enumerate(rows):
        if i in targets:
            continue
        if clean(row.get("qualification_status")).upper() == "ELIGIBLE":
            row["qualification_status"] = "REJECTED"
            rr = list(row.get("rejection_reasons") or [])
            if "market_status_unverified" not in rr:
                rr.append("market_status_unverified")
            row["rejection_reasons"] = rr

    eligible_sfr = [r for r in rows if r.get("qualification_status") == "ELIGIBLE" and r.get("candidate_type") == "SFR"]
    eligible_land = [r for r in rows if r.get("qualification_status") == "ELIGIBLE" and r.get("candidate_type") == "LAND"]
    summary = dict(report.get("summary") or {})
    summary.update({
        "off_market_verified": sum(r.get("market_status") == "OFF_MARKET" for r in rows),
        "active_or_pending_rejected": sum(r.get("market_status") in {"ACTIVE", "PENDING"} for r in rows),
        "market_unknown": sum(r.get("market_status") == "UNKNOWN" for r in rows),
        "eligible_sfr": len(eligible_sfr),
        "eligible_land": len(eligible_land),
        "multi_source_market_candidates_checked": len(targets),
    })
    report.update({
        "generated_at_et": datetime.now(timezone.utc).astimezone(ET).isoformat(),
        "summary": summary,
        "eligible_sfr": eligible_sfr,
        "eligible_land": eligible_land,
        "all_results": rows,
    })
    notes = [
        n for n in (report.get("notes") or [])
        if "zillow" not in clean(n).lower() or "off-market" not in clean(n).lower()
    ]
    notes.append(
        "Callable off-market status uses the Reaper multi-source exact-address resolver. Any confirmed active, coming-soon, pending, contingent, FSBO, or auction marketing excludes the property. A blocked portal is recorded as UNKNOWN and is not treated as a negative. OFF_MARKET requires either two independent direct explicit off-market pages, or one direct explicit off-market page plus completed checks across at least three independent market sources with no current public marketing found."
    )
    notes.append(
        "Zillow is retained as one market signal but is not a single point of failure; HTTP 403/429 responses do not by themselves determine market status."
    )
    report["notes"] = notes

    Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    Path(args.md).write_text(render_md(report), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
