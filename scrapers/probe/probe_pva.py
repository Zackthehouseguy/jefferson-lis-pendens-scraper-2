#!/usr/bin/env python3
"""Read-only PVA / GIS source probe.

Runs a small set of known-good test addresses against each county's public
property-assessment source and reports which target fields are recoverable.
Writes JSON + CSV artifacts. Makes no writes to any county system.

Usage:
  python -m scrapers.probe.probe_pva --counties jefferson,hardin --browser-fallback
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

from scrapers.probe.extractors import TARGET_FIELDS, probe  # noqa: E402

ALL_COUNTIES = ["jefferson", "hardin", "bullitt", "nelson", "spencer", "washington"]


def make_browser_fetch():
    """Return a Playwright-backed fetcher, or None if Playwright is unavailable."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        print(f"[probe] playwright unavailable ({exc}); browser fallback disabled")
        return None

    def fetch(url: str) -> tuple[int, str]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            ctx = browser.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            page = ctx.new_page()
            status = 0
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=45000)
                status = resp.status if resp else 0
                page.wait_for_timeout(6000)  # give any interstitial a chance to resolve
                html = page.content()
            except Exception as exc:  # noqa: BLE001
                html = f"<!-- browser error: {exc} -->"
            finally:
                browser.close()
            return status, html

    return fetch


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--counties", default=",".join(ALL_COUNTIES))
    ap.add_argument("--browser-fallback", nargs="?", const="true", default="false")
    ap.add_argument("--out-dir", "--out", dest="out_dir", default="probe_output")
    ap.add_argument("--seeds", default=str(HERE / "test_properties.json"))
    args = ap.parse_args()

    counties = [c.strip().lower() for c in args.counties.split(",") if c.strip()]
    seeds = json.loads(Path(args.seeds).read_text())
    browser_fetch = make_browser_fetch() if str(args.browser_fallback).lower() in ("true","1","yes") else None

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for county in counties:
        props = seeds.get(county, [])
        if not props:
            print(f"[probe] {county}: no seed properties, skipping")
            continue
        for prop in props:
            print(f"[probe] {county}: {prop['address']}...", flush=True)
            r = probe(county, prop["address"], prop.get("city", ""), browser_fetch)
            d = r.to_dict()
            results.append(d)
            print(
                f"    ok={d['ok']} status={d['http_status']} coverage={d['coverage_pct']}% "
                f"fetcher={d['fetcher']} blocked={d['blocked_reason']} err={d['error']}",
                flush=True,
            )

    # Per-county summary
    summary = {}
    for county in counties:
        rows = [r for r in results if r["county"] == county]
        if not rows:
            continue
        summary[county] = {
            "attempts": len(rows),
            "successes": sum(1 for r in rows if r["ok"]),
            "avg_coverage_pct": round(sum(r["coverage_pct"] for r in rows) / len(rows), 1),
            "best_coverage_pct": max(r["coverage_pct"] for r in rows),
            "blocked_reasons": sorted({r["blocked_reason"] for r in rows if r["blocked_reason"]}),
            "source": rows[0]["source"],
        }

    try:
        from .extractors import JEFFERSON_DIAG
        (out_dir / "jefferson_diagnostics.json").write_text(json.dumps(JEFFERSON_DIAG, indent=2, default=str))
    except Exception as exc:  # noqa: BLE001
        print("diag dump failed:", exc)

    (out_dir / "probe_results.json").write_text(
        json.dumps({"summary": summary, "results": results}, indent=2, default=str)
    )

    cols = ["county", "source", "address", "ok", "http_status", "fetcher",
            "blocked_reason", "error", "coverage_pct", "elapsed_ms"] + TARGET_FIELDS
    with (out_dir / "probe_results.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in cols}
            for f in TARGET_FIELDS:
                row[f] = r["fields"].get(f, "")
            w.writerow(row)

    print("\n=== SUMMARY ===")
    for county, s in summary.items():
        print(f"{county:12s} {s['successes']}/{s['attempts']} ok  avg={s['avg_coverage_pct']}%  "
              f"best={s['best_coverage_pct']}%  blocked={s['blocked_reasons'] or '-'}")
    print(f"\nArtifacts: {out_dir / 'probe_results.json'}, {out_dir / 'probe_results.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
