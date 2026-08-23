#!/usr/bin/env python3
"""Thin hotfix wrapper for Accela engine probe v2.

Corrects Accela's ../Cap parent-link resolution and uses a shorter HTTP timeout
for child pages so a slow child lookup cannot stall the bench test.
"""
from __future__ import annotations

import time

from scrapers.probe import accela_engine_probe_v2 as probe

# Child pages live under /LJCMG/Cap/. Their `../Cap/CapDetail.aspx` related-record
# links must resolve back to /LJCMG/Cap/, not to the site root /Cap/.
probe.ACCELA_ROOT = "https://aca-prod.accela.com/LJCMG/Cap/"


def fast_http_resolve_parent(session, group):
    rep = group["representative"]
    t0 = time.perf_counter()
    try:
        r = session.get(rep["child_url"], timeout=8)
        status = r.status_code
        r.raise_for_status()
        html = r.text
        parent_case, parent_url = probe.resolve_parent_from_html(html, group["parent_key"])
        return {
            "ok": bool(parent_case and parent_url),
            "parent_case": parent_case,
            "parent_url": parent_url,
            "child_http_status": status,
            "child_http_seconds": round(time.perf_counter() - t0, 3),
            "child_parcel_html": probe.parse_parcel(html),
            "inspector_comments": probe.parse_inspector_comments(html),
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


probe.http_resolve_parent = fast_http_resolve_parent

if __name__ == "__main__":
    raise SystemExit(probe.main())
