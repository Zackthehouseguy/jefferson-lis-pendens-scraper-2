#!/usr/bin/env python3
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from scrapers.agent_allocator import allocate_kind, grouped_candidates, load_rows

POOL = Path("reports/daily_mix_final/current.json")
OUT = Path("reports/agent_collision_acceptance/report.json")


def keys(rows):
    return {r.get("property_key") for r in rows if r.get("property_key")}


def parcels(rows):
    return {str(r.get("parcel_id")).strip() for r in rows if str(r.get("parcel_id") or "").strip()}


def compact(rows):
    return [
        {
            "property_key": r.get("property_key"),
            "parcel_id": r.get("parcel_id"),
            "property_address": r.get("property_address"),
            "priority_score": r.get("priority_score"),
            "assignment_status": r.get("assignment_status"),
            "property_event_rows_in_pool": r.get("property_event_rows_in_pool"),
        }
        for r in rows
    ]


def assert_true(assertions, name, condition, detail=None):
    assertions.append({"name": name, "pass": bool(condition), "detail": detail})


def allocate_mix(houses, land, agent_id, h_limit, l_limit, state, now):
    h, hs = allocate_kind(rows=houses, kind="house", agent_id=agent_id, limit=h_limit, state=state, now=now)
    l, ls = allocate_kind(rows=land, kind="land", agent_id=agent_id, limit=l_limit, state=state, now=now)
    return h, l, {"house": hs, "land": ls}


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    houses = load_rows(POOL, "house")
    land = load_rows(POOL, "land")
    h_groups, h_prep = grouped_candidates(houses, "house")
    l_groups, l_prep = grouped_candidates(land, "land")
    h_unique = len(h_groups)
    l_unique = len(l_groups)

    assertions = []
    assert_true(assertions, "real_pool_exists", POOL.exists(), str(POOL))
    assert_true(assertions, "real_house_pool_nonempty", h_unique > 0, h_unique)
    assert_true(assertions, "real_land_pool_nonempty", l_unique > 0, l_unique)

    # TEST A: Meaningful fair-share collision test. Both agents receive leads
    # from the exact same real qualified pool, with one shared ledger.
    fair_h = min(12, h_unique // 2)
    fair_l = min(10, l_unique // 2)
    fair_state = {"schema_version": 1, "properties": {}}
    zh, zl, zstats = allocate_mix(houses, land, "zack", fair_h, fair_l, fair_state, now)
    kh, kl, kstats = allocate_mix(houses, land, "kyle", fair_h, fair_l, fair_state, now)
    z_all = zh + zl
    k_all = kh + kl

    assert_true(assertions, "fair_share_zack_house_target", len(zh) == fair_h, {"target": fair_h, "actual": len(zh)})
    assert_true(assertions, "fair_share_kyle_house_target", len(kh) == fair_h, {"target": fair_h, "actual": len(kh)})
    assert_true(assertions, "fair_share_zack_land_target", len(zl) == fair_l, {"target": fair_l, "actual": len(zl)})
    assert_true(assertions, "fair_share_kyle_land_target", len(kl) == fair_l, {"target": fair_l, "actual": len(kl)})
    assert_true(assertions, "fair_share_zero_property_key_overlap", not (keys(z_all) & keys(k_all)), sorted(keys(z_all) & keys(k_all)))
    assert_true(assertions, "fair_share_zero_parcel_overlap", not (parcels(z_all) & parcels(k_all)), sorted(parcels(z_all) & parcels(k_all)))
    assert_true(assertions, "zack_no_internal_property_duplicates", len(keys(z_all)) == len(z_all), {"rows": len(z_all), "keys": len(keys(z_all))})
    assert_true(assertions, "kyle_no_internal_property_duplicates", len(keys(k_all)) == len(k_all), {"rows": len(k_all), "keys": len(keys(k_all))})
    assert_true(assertions, "zack_no_internal_parcel_duplicates", len(parcels(z_all)) == len([r for r in z_all if r.get("parcel_id")]), None)
    assert_true(assertions, "kyle_no_internal_parcel_duplicates", len(parcels(k_all)) == len([r for r in k_all if r.get("parcel_id")]), None)

    # TEST B: Both agents ask for the full 25/25 from the same finite real pool.
    # Kyle may receive fewer because Zack consumed the top unique properties,
    # but any Kyle leftovers must be completely disjoint.
    full_state = {"schema_version": 1, "properties": {}}
    zfh, zfl, zfstats = allocate_mix(houses, land, "zack", 25, 25, full_state, now)
    kfh, kfl, kfstats = allocate_mix(houses, land, "kyle", 25, 25, full_state, now)
    zf_all = zfh + zfl
    kf_all = kfh + kfl
    assert_true(assertions, "full_request_zero_property_key_overlap", not (keys(zf_all) & keys(kf_all)), sorted(keys(zf_all) & keys(kf_all)))
    assert_true(assertions, "full_request_zero_parcel_overlap", not (parcels(zf_all) & parcels(kf_all)), sorted(parcels(zf_all) & parcels(kf_all)))
    assert_true(assertions, "full_request_ledger_unique_owner", all(v.get("assigned_to") in {"zack", "kyle"} for v in full_state["properties"].values()), len(full_state["properties"]))

    # TEST C: Reactivation stickiness. Use one REAL Zack-owned property and inject
    # a controlled material-change marker solely for the test. First ask Kyle:
    # he must be blocked. Then ask Zack: he must receive REACTIVATED.
    reactivation_base = next((r for r in z_all if r.get("property_event_rows_in_pool") == 1), z_all[0] if z_all else None)
    reactivation = {"tested": False}
    if reactivation_base:
        changed = deepcopy(reactivation_base)
        changed.pop("property_key", None)
        changed.pop("material_revision", None)
        changed.pop("assignment_status", None)
        comments = list(changed.get("inspector_comments") or [])
        comments.append("CONTROLLED COLLISION TEST MATERIAL CHANGE — NOT SOURCE DATA")
        changed["inspector_comments"] = comments
        kind = "land" if reactivation_base in zl else "house"
        before_owner = fair_state["properties"][reactivation_base["property_key"]]["assigned_to"]
        k_try, k_try_stats = allocate_kind(rows=[changed], kind=kind, agent_id="kyle", limit=1, state=fair_state, now=now)
        z_re, z_re_stats = allocate_kind(rows=[changed], kind=kind, agent_id="zack", limit=1, state=fair_state, now=now)
        after_owner = fair_state["properties"][reactivation_base["property_key"]]["assigned_to"]
        reactivation = {
            "tested": True,
            "real_property": compact([reactivation_base])[0],
            "test_change": "Appended controlled material-change marker; this marker is NOT public-source evidence.",
            "owner_before": before_owner,
            "kyle_delivery_count": len(k_try),
            "kyle_stats": k_try_stats,
            "zack_delivery_count": len(z_re),
            "zack_assignment_status": z_re[0].get("assignment_status") if z_re else None,
            "zack_stats": z_re_stats,
            "owner_after": after_owner,
        }
        assert_true(assertions, "reactivation_wrong_agent_blocked", len(k_try) == 0, reactivation)
        assert_true(assertions, "reactivation_returns_to_original_agent", len(z_re) == 1 and z_re[0].get("assignment_status") == "REACTIVATED", reactivation)
        assert_true(assertions, "reactivation_owner_remains_zack", before_owner == "zack" and after_owner == "zack", reactivation)
    else:
        assert_true(assertions, "reactivation_test_has_real_property", False, "No Zack property allocated")

    failed = [a for a in assertions if not a["pass"]]
    report = {
        "status": "PASS" if not failed else "FAIL",
        "generated_at_utc": now,
        "source_pool": str(POOL),
        "source_is_saved_real_jefferson_data": True,
        "production_assignment_ledger_modified": False,
        "allocator_uses_shared_parcel_first_identity": True,
        "real_pool": {
            "house_rows_loaded": len(houses),
            "land_rows_loaded": len(land),
            "qualified_unique_house_properties": h_unique,
            "qualified_unique_land_properties": l_unique,
            "house_prep": h_prep,
            "land_prep": l_prep,
        },
        "fair_share_test": {
            "requested_each": {"houses": fair_h, "land": fair_l},
            "zack": {"houses": compact(zh), "land": compact(zl), "stats": zstats},
            "kyle": {"houses": compact(kh), "land": compact(kl), "stats": kstats},
            "shared_property_keys": sorted(keys(z_all) & keys(k_all)),
            "shared_parcels": sorted(parcels(z_all) & parcels(k_all)),
        },
        "full_25_25_request_test": {
            "zack_counts": {"houses": len(zfh), "land": len(zfl), "total": len(zf_all)},
            "kyle_counts": {"houses": len(kfh), "land": len(kfl), "total": len(kf_all)},
            "shared_property_keys": sorted(keys(zf_all) & keys(kf_all)),
            "shared_parcels": sorted(parcels(zf_all) & parcels(kf_all)),
            "zack_stats": zfstats,
            "kyle_stats": kfstats,
        },
        "reactivation_test": reactivation,
        "assertions_passed": len(assertions) - len(failed),
        "assertions_failed": len(failed),
        "failed_assertions": failed,
        "assertions": assertions,
    }
    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "status": report["status"],
        "assertions_passed": report["assertions_passed"],
        "assertions_failed": report["assertions_failed"],
        "fair_zack": len(z_all),
        "fair_kyle": len(k_all),
        "full_zack": len(zf_all),
        "full_kyle": len(kf_all),
        "fair_shared_parcels": report["fair_share_test"]["shared_parcels"],
        "full_shared_parcels": report["full_25_25_request_test"]["shared_parcels"],
        "reactivation": reactivation,
    }, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
