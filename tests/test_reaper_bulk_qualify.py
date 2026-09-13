from unittest.mock import patch

from scrapers.probe import reaper_bulk_qualify as bulk


def test_eligible_candidate_does_not_require_market_status_check():
    row = {
        "property_key": "test-property",
        "property_address": "123 Main St, Louisville, KY 40202",
        "parcel_id": "000000010000",
        "sources": ["louisville_code_violations"],
        "evidence": [],
        "motivation_score": 75,
    }
    parcel = {
        "lojic_parcel_verified": True,
        "parcel_type": 1,
        "pin": None,
        "lot_sqft": 5000,
        "lot_acres": 0.1148,
        "landuse_name": "SINGLE FAMILY",
    }
    pva = {
        "pva_verified": True,
        "pva_owner": "DOE JANE",
        "pva_parcel_id": "000000010000",
        "pva_assessed_value": None,
        "pva_acres": None,
        "pva_mailing_address": None,
        "pva_situs_address": "123 MAIN ST",
        "pva_url": "https://example.test/pva",
    }

    with (
        patch.object(bulk, "parcel_enrichment", return_value=(parcel, [])),
        patch.object(bulk, "pva_lookup", return_value=(pva, None)),
        patch.object(bulk, "priority_score", return_value=(75, ["test score"])),
    ):
        result = bulk.qualify_one(row, {}, {}, {}, {})

    assert result["qualification_status"] == "ELIGIBLE"
    assert result["rejection_reasons"] == []
    assert "market_status" not in result
    assert "market_source" not in result


def test_recent_verified_lojic_cache_avoids_network(tmp_path):
    import json
    from datetime import datetime, timezone

    cache = tmp_path / "lojic.json"
    cache.write_text(json.dumps({
        "version": 1,
        "parcels": {
            "ABC": {
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "lojic_parcel_verified": True,
                "parcel_type": 0,
                "pin": None,
                "lot_sqft": 5000,
                "lot_acres": 0.1148,
                "landuse_name": "SINGLE FAMILY",
            }
        },
    }), encoding="utf-8")
    bulk.load_lojic_cache(cache)
    with patch.object(bulk, "_request_with_retry", side_effect=AssertionError("network should not run")):
        result, errors = bulk.parcel_enrichment("ABC")
    assert errors == []
    assert result["lojic_parcel_verified"] is True
    assert result["landuse_name"] == "SINGLE FAMILY"
    assert result["lojic_enrichment_source"] == "recent_cache"


def test_stale_verified_lojic_cache_recovers_transport_outage(tmp_path):
    import json
    import requests
    from datetime import datetime, timedelta, timezone

    cache = tmp_path / "lojic.json"
    cache.write_text(json.dumps({
        "version": 1,
        "parcels": {
            "ABC": {
                "cached_at": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat(),
                "lojic_parcel_verified": True,
                "parcel_type": 0,
                "pin": None,
                "lot_sqft": 5000,
                "lot_acres": 0.1148,
                "landuse_name": "SINGLE FAMILY",
            }
        },
    }), encoding="utf-8")
    bulk.load_lojic_cache(cache)
    with patch.object(bulk, "_request_with_retry", side_effect=requests.ConnectionError("LOJIC unavailable")):
        result, errors = bulk.parcel_enrichment("ABC")
    assert result["lojic_parcel_verified"] is True
    assert result["lojic_enrichment_source"] == "stale_while_revalidate_cache"
    assert any(e.startswith("parcel:ConnectionError") for e in errors)


def test_land_needs_exact_vacant_landuse_and_explicit_vacant_lot_context():
    base = {
        "property_key": "land-test",
        "property_address": "123 Main St, Louisville, KY 40202",
        "parcel_id": "000000010000",
        "sources": ["louisville_code_violations"],
        "evidence": [{"details": "Occupancy: VACANT LOT"}],
        "motivation_score": 75,
    }
    pva = {
        "pva_verified": True,
        "pva_owner": "DOE JANE",
        "pva_parcel_id": "000000010000",
        "pva_assessed_value": None,
        "pva_acres": None,
        "pva_mailing_address": None,
        "pva_situs_address": "123 MAIN ST",
        "pva_url": "https://example.test/pva",
    }
    nonvacant = {
        "lojic_parcel_verified": True,
        "parcel_type": 0,
        "pin": None,
        "lot_sqft": 5000,
        "lot_acres": 0.1148,
        "landuse_name": "COMMERCIAL",
    }
    with (
        patch.object(bulk, "parcel_enrichment", return_value=(nonvacant, [])),
        patch.object(bulk, "pva_lookup", return_value=(pva, None)),
        patch.object(bulk, "priority_score", return_value=(75, ["test score"])),
    ):
        rejected = bulk.qualify_one(base, {}, {}, {}, {})
    assert rejected["candidate_type"] is None
    assert "property_type_not_target" in rejected["rejection_reasons"]

    vacant = {**nonvacant, "landuse_name": "VACANT"}
    with (
        patch.object(bulk, "parcel_enrichment", return_value=(vacant, [])),
        patch.object(bulk, "pva_lookup", return_value=(pva, None)),
        patch.object(bulk, "priority_score", return_value=(75, ["test score"])),
    ):
        eligible = bulk.qualify_one(base, {}, {}, {}, {})
    assert eligible["candidate_type"] == "LAND"
    assert eligible["qualification_status"] == "ELIGIBLE"
