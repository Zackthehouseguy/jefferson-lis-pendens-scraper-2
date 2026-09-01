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
