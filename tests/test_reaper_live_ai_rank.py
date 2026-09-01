import json
import sys
from datetime import datetime

import pytest

from scrapers.agent_allocator import qualify_house, qualify_land
from scrapers.probe.reaper_live_ai_rank import (
    CONTRACT_VERSION,
    HouseBatch,
    main,
    model_key,
    score_report,
)
from scrapers.probe.render_lead_cards import house_card, land_card


def _report():
    return {
        "status": "PASS",
        "generated_at_et": "2026-09-01T09:00:00-04:00",
        "summary": {"input_candidates": 3, "eligible_sfr": 1, "eligible_land": 1},
        "eligible_sfr": [],
        "eligible_land": [],
        "all_results": [
            {
                "property_key": "ADDR::101 TEST ST",
                "source_property_address": "101 Test St, Louisville, KY 40202",
                "sources": ["louisville_code_violations"],
                "evidence": [{
                    "source": "louisville_code_violations",
                    "signal_date": "09/01/2026",
                    "details": "Unsafe structural foundation collapse with sewage, mold, and a boarded opening. " * 4,
                    "source_url": "https://example.test/code/101",
                }],
                "parcel_id": "HOUSE101",
                "lojic_parcel_verified": True,
                "parcel_type": 0,
                "lot_sqft": 5000,
                "lot_acres": 0.1148,
                "landuse_name": "SINGLE FAMILY",
                "pva_verified": True,
                "pva_owner": "TEST OWNER",
                "pva_situs_address": "101 TEST ST",
                "pva_mailing_address": "PO BOX 101",
                "current_owner_individual": True,
                "vacant_lot_context": False,
                "candidate_type": "SFR",
                "freshness_state": "FRESH",
                "rejection_reasons": [],
                "qualification_status": "ELIGIBLE",
                "reaper_priority_score": 80,
            },
            {
                "property_key": "ADDR::202 TEST AVE",
                "source_property_address": "202 Test Ave, Louisville, KY 40203",
                "sources": ["tax_delinquent", "louisville_code_violations"],
                "evidence": [
                    {
                        "source": "tax_delinquent",
                        "signal_date": "09/01/2026",
                        "amount": 7200,
                        "details": "Published delinquent-tax list entry.",
                        "source_url": "https://example.test/tax/202",
                    },
                    {
                        "source": "louisville_code_violations",
                        "signal_date": "09/01/2026",
                        "details": "Vacant lot with repeated municipal cleanup and overgrown vegetation.",
                        "source_url": "https://example.test/code/202",
                    },
                ],
                "parcel_id": "LAND202",
                "lojic_parcel_verified": True,
                "parcel_type": 0,
                "lot_sqft": 6000,
                "lot_acres": 0.1377,
                "landuse_name": "VACANT",
                "pva_verified": True,
                "pva_owner": "LAND OWNER",
                "pva_situs_address": "202 TEST AVE",
                "pva_mailing_address": "999 ELSEWHERE RD",
                "current_owner_individual": True,
                "vacant_lot_context": True,
                "candidate_type": "LAND",
                "freshness_state": "FRESH",
                "rejection_reasons": ["priority_below_production_threshold"],
                "qualification_status": "REJECTED",
                "reaper_priority_score": 55,
            },
            {
                "property_key": "ADDR::303 REJECTED RD",
                "source_property_address": "303 Rejected Rd, Louisville, KY 40204",
                "sources": ["lis_pendens"],
                "evidence": [],
                "parcel_id": "REJECT303",
                "candidate_type": "SFR",
                "landuse_name": "SINGLE FAMILY",
                "rejection_reasons": ["current_owner_not_verified_individual"],
                "qualification_status": "REJECTED",
            },
        ],
        "notes": [],
    }


def _classifications(report):
    house, land = report["all_results"][:2]
    return {
        model_key(house): {
            "property_key": model_key(house),
            "distress_level": "HIGH",
            "signals": ["unsafe_structure", "structural_damage", "mold", "vacancy"],
            "confirmed_facts": ["Louisville code evidence reports unsafe structural conditions."],
            "speculative_claims": [],
            "summary": "Current code evidence reports multiple severe property-condition signals.",
            "acquisition_relevant": True,
        },
        model_key(land): {
            "property_key": model_key(land),
            "motivation_level": "HIGH",
            "signals": ["vacant_lot", "tax_delinquent", "municipal_cleanup", "absentee_owner"],
            "confirmed_facts": ["The supplied sources report a vacant lot and a tax-list entry."],
            "speculative_claims": [],
            "summary": "Two current public sources show vacant-land carrying burden.",
            "acquisition_relevant": True,
        },
    }


def test_end_to_end_scoring_populates_allocator_and_card_contracts():
    source = _report()
    scored = score_report(
        source,
        _classifications(source),
        scoring_status="LIVE",
        model="gpt-5.6",
        now=datetime.fromisoformat("2026-09-01T10:00:00-04:00"),
    )

    assert scored["status"] == "PASS"
    assert scored["summary"]["ai_targets"] == 2
    assert scored["summary"]["ai_classified"] == 2
    assert scored["summary"]["ai_unclassified"] == 0
    assert scored["summary"]["saturation_distinct_scores"] == 2
    assert len(scored["eligible_sfr"]) == 1
    assert len(scored["eligible_land"]) == 1

    house = scored["eligible_sfr"][0]
    land = scored["eligible_land"][0]
    assert qualify_house(house)
    assert qualify_land(land)
    assert house["ai_contract_version"] == CONTRACT_VERSION
    assert house["distress_score"] >= 50 and house["priority_score"] >= 60
    assert land["motivation_score"] >= 50 and land["builder_fit_score"] >= 50
    assert land["priority_score"] >= 60
    assert house["saturation_factors"] and land["saturation_factors"]
    assert house["priority_components"] == {
        "distress_score": house["distress_score"],
        "freshness_score": house["freshness_score"],
        "saturation_score": house["saturation_score"],
    }
    assert house["deterministic_score_inputs"]["ai_level"] == "HIGH"
    assert "0.60*distress_score" in house["priority_formula"]
    assert "0.46*motivation_score" in land["priority_formula"]
    assert "not observed competition" in house["saturation_method"]
    assert "**None/100**" not in house_card(house)
    assert "**—** — GPT evidence classification" not in house_card(house)
    assert "**None/100**" not in land_card(land)


def test_allocator_rejects_old_or_fixture_scoring():
    source = _report()
    scored = score_report(
        source,
        _classifications(source),
        scoring_status="TEST_FIXTURE",
        model="fixture",
        now=datetime.fromisoformat("2026-09-01T10:00:00-04:00"),
    )
    assert not qualify_house(scored["eligible_sfr"][0])
    assert not qualify_land(scored["eligible_land"][0])
    old = dict(scored["eligible_sfr"][0])
    old.pop("ai_scoring_status")
    assert not qualify_house(old)


def test_structured_output_schema_forbids_numeric_model_scores():
    schema = HouseBatch.model_json_schema()
    item = schema["$defs"]["HouseClassification"]
    assert item["additionalProperties"] is False
    assert "distress_score" not in item["properties"]
    assert "priority_score" not in item["properties"]
    assert set(item["required"]) == set(item["properties"])


def test_cli_fixture_path_writes_complete_scored_report(tmp_path, monkeypatch):
    source = _report()
    input_path = tmp_path / "input.json"
    fixture_path = tmp_path / "fixture.json"
    output_path = tmp_path / "output.json"
    markdown_path = tmp_path / "output.md"
    input_path.write_text(json.dumps(source), encoding="utf-8")
    fixture_path.write_text(json.dumps({"classifications": _classifications(source)}), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [
        "reaper_live_ai_rank",
        "--input", str(input_path),
        "--output", str(output_path),
        "--md", str(markdown_path),
        "--fixture-classifications", str(fixture_path),
    ])

    assert main() == 0
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["summary"]["ai_classified"] == output["summary"]["ai_targets"] == 2
    assert output["summary"]["ai_unclassified"] == 0
    assert "AI scoring status: TEST_FIXTURE" in markdown_path.read_text(encoding="utf-8")


def test_cli_fails_closed_without_api_key(tmp_path, monkeypatch):
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(_report()), encoding="utf-8")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(sys, "argv", [
        "reaper_live_ai_rank",
        "--input", str(input_path),
        "--output", str(tmp_path / "output.json"),
        "--md", str(tmp_path / "output.md"),
    ])

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY is required"):
        main()
    assert not (tmp_path / "output.json").exists()
