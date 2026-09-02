import json
import sys
from datetime import datetime

import pytest

from scrapers.agent_allocator import qualify_house, qualify_land
from scrapers.probe.reaper_live_ai_rank import (
    CONTRACT_VERSION,
    HouseBatch,
    _copilot_classify_batch,
    _ground_ai_signals,
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
                    "details": "Unsafe structural foundation and roof collapse with sewage, mold, and a boarded opening. " * 4,
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
            "signals": [
                "unsafe_structure", "structural_damage", "roof_risk", "mold", "vacancy",
                "multiple_distress_sources",
            ],
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
    assert "multiple_distress_sources" in house["ai_raw_signals"]
    assert "multiple_distress_sources" not in house["ai_signals"]
    assert house["ai_signal_adjustments"] == [{
        "signal": "multiple_distress_sources",
        "action": "REMOVED_BEFORE_SCORING",
        "reason": "requires_at_least_two_source_types",
    }]
    assert scored["summary"]["ai_signal_adjustments"] == 1
    assert scored["summary"]["ai_signal_adjustment_reasons"] == {
        "requires_at_least_two_source_types": 1,
    }
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


@pytest.mark.parametrize(("row", "signal", "reason"), [
    (
        {"candidate_type": "SFR", "sources": ["louisville_code_violations"]},
        "multiple_distress_sources",
        "requires_at_least_two_source_types",
    ),
    (
        {"candidate_type": "SFR", "sources": ["louisville_code_violations"]},
        "tax_delinquent",
        "requires_tax_delinquent_source",
    ),
    (
        {"candidate_type": "SFR", "sources": ["louisville_code_violations"]},
        "mortgage_distress",
        "requires_lis_pendens_source",
    ),
    (
        {"candidate_type": "SFR", "sources": ["louisville_code_violations"]},
        "probate_or_inherited",
        "requires_wills_source",
    ),
    (
        {
            "candidate_type": "SFR",
            "sources": ["louisville_code_violations"],
            "evidence": [{"source": "louisville_code_violations", "details": "Peeling paint."}],
        },
        "roof_risk",
        "requires_roof_or_gutter_evidence_text",
    ),
    (
        {
            "candidate_type": "LAND",
            "pva_situs_address": "100 TEST ST",
            "pva_mailing_address": "100 TEST ST",
        },
        "absentee_owner",
        "requires_verified_different_owner_mailing_address",
    ),
    (
        {"candidate_type": "LAND", "vacant_lot_context": False},
        "vacant_lot",
        "requires_land_vacant_lot_context",
    ),
])
def test_context_dependent_ai_signal_is_removed_before_scoring(row, signal, reason):
    grounded, adjustments = _ground_ai_signals(row, {"signals": [signal]})

    assert grounded["signals"] == []
    assert adjustments == [{
        "signal": signal,
        "action": "REMOVED_BEFORE_SCORING",
        "reason": reason,
    }]


def test_context_dependent_ai_signals_remain_when_property_record_supports_them():
    house = {
        "candidate_type": "SFR",
        "sources": ["lis_pendens", "tax_delinquent", "wills", "louisville_code_violations"],
        "evidence": [{"source": "louisville_code_violations", "details": "Roof and gutter failure."}],
    }
    house_signals = [
        "multiple_distress_sources", "tax_delinquent", "mortgage_distress",
        "probate_or_inherited", "roof_risk",
    ]
    grounded_house, house_adjustments = _ground_ai_signals(house, {"signals": house_signals})

    land = {
        "candidate_type": "LAND",
        "sources": ["tax_delinquent"],
        "pva_situs_address": "200 TEST AVE",
        "pva_mailing_address": "999 ELSEWHERE RD",
        "vacant_lot_context": True,
    }
    land_signals = ["tax_delinquent", "absentee_owner", "vacant_lot"]
    grounded_land, land_adjustments = _ground_ai_signals(land, {"signals": land_signals})

    assert grounded_house["signals"] == house_signals
    assert house_adjustments == []
    assert grounded_land["signals"] == land_signals
    assert land_adjustments == []


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


def test_copilot_retries_with_validation_feedback_and_tools_denied(monkeypatch):
    source = _report()
    house = source["all_results"][0]
    valid = _classifications(source)[model_key(house)]
    invalid = json.loads(json.dumps(valid))
    invalid["signals"] = ["roof_damage"]
    calls = []

    class Result:
        returncode = 0
        stderr = ""

        def __init__(self, stdout):
            self.stdout = stdout

    def fake_run(command, **kwargs):
        calls.append(command)
        item = invalid if len(calls) == 1 else valid
        return Result(json.dumps({"classifications": [item]}))

    monkeypatch.setattr("scrapers.probe.reaper_live_ai_rank.subprocess.run", fake_run)
    output = _copilot_classify_batch([house], "SFR", "auto", "test-token")

    assert list(output) == [model_key(house)]
    assert len(calls) == 2
    assert "--deny-tool=shell,write,read,url,memory" in calls[0]
    assert "previous response failed validation" in calls[1][2]


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


def test_cli_fails_closed_without_live_ai_credential(tmp_path, monkeypatch):
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(_report()), encoding="utf-8")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(sys, "argv", [
        "reaper_live_ai_rank",
        "--input", str(input_path),
        "--output", str(tmp_path / "output.json"),
        "--md", str(tmp_path / "output.md"),
    ])

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY or workflow GITHUB_TOKEN"):
        main()
    assert not (tmp_path / "output.json").exists()
