"""Tests for code-violation property dedupe + distress scoring + filtering."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers import code_violation_filter as cvf
from scrapers import louisville_code_violations as lcv


FIXTURE = REPO_ROOT / "tests" / "fixtures" / "louisville_distress_sample.json"


def _load_features() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["features"]


def _find_lead(leads: list[dict], street_substring: str) -> dict | None:
    for lead in leads:
        if street_substring.upper() in lead["Property Address"].upper():
            return lead
    return None


class NormalizeAddressTests(unittest.TestCase):
    def test_collapses_whitespace_and_punctuation(self) -> None:
        a = cvf.normalize_address("1234 S 4TH ST, LOUISVILLE, KY 40208")
        b = cvf.normalize_address("1234 S 4th  St., Louisville, KY 40208")
        self.assertEqual(a, b)

    def test_handles_none_and_empty(self) -> None:
        self.assertEqual(cvf.normalize_address(None), "")
        self.assertEqual(cvf.normalize_address(""), "")


class GroupingKeyTests(unittest.TestCase):
    def test_same_parcel_and_address_share_key(self) -> None:
        k1 = cvf.grouping_key("021A0001", "1234 S 4TH ST", "1234 S 4TH ST")
        k2 = cvf.grouping_key("021a0001", "1234 s 4th st", None)
        self.assertEqual(k1, k2)

    def test_falls_back_to_parcel_only(self) -> None:
        self.assertEqual(cvf.grouping_key("AAA", None, None), "PARCEL::AAA")

    def test_empty_returns_empty(self) -> None:
        self.assertEqual(cvf.grouping_key(None, None, None), "")


class GroupAndScoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.features = _load_features()
        self.leads_default = lcv.build_distressed_leads(self.features)
        self.leads_low = lcv.build_distressed_leads(
            self.features, include_low_signal=True
        )

    def test_default_filter_keeps_only_high_signal_leads(self) -> None:
        addrs = [lead["Property Address"] for lead in self.leads_default]
        # Distressed property — vacant + multiple themes — must be present.
        self.assertTrue(any("S 4TH ST" in a for a in addrs))
        # Pure rental registration must be filtered out.
        self.assertFalse(any("RENTAL CT" in a for a in addrs))
        # Pure street-number must be filtered out.
        self.assertFalse(any("NUMBERS LN" in a for a in addrs))

    def test_include_low_signal_emits_everything(self) -> None:
        addrs = {lead["Property Address"] for lead in self.leads_low}
        self.assertIn("555 RENTAL CT, LOUISVILLE, KY 40208", addrs)
        self.assertIn("777 NUMBERS LN, LOUISVILLE, KY 40208", addrs)
        self.assertIn("1234 S 4TH ST, LOUISVILLE, KY 40208", addrs)

    def test_three_violation_rows_collapse_to_one_lead(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertIsNotNone(lead)
        # The fixture has three distinct violation rows for this property.
        self.assertEqual(lead["_violation_row_count"], 3)
        # Notes should reflect the combined picture, not just one row.
        self.assertIn("Violation rows: 3", lead["Notes"])
        self.assertIn("02A", lead["Notes"])
        self.assertIn("X50", lead["Notes"])
        self.assertIn("X19", lead["Notes"])

    def test_distress_score_and_reasons_in_notes(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertIsNotNone(lead)
        notes = lead["Notes"]
        self.assertIn("Distress score:", notes)
        self.assertIn("Reasons:", notes)
        # Vacant occupancy theme must be reflected.
        self.assertTrue(
            "occupancy: vacant/abandoned/condemned" in notes
            or "vacant/abandoned" in notes
        )
        # Themes triggered by codes/keywords must surface.
        self.assertIn("cleaning/weeds/rubbish", notes)
        self.assertIn("roof/gutters", notes)
        # Citation total must sum across rows ($250 + $350 = $600).
        self.assertIn("$600", notes)

    def test_instrument_number_is_stable_across_runs(self) -> None:
        lead_a = _find_lead(self.leads_default, "S 4TH ST")
        lead_b = _find_lead(
            lcv.build_distressed_leads(self.features), "S 4TH ST"
        )
        self.assertEqual(lead_a["_instrument_number"], lead_b["_instrument_number"])
        self.assertTrue(lead_a["_instrument_number"].startswith("LOU_CODE::"))
        self.assertIn("021A00010000", lead_a["_instrument_number"])
        # The latest compliance date in the fixture is 2025-05-09 (epoch 1746748800000).
        self.assertTrue(lead_a["_instrument_number"].endswith("2025-05-09"))

    def test_filing_date_is_latest_compliance_date(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertEqual(lead["_filing_date_iso"], "2025-05-09")
        self.assertEqual(lead["Date"], "2025-05-09")

    def test_pdf_link_and_source_set(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertEqual(lead["PDF Link"], lcv.SOURCE_URL)
        self.assertIn("Source:", lead["Notes"])

    def test_low_signal_only_property_is_filtered(self) -> None:
        # Pure R01 / X69 properties should not be leads by default.
        rental = _find_lead(self.leads_default, "RENTAL CT")
        self.assertIsNone(rental)
        numbers = _find_lead(self.leads_default, "NUMBERS LN")
        self.assertIsNone(numbers)

    def test_min_score_threshold_can_drop_borderline_property(self) -> None:
        # A single broken-window violation is on the threshold (window theme = 1).
        # With default min_score=3 this should be filtered out.
        leads = lcv.build_distressed_leads(self.features)
        borderline = _find_lead(leads, "BORDERLINE WAY")
        self.assertIsNone(borderline)
        # When low-signal mode is on, it should appear.
        leads_low = lcv.build_distressed_leads(self.features, include_low_signal=True)
        self.assertIsNotNone(_find_lead(leads_low, "BORDERLINE WAY"))

    def test_porch_property_kept_with_citation(self) -> None:
        # Porch handrail + stairs deteriorated + a citation = clear distress.
        lead = _find_lead(self.leads_default, "PORCHVIEW RD")
        self.assertIsNotNone(lead)
        self.assertIn("porch/stairs", lead["Notes"])
        self.assertIn("$150", lead["Notes"])

    def test_dedupe_reduces_record_count(self) -> None:
        # 7 input rows -> 5 distinct properties; high-signal default keeps
        # only the genuinely distressed ones (S 4TH ST + PORCHVIEW RD).
        self.assertEqual(len(self.leads_default), 2)
        # With low-signal on we should have one lead per distinct property (5).
        self.assertEqual(len(self.leads_low), 5)


class ThemeHitTests(unittest.TestCase):
    def test_keyword_match_in_description(self) -> None:
        rows = [
            {
                "alt_id": "X1",
                "full_address": "1 RAT LN",
                "partial_address": "1 RAT LN",
                "parcel": "P1",
                "compl_date": "2026-05-01",
                "status": "OPEN",
                "status_date": "2026-05-01",
                "description": "Rats observed on premises; pest infestation",
                "violation_code": "I17",
                "citation_amount": 100.0,  # citation pushes score above threshold
                "occupancy": "VACANT STRUCTURE",  # plus high-signal occupancy
            }
        ]
        leads = cvf.group_and_score_rows(rows, include_low_signal=False)
        self.assertEqual(len(leads), 1)
        self.assertIn("infestation/vermin", leads[0]["Notes"])

    def test_unknown_code_no_keywords_excluded_by_default(self) -> None:
        rows = [
            {
                "alt_id": "X2",
                "full_address": "2 NOWHERE LN",
                "partial_address": "2 NOWHERE LN",
                "parcel": "P2",
                "compl_date": "2026-05-01",
                "status": "OPEN",
                "status_date": "2026-05-01",
                "description": "Routine administrative followup",
                "violation_code": "ZZZ",
                "citation_amount": None,
                "occupancy": "OCCUPIED",
            }
        ]
        # No themes hit, no occupancy signal, score 0 -> filtered out.
        self.assertEqual(cvf.group_and_score_rows(rows), [])
        # But emitted when low-signal mode is on.
        self.assertEqual(len(cvf.group_and_score_rows(rows, include_low_signal=True)), 1)


if __name__ == "__main__":
    unittest.main()
