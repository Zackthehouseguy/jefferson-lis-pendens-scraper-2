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


class StatusTaxonomyTests(unittest.TestCase):
    def test_closed_is_closed(self) -> None:
        self.assertTrue(cvf.is_closed_status("Closed"))
        self.assertTrue(cvf.is_closed_status("CLOSED"))
        self.assertFalse(cvf.is_open_status("Closed"))

    def test_open_includes_violation_notice_and_referrals(self) -> None:
        self.assertTrue(cvf.is_open_status("Violation Notice"))
        self.assertTrue(cvf.is_open_status("Citation"))
        self.assertTrue(cvf.is_open_status("Citation Referral"))
        self.assertTrue(cvf.is_open_status("Emergency Referral"))
        self.assertTrue(cvf.is_open_status("Hold"))

    def test_priority_weight_ordering(self) -> None:
        self.assertGreater(
            cvf.status_priority_weight("Emergency Referral"),
            cvf.status_priority_weight("Citation Referral"),
        )
        self.assertGreater(
            cvf.status_priority_weight("Citation Referral"),
            cvf.status_priority_weight("Citation"),
        )
        self.assertGreater(
            cvf.status_priority_weight("Citation"),
            cvf.status_priority_weight("Violation Notice"),
        )
        self.assertEqual(cvf.status_priority_weight("Closed"), 0)


class GroupAndScoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.features = _load_features()
        self.leads_default = lcv.build_distressed_leads(self.features)
        self.leads_low = lcv.build_distressed_leads(
            self.features, include_low_signal=True
        )
        self.leads_with_closed = lcv.build_distressed_leads(
            self.features, include_closed=True
        )

    def test_default_filter_keeps_only_high_signal_open_leads(self) -> None:
        addrs = [lead["Property Address"] for lead in self.leads_default]
        self.assertTrue(any("S 4TH ST" in a for a in addrs))
        self.assertFalse(any("RENTAL CT" in a for a in addrs))
        self.assertFalse(any("NUMBERS LN" in a for a in addrs))
        self.assertFalse(any("CLOSEDONLY" in a for a in addrs))
        self.assertTrue(any("MIXEDSTATUS" in a for a in addrs))
        self.assertTrue(any("EMERGENCY BLVD" in a for a in addrs))

    def test_include_low_signal_emits_everything(self) -> None:
        addrs = {lead["Property Address"] for lead in self.leads_low}
        self.assertIn("555 RENTAL CT, LOUISVILLE, KY 40208", addrs)
        self.assertIn("777 NUMBERS LN, LOUISVILLE, KY 40208", addrs)
        self.assertIn("1234 S 4TH ST, LOUISVILLE, KY 40208", addrs)
        self.assertIn("88 CLOSEDONLY DR, LOUISVILLE, KY 40208", addrs)

    def test_include_closed_flag_keeps_closed_only_leads(self) -> None:
        addrs = [lead["Property Address"] for lead in self.leads_with_closed]
        self.assertTrue(any("CLOSEDONLY" in a for a in addrs))
        self.assertFalse(any("RENTAL CT" in a for a in addrs))
        self.assertFalse(any("NUMBERS LN" in a for a in addrs))

    def test_three_violation_rows_collapse_to_one_lead(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertIsNotNone(lead)
        self.assertEqual(lead["_violation_row_count"], 3)
        self.assertIn("Violation rows: 3", lead["Notes"])
        self.assertIn("02A Cleaning", lead["Notes"])
        self.assertIn("X50 Roof/Gutters", lead["Notes"])
        self.assertIn("X19 Exterior/Foundation", lead["Notes"])

    def test_distress_score_signals_priority_in_notes(self) -> None:
        lead = _find_lead(self.leads_default, "S 4TH ST")
        self.assertIsNotNone(lead)
        notes = lead["Notes"]
        self.assertIn("Priority: HIGH", notes)
        self.assertIn("Distress score:", notes)
        self.assertIn("Distress signals:", notes)
        self.assertIn("vacant/abandoned", notes)
        self.assertIn("trash/weeds", notes)
        self.assertIn("roof/gutters", notes)
        # Citation total must sum across rows ($250 + $350 = $600).
        self.assertIn("$600", notes)

    def test_notes_do_not_contain_raw_legal_text(self) -> None:
        forbidden_substrings = [
            "Premises shall be free of weeds",
            "Roof and gutters shall be maintained",
            "Exterior surfaces shall be free of holes",
            "Porch handrail loose; stairs deteriorated",
            "Structural collapse risk",
            "Foundation cracking",
        ]
        for lead in self.leads_default:
            for phrase in forbidden_substrings:
                self.assertNotIn(
                    phrase, lead["Notes"],
                    msg=f"Raw legal text leaked into Notes for {lead['Property Address']}",
                )

    def test_priority_field_is_first_in_notes(self) -> None:
        for lead in self.leads_default:
            self.assertTrue(
                lead["Notes"].startswith("Priority: "),
                msg=f"Priority should lead Notes, got: {lead['Notes'][:80]!r}",
            )

    def test_output_is_sorted_by_priority_then_score_desc(self) -> None:
        rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        ranked_pairs = [
            (rank[lead["_priority"]], lead["_distress_score"])
            for lead in self.leads_default
        ]
        for i in range(len(ranked_pairs) - 1):
            self.assertGreaterEqual(
                ranked_pairs[i],
                ranked_pairs[i + 1],
                msg=f"Sort order violated at index {i}: {ranked_pairs}",
            )

    def test_citation_and_vacant_property_scores_high(self) -> None:
        lead = _find_lead(self.leads_default, "MIXEDSTATUS")
        self.assertIsNotNone(lead)
        self.assertEqual(lead["_priority"], "HIGH")
        self.assertGreaterEqual(lead["_distress_score"], 10)
        self.assertIn("Citation Referral", lead["Notes"])

    def test_emergency_referral_and_condemned_is_high(self) -> None:
        lead = _find_lead(self.leads_default, "EMERGENCY BLVD")
        self.assertIsNotNone(lead)
        self.assertEqual(lead["_priority"], "HIGH")
        self.assertIn("Emergency Referral", lead["Notes"])
        self.assertIn("CONDEMNED", lead["Notes"].upper())

    def test_closed_only_property_is_filtered_by_default(self) -> None:
        self.assertIsNone(_find_lead(self.leads_default, "CLOSEDONLY"))

    def test_mixed_closed_and_open_kept(self) -> None:
        lead = _find_lead(self.leads_default, "MIXEDSTATUS")
        self.assertIsNotNone(lead)
        self.assertIn("Closed", lead["Notes"])
        self.assertIn("Citation Referral", lead["Notes"])

    def test_instrument_number_is_stable_across_runs(self) -> None:
        lead_a = _find_lead(self.leads_default, "S 4TH ST")
        lead_b = _find_lead(
            lcv.build_distressed_leads(self.features), "S 4TH ST"
        )
        self.assertEqual(lead_a["_instrument_number"], lead_b["_instrument_number"])
        self.assertTrue(lead_a["_instrument_number"].startswith("LOU_CODE::"))
        self.assertIn("021A00010000", lead_a["_instrument_number"])
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
        self.assertIsNone(_find_lead(self.leads_default, "RENTAL CT"))
        self.assertIsNone(_find_lead(self.leads_default, "NUMBERS LN"))

    def test_min_score_threshold_can_drop_borderline_property(self) -> None:
        self.assertIsNone(_find_lead(self.leads_default, "BORDERLINE WAY"))
        leads_low = lcv.build_distressed_leads(self.features, include_low_signal=True)
        self.assertIsNotNone(_find_lead(leads_low, "BORDERLINE WAY"))

    def test_min_score_override_lowers_threshold(self) -> None:
        # Lowering the threshold should surface borderline strong-theme leads.
        leads_low_thresh = lcv.build_distressed_leads(self.features, min_score=2)
        self.assertIsNotNone(_find_lead(leads_low_thresh, "PORCHVIEW RD"))
        # Raising the threshold should drop everything moderate.
        leads_high_thresh = lcv.build_distressed_leads(self.features, min_score=20)
        self.assertIsNone(_find_lead(leads_high_thresh, "PORCHVIEW RD"))
        self.assertIsNone(_find_lead(leads_high_thresh, "MIXEDSTATUS"))

    def test_porch_property_kept_with_citation(self) -> None:
        lead = _find_lead(self.leads_default, "PORCHVIEW RD")
        self.assertIsNotNone(lead)
        self.assertIn("porch/stairs", lead["Notes"])
        self.assertIn("$150", lead["Notes"])

    def test_dedupe_reduces_record_count(self) -> None:
        # 12 input rows -> 8 distinct properties; high-signal default keeps
        # only the genuinely distressed open ones.
        self.assertEqual(len(self.leads_default), 4)
        self.assertEqual(len(self.leads_low), 8)


class ThemeHitTests(unittest.TestCase):
    def test_keyword_match_in_description(self) -> None:
        rows = [
            {
                "alt_id": "X1",
                "full_address": "1 RAT LN",
                "partial_address": "1 RAT LN",
                "parcel": "P1",
                "compl_date": "2026-05-01",
                "status": "Citation",
                "status_date": "2026-05-01",
                "description": "Rats observed on premises; pest infestation",
                "violation_code": "I17",
                "citation_amount": 100.0,
                "occupancy": "VACANT STRUCTURE",
            }
        ]
        leads = cvf.group_and_score_rows(rows, include_low_signal=False)
        self.assertEqual(len(leads), 1)
        self.assertIn("infestation", leads[0]["Notes"])

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
        self.assertEqual(cvf.group_and_score_rows(rows), [])
        self.assertEqual(
            len(cvf.group_and_score_rows(rows, include_low_signal=True)), 1
        )


if __name__ == "__main__":
    unittest.main()
