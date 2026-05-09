"""Tests for the Louisville code-violations transform + date helpers."""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers import louisville_code_violations as lcv
from scrapers.common import parse_date_input


FIXTURE = REPO_ROOT / "tests" / "fixtures" / "louisville_sample.json"


class LouisvilleTransformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.features = self.payload["features"]

    def test_epoch_ms_conversion(self) -> None:
        self.assertEqual(lcv.epoch_ms_to_iso_date(1746662400000), "2025-05-08")
        self.assertIsNone(lcv.epoch_ms_to_iso_date(None))
        self.assertIsNone(lcv.epoch_ms_to_iso_date(""))
        self.assertIsNone(lcv.epoch_ms_to_iso_date("not-a-number"))

    def test_transform_full_record(self) -> None:
        row = lcv.transform_feature(self.features[0])
        self.assertEqual(row["Date"], "2025-05-08")
        self.assertEqual(row["Property Address"], "1234 S 4TH ST, LOUISVILLE, KY 40208")
        self.assertEqual(row["_instrument_number"], "PMV-2026-001234")
        self.assertEqual(row["_filing_date_iso"], "2025-05-08")
        self.assertIn("LMG Codes & Regulations", row["Defendants/Parties"])
        self.assertIn("OPEN", row["Defendants/Parties"])
        self.assertIn("PMC-304.6", row["Notes"])
        self.assertIn("$250.0", row["Notes"])
        self.assertIn("Parcel: 021A00010000", row["Notes"])
        self.assertEqual(row["PDF Link"], lcv.SOURCE_URL)

    def test_transform_falls_back_to_partial_address(self) -> None:
        row = lcv.transform_feature(self.features[1])
        self.assertEqual(row["Property Address"], "4419 MALCOLM RD")
        self.assertNotIn("Citation amount", row["Notes"])
        self.assertIn("VACANT", row["Notes"])
        self.assertIn("CLOSED", row["Defendants/Parties"])

    def test_transform_features_preserves_order_and_count(self) -> None:
        rows = lcv.transform_features(self.features)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["_instrument_number"], "PMV-2026-001234")
        self.assertEqual(rows[1]["_instrument_number"], "PMV-2026-005678")

    def test_where_clause_format(self) -> None:
        start = datetime(2026, 5, 1)
        end = datetime(2026, 5, 7)
        where = lcv.build_where_clause(start, end)
        self.assertIn("G6A_G6_COMPL_DD >=", where)
        self.assertIn("TIMESTAMP '2026-05-01 00:00:00'", where)
        self.assertIn("TIMESTAMP '2026-05-07 23:59:59'", where)


class DateInputTests(unittest.TestCase):
    def test_accepts_mmddyyyy(self) -> None:
        self.assertEqual(
            parse_date_input("05/09/2026"), datetime(2026, 5, 9)
        )

    def test_accepts_iso(self) -> None:
        self.assertEqual(
            parse_date_input("2026-05-09"), datetime(2026, 5, 9)
        )

    def test_rejects_garbage(self) -> None:
        with self.assertRaises(ValueError):
            parse_date_input("not a date")


if __name__ == "__main__":
    unittest.main()
