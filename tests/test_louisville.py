"""Tests for the Louisville code-violations transform + date helpers."""
from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers import louisville_code_violations as lcv
from scrapers.common import (
    LOUISVILLE_CSV_COLUMNS,
    lead_to_louisville_row,
    parse_date_input,
    write_louisville_csv,
)


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


DISTRESS_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "louisville_distress_sample.json"


class LouisvilleCsvColumnTests(unittest.TestCase):
    """The Louisville code-violation CSV must have the required leading columns
    in the exact order requested by the user, with the distress fields broken
    out as their own columns instead of being buried inside Notes."""

    REQUIRED_LEADING_ORDER = [
        "Filing Date",
        "Distress Score",
        "Status",
        "Property Address",
        "Occupancy",
        "Parties",
        "PDF Link",
        "Distress Signals",
    ]

    def setUp(self) -> None:
        features = json.loads(DISTRESS_FIXTURE.read_text(encoding="utf-8"))[
            "features"
        ]
        self.leads = lcv.build_distressed_leads(features)
        self.assertGreater(len(self.leads), 0, "fixture should produce leads")

    def test_required_columns_appear_in_required_order(self) -> None:
        for i, col in enumerate(self.REQUIRED_LEADING_ORDER):
            self.assertEqual(
                LOUISVILLE_CSV_COLUMNS[i],
                col,
                f"Column at position {i} should be {col!r} but is "
                f"{LOUISVILLE_CSV_COLUMNS[i]!r}",
            )

    def test_csv_header_matches_louisville_column_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "louisville.csv"
            write_louisville_csv(self.leads, path)
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.reader(fh)
                header = next(reader)
            self.assertEqual(header, LOUISVILLE_CSV_COLUMNS)
            self.assertEqual(
                header[: len(self.REQUIRED_LEADING_ORDER)],
                self.REQUIRED_LEADING_ORDER,
            )

    def test_csv_rows_populate_distress_fields_as_columns(self) -> None:
        """Distress score, status, occupancy and signals must live in their
        own columns — not buried inside Notes."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "louisville.csv"
            write_louisville_csv(self.leads, path)
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), len(self.leads))
        # Find the high-priority "S 4TH ST" lead and confirm columns are populated.
        target = next(
            (r for r in rows if "S 4TH ST" in r["Property Address"].upper()), None
        )
        self.assertIsNotNone(target)
        self.assertTrue(target["Filing Date"], "Filing Date column must be populated")
        self.assertTrue(
            target["Distress Score"], "Distress Score column must be populated"
        )
        self.assertTrue(target["Status"], "Status column must be populated")
        self.assertTrue(
            target["Property Address"],
            "Property Address column must be populated",
        )
        self.assertTrue(
            target["Distress Signals"],
            "Distress Signals column must be populated",
        )
        # Parties keeps the LMG agency string.
        self.assertIn("LMG Codes", target["Parties"])
        # PDF Link is set to the public dataset URL.
        self.assertEqual(target["PDF Link"], lcv.SOURCE_URL)
        # Notes is still present as a brief — but the structured info above is
        # also available without parsing Notes.
        self.assertTrue(target["Notes"])

    def test_legacy_columns_are_no_longer_in_louisville_csv(self) -> None:
        """The Louisville CSV is now source-specific; the canonical 5-column
        Jefferson-style headers (Date, Defendants/Parties) should not appear."""
        self.assertNotIn("Date", LOUISVILLE_CSV_COLUMNS)
        self.assertNotIn("Defendants/Parties", LOUISVILLE_CSV_COLUMNS)

    def test_lead_to_louisville_row_preserves_extra_keys(self) -> None:
        lead = self.leads[0]
        mapped = lead_to_louisville_row(lead)
        self.assertEqual(mapped["Filing Date"], lead["_filing_date_iso"])
        self.assertEqual(mapped["Distress Score"], lead["_distress_score"])
        self.assertEqual(mapped["Priority"], lead["_priority"])
        self.assertEqual(mapped["Instrument Number"], lead["_instrument_number"])
        self.assertEqual(mapped["Parties"], lead["Defendants/Parties"])
        self.assertEqual(mapped["Property Address"], lead["Property Address"])
        self.assertEqual(mapped["Notes"], lead["Notes"])

    def test_transform_feature_emits_louisville_extras(self) -> None:
        sample = json.loads(FIXTURE.read_text(encoding="utf-8"))["features"][0]
        row = lcv.transform_feature(sample)
        # The legacy per-violation transform also populates the Louisville-CSV
        # extras so --no-dedupe output still hits every column.
        self.assertEqual(row["_status"], "OPEN")
        self.assertEqual(row["_occupancy"], "OCCUPIED")
        self.assertEqual(row["_parcel"], "021A00010000")
        self.assertEqual(row["_violation_codes"], "PMC-304.6")
        self.assertEqual(row["_source_link"], lcv.SOURCE_URL)


class LouisvilleSidecarPreservesExtrasTests(unittest.TestCase):
    """The structured JSON sidecar must keep all extra keys (so upload_results
    can build canonical Lovable records straight from the sidecar)."""

    def test_sidecar_lead_carries_louisville_extras(self) -> None:
        features = json.loads(DISTRESS_FIXTURE.read_text(encoding="utf-8"))[
            "features"
        ]
        leads = lcv.build_distressed_leads(features)
        self.assertGreater(len(leads), 0)
        for lead in leads:
            for required_extra in (
                "_instrument_number",
                "_filing_date_iso",
                "_distress_score",
                "_priority",
                "_status",
                "_occupancy",
                "_distress_signals",
                "_violation_codes",
                "_violation_row_count",
            ):
                self.assertIn(required_extra, lead)


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
