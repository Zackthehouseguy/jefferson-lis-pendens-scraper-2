"""End-to-end test for the Wills CSV writer.

Builds a couple of synthetic FilingRecord objects, runs `write_wills_csv`,
and verifies:
  - CSV has the leading column order required by the spec
  - smart fields are populated from cached OCR text
  - "Address not found" is normalized to "Unknown" in the CSV
"""
from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from jefferson_lis_pendens_scraper import (  # noqa: E402
    ActionLogger,
    FilingRecord,
    WILLS_OUTPUT_COLUMNS,
    write_wills_csv,
)


SIMPLE_WILL_OCR = """
LAST WILL AND TESTAMENT OF JOHN A. SMITH
I, JOHN A. SMITH, do hereby declare. I am survived by my wife Mary J. Smith.
Date of death: March 14, 2025.
I give and bequeath to my son Robert Smith all real property at 4419 Malcolm Ave.
"""

TRUST_WILL_OCR = """
LAST WILL AND TESTAMENT OF JANE DOE.
The Trustee of the Jane Doe Revocable Trust shall be Fifth Third Bank, N.A.
She died on April 2, 2026.
"""


class WillsCsvTests(unittest.TestCase):
    def test_writes_required_leading_columns_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            logger = ActionLogger(out_dir / "log.txt")
            simple = FilingRecord(
                instrument_number="0123456789",
                filing_date="05/07/2026",
                document_type="WILL",
                grantors=["SMITH JOHN A"],
                grantees=["SMITH ROBERT"],
                legal_description="Lot 7 Sec A",
                detail_url="https://example/detail?instnum=0123456789",
                document_url="https://example/p2.php?img=abc",
                property_address="4419 Malcolm Ave, Louisville, KY 40215",
            )
            simple.ocr_text = SIMPLE_WILL_OCR

            trust = FilingRecord(
                instrument_number="9876543210",
                filing_date="05/08/2026",
                document_type="WILL",
                grantors=["DOE JANE"],
                grantees=["FIFTH THIRD BANK NA"],
                legal_description="",
                detail_url="https://example/detail?instnum=9876543210",
                document_url="https://example/p2.php?img=def",
                property_address="Address not found",
            )
            trust.ocr_text = TRUST_WILL_OCR

            csv_path = out_dir / "wills_results.csv"
            write_wills_csv([simple, trust], csv_path, logger, source_tag="Source: WILLS")

            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                rows = list(reader)

            header = rows[0]
            # Spec: first 10 columns are the required smart fields in this exact order.
            required_leading = [
                "Filing Date",
                "Decedent",
                "Date of Death",
                "Property Address",
                "Surviving Spouse",
                "Beneficiary/Heir/Devisee",
                "Complexity Flag",
                "Parties",
                "PDF Link",
                "Notes",
            ]
            self.assertEqual(header[: len(required_leading)], required_leading)
            self.assertEqual(header, WILLS_OUTPUT_COLUMNS)

            # Data row indices line up with header.
            simple_row = dict(zip(header, rows[1]))
            self.assertEqual(simple_row["Filing Date"], "05/07/2026")
            self.assertIn("john a. smith", simple_row["Decedent"].lower())
            self.assertEqual(simple_row["Date of Death"], "March 14, 2025")
            self.assertEqual(
                simple_row["Property Address"],
                "4419 Malcolm Ave, Louisville, KY 40215",
            )
            self.assertEqual(simple_row["Complexity Flag"], "Simple")
            self.assertEqual(simple_row["Instrument Number"], "0123456789")
            self.assertIn("Source: WILLS", simple_row["Notes"])

            trust_row = dict(zip(header, rows[2]))
            self.assertEqual(trust_row["Complexity Flag"], "Avoid - Trust/Complex")
            # Address not found should not leak through as a literal string;
            # the wills extractor maps it to "Unknown".
            self.assertEqual(trust_row["Property Address"], "Unknown")


if __name__ == "__main__":
    unittest.main()
