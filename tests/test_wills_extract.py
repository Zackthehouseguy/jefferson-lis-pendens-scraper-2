"""Unit tests for the Wills smart-field extractor.

Synthetic OCR fixtures cover the four cases the spec calls out:
  - simple will: clean decedent + heir + property address -> Simple
  - trust will: trust language present -> Avoid - Trust/Complex
  - unknown / blank: opaque text returns Unknown for everything (no
    hallucination)
  - legal-description-only: a legal description is NOT used as the
    property address
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers.wills_extract import (
    COMPLEXITY_AVOID,
    COMPLEXITY_NEEDS_REVIEW,
    COMPLEXITY_SIMPLE,
    UNKNOWN,
    extract_wills_fields,
)


SIMPLE_WILL_OCR = """
LAST WILL AND TESTAMENT OF JOHN A. SMITH

I, JOHN A. SMITH, being of sound mind and body, do hereby declare this
to be my Last Will and Testament.

I am survived by my wife Mary J. Smith, and we reside at 4419 Malcolm Ave,
Louisville, KY 40215.

Date of death: March 14, 2025.

I give and bequeath to my son Robert Smith all of my real property,
specifically the home located at 4419 Malcolm Ave, Louisville, KY 40215.
"""


TRUST_WILL_OCR = """
LAST WILL AND TESTAMENT OF JANE DOE

I, JANE DOE, of Louisville, Kentucky, hereby declare this to be my last
will. I direct that my entire residuary estate be poured over into the
JANE DOE REVOCABLE TRUST dated January 1, 2010.

The Trustee of the Jane Doe Revocable Trust shall be Fifth Third Bank, N.A.,
serving as Successor Trustee.

She died on April 2, 2026.
"""


UNKNOWN_OCR = """
This document was filed in the County Clerk office. It contains scanned
material that was not legible during OCR. No additional information can
be inferred from this content.
"""


# Legal description text only. The address extractor upstream returned
# "Address not found" for this filing, so we should NOT promote the legal
# description to the Property Address field.
LEGAL_DESC_ONLY_OCR = """
LAST WILL AND TESTAMENT OF EDNA ROSE

Decedent: EDNA ROSE.

Real property: Lot 14, Block 3, Section 2 of the Highland Park Subdivision
as recorded in Deed Book 9876, Page 432, Jefferson County, KY.

Date of death: 02/14/2024.

I devise to my daughter Patricia Rose the entirety of my estate.
"""


class WillsExtractTests(unittest.TestCase):
    def test_simple_will_extracts_clean_fields(self) -> None:
        fields = extract_wills_fields(
            text=SIMPLE_WILL_OCR,
            parties="Smith John A; Smith Robert",
            legal_description="Lot 7 Section A",
            existing_address="4419 Malcolm Ave, Louisville, KY 40215",
        )
        self.assertIn("john a. smith", fields.decedent.lower())
        self.assertEqual(fields.date_of_death, "March 14, 2025")
        self.assertEqual(fields.property_address, "4419 Malcolm Ave, Louisville, KY 40215")
        self.assertIn("Mary", fields.surviving_spouse)
        self.assertIn("Robert", fields.beneficiary_heir_devisee)
        self.assertEqual(fields.complexity_flag, COMPLEXITY_SIMPLE)
        # Simple flag should not list the "no X" reasons.
        joined_reasons = "; ".join(fields.complexity_reasons).lower()
        self.assertNotIn("no clear beneficiary", joined_reasons)
        self.assertNotIn("no property address", joined_reasons)

    def test_trust_will_is_flagged_avoid(self) -> None:
        fields = extract_wills_fields(
            text=TRUST_WILL_OCR,
            parties="Doe Jane",
            legal_description="",
            existing_address="123 Main St, Louisville, KY 40202",
        )
        self.assertEqual(fields.complexity_flag, COMPLEXITY_AVOID)
        joined_reasons = "; ".join(fields.complexity_reasons).lower()
        self.assertTrue(
            "trust" in joined_reasons,
            f"Expected trust language in reasons, got: {fields.complexity_reasons}",
        )

    def test_unknown_text_returns_unknowns_no_hallucination(self) -> None:
        fields = extract_wills_fields(
            text=UNKNOWN_OCR,
            parties="",
            legal_description="",
            existing_address="",
        )
        self.assertEqual(fields.decedent, UNKNOWN)
        self.assertEqual(fields.date_of_death, UNKNOWN)
        self.assertEqual(fields.property_address, UNKNOWN)
        self.assertEqual(fields.surviving_spouse, UNKNOWN)
        self.assertEqual(fields.beneficiary_heir_devisee, UNKNOWN)
        # No clean beneficiary + no decedent + no address -> Needs Review.
        self.assertEqual(fields.complexity_flag, COMPLEXITY_NEEDS_REVIEW)

    def test_legal_description_is_not_used_as_address(self) -> None:
        fields = extract_wills_fields(
            text=LEGAL_DESC_ONLY_OCR,
            parties="Rose Edna; Rose Patricia",
            legal_description="Lot 14, Block 3, Section 2 of Highland Park",
            existing_address="Address not found",
        )
        self.assertEqual(fields.property_address, UNKNOWN)
        # Legal description should NOT leak into the address field even
        # though it is the only locator visible in the OCR text.
        self.assertNotIn("Lot", fields.property_address)
        self.assertNotIn("Section", fields.property_address)
        # Other smart fields should still extract correctly.
        self.assertIn("edna rose", fields.decedent.lower())
        self.assertIn("Patricia", fields.beneficiary_heir_devisee)
        # Without a property address, this is at best Needs Review.
        self.assertIn(fields.complexity_flag, {COMPLEXITY_NEEDS_REVIEW, COMPLEXITY_AVOID})
        # Notes should preserve the legal description for the human reader.
        self.assertTrue(any("Legal Desc:" in n for n in fields.notes))

    def test_existing_address_is_trusted_when_provided(self) -> None:
        fields = extract_wills_fields(
            text=SIMPLE_WILL_OCR,
            parties="",
            legal_description="",
            existing_address="999 Oak Rd, Louisville, KY 40207",
        )
        self.assertEqual(fields.property_address, "999 Oak Rd, Louisville, KY 40207")

    def test_address_not_found_treated_as_missing(self) -> None:
        fields = extract_wills_fields(
            text=SIMPLE_WILL_OCR,
            parties="",
            legal_description="",
            existing_address="Address not found",
        )
        self.assertEqual(fields.property_address, UNKNOWN)

    def test_corporate_executor_flagged_avoid(self) -> None:
        ocr = (
            "LAST WILL AND TESTAMENT OF HARRY R. WILSON. I appoint as my "
            "Executor PNC Bank, N.A., a corporate fiduciary, to administer "
            "this estate. He died on 06/01/2025. I devise to my son Daniel "
            "Wilson all my property at 100 Elm St."
        )
        fields = extract_wills_fields(
            text=ocr,
            parties="",
            legal_description="",
            existing_address="100 Elm St, Louisville, KY 40202",
        )
        self.assertEqual(fields.complexity_flag, COMPLEXITY_AVOID)

    def test_predeceased_spouse_recorded_in_reasons(self) -> None:
        ocr = (
            "Last Will and Testament of Mark T. Adams. My wife predeceased me. "
            "I devise to my daughter Sarah Adams all real property. "
            "Date of death: 01/15/2026."
        )
        fields = extract_wills_fields(
            text=ocr,
            parties="",
            legal_description="",
            existing_address="222 Pine Ct, Louisville, KY 40220",
        )
        self.assertEqual(fields.surviving_spouse, UNKNOWN)
        joined = "; ".join(fields.complexity_reasons).lower()
        self.assertIn("predeceased", joined)


if __name__ == "__main__":
    unittest.main()
