"""Tests for the Jefferson tax-delinquent scraper, CSV shape, dispatcher,
and upload_results sidecar handling.

These cover:
  - Row-clustering / span parsing of the published PDF format (using a
    pure-Python fake page stub so we do not depend on PyMuPDF or a live
    PDF at test time).
  - Canonical-row construction: NO field is fabricated. Filing date and
    bill number stay blank/None when the source does not carry them.
  - Tax-delinquent CSV writer column order (Filing Date, Tax Year,
    Amount Due, Status, Property Address, Parcel ID, Parties, Source
    Link, Notes).
  - run_source dispatcher routes source_type=tax_delinquent to the new
    scraper module with the expected argv.
  - upload_results sidecar -> canonical ingest record mapping.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers import jefferson_tax_delinquent as jtd  # noqa: E402
from scrapers.common import (  # noqa: E402
    TAX_DELINQUENT_CSV_COLUMNS,
    write_tax_delinquent_csv,
)
from scrapers.run_source import SOURCES, _tax_delinquent_command  # noqa: E402


# Load upload_results.py as a module under a clean name (mirrors the
# pattern in test_upload_results.py).
_spec = importlib.util.spec_from_file_location(
    "upload_results_tax",
    REPO_ROOT / ".github" / "scripts" / "upload_results.py",
)
upload_results = importlib.util.module_from_spec(_spec)
sys.modules["upload_results_tax"] = upload_results
_spec.loader.exec_module(upload_results)  # type: ignore[union-attr]


def _row_spans(parcel: str, name: str, address: str, amount_text: str) -> list[tuple[float, str]]:
    """Build a fake (x, text) span list matching what PyMuPDF emits.

    PyMuPDF splits the column "$ 1,234.56" into two adjacent spans on the
    Clerk PDFs — we replicate that here.
    """
    out: list[tuple[float, str]] = []
    out.append((50.0, parcel))
    out.append((150.0, name))
    if address:
        out.append((300.0, address))
    out.append((480.0, "$"))
    out.append((500.0, amount_text))
    return out


class ParseRowTests(unittest.TestCase):
    def test_parses_real_row(self) -> None:
        spans = _row_spans(
            "21245501200000", "A & S ARCHITECTURAL LLC", "8804 US HIGHWAY 42 Slip 120", "64.83"
        )
        parsed = jtd._parse_row(spans)
        self.assertIsNotNone(parsed)
        parcel, name, address, amount_str, amount_value = parsed
        self.assertEqual(parcel, "21245501200000")
        self.assertEqual(name, "A & S ARCHITECTURAL LLC")
        self.assertEqual(address, "8804 US HIGHWAY 42 Slip 120")
        self.assertEqual(amount_str, "$64.83")
        self.assertEqual(amount_value, 64.83)

    def test_rejects_header_row(self) -> None:
        spans = [(50.0, "ParcelID"), (150.0, "Name"), (300.0, "Property Address"), (500.0, "Account Balance")]
        # "ParcelID" is not 14-char alphanumeric -> rejected by parcel regex.
        self.assertIsNone(jtd._parse_row(spans))

    def test_blank_address_does_not_fabricate(self) -> None:
        # Some delinquent rows in the published PDF have no Property
        # Address (the source field is simply blank). The parser must
        # preserve that blank rather than guessing.
        spans = [
            (50.0, "25101800330000"),
            (150.0, "ABLE CLAUDE & MARY"),
            (480.0, "$"),
            (500.0, "32.14"),
        ]
        parsed = jtd._parse_row(spans)
        self.assertIsNotNone(parsed)
        _, _, address, _, value = parsed
        self.assertEqual(address, "")
        self.assertEqual(value, 32.14)

    def test_unparseable_amount_returns_none_not_zero(self) -> None:
        spans = [
            (50.0, "21245501200000"),
            (150.0, "FOO BAR LLC"),
            (300.0, "123 MAIN ST"),
        ]
        parsed = jtd._parse_row(spans)
        self.assertIsNotNone(parsed)
        _, _, _, amount_str, amount_value = parsed
        self.assertEqual(amount_str, "")
        self.assertIsNone(amount_value)


class CanonicalRowTests(unittest.TestCase):
    def _record(self, **overrides) -> jtd.TaxDelinquentRecord:
        base = dict(
            parcel_id="21245501200000",
            taxpayer_name="A & S ARCHITECTURAL LLC",
            property_address="8804 US HIGHWAY 42",
            amount_due="$64.83",
            amount_due_value=64.83,
            tax_year="2024",
            list_published_date="2025-06-01",
            source_pdf_url="https://example/test.pdf",
            document_lookup_url="https://cclix.us/?parcel=21245501200000",
            kind="real_estate",
            page_number=1,
        )
        base.update(overrides)
        return jtd.TaxDelinquentRecord(**base)

    def test_filing_date_is_blank_not_fabricated(self) -> None:
        row = self._record().to_canonical_row()
        self.assertEqual(row["Filing Date"], "")
        self.assertIsNone(row["_filing_date_iso"])

    def test_status_is_constant_delinquent(self) -> None:
        row = self._record().to_canonical_row()
        self.assertEqual(row["Status"], "Delinquent")

    def test_missing_amount_logged_in_notes(self) -> None:
        row = self._record(amount_due="", amount_due_value=None).to_canonical_row()
        self.assertIn("Missing source field: amount_due", row["Notes"])

    def test_missing_address_logged_in_notes(self) -> None:
        row = self._record(property_address="").to_canonical_row()
        self.assertEqual(row["Property Address"], "Address not found")
        self.assertIn("Missing source field: property_address", row["Notes"])

    def test_instrument_number_is_stable_composite(self) -> None:
        row = self._record().to_canonical_row()
        self.assertEqual(row["_instrument_number"], "21245501200000-2024")
        self.assertEqual(row["_source_record_id"], "21245501200000-2024")

    def test_verification_warning_present(self) -> None:
        row = self._record().to_canonical_row()
        self.assertIn(
            "verify status before contact", row["Notes"]
        )


class CsvShapeTests(unittest.TestCase):
    def test_column_order(self) -> None:
        self.assertEqual(
            TAX_DELINQUENT_CSV_COLUMNS,
            [
                "Filing Date",
                "Tax Year",
                "Amount Due",
                "Status",
                "Property Address",
                "Parcel ID",
                "Parties",
                "Source Link",
                "Notes",
            ],
        )

    def test_writes_csv_with_required_leading_columns(self) -> None:
        record = jtd.TaxDelinquentRecord(
            parcel_id="21245501200000",
            taxpayer_name="A & S ARCHITECTURAL LLC",
            property_address="8804 US HIGHWAY 42",
            amount_due="$64.83",
            amount_due_value=64.83,
            tax_year="2024",
            list_published_date="2025-06-01",
            source_pdf_url="https://example/test.pdf",
            document_lookup_url="https://cclix.us/?parcel=21245501200000",
            kind="real_estate",
            page_number=1,
        )
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "out.csv"
            count = write_tax_delinquent_csv([record.to_canonical_row()], csv_path)
            self.assertEqual(count, 1)
            with csv_path.open(encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(reader.fieldnames, TAX_DELINQUENT_CSV_COLUMNS)
                rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["Parcel ID"], "21245501200000")
            self.assertEqual(rows[0]["Status"], "Delinquent")
            self.assertEqual(rows[0]["Filing Date"], "")
            self.assertEqual(rows[0]["Tax Year"], "2024")
            self.assertEqual(rows[0]["Amount Due"], "$64.83")


class DispatcherTests(unittest.TestCase):
    def test_source_registered(self) -> None:
        self.assertIn("tax_delinquent", SOURCES)
        self.assertEqual(SOURCES["tax_delinquent"]["schema"], "jefferson_tax_delinquent")
        self.assertEqual(
            SOURCES["tax_delinquent"]["csv_name"],
            "jefferson_tax_delinquent_results.csv",
        )

    def test_command_targets_tax_delinquent_module(self) -> None:
        args = argparse.Namespace(
            start_date="01/01/2025",
            end_date="12/31/2025",
            output_dir="scraper_output",
            search_mode="auto",
            resume=False,
            pva_cross_check=False,
        )
        cmd = _tax_delinquent_command(args)
        self.assertIn("scrapers.jefferson_tax_delinquent", cmd)
        # CSV name must match the registry — uploader keys off it.
        self.assertIn("--csv-name", cmd)
        self.assertEqual(
            cmd[cmd.index("--csv-name") + 1],
            SOURCES["tax_delinquent"]["csv_name"],
        )
        self.assertIn("--start-date", cmd)
        self.assertIn("01/01/2025", cmd)


class UploadResultsTaxDelinquentTests(unittest.TestCase):
    def test_resolve_meta_falls_back_for_tax_delinquent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            meta = upload_results._resolve_meta(Path(tmp), "tax_delinquent")
            self.assertEqual(meta["source_type"], "tax_delinquent")
            self.assertEqual(meta["schema"], "jefferson_tax_delinquent")
            self.assertEqual(
                meta["csv_name"], "jefferson_tax_delinquent_results.csv"
            )

    def test_sidecar_to_record_preserves_no_fabrication(self) -> None:
        item = {
            "Filing Date": "",
            "Tax Year": "2024",
            "Amount Due": "$1,234.56",
            "Status": "Delinquent",
            "Property Address": "123 MAIN ST",
            "Parcel ID": "21245501200000",
            "Parties": "SMITH JOHN",
            "Source Link": "https://example/list.pdf",
            "Notes": "Source: JCC | List type: real_estate | Tax year: 2024",
            "_instrument_number": "21245501200000-2024",
            "_source_record_id": "21245501200000-2024",
            "_filing_date_iso": None,
            "_amount_due_value": 1234.56,
            "_bill_number": "",
            "_document_url": "https://cclix.us/?parcel=21245501200000",
            "_kind": "real_estate",
            "_list_published_date": "2025-06-01",
            "_source_pdf_url": "https://example/list.pdf",
        }
        rec = upload_results._tax_delinquent_sidecar_to_record(item, "run-7")
        self.assertEqual(rec["run_id"], "run-7")
        self.assertIsNone(rec["filing_date"])  # source has none
        self.assertIsNone(rec["bill_number"])
        self.assertEqual(rec["instrument_number"], "21245501200000-2024")
        self.assertEqual(rec["source_record_id"], "21245501200000-2024")
        self.assertEqual(rec["parcel_id"], "21245501200000")
        self.assertEqual(rec["tax_year"], "2024")
        self.assertEqual(rec["amount_due"], "$1,234.56")
        self.assertEqual(rec["amount_due_value"], 1234.56)
        self.assertEqual(rec["status"], "Delinquent")
        self.assertEqual(rec["parties"], "SMITH JOHN")
        self.assertEqual(rec["owner"], "SMITH JOHN")
        self.assertEqual(rec["taxpayer"], "SMITH JOHN")
        self.assertEqual(rec["property_address"], "123 MAIN ST")
        self.assertEqual(rec["kind"], "real_estate")
        self.assertEqual(rec["list_published_date"], "2025-06-01")
        self.assertEqual(
            rec["document_url"], "https://cclix.us/?parcel=21245501200000"
        )
        self.assertIsNone(rec["pva_verification_link"])

    def test_read_records_uses_sidecar_for_tax_delinquent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "jefferson_tax_delinquent_results.csv"
            csv_path.write_text(
                "Filing Date,Tax Year,Amount Due,Status,Property Address,"
                "Parcel ID,Parties,Source Link,Notes\r\n"
                ",2024,$64.83,Delinquent,8804 US HIGHWAY 42,21245501200000,"
                "A & S ARCHITECTURAL LLC,https://example/list.pdf,test\r\n",
                encoding="utf-8-sig",
            )
            sidecar = tmpdir / "jefferson_tax_delinquent_records.json"
            sidecar.write_text(
                json.dumps(
                    [
                        {
                            "Filing Date": "",
                            "Tax Year": "2024",
                            "Amount Due": "$64.83",
                            "Status": "Delinquent",
                            "Property Address": "8804 US HIGHWAY 42",
                            "Parcel ID": "21245501200000",
                            "Parties": "A & S ARCHITECTURAL LLC",
                            "Source Link": "https://example/list.pdf",
                            "Notes": "Source: JCC",
                            "_instrument_number": "21245501200000-2024",
                            "_filing_date_iso": None,
                            "_amount_due_value": 64.83,
                            "_kind": "real_estate",
                            "_list_published_date": "2025-06-01",
                            "_document_url": "https://cclix.us/?parcel=21245501200000",
                            "_source_pdf_url": "https://example/list.pdf",
                            "_bill_number": "",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            records = upload_results.read_records(
                csv_path, "run-9", "jefferson_tax_delinquent", sidecar
            )
            self.assertEqual(len(records), 1)
            r = records[0]
            self.assertEqual(r["parcel_id"], "21245501200000")
            self.assertEqual(r["tax_year"], "2024")
            self.assertIsNone(r["filing_date"])
            self.assertEqual(r["amount_due_value"], 64.83)


if __name__ == "__main__":
    unittest.main()
