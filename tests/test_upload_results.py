"""Schema-routing and CSV-parsing tests for upload_results.py."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Load .github/scripts/upload_results.py as a module under a clean name.
spec = importlib.util.spec_from_file_location(
    "upload_results",
    REPO_ROOT / ".github" / "scripts" / "upload_results.py",
)
upload_results = importlib.util.module_from_spec(spec)
sys.modules["upload_results"] = upload_results
spec.loader.exec_module(upload_results)  # type: ignore[union-attr]


JEFFERSON_CSV = (
    "Date,Defendants/Parties,Property Address,PDF Link,Notes\r\n"
    "05/08/2026,SMITH JOHN,123 MAIN ST,"
    "https://search.jeffersondeeds.com/getimage.php?img=Zm9vMTIzNDU2Nzg5MC50aWY=,"
    "Sample notes\r\n"
)

LOUISVILLE_CSV = (
    "Date,Defendants/Parties,Property Address,PDF Link,Notes\r\n"
    "2025-05-08,LMG Codes & Regulations - OPEN,"
    "1234 S 4TH ST LOUISVILLE KY 40208,"
    "https://services1.arcgis.com/example,"
    "Violation code: PMC-304.6\r\n"
)


class ResolveMetaTests(unittest.TestCase):
    def test_meta_file_is_used_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            (tmpdir / "source_meta.json").write_text(
                json.dumps(
                    {
                        "source_type": "wills",
                        "label": "Jefferson Wills",
                        "csv_name": "wills_results.csv",
                        "schema": "jefferson_deeds",
                    }
                )
            )
            meta = upload_results._resolve_meta(tmpdir, None)
            self.assertEqual(meta["source_type"], "wills")
            self.assertEqual(meta["csv_name"], "wills_results.csv")
            self.assertEqual(meta["schema"], "jefferson_deeds")

    def test_default_is_lis_pendens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            meta = upload_results._resolve_meta(Path(tmp), None)
            self.assertEqual(meta["source_type"], "lis_pendens")
            self.assertEqual(meta["schema"], "jefferson_deeds")

    def test_louisville_fallback_uses_correct_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            meta = upload_results._resolve_meta(Path(tmp), "louisville_code_violations")
            self.assertEqual(meta["schema"], "louisville_code_violations")
            self.assertEqual(meta["csv_name"], "louisville_code_violations_results.csv")


class JeffersonRecordParsingTests(unittest.TestCase):
    def test_parses_with_jefferson_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "lis_pendens_results.csv"
            csv_path.write_text(JEFFERSON_CSV, encoding="utf-8-sig")
            records = upload_results.read_records(csv_path, "run-1", "jefferson_deeds")
            self.assertEqual(len(records), 1)
            r = records[0]
            self.assertEqual(r["filing_date"], "2026-05-08")
            self.assertEqual(r["parties"], "SMITH JOHN")
            self.assertEqual(r["property_address"], "123 MAIN ST")
            # The base64 'Zm9vMTIzNDU2Nzg5MC50aWY=' decodes to 'foo1234567890.tif'
            self.assertEqual(r["instrument_number"], "1234567890")


class LouisvilleRecordParsingTests(unittest.TestCase):
    def test_parses_louisville_schema_with_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "louisville_code_violations_results.csv"
            csv_path.write_text(LOUISVILLE_CSV, encoding="utf-8-sig")
            sidecar_path = tmpdir / "louisville_code_violations_records.json"
            sidecar_path.write_text(
                json.dumps(
                    [
                        {
                            "Date": "2025-05-08",
                            "Defendants/Parties": "LMG Codes & Regulations - OPEN",
                            "Property Address": "1234 S 4TH ST LOUISVILLE KY 40208",
                            "PDF Link": "https://services1.arcgis.com/example",
                            "Notes": "Violation code: PMC-304.6",
                            "_instrument_number": "PMV-2026-001234",
                            "_filing_date_iso": "2025-05-08",
                        }
                    ]
                )
            )
            records = upload_results.read_records(
                csv_path, "run-2", "louisville_code_violations", sidecar_path
            )
            self.assertEqual(len(records), 1)
            r = records[0]
            self.assertEqual(r["instrument_number"], "PMV-2026-001234")
            self.assertEqual(r["filing_date"], "2025-05-08")
            self.assertEqual(r["property_address"], "1234 S 4TH ST LOUISVILLE KY 40208")
            self.assertIsNone(r["pva_verification_link"])


if __name__ == "__main__":
    unittest.main()
