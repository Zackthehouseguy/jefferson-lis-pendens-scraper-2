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


# New Louisville CSV column order — used to verify upload_results can still
# build canonical Lovable records when the CSV is in source-specific shape
# and no sidecar is available (fallback path).
LOUISVILLE_CSV_NEW = (
    "Filing Date,Distress Score,Status,Property Address,Occupancy,Parties,"
    "PDF Link,Distress Signals,Priority,Violation Codes,Citation Total,"
    "Violation Rows,Case IDs,Parcel,Source Link,Instrument Number,Notes\r\n"
    "2025-05-08,12,OPEN,1234 S 4TH ST LOUISVILLE KY 40208,VACANT,"
    "LMG Codes & Regulations - OPEN,https://services1.arcgis.com/example,"
    "vacant/abandoned,HIGH,PMC-304.6,$250,1,PMV-2026-001234,021A00010000,"
    "https://services1.arcgis.com/example,"
    "LOU_CODE::1234 S 4TH ST::2025-05-08,"
    "Priority: HIGH | Distress score: 12\r\n"
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
            # Lis Pendens rows should NOT carry the wills_fields extras.
            self.assertNotIn("wills_fields", r)

    def test_parses_wills_csv_shape_with_jefferson_schema(self) -> None:
        """The Wills CSV ships with smart-field columns up front
        ("Filing Date", "Decedent", "Parties", ...) instead of the
        canonical Lis Pendens 5-column shape. The jefferson_deeds reader
        must tolerate both shapes so the same ingest pipeline handles
        both sources.
        """
        wills_csv = (
            "Filing Date,Decedent,Date of Death,Property Address,"
            "Surviving Spouse,Beneficiary/Heir/Devisee,Complexity Flag,"
            "Parties,PDF Link,Notes,Instrument Number,Legal Description,"
            "Confidence,Complexity Reasons\n"
            "05/08/2026,JOHN A. SMITH,March 14 2025,"
            "\"4419 Malcolm Ave, Louisville, KY 40215\",Mary J. Smith,"
            "Robert Smith,Simple,SMITH JOHN; SMITH ROBERT,"
            "https://example/p2.php,Source: WILLS,1234567890,Lot 7 Sec A,"
            "medium,\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "wills_results.csv"
            csv_path.write_text(wills_csv, encoding="utf-8-sig")
            records = upload_results.read_records(csv_path, "run-1", "jefferson_deeds")
            self.assertEqual(len(records), 1)
            r = records[0]
            self.assertEqual(r["filing_date"], "2026-05-08")
            self.assertEqual(r["parties"], "SMITH JOHN; SMITH ROBERT")
            self.assertEqual(r["property_address"], "4419 Malcolm Ave, Louisville, KY 40215")
            # Instrument number column wins over the PDF-link fallback.
            self.assertEqual(r["instrument_number"], "1234567890")
            # Wills extras are surfaced under a sub-key so the canonical
            # ingest fields (filing_date, parties, property_address, ...)
            # are unchanged.
            self.assertIn("wills_fields", r)
            self.assertEqual(r["wills_fields"]["complexity_flag"], "Simple")
            self.assertEqual(r["wills_fields"]["surviving_spouse"], "Mary J. Smith")


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


class LouisvilleNewCsvShapeTests(unittest.TestCase):
    """The Louisville CSV is now source-specific (Filing Date/Distress
    Score/...). upload_results should still produce canonical Lovable records
    via the sidecar-first path, and via the CSV-fallback path when no sidecar
    is available."""

    def test_sidecar_first_for_louisville_with_new_csv_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "louisville_code_violations_results.csv"
            csv_path.write_text(LOUISVILLE_CSV_NEW, encoding="utf-8-sig")
            sidecar_path = tmpdir / "louisville_code_violations_records.json"
            sidecar_path.write_text(
                json.dumps(
                    [
                        {
                            "Date": "2025-05-08",
                            "Defendants/Parties": "LMG Codes & Regulations - OPEN",
                            "Property Address": "1234 S 4TH ST LOUISVILLE KY 40208",
                            "PDF Link": "https://services1.arcgis.com/example",
                            "Notes": "Priority: HIGH | Distress score: 12",
                            "_instrument_number": "LOU_CODE::1234 S 4TH ST::2025-05-08",
                            "_filing_date_iso": "2025-05-08",
                            "_distress_score": 12,
                            "_priority": "HIGH",
                            "_status": "OPEN",
                            "_occupancy": "VACANT",
                            "_distress_signals": "vacant/abandoned",
                        }
                    ]
                )
            )
            records = upload_results.read_records(
                csv_path, "run-3", "louisville_code_violations", sidecar_path
            )
            self.assertEqual(len(records), 1)
            r = records[0]
            self.assertEqual(
                r["instrument_number"], "LOU_CODE::1234 S 4TH ST::2025-05-08"
            )
            self.assertEqual(r["filing_date"], "2025-05-08")
            self.assertEqual(
                r["property_address"], "1234 S 4TH ST LOUISVILLE KY 40208"
            )
            self.assertEqual(r["parties"], "LMG Codes & Regulations - OPEN")
            self.assertEqual(
                r["pdf_link"], "https://services1.arcgis.com/example"
            )
            self.assertIsNone(r["pva_verification_link"])

    def test_louisville_fallback_to_csv_when_no_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "louisville_code_violations_results.csv"
            csv_path.write_text(LOUISVILLE_CSV_NEW, encoding="utf-8-sig")
            sidecar_path = tmpdir / "louisville_code_violations_records.json"
            # Sidecar path passed but file does not exist on disk.
            records = upload_results.read_records(
                csv_path, "run-4", "louisville_code_violations", sidecar_path
            )
            self.assertEqual(len(records), 1)
            r = records[0]
            # Without a sidecar we still get a valid canonical record from the
            # source-specific CSV shape (Filing Date / Parties / Instrument
            # Number columns), so Lovable ingest still works.
            self.assertEqual(
                r["instrument_number"], "LOU_CODE::1234 S 4TH ST::2025-05-08"
            )
            self.assertEqual(r["filing_date"], "2025-05-08")
            self.assertEqual(r["parties"], "LMG Codes & Regulations - OPEN")
            self.assertEqual(
                r["property_address"], "1234 S 4TH ST LOUISVILLE KY 40208"
            )
            self.assertEqual(
                r["pdf_link"], "https://services1.arcgis.com/example"
            )

    def test_jefferson_csv_unchanged(self) -> None:
        """Sanity check that Jefferson Lis Pendens parsing is untouched."""
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            csv_path = tmpdir / "lis_pendens_results.csv"
            csv_path.write_text(JEFFERSON_CSV, encoding="utf-8-sig")
            records = upload_results.read_records(csv_path, "run-j", "jefferson_deeds")
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["filing_date"], "2026-05-08")
            self.assertEqual(records[0]["instrument_number"], "1234567890")


def _meta_louisville() -> dict:
    return {
        "source_type": "louisville_code_violations",
        "label": "Louisville Code Violations",
        "csv_name": "louisville_code_violations_results.csv",
        "schema": "louisville_code_violations",
    }


def _make_records(n: int) -> list[dict]:
    return [
        {
            "run_id": "run-x",
            "filing_date": "2026-05-08",
            "instrument_number": f"PMV-2026-{i:06d}",
            "parties": "X",
            "property_address": f"{i} MAIN ST",
            "pdf_link": "",
            "notes": "",
            "pva_verification_link": None,
        }
        for i in range(n)
    ]


class BuildPayloadsTests(unittest.TestCase):
    def test_small_completed_run_emits_single_payload(self) -> None:
        records = _make_records(50)
        files = {"csv": {"filename": "x.csv", "content_type": "text/csv", "base64": "", "size": 0}}
        payloads = upload_results.build_payloads(
            run_id="run-x",
            status="completed",
            error_message="",
            meta=_meta_louisville(),
            records=records,
            files=files,
            summary={"total_records": 50, "addresses_found": 50, "failures": 0, "source_type": "louisville_code_violations"},
            record_batch_size=100,
        )
        self.assertEqual(len(payloads), 1)
        only = payloads[0]
        self.assertEqual(only["status"], "completed")
        self.assertEqual(len(only["records"]), 50)
        self.assertEqual(only["files"], files)
        self.assertTrue(only["is_final"])
        self.assertFalse(only["is_partial"])
        self.assertEqual(only["batch_count"], 1)

    def test_failed_status_never_batches(self) -> None:
        records = _make_records(841)
        payloads = upload_results.build_payloads(
            run_id="run-fail",
            status="failed",
            error_message="boom",
            meta=_meta_louisville(),
            records=records,
            files={},
            summary={"total_records": 841, "addresses_found": 0, "failures": 0, "source_type": "louisville_code_violations"},
            record_batch_size=100,
        )
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["status"], "failed")
        self.assertEqual(payloads[0]["error_message"], "boom")
        self.assertEqual(len(payloads[0]["records"]), 841)

    def test_large_completed_run_splits_records_then_files_only_final(self) -> None:
        records = _make_records(841)
        files = {
            "csv": {"filename": "x.csv", "content_type": "text/csv", "base64": "", "size": 0},
            "source_records_json": {"filename": "j.json", "content_type": "application/json", "base64": "", "size": 0},
        }
        summary = {"total_records": 841, "addresses_found": 800, "failures": 41, "source_type": "louisville_code_violations"}
        payloads = upload_results.build_payloads(
            run_id="run-big",
            status="completed",
            error_message="",
            meta=_meta_louisville(),
            records=records,
            files=files,
            summary=summary,
            record_batch_size=100,
        )
        # 9 record batches (8 full, 1 partial of 41) + 1 final files-only
        self.assertEqual(len(payloads), 10)

        # Partial batches: status completed, records present, files empty, is_partial true.
        recombined: list[dict] = []
        for i, p in enumerate(payloads[:-1]):
            self.assertEqual(p["status"], "completed")
            self.assertTrue(p["is_partial"])
            self.assertFalse(p["is_final"])
            self.assertEqual(p["files"], {})
            self.assertEqual(p["batch_index"], i)
            self.assertEqual(p["batch_count"], 10)
            self.assertEqual(p["run_id"], "run-big")
            self.assertEqual(p["action"], "finalize_results")
            self.assertEqual(p["type"], "scraper_results")
            self.assertEqual(p["summary"], summary)
            recombined.extend(p["records"])
        self.assertEqual(len(recombined), 841)
        # Records preserved in order, no duplicates, no losses.
        self.assertEqual(
            [r["instrument_number"] for r in recombined],
            [r["instrument_number"] for r in records],
        )

        final = payloads[-1]
        self.assertEqual(final["records"], [])
        self.assertEqual(final["files"], files)
        self.assertTrue(final["is_final"])
        self.assertFalse(final["is_partial"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["batch_index"], 9)
        self.assertEqual(final["batch_count"], 10)
        # The summary on the final payload reflects the full record count even
        # though records=[] (Lovable should rely on this for run-level state).
        self.assertEqual(final["summary"]["total_records"], 841)

    def test_exactly_batch_size_is_single_payload(self) -> None:
        records = _make_records(100)
        payloads = upload_results.build_payloads(
            run_id="run-eq",
            status="completed",
            error_message="",
            meta=_meta_louisville(),
            records=records,
            files={},
            summary={"total_records": 100, "addresses_found": 0, "failures": 0, "source_type": "louisville_code_violations"},
            record_batch_size=100,
        )
        self.assertEqual(len(payloads), 1)
        self.assertEqual(len(payloads[0]["records"]), 100)

    def test_empty_records_completed_emits_single_payload(self) -> None:
        payloads = upload_results.build_payloads(
            run_id="run-empty",
            status="completed",
            error_message="",
            meta=_meta_louisville(),
            records=[],
            files={},
            summary={"total_records": 0, "addresses_found": 0, "failures": 0, "source_type": "louisville_code_violations"},
            record_batch_size=100,
        )
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["records"], [])
        self.assertTrue(payloads[0]["is_final"])


class ResolveBatchSizeTests(unittest.TestCase):
    def test_cli_value_wins(self) -> None:
        self.assertEqual(upload_results._resolve_record_batch_size(50), 50)

    def test_env_var_used_when_no_cli(self) -> None:
        import os
        original = os.environ.get("UPLOAD_RECORD_BATCH_SIZE")
        os.environ["UPLOAD_RECORD_BATCH_SIZE"] = "25"
        try:
            self.assertEqual(upload_results._resolve_record_batch_size(None), 25)
        finally:
            if original is None:
                os.environ.pop("UPLOAD_RECORD_BATCH_SIZE", None)
            else:
                os.environ["UPLOAD_RECORD_BATCH_SIZE"] = original

    def test_default_when_nothing_set(self) -> None:
        import os
        original = os.environ.pop("UPLOAD_RECORD_BATCH_SIZE", None)
        try:
            self.assertEqual(
                upload_results._resolve_record_batch_size(None),
                upload_results.DEFAULT_RECORD_BATCH_SIZE,
            )
        finally:
            if original is not None:
                os.environ["UPLOAD_RECORD_BATCH_SIZE"] = original

    def test_invalid_env_falls_back_to_default(self) -> None:
        import os
        original = os.environ.get("UPLOAD_RECORD_BATCH_SIZE")
        os.environ["UPLOAD_RECORD_BATCH_SIZE"] = "not-a-number"
        try:
            self.assertEqual(
                upload_results._resolve_record_batch_size(None),
                upload_results.DEFAULT_RECORD_BATCH_SIZE,
            )
        finally:
            if original is None:
                os.environ.pop("UPLOAD_RECORD_BATCH_SIZE", None)
            else:
                os.environ["UPLOAD_RECORD_BATCH_SIZE"] = original


class PostIngestErrorTests(unittest.TestCase):
    def test_http_error_includes_body_and_redacts_token(self) -> None:
        import requests

        class FakeResponse:
            ok = False
            status_code = 500
            text = "boom: token=secret-token-123 was used"

        original_post = requests.post
        requests.post = lambda *a, **kw: FakeResponse()  # type: ignore[assignment]
        try:
            with self.assertRaises(requests.HTTPError) as ctx:
                upload_results.post_ingest(
                    "https://example.com/ingest", "secret-token-123", {"x": 1}
                )
            msg = str(ctx.exception)
            self.assertIn("HTTP 500", msg)
            self.assertIn("boom", msg)
            self.assertNotIn("secret-token-123", msg)
            self.assertIn("[REDACTED]", msg)
        finally:
            requests.post = original_post  # type: ignore[assignment]


if __name__ == "__main__":
    unittest.main()
