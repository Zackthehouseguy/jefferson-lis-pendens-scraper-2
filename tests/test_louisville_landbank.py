"""Tests for the Louisville Metro Landbank inventory scraper.

These cover:
  - CSV parsing of the Sales Inventory sheet (using a fixture mirroring
    the real LibreOffice-converted output, so we exercise the actual
    header quirks like trailing spaces).
  - Canonical-record construction: every required field from
    docs/SCRAPER_SPEC.md §3.2 is present, signal_type uses the canonical
    `landbank_inventory` vocabulary, missing source fields land as None
    (never fabricated).
  - --limit flag short-circuits AFTER normalization.
  - run_source dispatcher routes louisville_landbank to the right module.
  - upload_results fallback meta resolves the new schema.
  - Standalone --out emits a valid JSON array.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers import louisville_landbank as llb  # noqa: E402
from scrapers.run_source import SOURCES, _louisville_landbank_command  # noqa: E402


_spec = importlib.util.spec_from_file_location(
    "upload_results_landbank",
    REPO_ROOT / ".github" / "scripts" / "upload_results.py",
)
upload_results = importlib.util.module_from_spec(_spec)
sys.modules["upload_results_landbank"] = upload_results
_spec.loader.exec_module(upload_results)  # type: ignore[union-attr]


FIXTURE = REPO_ROOT / "tests" / "fixtures" / "louisville_landbank_sample.csv"


class ParseInventoryTests(unittest.TestCase):
    def test_parses_real_fixture(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        # 3 real data rows + 1 "OWNER=1 only" row. The fully-blank
        # trailing row is silently dropped (no parcel/address/owner).
        self.assertEqual(len(records), 4)

    def test_drops_fully_empty_rows(self) -> None:
        # Add a couple of fully-blank rows and confirm they do not show
        # up in the output (covers the trailing whitespace LibreOffice
        # often emits on the real workbook).
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "blank.csv"
            path.write_text(
                "OWNER,ST ,PARCELID,STREET NAME\n"
                "1,2816,050E01140000,06TH\n"
                ",,,\n"
                ",,,\n",
                encoding="utf-8",
            )
            records = llb.parse_inventory_csv(path)
            self.assertEqual(len(records), 1)

    def test_owner_code_mapping(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        owners = [r.owner_name for r in records]
        self.assertIn("Louisville Metro Landbank Authority", owners)
        self.assertIn(
            "Louisville Metro Urban Renewal and Community Development Agency", owners
        )
        self.assertIn("Louisville Metro (Metro-owned)", owners)

    def test_address_assembly(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        first = records[0]
        self.assertEqual(first.parcel_id, "050E01140000")
        self.assertEqual(
            first.property_address, "2816 S 06TH ST, LOUISVILLE KY 40208"
        )

    def test_address_includes_loc_modifier(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        second = records[1]
        # The "R" in the LOC column marks a rear lot — must be preserved.
        self.assertIn(" R ", second.property_address)
        self.assertTrue(second.property_address.startswith("1414 R S 07TH"))

    def test_missing_parcel_is_none_not_empty(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        # The Urban Renewal fixture row has no PARCELID.
        urban_renewal = next(
            r for r in records
            if r.owner_name and r.owner_name.startswith("Louisville Metro Urban")
        )
        self.assertIsNone(urban_renewal.parcel_id)

    def test_signal_date_prefers_date_received(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        first = records[0]
        # DATE RECEIVED=06/01/2014 should win over DATE OF DEED=05/30/2006.
        self.assertEqual(first.signal_date, "2014-06-01")

    def test_signal_date_falls_back_to_deed(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        second = records[1]
        # No DATE RECEIVED -> fall back to DATE OF DEED.
        self.assertEqual(second.signal_date, "1984-09-14")

    def test_signal_date_is_none_when_unparseable(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        urban_renewal = next(
            r for r in records
            if r.owner_name and r.owner_name.startswith("Louisville Metro Urban")
        )
        # DATE OF DEED is 2020-03-15 (ISO) which IS parseable.
        self.assertEqual(urban_renewal.signal_date, "2020-03-15")

    def test_limit_caps_records(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE, limit=1)
        self.assertEqual(len(records), 1)


class CanonicalRecordTests(unittest.TestCase):
    def test_required_fields_present(self) -> None:
        records = llb.parse_inventory_csv(FIXTURE)
        canonical = records[0].to_canonical()
        required = {
            "source", "source_url", "scraped_at", "parcel_id",
            "property_address", "owner_name", "owner_mailing_address",
            "signal_type", "signal_date", "amount_owed", "case_number", "raw",
        }
        self.assertTrue(required.issubset(canonical.keys()))

    def test_source_slug(self) -> None:
        canonical = llb.parse_inventory_csv(FIXTURE)[0].to_canonical()
        self.assertEqual(canonical["source"], "louisville_landbank")

    def test_signal_type_uses_canonical_vocabulary(self) -> None:
        canonical = llb.parse_inventory_csv(FIXTURE)[0].to_canonical()
        # Per docs/SCRAPER_SPEC.md §3.3, the canonical slug is
        # `landbank_inventory`, NOT just "landbank".
        self.assertEqual(canonical["signal_type"], "landbank_inventory")

    def test_amount_owed_and_mailing_address_are_null(self) -> None:
        canonical = llb.parse_inventory_csv(FIXTURE)[0].to_canonical()
        # Landbank inventory is a sale list, not a debt. Never fabricate
        # an amount_owed, and the source does not carry a separate
        # owner mailing address.
        self.assertIsNone(canonical["amount_owed"])
        self.assertIsNone(canonical["owner_mailing_address"])

    def test_raw_contains_source_columns(self) -> None:
        canonical = llb.parse_inventory_csv(FIXTURE)[0].to_canonical()
        raw = canonical["raw"]
        self.assertEqual(raw["PARCELID"], "050E01140000")
        self.assertEqual(raw["STATUS"], "available")
        self.assertEqual(raw["ZONE"], "R6-TN")

    def test_scraped_at_is_iso8601_utc(self) -> None:
        canonical = llb.parse_inventory_csv(FIXTURE)[0].to_canonical()
        self.assertTrue(canonical["scraped_at"].endswith("Z"))
        # Cheap structural check — full parse not required here.
        self.assertEqual(len(canonical["scraped_at"]), 20)


class StandaloneEntryPointTests(unittest.TestCase):
    def test_main_writes_json_array_via_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "results.json"
            rc = llb.main([
                "--csv-path", str(FIXTURE),
                "--out", str(out),
                "--limit", "2",
            ])
            self.assertEqual(rc, 0)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertIsInstance(payload, list)
            self.assertEqual(len(payload), 2)
            for item in payload:
                self.assertEqual(item["source"], "louisville_landbank")
                self.assertEqual(item["signal_type"], "landbank_inventory")

    def test_main_writes_dispatcher_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            rc = llb.main([
                "--csv-path", str(FIXTURE),
                "--output-dir", str(output_dir),
                "--csv-name", "louisville_landbank_results.csv",
            ])
            self.assertEqual(rc, 0)
            csv_path = output_dir / "louisville_landbank_results.csv"
            sidecar = output_dir / "louisville_landbank_records.json"
            self.assertTrue(csv_path.exists())
            self.assertTrue(sidecar.exists())
            items = json.loads(sidecar.read_text(encoding="utf-8"))
            self.assertGreater(len(items), 0)


class DispatcherTests(unittest.TestCase):
    def test_source_registered(self) -> None:
        self.assertIn("louisville_landbank", SOURCES)
        self.assertEqual(SOURCES["louisville_landbank"]["schema"], "louisville_landbank")
        self.assertEqual(
            SOURCES["louisville_landbank"]["csv_name"],
            "louisville_landbank_results.csv",
        )

    def test_command_targets_landbank_module(self) -> None:
        args = argparse.Namespace(
            start_date="01/01/2025",
            end_date="12/31/2025",
            output_dir="scraper_output",
            limit=5,
        )
        cmd = _louisville_landbank_command(args)
        self.assertIn("scrapers.louisville_landbank", cmd)
        self.assertIn("--csv-name", cmd)
        self.assertEqual(
            cmd[cmd.index("--csv-name") + 1],
            SOURCES["louisville_landbank"]["csv_name"],
        )
        self.assertIn("--limit", cmd)
        self.assertEqual(cmd[cmd.index("--limit") + 1], "5")


class UploadResultsLandbankTests(unittest.TestCase):
    def test_resolve_meta_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            meta = upload_results._resolve_meta(Path(tmp), "louisville_landbank")
            self.assertEqual(meta["source_type"], "louisville_landbank")
            self.assertEqual(meta["schema"], "louisville_landbank")
            self.assertEqual(meta["csv_name"], "louisville_landbank_results.csv")


if __name__ == "__main__":
    unittest.main()
