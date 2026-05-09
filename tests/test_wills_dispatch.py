"""Verifies that source_type=wills targets a different Jefferson Deeds
instrument type than source_type=lis_pendens, and that the values match
the live insttype.php dropdown (LP / WIL).

This is the minimal config-level guarantee that the Wills source is not
accidentally pulling Lis Pendens data — the two sources MUST emit
distinct itype1 codes when the dispatcher builds the scraper command.
"""
from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scrapers.run_source import _jefferson_command


def _make_args(**overrides) -> argparse.Namespace:
    base = dict(
        start_date="05/07/2026",
        end_date="05/08/2026",
        output_dir="scraper_output",
        search_mode="auto",
        resume=False,
        pva_cross_check=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _flag_value(cmd: list[str], flag: str) -> str | None:
    if flag not in cmd:
        return None
    return cmd[cmd.index(flag) + 1]


class WillsDispatchTests(unittest.TestCase):
    def test_lis_pendens_uses_lp_code(self) -> None:
        cmd = _jefferson_command(_make_args(), "lis_pendens")
        self.assertEqual(_flag_value(cmd, "--instrument-code"), "LP ")
        self.assertEqual(_flag_value(cmd, "--instrument-label"), "LIS PENDENS")
        self.assertEqual(_flag_value(cmd, "--csv-name"), "lis_pendens_results.csv")

    def test_wills_uses_wil_code(self) -> None:
        cmd = _jefferson_command(_make_args(), "wills")
        # Verified against the live insttype.php dropdown: option value=WIL → WILL.
        self.assertEqual(_flag_value(cmd, "--instrument-code"), "WIL")
        self.assertEqual(_flag_value(cmd, "--instrument-label"), "WILLS")
        self.assertEqual(_flag_value(cmd, "--csv-name"), "wills_results.csv")

    def test_wills_and_lis_pendens_use_distinct_instrument_types(self) -> None:
        lp = _jefferson_command(_make_args(), "lis_pendens")
        wills = _jefferson_command(_make_args(), "wills")
        self.assertNotEqual(
            _flag_value(lp, "--instrument-code"),
            _flag_value(wills, "--instrument-code"),
            "Wills must search a different instrument type than Lis Pendens",
        )
        self.assertNotEqual(
            _flag_value(lp, "--instrument-label"),
            _flag_value(wills, "--instrument-label"),
        )
        self.assertNotEqual(
            _flag_value(lp, "--csv-name"),
            _flag_value(wills, "--csv-name"),
        )

    def test_wills_tags_notes_and_keeps_legal_desc(self) -> None:
        cmd = _jefferson_command(_make_args(), "wills")
        self.assertEqual(_flag_value(cmd, "--source-tag"), "Source: WILLS")
        self.assertIn("--always-include-legal-desc", cmd)
        # Lis-Pendens-specific behaviors must NOT be applied to Wills.
        self.assertNotIn("--pva-cross-check", cmd)
        self.assertIn("--skip-validation", cmd)

    def test_lis_pendens_does_not_get_wills_tag(self) -> None:
        cmd = _jefferson_command(_make_args(pva_cross_check=True), "lis_pendens")
        self.assertNotIn("--source-tag", cmd)
        self.assertNotIn("--always-include-legal-desc", cmd)
        self.assertIn("--pva-cross-check", cmd)
        self.assertNotIn("--skip-validation", cmd)


if __name__ == "__main__":
    unittest.main()
