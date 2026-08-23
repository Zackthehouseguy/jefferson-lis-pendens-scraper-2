#!/usr/bin/env python3
"""50-record unseen live extraction stress test.

Extends the previous full-system live extraction bench without touching
production ingestion. Excludes every case used in earlier calibration/acceptance
runs, then requires 50 newly verified OPEN parent cases.
"""
from __future__ import annotations

import sys

from scrapers.probe import full_system_live_extract as live

PRIOR_BENCH_CASES = {
    # Original five development fixtures.
    "ENF-PMNT-26-019301",
    "ENF-PMNT-26-016300",
    "ENF-PMNT-26-013339",
    "ENF-PMNT-26-015609",
    "ENF-PMNT-26-016665",
    # Eight unseen cases used in the first full-system acceptance run.
    "ENF-PMNT-26-018885",
    "ENF-PMNT-26-018540",
    "ENF-PMNT-26-018701",
    "ENF-PMNT-26-016289",
    "ENF-PMNT-26-008705",
    "ENF-PMNT-26-014897",
    "ENF-PMNT-26-019037",
    "ENF-PMNT-26-005440",
}

live.KNOWN_TUNING_CASES.update(PRIOR_BENCH_CASES)


def main() -> int:
    sys.argv = [
        "stress_50_extract",
        "--target-open", "50",
        "--arcgis-limit", "1500",
        "--max-parent-attempts", "180",
        "--out", "reports/stress_50_live",
    ]
    return live.main()


if __name__ == "__main__":
    raise SystemExit(main())
