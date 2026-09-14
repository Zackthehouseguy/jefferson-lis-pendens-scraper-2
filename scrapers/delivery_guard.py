#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def parse_time(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def should_skip_scheduled_delivery(event_name, source_event, batch, response, now=None):
    if event_name != "workflow_run" or source_event != "schedule":
        return False

    generated_raw = batch.get("generated_at")
    generated = parse_time(generated_raw)
    current = now or datetime.now(timezone.utc)
    if not generated or generated.astimezone(EASTERN).date() != current.astimezone(EASTERN).date():
        return False

    expected_run_id = f"delivery::{generated_raw}::zack"
    return (
        response.get("ok") is True
        and response.get("run_id") == expected_run_id
        and int(response.get("received") or 0) > 0
        and int(response.get("rejected") or 0) == 0
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--source-event", default="")
    parser.add_argument("--batch", required=True)
    parser.add_argument("--response", required=True)
    args = parser.parse_args()

    try:
        batch = json.loads(Path(args.batch).read_text(encoding="utf-8"))
        response = json.loads(Path(args.response).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        skip = False
    else:
        skip = should_skip_scheduled_delivery(args.event_name, args.source_event, batch, response)

    output = os.getenv("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"skip={'true' if skip else 'false'}\n")
    print("skip duplicate scheduled delivery" if skip else "delivery allowed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
