#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

import requests


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def post_ingest(ingest_url: str, ingest_token: str, payload: dict) -> None:
    response = requests.post(
        ingest_url,
        headers={
            "Authorization": f"Bearer {ingest_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    response.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser(description="Send scraper run status to Lovable ingest endpoint.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingest-url", required=True)
    parser.add_argument("--ingest-token", required=True)
    parser.add_argument("--status", required=True, choices=["queued", "running", "completed", "failed", "cancelled"])
    parser.add_argument("--message", default="")
    args = parser.parse_args()

    payload = {
        "action": "status_update",
        "type": "status_update",
        "run_id": args.run_id,
        "status": args.status,
        "message": args.message,
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "github_repository": os.environ.get("GITHUB_REPOSITORY"),
        "timestamp": utc_now(),
    }
    post_ingest(args.ingest_url, args.ingest_token, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
