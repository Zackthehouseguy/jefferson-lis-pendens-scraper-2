from datetime import datetime, timezone

from scrapers.delivery_guard import should_skip_scheduled_delivery


NOW = datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc)
BATCH = {"generated_at": "2026-09-14T13:16:10.350216+00:00"}
RESPONSE = {
    "ok": True,
    "run_id": "delivery::2026-09-14T13:16:10.350216+00:00::zack",
    "received": 25,
    "rejected": 0,
}


def test_delayed_schedule_skips_after_confirmed_same_day_delivery():
    assert should_skip_scheduled_delivery("workflow_run", "schedule", BATCH, RESPONSE, NOW)


def test_manual_and_push_retries_are_never_suppressed():
    assert not should_skip_scheduled_delivery("workflow_dispatch", "", BATCH, RESPONSE, NOW)
    assert not should_skip_scheduled_delivery("workflow_run", "push", BATCH, RESPONSE, NOW)


def test_failed_or_mismatched_crm_ack_does_not_skip():
    assert not should_skip_scheduled_delivery("workflow_run", "schedule", BATCH, {**RESPONSE, "ok": False}, NOW)
    assert not should_skip_scheduled_delivery("workflow_run", "schedule", BATCH, {**RESPONSE, "run_id": "other"}, NOW)
    assert not should_skip_scheduled_delivery("workflow_run", "schedule", BATCH, {**RESPONSE, "received": 0}, NOW)


def test_previous_eastern_day_does_not_skip():
    old_batch = {"generated_at": "2026-09-13T23:59:00-04:00"}
    old_response = {**RESPONSE, "run_id": "delivery::2026-09-13T23:59:00-04:00::zack"}
    assert not should_skip_scheduled_delivery("workflow_run", "schedule", old_batch, old_response, NOW)
