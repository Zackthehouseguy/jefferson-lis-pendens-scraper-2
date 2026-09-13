from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def text(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_live_workflow_is_scheduled_and_fails_false_green_runs():
    w = text(".github/workflows/reaper-multi-source-live.yml")
    assert 'cron: "0 12 * * *"' in w
    assert "reaper_health_check" in w
    assert "--lojic-cache data/lojic_parcel_cache.json" in w
    assert "Enforce production health" in w


def test_delivery_is_chained_and_crm_ack_precedes_sticky_persist():
    w = text(".github/workflows/reaper-zack-delivery.yml")
    assert 'workflows: ["Reaper Multi-Source Live"]' in w
    assert w.index("Import leads into Zack CRM") < w.index("Persist delivered batch")
    assert "CRM acknowledgement mismatch" in w
    assert "Market status is not a delivery gate" in w
    assert "triggers/reaper_strict_zack" not in w


def test_distress_labels_keep_lis_pendens_distinct():
    w = text(".github/workflows/reaper-zack-delivery.yml")
    assert "'lis_pendens': 'Lis Pendens'" in w
    assert "'pre_foreclosure': 'Pre-Foreclosure'" in w
    assert "Pre-Foreclosure / Lis Pendens" not in w
    assert "'distress_stacked': signal_stacked" in w
