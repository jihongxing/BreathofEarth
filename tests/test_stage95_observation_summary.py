import json
from datetime import datetime, timedelta, timezone

from live import stage95_observation_summary


def _cycle(
    *,
    timestamp,
    status="HEALTHY",
    margin_status="OBSERVED",
    shadow_status="OK",
    margin_fields=None,
    slippage_bps=1.25,
    warning_count=0,
):
    margin_fields = margin_fields if margin_fields is not None else {
        "NetLiquidation": {"value": 2_000_000},
        "ExcessLiquidity": {"value": 1_500_000},
        "FullMaintainMarginReq": {"value": 350_000},
    }
    margin_warnings = []
    if margin_status == "UNAVAILABLE":
        margin_warnings = ["broker read-only connection unavailable"]
    return {
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "status": status,
        "warning_count": warning_count,
        "components": {
            "shadow_sync": {
                "status": shadow_status,
                "level": "healthy" if shadow_status == "OK" else "warning",
                "requires_attention": shadow_status != "OK",
                "warning_count": 0,
                "warnings": [],
                "report": {
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "status": shadow_status,
                    "dry_run": True,
                    "trading_disabled": True,
                    "slippage_audit": {"max_observed_half_spread_bps": slippage_bps},
                },
            },
            "margin_snapshot": {
                "status": margin_status,
                "level": "healthy" if margin_status == "OBSERVED" else "warning",
                "requires_attention": margin_status != "OBSERVED",
                "warning_count": len(margin_warnings),
                "warnings": margin_warnings,
                "report": {
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "status": margin_status,
                    "dry_run": True,
                    "trading_disabled": True,
                    "margin_fields": margin_fields,
                    "warnings": margin_warnings,
                },
            },
        },
    }


def _write_cycle(path, payload):
    stamp = payload["timestamp"].replace("-", "").replace(":", "").replace("T", "_").replace("Z", "")
    (path / f"stage95_cycle_{stamp}.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def test_stage95_summary_collecting_with_fresh_healthy_cycles(tmp_path):
    now = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    for index in range(3):
        _write_cycle(tmp_path, _cycle(timestamp=now - timedelta(hours=3 - index)))

    summary = stage95_observation_summary.run_stage95_observation_summary(
        shadow_dir=tmp_path,
        expected_cycles=60,
        limit=60,
        stale_after_hours=24,
        now=now,
    )

    assert summary["status"] == "COLLECTING"
    assert summary["requires_attention"] is False
    assert summary["observed_cycles"] == 3
    assert summary["coverage_ratio"] == 0.05
    assert summary["slippage_bps"]["max"] == 1.25
    assert summary["margin_field_coverage"]["all_required_ratio"] == 1.0
    assert summary["live_leverage_approved"] is False
    assert (tmp_path / "latest_stage95_observation_summary.json").exists()


def test_stage95_summary_attention_when_broker_is_unavailable(tmp_path):
    now = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    _write_cycle(tmp_path, _cycle(timestamp=now - timedelta(hours=2)))
    _write_cycle(
        tmp_path,
        _cycle(
            timestamp=now - timedelta(hours=1),
            status="ATTENTION",
            margin_status="UNAVAILABLE",
            margin_fields={},
            warning_count=2,
        ),
    )

    summary = stage95_observation_summary.run_stage95_observation_summary(
        shadow_dir=tmp_path,
        expected_cycles=2,
        limit=60,
        stale_after_hours=24,
        now=now,
    )

    assert summary["status"] == "ATTENTION"
    assert summary["requires_attention"] is True
    assert summary["broker_unavailable_cycles"] == 1
    assert summary["broker_unavailable_streak"]["current"] == 1
    assert summary["attention_cycles"] == 1
    assert summary["margin_field_coverage"]["all_required_cycles"] == 1


def test_stage95_summary_stale_when_latest_cycle_is_too_old(tmp_path):
    now = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)
    _write_cycle(tmp_path, _cycle(timestamp=now - timedelta(hours=30)))

    summary = stage95_observation_summary.run_stage95_observation_summary(
        shadow_dir=tmp_path,
        expected_cycles=1,
        limit=60,
        stale_after_hours=24,
        now=now,
    )

    assert summary["status"] == "STALE"
    assert summary["requires_attention"] is True
    assert summary["latest_is_stale"] is True
    assert summary["latest_age_hours"] == 30.0


def test_stage95_summary_missing_when_no_cycles_exist(tmp_path):
    summary = stage95_observation_summary.run_stage95_observation_summary(
        shadow_dir=tmp_path,
        expected_cycles=60,
        limit=60,
        stale_after_hours=24,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert summary["status"] == "MISSING"
    assert summary["observed_cycles"] == 0
    assert summary["requires_attention"] is True
