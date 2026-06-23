import json

from live import stage95_shadow_runner


def test_stage95_shadow_cycle_persists_combined_readonly_report(monkeypatch, tmp_path):
    calls = {}

    def fake_shadow_sync(**kwargs):
        calls["shadow_no_broker"] = kwargs.get("no_broker")
        return {
            "timestamp": "2026-06-23T10:00:00Z",
            "status": "OK",
            "requires_attention": False,
            "warnings": [],
            "dry_run": True,
            "trading_disabled": True,
            "output_path": str(tmp_path / "latest_shadow_sync.json"),
            "target_weights": {"SPY": 0.255},
        }

    def fake_margin_monitor(**kwargs):
        calls["margin_no_broker"] = kwargs.get("no_broker")
        return {
            "timestamp": "2026-06-23T10:01:00Z",
            "status": "OBSERVED",
            "requires_attention": False,
            "warnings": [],
            "dry_run": True,
            "trading_disabled": True,
            "output_path": str(tmp_path / "latest_margin_snapshot.json"),
            "margin_fields": {"NetLiquidation": {"value": 2_000_000}},
        }

    monkeypatch.setattr(stage95_shadow_runner.shadow_sync, "run_shadow_sync", fake_shadow_sync)
    monkeypatch.setattr(stage95_shadow_runner.margin_monitor, "run_margin_monitor", fake_margin_monitor)

    report = stage95_shadow_runner.run_stage95_shadow_cycle(
        output_dir=tmp_path,
        persist_db=False,
        no_broker=True,
    )

    assert report["status"] == "HEALTHY"
    assert report["live_leverage_approved"] is False
    assert report["trading_disabled"] is True
    assert report["components"]["shadow_sync"]["trading_disabled"] is True
    assert calls == {"shadow_no_broker": True, "margin_no_broker": True}
    latest = tmp_path / "latest_stage95_cycle.json"
    assert latest.exists()
    payload = json.loads(latest.read_text(encoding="utf-8"))
    assert payload["status"] == "HEALTHY"
    assert payload["output_path"] == report["output_path"]


def test_stage95_shadow_cycle_fails_closed_when_component_raises(monkeypatch, tmp_path):
    def broken_shadow_sync(**kwargs):
        raise RuntimeError("quote source broken")

    def fake_margin_monitor(**kwargs):
        return {
            "timestamp": "2026-06-23T10:01:00Z",
            "status": "UNAVAILABLE",
            "requires_attention": True,
            "warnings": ["broker read-only connection unavailable"],
            "dry_run": True,
            "trading_disabled": True,
            "output_path": str(tmp_path / "latest_margin_snapshot.json"),
        }

    monkeypatch.setattr(stage95_shadow_runner.shadow_sync, "run_shadow_sync", broken_shadow_sync)
    monkeypatch.setattr(stage95_shadow_runner.margin_monitor, "run_margin_monitor", fake_margin_monitor)

    report = stage95_shadow_runner.run_stage95_shadow_cycle(
        output_dir=tmp_path,
        persist_db=False,
        no_broker=True,
    )

    assert report["status"] == "CRITICAL"
    assert report["level"] == "critical"
    assert report["requires_attention"] is True
    assert report["components"]["shadow_sync"]["status"] == "FAILED"
    assert report["components"]["shadow_sync"]["trading_disabled"] is True
    assert "quote source broken" in " ".join(report["components"]["shadow_sync"]["warnings"])
    assert (tmp_path / "latest_stage95_cycle.json").exists()
