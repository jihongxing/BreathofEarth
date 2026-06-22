import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

import main
from api.auth import hash_password
from api.routes import shadow_audit_routes
from db.database import Database


def _client_with_admin():
    temp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = Path(temp.name)
    temp.close()
    db = Database(db_path)
    db.create_user(
        username="admin",
        password_hash=hash_password("pw"),
        role="admin",
        display_name="Admin",
    )
    main.app.state.db = db
    client = TestClient(main.app)
    token = client.post("/api/auth/login", json={"username": "admin", "password": "pw"}).json()["access_token"]
    return client, db_path, {"Authorization": f"Bearer {token}"}


def test_build_shadow_audit_payload_reads_latest_reports(tmp_path):
    (tmp_path / "latest_shadow_sync.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "WARNING",
                "requires_attention": True,
                "dry_run": True,
                "trading_disabled": True,
                "warnings": ["current positions unavailable"],
                "broker": {"name": "offline"},
                "candidate_policy": {"name": "90pct_fixed_defensive_10pct_qqq_spy_gld"},
                "target_weights": {"SPY": 0.255, "TLT": 0.225, "GLD": 0.255, "SHV": 0.225, "QQQ": 0.04},
                "target_notionals": {"SPY": 510000.0},
                "slippage_audit": {"status": "LOCAL_PRICE_ONLY"},
                "shadow_orders": [],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_margin_snapshot.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:01:00Z",
                "status": "UNAVAILABLE",
                "requires_attention": True,
                "warnings": ["broker read-only connection unavailable"],
                "margin_fields": {},
                "production_conclusion": "OBSERVATION_ONLY_NO_LEVERAGE_APPROVAL",
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_shadow_audit_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "ATTENTION"
    assert payload["level"] == "warning"
    assert payload["live_leverage_approved"] is False
    assert payload["components"]["shadow_sync"]["target_weights"]["QQQ"] == 0.04
    assert payload["components"]["margin_snapshot"]["status"] == "UNAVAILABLE"
    assert payload["warning_count"] == 2


def test_build_shadow_audit_payload_reports_missing_files(tmp_path):
    payload = shadow_audit_routes.build_shadow_audit_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "MISSING"
    assert payload["level"] == "missing"
    assert payload["requires_attention"] is False
    assert payload["components"]["shadow_sync"]["status"] == "missing"
    assert payload["components"]["margin_snapshot"]["status"] == "missing"


def test_shadow_audit_api_requires_login_and_returns_readonly_payload(monkeypatch, tmp_path):
    (tmp_path / "latest_shadow_sync.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "OK",
                "requires_attention": False,
                "warnings": [],
                "shadow_orders": [],
                "target_weights": {"SPY": 0.255, "TLT": 0.225, "GLD": 0.255, "SHV": 0.225, "QQQ": 0.04},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_margin_snapshot.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:01:00Z",
                "status": "OBSERVED",
                "requires_attention": False,
                "warnings": [],
                "margin_fields": {"NetLiquidation": {"value": 2_000_000}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(shadow_audit_routes, "DEFAULT_SHADOW_DIR", tmp_path)
    client, db_path, headers = _client_with_admin()
    try:
        unauthorized = client.get("/api/shadow-audit/us")
        assert unauthorized.status_code == 401

        response = client.get("/api/shadow-audit/us", headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "HEALTHY"
        assert body["live_leverage_approved"] is False
        assert body["components"]["shadow_sync"]["trading_disabled"] is True
    finally:
        client.close()
        db_path.unlink(missing_ok=True)


def test_build_shadow_audit_payload_marks_stale_reports_as_attention(tmp_path):
    old_timestamp = (datetime(2026, 6, 22, 8, 0, tzinfo=timezone.utc)).isoformat().replace("+00:00", "Z")
    now = datetime(2026, 6, 23, 10, 30, tzinfo=timezone.utc)
    (tmp_path / "latest_shadow_sync.json").write_text(
        json.dumps(
            {
                "timestamp": old_timestamp,
                "status": "OK",
                "requires_attention": False,
                "warnings": [],
                "shadow_orders": [],
                "target_weights": {"SPY": 0.255},
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_margin_snapshot.json").write_text(
        json.dumps(
            {
                "timestamp": old_timestamp,
                "status": "OBSERVED",
                "requires_attention": False,
                "warnings": [],
                "margin_fields": {"NetLiquidation": {"value": 2_000_000}},
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_shadow_audit_payload(
        "us",
        shadow_dir=tmp_path,
        stale_after_hours=24,
        now=now,
    )

    assert payload["status"] == "ATTENTION"
    assert payload["level"] == "warning"
    assert payload["stale_report_count"] == 2
    assert payload["live_leverage_approved"] is False
    assert payload["components"]["shadow_sync"]["stale_report"] is True
    assert payload["components"]["shadow_sync"]["requires_attention"] is True
    assert "stale" in " ".join(payload["components"]["shadow_sync"]["warnings"])


def test_build_shadow_audit_payload_keeps_fresh_reports_healthy(tmp_path):
    now = datetime(2026, 6, 23, 10, 30, tzinfo=timezone.utc)
    fresh_timestamp = (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    (tmp_path / "latest_shadow_sync.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_timestamp,
                "status": "OK",
                "requires_attention": False,
                "warnings": [],
                "shadow_orders": [],
                "target_weights": {"SPY": 0.255},
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_margin_snapshot.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_timestamp,
                "status": "OBSERVED",
                "requires_attention": False,
                "warnings": [],
                "margin_fields": {"NetLiquidation": {"value": 2_000_000}},
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_shadow_audit_payload(
        "us",
        shadow_dir=tmp_path,
        stale_after_hours=24,
        now=now,
    )

    assert payload["status"] == "HEALTHY"
    assert payload["stale_report_count"] == 0
    assert payload["components"]["margin_snapshot"]["stale_report"] is False


def test_build_observation_summary_payload_reads_latest_summary(tmp_path):
    (tmp_path / "latest_stage95_observation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-06-23T10:00:00Z",
                "status": "ATTENTION",
                "requires_attention": True,
                "expected_cycles": 60,
                "observed_cycles": 1,
                "coverage_ratio": 0.0167,
                "broker_unavailable_cycles": 1,
                "abnormal_streak": {"current": 1, "max": 1},
                "broker_unavailable_streak": {"current": 1, "max": 1},
                "slippage_bps": {"observations": 1, "max": 0.0, "avg": 0.0},
                "margin_field_coverage": {
                    "all_required_cycles": 0,
                    "all_required_ratio": 0.0,
                    "fields": {"NetLiquidation": {"coverage_ratio": 0.0}},
                },
                "live_leverage_approved": False,
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_observation_summary_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "ATTENTION"
    assert payload["level"] == "warning"
    assert payload["live_leverage_approved"] is False
    assert payload["summary"]["observed_cycles"] == 1
    assert payload["summary"]["expected_cycles"] == 60
    assert payload["summary"]["broker_unavailable_cycles"] == 1
    assert payload["summary"]["margin_field_coverage"]["all_required_ratio"] == 0.0


def test_build_observation_summary_payload_reports_missing_file(tmp_path):
    payload = shadow_audit_routes.build_observation_summary_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "missing"
    assert payload["level"] == "missing"
    assert payload["requires_attention"] is False
    assert payload["live_leverage_approved"] is False
    assert payload["summary"]["source_path"].endswith("latest_stage95_observation_summary.json")


def test_stage95_observation_summary_api_requires_login_and_returns_readonly_payload(monkeypatch, tmp_path):
    (tmp_path / "latest_stage95_observation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-06-23T10:00:00Z",
                "status": "COLLECTING",
                "requires_attention": False,
                "expected_cycles": 60,
                "observed_cycles": 3,
                "coverage_ratio": 0.05,
                "broker_unavailable_cycles": 0,
                "slippage_bps": {"observations": 3, "max": 1.5, "avg": 0.8},
                "margin_field_coverage": {"all_required_ratio": 1.0, "fields": {}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(shadow_audit_routes, "DEFAULT_SHADOW_DIR", tmp_path)
    client, db_path, headers = _client_with_admin()
    try:
        unauthorized = client.get("/api/stage95-observation-summary/us")
        assert unauthorized.status_code == 401

        response = client.get("/api/stage95-observation-summary/us", headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "COLLECTING"
        assert body["level"] == "healthy"
        assert body["live_leverage_approved"] is False
        assert body["summary"]["coverage_ratio"] == 0.05
    finally:
        client.close()
        db_path.unlink(missing_ok=True)
