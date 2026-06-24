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
    assert payload["requires_attention"] is True
    assert payload["components"]["shadow_sync"]["status"] == "missing"
    assert payload["components"]["shadow_sync"]["requires_attention"] is True
    assert payload["components"]["margin_snapshot"]["status"] == "missing"
    assert payload["components"]["margin_snapshot"]["requires_attention"] is True
    assert payload["live_leverage_approved"] is False


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
    assert payload["requires_attention"] is True
    assert payload["live_leverage_approved"] is False
    assert payload["summary"]["requires_attention"] is True
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


def test_build_stage95_admission_payload_blocks_missing_evidence(tmp_path):
    payload = shadow_audit_routes.build_stage95_admission_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "NOT_APPROVED"
    assert payload["requires_attention"] is True
    assert payload["live_leverage_approved"] is False
    assert payload["human_review_required"] is True
    assert payload["readonly"] is True
    blocker_codes = {item["code"] for item in payload["blockers"]}
    assert "latest_shadow_audit_healthy" in blocker_codes
    assert "observation_window_complete" in blocker_codes
    assert "margin_fields_complete" in blocker_codes


def test_build_stage95_admission_payload_ready_for_human_review_when_evidence_is_complete(tmp_path):
    timestamp = "2026-06-23T10:00:00Z"
    (tmp_path / "latest_shadow_sync.json").write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "status": "OK",
                "requires_attention": False,
                "warnings": [],
                "shadow_orders": [],
                "target_weights": {"SPY": 0.255, "TLT": 0.225, "GLD": 0.255, "SHV": 0.225, "QQQ": 0.04},
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_margin_snapshot.json").write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "status": "OBSERVED",
                "requires_attention": False,
                "warnings": [],
                "margin_fields": {
                    "NetLiquidation": {"value": 2_000_000},
                    "ExcessLiquidity": {"value": 1_800_000},
                    "FullMaintainMarginReq": {"value": 200_000},
                },
                "trading_disabled": True,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "latest_stage95_observation_summary.json").write_text(
        json.dumps(
            {
                "generated_at": timestamp,
                "status": "OBSERVED",
                "requires_attention": False,
                "expected_cycles": 60,
                "observed_cycles": 60,
                "coverage_ratio": 1.0,
                "latest_is_stale": False,
                "stale_gap_count": 0,
                "critical_cycles": 0,
                "broker_unavailable_cycles": 0,
                "margin_field_coverage": {
                    "required_fields": ["NetLiquidation", "ExcessLiquidity", "FullMaintainMarginReq"],
                    "all_required_cycles": 60,
                    "all_required_ratio": 1.0,
                },
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_stage95_admission_payload(
        "us",
        shadow_dir=tmp_path,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "READY_FOR_HUMAN_REVIEW"
    assert payload["level"] == "healthy"
    assert payload["blockers"] == []
    assert payload["live_leverage_approved"] is False
    assert payload["human_review_required"] is True
    assert all(check["passed"] for check in payload["checks"])


def test_stage95_admission_api_requires_login_and_returns_readonly_gate(monkeypatch, tmp_path):
    monkeypatch.setattr(shadow_audit_routes, "DEFAULT_SHADOW_DIR", tmp_path)
    client, db_path, headers = _client_with_admin()
    try:
        unauthorized = client.get("/api/stage95-admission/us")
        assert unauthorized.status_code == 401

        response = client.get("/api/stage95-admission/us", headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "NOT_APPROVED"
        assert body["live_leverage_approved"] is False
        assert body["readonly"] is True
        assert body["shadow_audit"]["components"]["shadow_sync"]["requires_attention"] is True
    finally:
        client.close()
        db_path.unlink(missing_ok=True)


def test_build_ibkr_readonly_preflight_payload_reports_missing_file(tmp_path):
    payload = shadow_audit_routes.build_ibkr_readonly_preflight_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "MISSING"
    assert payload["level"] == "missing"
    assert payload["requires_attention"] is True
    assert payload["live_leverage_approved"] is False
    assert payload["readonly"] is True
    assert payload["trading_disabled"] is True
    assert payload["preflight"]["requires_attention"] is True
    assert payload["preflight"]["source_path"].endswith("latest_ibkr_readonly_preflight.json")


def test_build_ibkr_readonly_preflight_payload_static_ready_requires_attention(tmp_path):
    (tmp_path / "latest_ibkr_readonly_preflight.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "READY_FOR_READONLY_CONNECT",
                "readonly": True,
                "dry_run": True,
                "trading_disabled": True,
                "live_leverage_approved": False,
                "human_review_required": True,
                "warnings": [],
                "blockers": [],
                "connection": {
                    "requested": False,
                    "attempted": False,
                    "broker": "ibkr",
                    "mode": "read_only",
                    "connected": False,
                },
                "assets": ["SPY", "TLT", "GLD", "SHV", "QQQ"],
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_ibkr_readonly_preflight_payload(
        "us",
        shadow_dir=tmp_path,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "ATTENTION"
    assert payload["level"] == "warning"
    assert payload["requires_attention"] is True
    assert payload["preflight"]["status"] == "READY_FOR_READONLY_CONNECT"
    assert payload["preflight"]["connection"]["attempted"] is False


def test_build_ibkr_readonly_preflight_payload_ready_after_connect_is_healthy(tmp_path):
    (tmp_path / "latest_ibkr_readonly_preflight.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "READY",
                "readonly": True,
                "dry_run": True,
                "trading_disabled": True,
                "live_leverage_approved": False,
                "human_review_required": True,
                "warnings": [],
                "blockers": [],
                "connection": {
                    "requested": True,
                    "attempted": True,
                    "broker": "ibkr",
                    "mode": "read_only",
                    "connected": True,
                },
                "assets": ["SPY", "TLT", "GLD", "SHV", "QQQ"],
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_ibkr_readonly_preflight_payload(
        "us",
        shadow_dir=tmp_path,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "HEALTHY"
    assert payload["level"] == "healthy"
    assert payload["requires_attention"] is False
    assert payload["live_leverage_approved"] is False
    assert payload["preflight"]["connection"]["connected"] is True


def test_build_ibkr_readonly_preflight_payload_fail_closed_is_critical(tmp_path):
    (tmp_path / "latest_ibkr_readonly_preflight.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "FAIL_CLOSED",
                "readonly": True,
                "dry_run": True,
                "trading_disabled": True,
                "live_leverage_approved": False,
                "human_review_required": True,
                "warnings": [],
                "blockers": ["IBKR_ENABLE_ORDER_SUBMISSION is enabled; read-only preflight must fail closed"],
                "connection": {"requested": False, "attempted": False, "connected": False},
            }
        ),
        encoding="utf-8",
    )

    payload = shadow_audit_routes.build_ibkr_readonly_preflight_payload(
        "us",
        shadow_dir=tmp_path,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "FAIL_CLOSED"
    assert payload["level"] == "critical"
    assert payload["requires_attention"] is True
    assert payload["blocker_count"] == 1
    assert payload["preflight"]["live_leverage_approved"] is False


def test_ibkr_readonly_preflight_api_requires_login_and_returns_readonly_payload(monkeypatch, tmp_path):
    (tmp_path / "latest_ibkr_readonly_preflight.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-23T10:00:00Z",
                "status": "READY",
                "readonly": True,
                "dry_run": True,
                "trading_disabled": True,
                "live_leverage_approved": False,
                "human_review_required": True,
                "warnings": [],
                "blockers": [],
                "connection": {"requested": True, "attempted": True, "connected": True},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(shadow_audit_routes, "DEFAULT_SHADOW_DIR", tmp_path)
    client, db_path, headers = _client_with_admin()
    try:
        unauthorized = client.get("/api/ibkr-readonly-preflight/us")
        assert unauthorized.status_code == 401

        response = client.get("/api/ibkr-readonly-preflight/us", headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "HEALTHY"
        assert body["readonly"] is True
        assert body["trading_disabled"] is True
        assert body["live_leverage_approved"] is False
        assert body["preflight"]["connection"]["connected"] is True
    finally:
        client.close()
        db_path.unlink(missing_ok=True)
