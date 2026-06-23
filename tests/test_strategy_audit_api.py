import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

import main
from api.auth import hash_password
from api.routes import strategy_audit_routes
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


def _write_multi_strategy_report(path: Path, *, timestamp: str = "2026-06-23T10:00:00Z") -> None:
    path.write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "status": "ATTENTION",
                "level": "warning",
                "requires_attention": True,
                "warnings": ["multi-strategy shadow runner is report-only"],
                "readonly": True,
                "trading_disabled": True,
                "live_leverage_approved": False,
                "human_review_required": True,
                "strategies": {
                    "production_90_10": {
                        "strategy_id": "production_90_10",
                        "status": "ATTENTION",
                        "requires_attention": True,
                        "target_weights": {"SPY": 0.255, "TLT": 0.225, "GLD": 0.255, "SHV": 0.225, "QQQ": 0.04},
                        "admission_status": "NOT_APPROVED",
                        "warnings": ["layered audit evidence is unavailable in this skeleton run"],
                        "slippage_audit": {"status": "UNAVAILABLE", "requires_attention": True},
                        "margin_snapshot": {"status": "UNAVAILABLE", "requires_attention": True},
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_build_multi_strategy_shadow_payload_reports_missing_file(tmp_path):
    payload = strategy_audit_routes.build_multi_strategy_shadow_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "MISSING"
    assert payload["level"] == "missing"
    assert payload["requires_attention"] is True
    assert payload["live_leverage_approved"] is False
    assert payload["readonly"] is True
    assert payload["trading_disabled"] is True
    assert payload["multi_strategy_shadow"]["strategy_count"] == 0
    assert payload["multi_strategy_shadow"]["stale_report"] is False


def test_build_multi_strategy_shadow_payload_normalizes_report(tmp_path):
    _write_multi_strategy_report(tmp_path / "latest_multi_strategy_shadow.json")

    payload = strategy_audit_routes.build_multi_strategy_shadow_payload(
        "us",
        shadow_dir=tmp_path,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "ATTENTION"
    assert payload["level"] == "warning"
    assert payload["requires_attention"] is True
    assert payload["live_leverage_approved"] is False
    assert payload["multi_strategy_shadow"]["strategy_count"] == 1
    assert payload["multi_strategy_shadow"]["strategies"]["production_90_10"]["admission_status"] == "NOT_APPROVED"
    assert payload["multi_strategy_shadow"]["stale_report"] is False
    assert payload["multi_strategy_shadow"]["warning_count"] == 2


def test_build_multi_strategy_shadow_payload_marks_stale_report(tmp_path):
    _write_multi_strategy_report(
        tmp_path / "latest_multi_strategy_shadow.json",
        timestamp="2026-06-22T08:00:00Z",
    )

    payload = strategy_audit_routes.build_multi_strategy_shadow_payload(
        "us",
        shadow_dir=tmp_path,
        stale_after_hours=24,
        now=datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "ATTENTION"
    assert payload["multi_strategy_shadow"]["stale_report"] is True
    assert "stale" in " ".join(payload["multi_strategy_shadow"]["warnings"])
    assert payload["live_leverage_approved"] is False


def test_multi_strategy_shadow_api_requires_login_and_returns_readonly_payload(monkeypatch, tmp_path):
    _write_multi_strategy_report(tmp_path / "latest_multi_strategy_shadow.json")
    monkeypatch.setattr(strategy_audit_routes, "DEFAULT_SHADOW_DIR", tmp_path)
    client, db_path, headers = _client_with_admin()
    try:
        unauthorized = client.get("/api/multi-strategy-shadow/us")
        assert unauthorized.status_code == 401

        response = client.get("/api/multi-strategy-shadow/us", headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "ATTENTION"
        assert body["readonly"] is True
        assert body["trading_disabled"] is True
        assert body["live_leverage_approved"] is False
        assert body["multi_strategy_shadow"]["strategies"]["production_90_10"]["target_weights"]["QQQ"] == 0.04
    finally:
        client.close()
        db_path.unlink(missing_ok=True)


def test_invalid_json_fails_closed(tmp_path):
    (tmp_path / "latest_multi_strategy_shadow.json").write_text("{bad", encoding="utf-8")

    payload = strategy_audit_routes.build_multi_strategy_shadow_payload("us", shadow_dir=tmp_path)

    assert payload["status"] == "UNAVAILABLE"
    assert payload["level"] == "warning"
    assert payload["requires_attention"] is True
    assert payload["multi_strategy_shadow"]["strategies"] == {}
    assert payload["multi_strategy_shadow"]["stale_report"] is False


def test_api_forces_readonly_flags_even_if_report_claims_otherwise(tmp_path):
    _write_multi_strategy_report(tmp_path / "latest_multi_strategy_shadow.json")
    payload = json.loads((tmp_path / "latest_multi_strategy_shadow.json").read_text(encoding="utf-8"))
    payload["readonly"] = False
    payload["trading_disabled"] = False
    payload["live_leverage_approved"] = True
    payload["human_review_required"] = False
    (tmp_path / "latest_multi_strategy_shadow.json").write_text(json.dumps(payload), encoding="utf-8")

    result = strategy_audit_routes.build_multi_strategy_shadow_payload("us", shadow_dir=tmp_path)

    assert result["readonly"] is True
    assert result["trading_disabled"] is True
    assert result["live_leverage_approved"] is False
    assert result["human_review_required"] is True
    assert result["multi_strategy_shadow"]["readonly"] is True
    assert result["multi_strategy_shadow"]["trading_disabled"] is True
    assert result["multi_strategy_shadow"]["live_leverage_approved"] is False
    assert result["multi_strategy_shadow"]["human_review_required"] is True
