"""Week 9-12 综合验证：Alpha 沙盒 + 策略竞技场"""

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main
from api.auth import hash_password
from db.database import Database
from engine.insurance import InsuranceState, build_authority_decision


@pytest.fixture
def client():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    db.create_user(
        username="admin",
        password_hash=hash_password("xirang2026"),
        role="admin",
        display_name="admin",
        email="",
    )
    with db.insurance_decision_writer("test"):
        decision_id = db.save_insurance_decision(
            portfolio_id="us",
            previous_state="SAFE",
            decision=build_authority_decision(InsuranceState.SAFE, reasons=["test safe"]),
            risk_score=0.0,
            hard_blocks=[],
            source_signals=[],
        )
    db.record_alpha_ledger_entry(
        portfolio_id="us",
        direction="IN",
        amount=100_000.0,
        actor="test",
        insurance_decision_id=decision_id,
        note="test alpha seed",
    )

    main.app.state.db = db
    test_client = TestClient(main.app)
    try:
        yield test_client
    finally:
        test_client.close()
        db_path.unlink(missing_ok=True)


def req(client, method, path, data=None, token=None, expected=200):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = client.request(method, path, json=data, headers=headers)
    assert response.status_code == expected, response.text
    content_type = response.headers.get("content-type", "")
    return response.json() if "json" in content_type else response.text


def test_all(client):
    # 1. Login
    result = req(client, "POST", "/api/auth/login", {"username": "admin", "password": "xirang2026"})
    token = result["access_token"]

    # 2. Get strategies (auto-register)
    strategies = req(client, "GET", "/api/alpha/strategies", token=token)
    assert len(strategies) >= 1
    cc = [s for s in strategies if s["id"] == "covered_call"][0]
    assert cc["status"] in ("DISABLED", "ENABLED", "SUSPENDED")

    # 3. Disabled strategy cannot be run
    req(client, "POST", "/api/alpha/strategies/covered_call/toggle",
        {"action": "disable"}, token=token)
    req(client, "POST", "/api/alpha/strategies/covered_call/run", token=token, expected=400)

    # 4. Enable strategy
    result = req(client, "POST", "/api/alpha/strategies/covered_call/toggle",
                 {"action": "enable", "allocation_pct": 0.10}, token=token)
    assert result["status"] == "ENABLED"

    # 5. Verify enabled
    strategies = req(client, "GET", "/api/alpha/strategies", token=token)
    cc = [s for s in strategies if s["id"] == "covered_call"][0]
    assert cc["status"] == "ENABLED"

    # 6. Run strategy
    result = req(client, "POST", "/api/alpha/strategies/covered_call/run?spy_price=450", token=token)
    assert result.get("action") in {"SELL_CALL", "HOLD", "SKIP"}

    # 7. Check transactions
    txs = req(client, "GET", "/api/alpha/strategies/covered_call/transactions", token=token)
    assert isinstance(txs, list)

    # 8. Strategy detail
    detail = req(client, "GET", "/api/alpha/strategies/covered_call", token=token)
    assert detail["id"] == "covered_call"
    assert detail["capital_source"] == "alpha_ledger"

    # 9. Leaderboard is formal-only and may be empty while all strategies remain sandbox-only.
    board = req(client, "GET", "/api/alpha/arena/leaderboard", token=token)
    assert isinstance(board, list)
    if board:
        assert board[0]["rank"] == 1

    # 10. Run all strategies
    result = req(client, "POST", "/api/alpha/arena/run-all?spy_price=450", token=token)
    assert "strategies_run" in result

    # 11. Quarterly evaluation
    report = req(client, "POST", "/api/alpha/arena/evaluate", token=token)
    assert "evaluations" in report
    assert "summary" in report
    assert report["reporting_scope"] == "formal_only"

    # 12. Disable and verify
    req(client, "POST", "/api/alpha/strategies/covered_call/toggle",
        {"action": "disable"}, token=token)
    detail = req(client, "GET", "/api/alpha/strategies/covered_call", token=token)
    assert detail["status"] == "DISABLED"

    # 13. Audit log check
    audit = req(client, "GET", "/api/governance/audit-log", token=token)
    alpha_events = [a for a in audit if "ALPHA" in a["action"] or "ARENA" in a["action"]]
    assert len(alpha_events) >= 2

    # 14. Frontend checks
    html = req(client, "GET", "/")
    assert "tab-alpha" in html
    assert "达尔文沙盒" in html
    assert "arena-leaderboard" in html
    assert "arena-controls" in html

    js = req(client, "GET", "/static/app.js")
    assert "loadAlpha" in js
    assert "loadArenaLeaderboard" in js
    assert "btn-arena-evaluate" in js
