import tempfile
from datetime import datetime
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
    with db.insurance_decision_writer("test"):
        db.save_insurance_decision(
            portfolio_id="us",
            previous_state="SAFE",
            decision=build_authority_decision(InsuranceState.SAFE, reasons=["test safe"]),
            risk_score=0.0,
            hard_blocks=[],
            source_signals=[],
        )
    db.create_user("admin", hash_password("admin-pass"), role="admin", display_name="Admin")
    db.create_user("alice", hash_password("alice-pass"), role="member", display_name="Alice")
    db.create_user("bob", hash_password("bob-pass"), role="member", display_name="Bob")

    alice_member = db.create_family_member(display_name="Alice Family")
    bob_member = db.create_family_member(display_name="Bob Family")
    db.bind_user_member("alice", alice_member["id"])
    db.bind_user_member("bob", bob_member["id"])
    alice_account = db.create_capital_account(alice_member["id"], "Alice Main", default_portfolio_id="us")
    bob_account = db.create_capital_account(bob_member["id"], "Bob Main", default_portfolio_id="us")
    db.grant_account_permission("bob", alice_account["id"], "approve_withdrawal")

    main.app.state.db = db
    test_client = TestClient(main.app)
    try:
        yield test_client, db, alice_account, bob_account
    finally:
        test_client.close()
        db_path.unlink(missing_ok=True)


def _login(client, username, password):
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _deposit_and_confirm(http, account_id, amount, member_headers, admin_headers):
    request = http.post(
        "/api/governance/deposit/requests",
        json={"account_id": account_id, "amount": amount},
        headers=member_headers,
    )
    assert request.status_code == 200, request.text
    request_id = request.json()["deposit_request_id"]
    confirmed = http.post(
        f"/api/governance/deposit/requests/{request_id}/confirm",
        json={},
        headers=admin_headers,
    )
    assert confirmed.status_code == 200, confirmed.text
    return confirmed.json()


def test_withdrawal_redeems_account_pool_shares_and_reports(client):
    http, db, alice_account, _ = client
    admin_headers = {"Authorization": f"Bearer {_login(http, 'admin', 'admin-pass')}"}
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}
    bob_headers = {"Authorization": f"Bearer {_login(http, 'bob', 'bob-pass')}"}

    _deposit_and_confirm(http, alice_account["id"], 10_000, alice_headers, admin_headers)

    preview = http.post(
        "/api/governance/withdraw/preview",
        json={"account_id": alice_account["id"], "amount": 5_500, "reason": "tuition"},
        headers=alice_headers,
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["redemption"]["requested_shares"] == pytest.approx(55.0)

    request = http.post(
        "/api/governance/withdraw",
        json={"account_id": alice_account["id"], "amount": 5_500, "reason": "tuition"},
        headers=alice_headers,
    )
    assert request.status_code == 200, request.text
    withdrawal_id = request.json()["id"]
    assert request.json()["shares_requested"] == pytest.approx(55.0)

    stored = db.get_withdrawal_request(withdrawal_id)
    assert stored["account_id"] == alice_account["id"]
    assert stored["source_pool_id"] == "us"
    assert stored["share_price"] == pytest.approx(100.0)
    assert stored["shares_requested"] == pytest.approx(55.0)

    approval = http.post(
        f"/api/governance/withdraw/{withdrawal_id}/approve",
        json={"decision": "APPROVED", "comment": "approved by reviewer"},
        headers=bob_headers,
    )
    assert approval.status_code == 200, approval.text
    assert approval.json()["status"] == "APPROVED"

    executed = http.post(f"/api/governance/withdraw/{withdrawal_id}/execute", headers=admin_headers)
    assert executed.status_code == 200, executed.text
    body = executed.json()
    assert body["status"] == "SUCCESS"
    assert body["shares_redeemed"] == pytest.approx(55.0)
    assert len(body["ledger_entry_ids"]) == 2

    position = db.get_account_pool_position(alice_account["id"], "us")
    assert position["shares"] == pytest.approx(45.0)
    assert position["cost_basis"] == pytest.approx(4_500.0)

    asset_view = http.get(f"/api/accounts/{alice_account['id']}/asset-view", headers=alice_headers)
    assert asset_view.status_code == 200, asset_view.text
    assert asset_view.json()["total_value"] == pytest.approx(4_500.0)

    pool = db.get_investment_pool("us")
    assert pool["shares_outstanding"] == pytest.approx(1045.0)
    assert pool["share_price"] == pytest.approx(100.0)

    ledger_types = {
        entry["entry_type"]
        for entry in db.list_ledger_entries(account_id=alice_account["id"], portfolio_id="us")
    }
    assert {
        "WITHDRAWAL_REQUESTED",
        "WITHDRAWAL_APPROVED",
        "POOL_REDEMPTION",
        "WITHDRAWAL_EXECUTED",
    } <= ledger_types

    now = datetime.now()
    monthly = http.get(
        f"/api/governance/reports/accounts/{alice_account['id']}/monthly",
        params={"year": now.year, "month": now.month},
        headers=alice_headers,
    )
    assert monthly.status_code == 200, monthly.text
    assert monthly.json()["cashflows"]["withdrawals"] == pytest.approx(5_500.0)
    assert monthly.json()["asset_view"]["total_value"] == pytest.approx(4_500.0)
    assert monthly.json()["withdrawals"][0]["status"] == "EXECUTED"

    family = http.get(
        "/api/governance/reports/family/monthly",
        params={"year": now.year, "month": now.month},
        headers=admin_headers,
    )
    assert family.status_code == 200, family.text
    assert family.json()["withdrawal_status_counts"]["EXECUTED"] == 1
    assert family.json()["aum"]["total_account_value"] == pytest.approx(4_500.0)

    audit = http.get("/api/governance/audit-log/export", params={"action": "WITHDRAWAL_EXECUTED"}, headers=admin_headers)
    assert audit.status_code == 200, audit.text
    assert audit.json()["count"] >= 1
    assert withdrawal_id in audit.json()["rows"][0]["detail"]


def test_member_cannot_withdraw_another_account_or_over_available_shares(client):
    http, _, alice_account, bob_account = client
    admin_headers = {"Authorization": f"Bearer {_login(http, 'admin', 'admin-pass')}"}
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}

    _deposit_and_confirm(http, alice_account["id"], 1_000, alice_headers, admin_headers)

    other_account = http.post(
        "/api/governance/withdraw",
        json={"account_id": bob_account["id"], "amount": 100, "reason": "bad scope"},
        headers=alice_headers,
    )
    assert other_account.status_code == 403

    too_large = http.post(
        "/api/governance/withdraw",
        json={"account_id": alice_account["id"], "amount": 2_000, "reason": "too much"},
        headers=alice_headers,
    )
    assert too_large.status_code == 400
