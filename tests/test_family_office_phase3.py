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


def test_investment_pool_revalue_updates_account_assets_by_shares(client):
    http, db, alice_account, bob_account = client
    admin_headers = {"Authorization": f"Bearer {_login(http, 'admin', 'admin-pass')}"}
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}
    bob_headers = {"Authorization": f"Bearer {_login(http, 'bob', 'bob-pass')}"}

    alice_deposit = _deposit_and_confirm(http, alice_account["id"], 10_000, alice_headers, admin_headers)
    bob_deposit = _deposit_and_confirm(http, bob_account["id"], 20_000, bob_headers, admin_headers)

    assert alice_deposit["shares_issued"] == pytest.approx(100.0)
    assert bob_deposit["shares_issued"] == pytest.approx(200.0)

    pools = http.get("/api/investment-pools", headers=admin_headers)
    assert pools.status_code == 200, pools.text
    us_pool = next(pool for pool in pools.json() if pool["id"] == "us")
    assert us_pool["shares_outstanding"] == pytest.approx(1300.0)
    assert us_pool["share_price"] == pytest.approx(100.0)

    revalued = http.post(
        "/api/investment-pools/us/revalue",
        json={"nav": 143_000, "snapshot_date": "2026-05-22"},
        headers=admin_headers,
    )
    assert revalued.status_code == 200, revalued.text
    assert revalued.json()["share_price"] == pytest.approx(110.0)

    alice_view = http.get(f"/api/accounts/{alice_account['id']}/asset-view", headers=alice_headers)
    assert alice_view.status_code == 200, alice_view.text
    assert alice_view.json()["total_value"] == pytest.approx(11_000.0)
    assert alice_view.json()["unrealized_pnl"] == pytest.approx(1_000.0)

    bob_view = db.get_account_asset_view(bob_account["id"])
    assert bob_view["total_value"] == pytest.approx(22_000.0)
    assert bob_view["unrealized_pnl"] == pytest.approx(2_000.0)

    assert len(db.list_investment_pools(portfolio_id="us")) == 1
    assert db.get_portfolio("us")["nav"] == pytest.approx(130_000.0)


def test_family_aum_uses_pool_nav_not_duplicate_member_portfolios(client):
    http, _, alice_account, bob_account = client
    admin_headers = {"Authorization": f"Bearer {_login(http, 'admin', 'admin-pass')}"}
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}
    bob_headers = {"Authorization": f"Bearer {_login(http, 'bob', 'bob-pass')}"}

    _deposit_and_confirm(http, alice_account["id"], 10_000, alice_headers, admin_headers)
    _deposit_and_confirm(http, bob_account["id"], 20_000, bob_headers, admin_headers)
    http.post("/api/investment-pools/us/revalue", json={"nav": 143_000}, headers=admin_headers)

    aum = http.get("/api/admin/aum", headers=admin_headers)
    assert aum.status_code == 200, aum.text
    body = aum.json()
    pools = [pool for pool in body["pools"] if pool["id"] == "us"]
    assert len(pools) == 1
    assert pools[0]["nav"] == pytest.approx(143_000.0)
    assert body["total_account_value"] == pytest.approx(33_000.0)
