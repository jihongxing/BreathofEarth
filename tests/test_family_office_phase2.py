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
    db.create_user(
        username="admin",
        password_hash=hash_password("admin-pass"),
        role="admin",
        display_name="Admin",
        email="",
    )
    db.create_user(
        username="alice",
        password_hash=hash_password("alice-pass"),
        role="member",
        display_name="Alice",
        email="",
    )
    db.create_user(
        username="bob",
        password_hash=hash_password("bob-pass"),
        role="member",
        display_name="Bob",
        email="",
    )

    alice_member = db.create_family_member(display_name="Alice Family")
    bob_member = db.create_family_member(display_name="Bob Family")
    db.bind_user_member("alice", alice_member["id"])
    db.bind_user_member("bob", bob_member["id"])
    alice_account = db.create_capital_account(
        member_id=alice_member["id"],
        account_name="Alice Main",
        default_portfolio_id="us",
    )
    bob_account = db.create_capital_account(
        member_id=bob_member["id"],
        account_name="Bob Main",
        default_portfolio_id="us",
    )

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


def test_deposit_request_is_confirmed_into_ledger(client):
    http, db, alice_account, _ = client
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}
    admin_headers = {"Authorization": f"Bearer {_login(http, 'admin', 'admin-pass')}"}

    before_nav = db.get_portfolio("us")["nav"]
    req_resp = http.post(
        "/api/governance/deposit/requests",
        json={"account_id": alice_account["id"], "amount": 10_000},
        headers=alice_headers,
    )
    assert req_resp.status_code == 200, req_resp.text
    request_id = req_resp.json()["deposit_request_id"]
    assert db.get_portfolio("us")["nav"] == before_nav
    requested_entries = db.list_ledger_entries(
        account_id=alice_account["id"],
        portfolio_id="us",
        entry_type="DEPOSIT_REQUESTED",
    )
    assert len(requested_entries) == 1

    confirm_resp = http.post(
        f"/api/governance/deposit/requests/{request_id}/confirm",
        json={"external_reference": "bank-001", "note": "到账确认"},
        headers=admin_headers,
    )
    assert confirm_resp.status_code == 200, confirm_resp.text
    body = confirm_resp.json()
    assert body["status"] == "SUCCESS"
    assert body["account_id"] == alice_account["id"]
    assert len(body["ledger_entry_ids"]) == 2
    assert body["share_price"] == pytest.approx(100.0)
    assert body["shares_issued"] == pytest.approx(100.0)

    stored = db.get_deposit_request(request_id)
    assert stored["status"] == "CONFIRMED"
    assert stored["confirmed_by"] == "admin"
    assert stored["legacy_deposit_record_id"]

    ledger = db.list_ledger_entries(account_id=alice_account["id"], portfolio_id="us")
    ledger_types = {entry["entry_type"] for entry in ledger}
    assert {"DEPOSIT_REQUESTED", "DEPOSIT_CONFIRMED", "POOL_SUBSCRIPTION"} <= ledger_types

    subscription = next(entry for entry in ledger if entry["entry_type"] == "POOL_SUBSCRIPTION")
    assert subscription["shares_delta"] == pytest.approx(100.0)
    assert subscription["share_price"] == pytest.approx(100.0)

    position = db.get_account_pool_position(alice_account["id"], "us")
    assert position["shares"] == pytest.approx(100.0)
    assert position["cost_basis"] == pytest.approx(10_000.0)

    asset_view = db.get_account_asset_view(alice_account["id"])
    assert asset_view["total_value"] == pytest.approx(10_000.0)
    assert asset_view["positions"][0]["market_value"] == pytest.approx(10_000.0)

    deposits = db.list_deposit_records(account_id=alice_account["id"])
    assert deposits[0]["account_id"] == alice_account["id"]
    assert deposits[0]["deposit_request_id"] == request_id
    assert deposits[0]["shares_issued"] == pytest.approx(100.0)
    assert db.get_portfolio("us")["nav"] > before_nav


def test_member_cannot_request_deposit_for_another_account(client):
    http, _, _, bob_account = client
    alice_headers = {"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"}

    resp = http.post(
        "/api/governance/deposit/requests",
        json={"account_id": bob_account["id"], "amount": 1_000},
        headers=alice_headers,
    )

    assert resp.status_code == 403
