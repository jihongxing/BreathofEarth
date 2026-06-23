import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main
from api.auth import hash_password
from db.database import Database


@pytest.fixture
def client():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
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
    main.app.state.db = db
    test_client = TestClient(main.app)
    try:
        yield test_client, db
    finally:
        test_client.close()
        db_path.unlink(missing_ok=True)


def _login(client, username, password):
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def test_admin_can_create_member_account_and_bind_user(client):
    http, db = client
    token = _login(http, "admin", "admin-pass")
    headers = {"Authorization": f"Bearer {token}"}

    member_resp = http.post(
        "/api/accounts/admin/family-members",
        json={"display_name": "Alice Family", "member_type": "individual", "risk_profile": "balanced"},
        headers=headers,
    )
    assert member_resp.status_code == 200, member_resp.text
    member_id = member_resp.json()["id"]

    account_resp = http.post(
        "/api/accounts/admin/capital-accounts",
        json={"member_id": member_id, "account_name": "Alice Main"},
        headers=headers,
    )
    assert account_resp.status_code == 200, account_resp.text
    account_id = account_resp.json()["id"]

    bind_resp = http.post(
        "/api/admin/bind-member",
        json={"username": "alice", "member_id": member_id},
        headers=headers,
    )
    assert bind_resp.status_code == 200, bind_resp.text

    perm_resp = http.post(
        "/api/accounts/admin/permissions",
        json={"username": "alice", "account_id": account_id, "permission": "view"},
        headers=headers,
    )
    assert perm_resp.status_code == 200, perm_resp.text

    me_resp = http.get("/api/auth/me", headers={"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"})
    assert me_resp.status_code == 200
    assert me_resp.json()["member_id"] == member_id

    my_accounts = http.get("/api/accounts/my", headers={"Authorization": f"Bearer {_login(http, 'alice', 'alice-pass')}"})
    assert my_accounts.status_code == 200, my_accounts.text
    accounts = my_accounts.json()
    assert len(accounts) == 1
    assert accounts[0]["id"] == account_id


def test_member_cannot_access_unassigned_portfolio(client):
    http, db = client
    token = _login(http, "alice", "alice-pass")
    headers = {"Authorization": f"Bearer {token}"}

    resp = http.get("/api/portfolio/us", headers=headers)
    assert resp.status_code == 403


def test_member_with_permission_can_access_portfolio(client):
    http, db = client
    admin_token = _login(http, "admin", "admin-pass")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    member_resp = http.post(
        "/api/accounts/admin/family-members",
        json={"display_name": "Alice Family", "member_type": "individual", "risk_profile": "balanced"},
        headers=admin_headers,
    )
    member_id = member_resp.json()["id"]
    http.post(
        "/api/admin/bind-member",
        json={"username": "alice", "member_id": member_id},
        headers=admin_headers,
    )
    account_resp = http.post(
        "/api/accounts/admin/capital-accounts",
        json={"member_id": member_id, "account_name": "Alice Main", "default_portfolio_id": "us"},
        headers=admin_headers,
    )
    account_id = account_resp.json()["id"]
    http.post(
        "/api/accounts/admin/permissions",
        json={"username": "alice", "account_id": account_id, "permission": "view"},
        headers=admin_headers,
    )

    member_token = _login(http, "alice", "alice-pass")
    resp = http.get("/api/portfolio/us", headers={"Authorization": f"Bearer {member_token}"})
    assert resp.status_code == 200, resp.text
