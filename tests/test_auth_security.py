import importlib
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main
from api.auth import hash_password, verify_password
from db.database import Database


@pytest.fixture
def client():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    main.app.state.db = db
    test_client = TestClient(main.app)
    try:
        yield test_client
    finally:
        test_client.close()
        db_path.unlink(missing_ok=True)


def test_password_hash_uses_pbkdf2_and_verifies():
    hashed = hash_password("xirang2026")

    assert hashed.startswith("pbkdf2_sha256$")
    assert verify_password("xirang2026", hashed)
    assert not verify_password("wrong-password", hashed)


def test_legacy_sha256_password_hash_still_verifies():
    legacy = "23bd846b20ebe72d48c9463eb90f6b2d215f17991302db3dd44a226d47b143c5"

    assert verify_password("xirang2026", legacy)
    assert not verify_password("wrong-password", legacy)


def test_production_auth_requires_strong_jwt_secret(monkeypatch):
    import api.auth as auth_module

    monkeypatch.setenv("XIRANG_ENV", "production")
    monkeypatch.delenv("XIRANG_JWT_SECRET", raising=False)
    auth_module = importlib.reload(auth_module)
    with pytest.raises(RuntimeError, match="XIRANG_JWT_SECRET"):
        auth_module.validate_auth_config()

    monkeypatch.setenv("XIRANG_JWT_SECRET", "x" * 40)
    auth_module = importlib.reload(auth_module)
    auth_module.validate_auth_config()

    monkeypatch.delenv("XIRANG_ENV", raising=False)
    monkeypatch.delenv("XIRANG_JWT_SECRET", raising=False)
    importlib.reload(auth_module)


def test_initial_user_must_be_admin(client):
    response = client.post(
        "/api/admin/init-user",
        json={"username": "viewer", "password": "pw", "role": "viewer"},
    )

    assert response.status_code == 400
    assert "首次初始化只能创建 admin" in response.text


def test_initial_admin_can_be_created_from_local_request(client):
    response = client.post(
        "/api/admin/init-user",
        json={"username": "admin", "password": "pw", "role": "admin"},
    )

    assert response.status_code == 200
    assert response.json()["role"] == "admin"

    second = client.post(
        "/api/admin/init-user",
        json={"username": "admin2", "password": "pw", "role": "admin"},
    )
    assert second.status_code == 403


def test_member_can_only_see_own_account(client):
    db = main.app.state.db
    member = db.create_family_member(display_name="张三")
    own_account = db.create_capital_account(
        member_id=member["id"],
        account_name="张三主账户",
        default_portfolio_id="us",
    )
    other_member = db.create_family_member(display_name="李四")
    other_account = db.create_capital_account(
        member_id=other_member["id"],
        account_name="李四账户",
        default_portfolio_id="cn",
    )
    db.create_user(
        username="member",
        password_hash=hash_password("pw"),
        role="member",
        display_name="member",
        email="",
        member_id=member["id"],
    )
    db.create_user(
        username="outsider",
        password_hash=hash_password("pw"),
        role="member",
        display_name="outsider",
        email="",
        member_id=other_member["id"],
    )

    token = client.post("/api/auth/login", json={"username": "member", "password": "pw"}).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    my_accounts = client.get("/api/accounts/my", headers=headers)
    assert my_accounts.status_code == 200
    ids = {item["id"] for item in my_accounts.json()}
    assert own_account["id"] in ids
    assert other_account["id"] not in ids

    forbidden = client.get(f"/api/accounts/{other_account['id']}", headers=headers)
    assert forbidden.status_code == 403
