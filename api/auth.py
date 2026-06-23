"""
息壤 API 认证模块（JWT）

角色：
- admin: 可触发运行、发起出金
- member: 可审批出金、查看所有数据
- viewer: 只读访问（家族看板）
"""

import os
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import jwt as pyjwt

from db.database import Database

SECRET_KEY = os.environ.get("XIRANG_JWT_SECRET", "xirang-dev-secret-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24
PBKDF2_ITERATIONS = 210_000

security = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PBKDF2_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${digest}"


def verify_password(plain: str, hashed: str) -> bool:
    if hashed.startswith("pbkdf2_sha256$"):
        try:
            _, iterations, salt, expected = hashed.split("$", 3)
            digest = hashlib.pbkdf2_hmac(
                "sha256",
                plain.encode("utf-8"),
                salt.encode("utf-8"),
                int(iterations),
            ).hex()
            return hmac.compare_digest(digest, expected)
        except (ValueError, TypeError):
            return False

    # Backward compatibility for existing users created with the legacy
    # unsalted SHA-256 format.
    legacy = hashlib.sha256(plain.encode()).hexdigest()
    return hmac.compare_digest(legacy, hashed)


def validate_auth_config():
    require_strong = os.environ.get("XIRANG_REQUIRE_STRONG_AUTH", "").lower() in {"1", "true", "yes"}
    env_name = os.environ.get("XIRANG_ENV", "").lower()
    if env_name in {"prod", "production"}:
        require_strong = True

    if not require_strong:
        return

    if SECRET_KEY == "xirang-dev-secret-change-in-production" or len(SECRET_KEY) < 32:
        raise RuntimeError(
            "生产环境必须设置强 XIRANG_JWT_SECRET，且长度至少 32 个字符"
        )


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode["exp"] = expire
    return pyjwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        return pyjwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except pyjwt.PyJWTError:
        return None
