"""
API 依赖注入
"""

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from db.database import Database
from api.auth import decode_token, security


ADMIN_ROLES = {"admin", "platform_admin", "family_principal", "investment_manager"}


def get_db(request: Request) -> Database:
    return request.app.state.db


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Database = Depends(get_db),
) -> dict:
    """从 JWT 获取当前用户"""
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未登录")

    payload = decode_token(credentials.credentials)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token 无效或已过期")

    user = db.get_user_by_username(payload.get("sub", ""))
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户不存在")

    return user


def is_admin_user(user: dict) -> bool:
    return user.get("role") in ADMIN_ROLES


def require_role(*roles: str):
    """角色权限装饰器"""
    async def checker(user: dict = Depends(get_current_user)):
        if user["role"] not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="权限不足")
        return user
    return checker


def ensure_account_permission(
    db: Database,
    user: dict,
    account_id: str,
    permission: str = "view",
) -> dict:
    """确保当前用户有指定资产账户权限，管理员默认放行。"""
    account = db.get_capital_account(account_id)
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="资产账户不存在")
    if is_admin_user(user):
        return account
    if user.get("member_id") and account.get("member_id") == user.get("member_id"):
        return account
    if not db.user_has_account_permission(user["id"], account_id, permission):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权访问该资产账户")
    return account


def ensure_portfolio_access(
    db: Database,
    user: dict,
    portfolio_id: str,
    permission: str = "view",
):
    """确保当前用户可访问某个投资组合/资金池视图。"""
    if is_admin_user(user):
        return
    allowed = set(
        db.list_authorized_portfolio_ids(
            user["id"],
            member_id=user.get("member_id"),
            permission=permission,
        )
    )
    if portfolio_id not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权访问该组合")


def get_accessible_portfolio_ids(
    db: Database,
    user: dict,
    permission: str = "view",
) -> set[str]:
    """返回当前用户可访问的组合 ID 集合，管理员返回全部。"""
    if is_admin_user(user):
        from engine.config import PORTFOLIOS
        return set(PORTFOLIOS.keys())
    return set(
        db.list_authorized_portfolio_ids(
            user["id"],
            member_id=user.get("member_id"),
            permission=permission,
        )
    )
