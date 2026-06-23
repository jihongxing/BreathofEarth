"""
家族办公室账户路由。

Phase 1 处理成员、资产账户和授权边界。
Phase 2 开始提供账户份额资产视图。
"""

from fastapi import APIRouter, Depends, HTTPException

from api.deps import (
    ensure_account_permission,
    get_current_user,
    get_db,
    is_admin_user,
    require_role,
)
from api.models import (
    AccountPermissionRequest,
    CapitalAccountCreateRequest,
    FamilyMemberCreateRequest,
)
from db.database import Database

router = APIRouter(prefix="/api/accounts", tags=["账户"])


@router.get("/my")
async def my_accounts(
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """当前用户可见的资产账户列表。"""
    if is_admin_user(user):
        return db.list_capital_accounts()
    return db.list_user_accounts(user["id"], member_id=user.get("member_id"), permission="view")


@router.get("/admin/family-members")
async def list_family_members(
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    return db.list_family_members()


@router.post("/admin/family-members")
async def create_family_member(
    req: FamilyMemberCreateRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    member = db.create_family_member(
        display_name=req.display_name,
        member_type=req.member_type,
        risk_profile=req.risk_profile,
    )
    db.save_audit_log("FAMILY_MEMBER_CREATED", user["username"], member["id"])
    return member


@router.get("/admin/family-members/{member_id}")
async def get_family_member(
    member_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    member = db.get_family_member(member_id)
    if member is None:
        raise HTTPException(status_code=404, detail="家族成员不存在")
    return member


@router.get("/admin/capital-accounts")
async def list_capital_accounts(
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    return db.list_capital_accounts()


@router.post("/admin/capital-accounts")
async def create_capital_account(
    req: CapitalAccountCreateRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    if db.get_family_member(req.member_id) is None:
        raise HTTPException(status_code=404, detail="家族成员不存在")
    account = db.create_capital_account(
        member_id=req.member_id,
        account_name=req.account_name,
        base_currency=req.base_currency,
        default_portfolio_id=req.default_portfolio_id,
    )
    db.save_audit_log("CAPITAL_ACCOUNT_CREATED", user["username"], account["id"])
    return account


@router.get("/admin/capital-accounts/{account_id}")
async def get_capital_account(
    account_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    account = db.get_capital_account(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="资产账户不存在")
    return account


@router.post("/admin/permissions")
async def grant_account_permission(
    req: AccountPermissionRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    try:
        result = db.grant_account_permission(
            username=req.username,
            account_id=req.account_id,
            permission=req.permission,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    db.save_audit_log(
        "ACCOUNT_PERMISSION_GRANTED",
        user["username"],
        f"{req.username}:{req.account_id}:{req.permission}",
    )
    return result


@router.get("/{account_id}")
async def get_account(
    account_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看单个资产账户。"""
    return ensure_account_permission(db, user, account_id, permission="view")


@router.get("/{account_id}/asset-view")
async def get_account_asset_view(
    account_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """按份额和最近锁定净值计算账户资产。"""
    ensure_account_permission(db, user, account_id, permission="view")
    try:
        return db.get_account_asset_view(account_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
