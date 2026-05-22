"""
管理路由 — 初始化用户、手动触发运行
"""

import hmac
import os

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from db.database import Database
from runner.daily_runner import DailyRunner
from api.auth import hash_password
from api.deps import get_db, require_role
from api.models import InitUserRequest, BindMemberRequest

router = APIRouter(prefix="/api/admin", tags=["管理"])


def _is_local_request(request: Request) -> bool:
    host = request.client.host if request.client else ""
    return host in {"127.0.0.1", "::1", "localhost", "testclient"}


def _has_valid_init_secret(header_value: str | None) -> bool:
    expected = os.environ.get("XIRANG_INIT_USER_SECRET", "").strip()
    if not expected:
        return False
    return bool(header_value) and hmac.compare_digest(header_value, expected)


def _validate_role(role: str):
    if role not in ("admin", "member", "viewer"):
        raise HTTPException(status_code=400, detail="角色必须是 admin/member/viewer")


@router.post("/init-user")
async def init_user(
    req: InitUserRequest,
    request: Request,
    db: Database = Depends(get_db),
    init_secret: str | None = Header(default=None, alias="X-Xirang-Init-Secret"),
):
    """
    创建用户（首次部署时使用）。

    首次初始化只允许本机请求，或携带一次性初始化密钥。
    """
    # 检查是否已有用户（首次部署时不需要认证）
    with db._conn() as conn:
        count = conn.execute("SELECT COUNT(*) as cnt FROM api_users").fetchone()["cnt"]

    if count > 0:
        # 已有用户，此端点不再可用（需要通过管理员创建）
        raise HTTPException(status_code=403, detail="系统已初始化，请联系管理员创建用户")

    if not (_is_local_request(request) or _has_valid_init_secret(init_secret)):
        raise HTTPException(
            status_code=403,
            detail="首次初始化仅允许本机请求，或携带 X-Xirang-Init-Secret 一次性密钥",
        )

    _validate_role(req.role)
    if req.role != "admin":
        raise HTTPException(status_code=400, detail="首次初始化只能创建 admin 用户")

    db.create_user(
        username=req.username,
        password_hash=hash_password(req.password),
        role=req.role,
        member_id=req.member_id,
        display_name=req.display_name or req.username,
        email=req.email,
    )

    db.save_audit_log("USER_CREATED", req.username, f"首次初始化，角色: {req.role}")
    return {"message": f"用户 {req.username} 创建成功", "role": req.role}


@router.post("/create-user")
async def create_user(
    req: InitUserRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """管理员创建新用户"""
    _validate_role(req.role)
    existing = db.get_user_by_username(req.username)
    if existing:
        raise HTTPException(status_code=400, detail=f"用户 {req.username} 已存在")

    db.create_user(
        username=req.username,
        password_hash=hash_password(req.password),
        role=req.role,
        member_id=req.member_id,
        display_name=req.display_name or req.username,
        email=req.email,
    )

    db.save_audit_log("USER_CREATED", user["username"], f"创建用户 {req.username}，角色: {req.role}")
    return {"message": f"用户 {req.username} 创建成功", "role": req.role}


@router.post("/bind-member")
async def bind_member(
    req: BindMemberRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """把登录用户绑定到家族成员。"""
    if req.member_id is not None and db.get_family_member(req.member_id) is None:
        raise HTTPException(status_code=404, detail="家族成员不存在")
    existing = db.get_user_by_username(req.username)
    if existing is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    db.bind_user_member(req.username, req.member_id)
    db.save_audit_log(
        "USER_BOUND_MEMBER",
        user["username"],
        f"{req.username} -> {req.member_id or 'null'}",
    )
    return {"message": "绑定成功", "username": req.username, "member_id": req.member_id}


@router.post("/run")
async def trigger_run(
    force: bool = False,
    portfolio: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动触发每日运行（仅管理员）"""
    try:
        runner = DailyRunner(db=db)
        if portfolio:
            result = runner.run_portfolio(portfolio, force=force)
        else:
            result = runner.run_all(force=force)

        db.save_audit_log("MANUAL_RUN", user["username"], f"force={force}, portfolio={portfolio}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health(db: Database = Depends(get_db)):
    """健康检查（无需认证）"""
    try:
        portfolio = db.get_portfolio("default")
        return {
            "status": "healthy",
            "portfolio_state": portfolio["state"],
            "nav": portfolio["nav"],
        }
    except Exception:
        return {"status": "healthy", "portfolio_state": "N/A", "nav": 0}
