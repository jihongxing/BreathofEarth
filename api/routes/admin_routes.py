"""
管理路由 — 初始化用户、手动触发运行
"""

from fastapi import APIRouter, Depends, HTTPException

from db.database import Database
from runner.daily_runner import DailyRunner
from api.auth import hash_password
from api.deps import get_db, require_role
from api.models import InitUserRequest

router = APIRouter(prefix="/api/admin", tags=["管理"])


@router.post("/init-user")
async def init_user(req: InitUserRequest, db: Database = Depends(get_db)):
    """
    创建用户（首次部署时使用）。

    如果已存在任何 admin 用户，则需要 admin 权限才能创建新用户。
    """
    # 检查是否已有用户（首次部署时不需要认证）
    with db._conn() as conn:
        count = conn.execute("SELECT COUNT(*) as cnt FROM api_users").fetchone()["cnt"]

    if count > 0:
        # 已有用户，此端点不再可用（需要通过管理员创建）
        raise HTTPException(status_code=403, detail="系统已初始化，请联系管理员创建用户")

    if req.role not in ("admin", "member", "viewer"):
        raise HTTPException(status_code=400, detail="角色必须是 admin/member/viewer")

    db.create_user(
        username=req.username,
        password_hash=hash_password(req.password),
        role=req.role,
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
    existing = db.get_user_by_username(req.username)
    if existing:
        raise HTTPException(status_code=400, detail=f"用户 {req.username} 已存在")

    db.create_user(
        username=req.username,
        password_hash=hash_password(req.password),
        role=req.role,
        display_name=req.display_name or req.username,
        email=req.email,
    )

    db.save_audit_log("USER_CREATED", user["username"], f"创建用户 {req.username}，角色: {req.role}")
    return {"message": f"用户 {req.username} 创建成功", "role": req.role}


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
