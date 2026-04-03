"""
Alpha 沙盒策略路由

管理策略的启用/禁用、查看状态、手动触发、竞技场评估。
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional

from db.database import Database
from engine.alpha.registry import get_strategy_class, list_available_strategies, REGISTRY
from engine.alpha.arena import StrategyArena
from api.deps import get_db, get_current_user, require_role

router = APIRouter(prefix="/api/alpha", tags=["Alpha 沙盒"])


class StrategyToggleRequest(BaseModel):
    action: str = Field(pattern="^(enable|disable)$")
    allocation_pct: Optional[float] = Field(default=None, ge=0.01, le=0.50)


# ── 策略列表 ──────────────────────────────────────────

@router.get("/strategies")
async def get_strategies(
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取所有策略状态（含可用未注册的）"""
    for sid, cls in REGISTRY.items():
        instance = cls(db)
        instance.ensure_registered()

    available = list_available_strategies()
    db_strategies = db.list_strategies()
    db_map = {s["id"]: s for s in db_strategies}

    result = []
    for a in available:
        db_info = db_map.get(a["id"], {})
        result.append({
            **a,
            "status": db_info.get("status", "DISABLED"),
            "allocation_pct": db_info.get("allocation_pct", a["default_allocation"]),
            "capital": db_info.get("capital", 0),
            "total_premium": db_info.get("total_premium", 0),
            "total_pnl": db_info.get("total_pnl", 0),
            "trade_count": db_info.get("trade_count", 0),
            "enabled_at": db_info.get("enabled_at"),
            "disabled_at": db_info.get("disabled_at"),
        })
    return result


# ── 启用/禁用策略 ─────────────────────────────────────

@router.post("/strategies/{strategy_id}/toggle")
async def toggle_strategy(
    strategy_id: str,
    req: StrategyToggleRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动启用/禁用策略（仅 admin）"""
    cls = get_strategy_class(strategy_id)
    if not cls:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    instance = cls(db)
    instance.ensure_registered()

    new_status = "ENABLED" if req.action == "enable" else "DISABLED"
    db.update_strategy_status(strategy_id, new_status)

    if req.allocation_pct is not None:
        db.upsert_strategy(strategy_id, allocation_pct=req.allocation_pct)

    db.save_audit_log(
        f"ALPHA_{new_status}", user["username"],
        f"策略 {strategy_id} 已{'启用' if new_status == 'ENABLED' else '禁用'}"
        + (f"，分配 {req.allocation_pct:.0%}" if req.allocation_pct else ""),
    )

    return {
        "strategy_id": strategy_id,
        "status": new_status,
        "message": f"策略{'已启用' if new_status == 'ENABLED' else '已禁用'}",
    }


# ── 手动触发策略 ──────────────────────────────────────

@router.post("/strategies/{strategy_id}/run")
async def run_strategy(
    strategy_id: str,
    spy_price: float = Query(default=None, description="SPY 当前价格"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动触发策略执行（仅 admin，策略必须 ENABLED）"""
    cls = get_strategy_class(strategy_id)
    if not cls:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    instance = cls(db)
    if not instance.is_enabled():
        raise HTTPException(status_code=400, detail=f"策略 {strategy_id} 未启用，请先启用")

    if spy_price is None:
        spy_price = 450.0

    try:
        portfolio = db.get_portfolio("us")
        nav = portfolio["nav"]
    except (ValueError, KeyError):
        nav = 1_000_000

    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")

    result = instance.run(
        portfolio_id="us",
        current_date=current_date,
        spy_price=spy_price,
        nav=nav,
    )

    db.save_audit_log(
        "ALPHA_RUN", user["username"],
        f"手动触发策略 {strategy_id}: {result.get('action', 'N/A')}",
    )

    return result


# ── 策略交易记录 ──────────────────────────────────────

@router.get("/strategies/{strategy_id}/transactions")
async def get_strategy_transactions(
    strategy_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看策略交易记录"""
    return db.get_alpha_transactions(strategy_id, limit=limit)


# ── 策略详情 ──────────────────────────────────────────

@router.get("/strategies/{strategy_id}")
async def get_strategy_detail(
    strategy_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看策略详情（含近期交易）"""
    strategy = db.get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    transactions = db.get_alpha_transactions(strategy_id, limit=10)
    return {
        **strategy,
        "recent_transactions": transactions,
    }


# ── 竞技场：排行榜 ────────────────────────────────────

@router.get("/arena/leaderboard")
async def get_leaderboard(
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """策略排行榜（按夏普比率排序）"""
    arena = StrategyArena(db)
    return arena.get_leaderboard()


# ── 竞技场：季度评估 ──────────────────────────────────

@router.post("/arena/evaluate")
async def run_evaluation(
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动触发季度评估（仅 admin）"""
    arena = StrategyArena(db)
    report = arena.quarterly_evaluation()
    return report


# ── 竞技场：运行所有策略 ──────────────────────────────

@router.post("/arena/run-all")
async def run_all_strategies(
    spy_price: float = Query(default=None, description="SPY 当前价格"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """运行所有已启用的策略（仅 admin）"""
    if spy_price is None:
        spy_price = 450.0

    try:
        portfolio = db.get_portfolio("us")
        nav = portfolio["nav"]
    except (ValueError, KeyError):
        nav = 1_000_000

    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")

    arena = StrategyArena(db)
    results = arena.run_all(
        portfolio_id="us",
        current_date=current_date,
        spy_price=spy_price,
        nav=nav,
    )

    db.save_audit_log(
        "ARENA_RUN_ALL", user["username"],
        f"批量运行 {len(results)} 个策略",
    )

    return {"strategies_run": len(results), "results": results}
