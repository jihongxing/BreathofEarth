"""
Alpha 沙盒策略路由

管理策略的启用/禁用、查看状态、手动触发、竞技场评估。
"""

import uuid
from datetime import datetime

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


class AlphaLedgerAdjustRequest(BaseModel):
    amount: float
    note: str = Field(default="", max_length=300)


class AlphaLedgerEntryRequest(BaseModel):
    direction: str = Field(pattern="^(IN|OUT)$")
    amount: float = Field(gt=0)
    note: str = Field(default="", max_length=300)
    external_reference: str = Field(default="", max_length=120)
    related_request_id: str = Field(default="", max_length=64)


class AlphaLedgerWithdrawalRequest(BaseModel):
    amount: float
    reason: str = Field(default="", max_length=300)


class AlphaLedgerWithdrawalStatusRequest(BaseModel):
    status: str = Field(pattern="^(HANDLED|REJECTED|CANCELLED)$")
    note: str = Field(default="", max_length=300)
    external_reference: str = Field(default="", max_length=120)


@router.get("/ledger")
async def get_alpha_ledger(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看 Alpha 独立资金账本"""
    account = db.get_alpha_account(portfolio_id)
    strategies = db.list_strategies(portfolio_id)
    balance = float(account.get("cash_balance", 0.0))
    return {
        "portfolio_id": portfolio_id,
        "cash_balance": round(balance, 2),
        "allocation_base": "alpha_ledger",
        "total_inflows": round(float(account.get("total_inflows", 0.0)), 2),
        "total_outflows": round(float(account.get("total_outflows", 0.0)), 2),
        "last_manual_adjustment": account.get("last_manual_adjustment"),
        "strategies": [
            {
                "id": s["id"],
                "status": s["status"],
                "allocation_pct": s.get("allocation_pct", 0),
                "allocated_capital": round(balance * float(s.get("allocation_pct", 0)), 2),
                "capital_source": "alpha_ledger",
            }
            for s in strategies
        ],
    }


@router.post("/ledger/fund")
async def fund_alpha_ledger(
    req: AlphaLedgerAdjustRequest,
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手工向 Alpha 独立账本注入实验资金"""
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="入账金额必须大于 0")

    entry, account = db.record_alpha_ledger_entry(
        portfolio_id=portfolio_id,
        direction="IN",
        amount=req.amount,
        actor=user["username"],
        note=req.note or f"Alpha 入账 +{req.amount:.2f}",
    )
    db.save_audit_log(
        "ALPHA_LEDGER_FUND",
        user["username"],
        f"组合 {portfolio_id} Alpha 账本入账 ${req.amount:,.2f}" + (f"，备注: {req.note}" if req.note else ""),
    )
    return {
        "message": f"组合 {portfolio_id} Alpha 账本入账成功",
        "portfolio_id": portfolio_id,
        "cash_balance": round(float(account["cash_balance"]), 2),
        "entry": entry,
    }


@router.get("/ledger/entries")
async def list_alpha_ledger_entries(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    direction: Optional[str] = Query(default=None, description="IN 或 OUT"),
    limit: int = Query(default=20, ge=1, le=100),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看 Alpha 账本人工记账流水"""
    return db.list_alpha_ledger_entries(portfolio_id=portfolio_id, direction=direction, limit=limit)


@router.post("/ledger/entries")
async def create_alpha_ledger_entry(
    req: AlphaLedgerEntryRequest,
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """登记 Alpha 人工入账/出账流水。仅用于线下资金动作补记。"""
    try:
        entry, account = db.record_alpha_ledger_entry(
            portfolio_id=portfolio_id,
            direction=req.direction,
            amount=req.amount,
            actor=user["username"],
            note=req.note,
            external_reference=req.external_reference,
            related_request_id=req.related_request_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    action = "ALPHA_LEDGER_ENTRY_IN" if req.direction == "IN" else "ALPHA_LEDGER_ENTRY_OUT"
    db.save_audit_log(
        action,
        user["username"],
        f"组合 {portfolio_id} Alpha 人工{'入账' if req.direction == 'IN' else '出账'} ${req.amount:,.2f}"
        + (f"，备注: {req.note}" if req.note else "")
        + (f"，外部流水: {req.external_reference}" if req.external_reference else "")
        + (f"，关联申请: {req.related_request_id}" if req.related_request_id else ""),
    )
    return {
        "message": f"组合 {portfolio_id} Alpha 人工{'入账' if req.direction == 'IN' else '出账'}已登记",
        "portfolio_id": portfolio_id,
        "cash_balance": round(float(account["cash_balance"]), 2),
        "entry": entry,
        "execution_mode": "manual_bookkeeping_only",
    }


@router.post("/ledger/withdraw")
async def withdraw_alpha_ledger(
    req: AlphaLedgerWithdrawalRequest,
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """发起 Alpha 账本出金申请。系统只登记请求，不自动执行扣账。"""
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="申请金额必须大于 0")

    account = db.get_alpha_account(portfolio_id)
    if float(account.get("cash_balance", 0.0)) < req.amount:
        raise HTTPException(status_code=400, detail="Alpha 账本余额不足，不能发起超额出金申请")

    request_id = str(uuid.uuid4())[:8]
    reason = req.reason or f"Alpha 出金申请 ${req.amount:.2f}"
    db.create_alpha_withdrawal_request(
        request_id=request_id,
        portfolio_id=portfolio_id,
        amount=req.amount,
        reason=reason,
        requester=user["username"],
    )

    db.save_audit_log(
        "ALPHA_LEDGER_WITHDRAW_REQUEST",
        user["username"],
        f"组合 {portfolio_id} Alpha 账本出金申请 ${req.amount:,.2f}" + (f"，原因: {reason}" if reason else ""),
    )
    return {
        "message": f"组合 {portfolio_id} Alpha 出金申请已登记，系统不会自动扣减账本，请线下人工处理",
        "request_id": request_id,
        "portfolio_id": portfolio_id,
        "amount": round(req.amount, 2),
        "cash_balance": round(float(account["cash_balance"]), 2),
        "status": "PENDING_MANUAL",
        "execution_mode": "manual_only",
    }


@router.get("/ledger/withdrawals")
async def list_alpha_withdrawal_requests(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    status: Optional[str] = Query(default=None, description="按状态筛选"),
    limit: int = Query(default=20, ge=1, le=100),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看 Alpha 账本出金申请列表"""
    return db.list_alpha_withdrawal_requests(portfolio_id=portfolio_id, status=status, limit=limit)


@router.get("/ledger/withdrawals/{request_id}")
async def get_alpha_withdrawal_request(
    request_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看单个 Alpha 账本出金申请"""
    request = db.get_alpha_withdrawal_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Alpha 出金申请不存在")
    return request


@router.post("/ledger/withdrawals/{request_id}/status")
async def update_alpha_withdrawal_request_status(
    request_id: str,
    req: AlphaLedgerWithdrawalStatusRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """回填 Alpha 出金申请的人工处理结果，不执行任何自动扣账。"""
    request = db.get_alpha_withdrawal_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Alpha 出金申请不存在")

    if request["status"] != "PENDING_MANUAL":
        raise HTTPException(status_code=400, detail=f"当前状态为 {request['status']}，不能重复回填")

    db.update_alpha_withdrawal_request(
        request_id,
        status=req.status,
        handled_by=user["username"],
        handled_note=req.note,
        external_reference=req.external_reference,
        handled_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    db.save_audit_log(
        f"ALPHA_LEDGER_WITHDRAW_{req.status}",
        user["username"],
        f"组合 {request['portfolio_id']} Alpha 出金申请 #{request_id} 已回填为 {req.status}"
        + (f"，备注: {req.note}" if req.note else "")
        + (f"，外部流水: {req.external_reference}" if req.external_reference else ""),
    )

    updated = db.get_alpha_withdrawal_request(request_id)
    return {
        "message": "Alpha 出金申请状态已回填，系统未执行任何自动扣账",
        "request": updated,
        "execution_mode": "manual_only",
    }


# ── 策略列表 ──────────────────────────────────────────

@router.get("/strategies")
async def get_strategies(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取所有策略状态（含可用未注册的）"""
    alpha_account = db.get_alpha_account(portfolio_id)
    alpha_balance = float(alpha_account.get("cash_balance", 0.0))

    for sid, cls in REGISTRY.items():
        instance = cls(db)
        instance.ensure_registered(portfolio_id)

    available = list_available_strategies()
    db_strategies = db.list_strategies(portfolio_id)
    db_map = {s["id"]: s for s in db_strategies}

    result = []
    for a in available:
        db_info = db_map.get(a["id"], {})
        allocation_pct = db_info.get("allocation_pct", a["default_allocation"])
        result.append({
            **a,
            "status": db_info.get("status", "DISABLED"),
            "allocation_pct": allocation_pct,
            "capital": round(alpha_balance * float(allocation_pct), 2),
            "alpha_balance": round(alpha_balance, 2),
            "capital_source": "alpha_ledger",
            "total_premium": db_info.get("total_premium", 0),
            "total_pnl": db_info.get("total_pnl", 0),
            "trade_count": db_info.get("trade_count", 0),
            "enabled_at": db_info.get("enabled_at"),
            "disabled_at": db_info.get("disabled_at"),
            "formal_metrics_included": bool(a.get("formal_reporting_eligible")),
        })
    return result


# ── 启用/禁用策略 ─────────────────────────────────────

@router.post("/strategies/{strategy_id}/toggle")
async def toggle_strategy(
    strategy_id: str,
    req: StrategyToggleRequest,
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动启用/禁用策略（仅 admin）"""
    cls = get_strategy_class(strategy_id)
    if not cls:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    instance = cls(db)
    instance.ensure_registered(portfolio_id)

    new_status = "ENABLED" if req.action == "enable" else "DISABLED"
    db.update_strategy_status(strategy_id, new_status, portfolio_id=portfolio_id)

    if req.allocation_pct is not None:
        db.upsert_strategy(strategy_id, portfolio_id=portfolio_id, allocation_pct=req.allocation_pct)

    db.save_audit_log(
        f"ALPHA_{new_status}", user["username"],
        f"组合 {portfolio_id} 的策略 {strategy_id} 已{'启用' if new_status == 'ENABLED' else '禁用'}"
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
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动触发策略执行（仅 admin，策略必须 ENABLED）"""
    cls = get_strategy_class(strategy_id)
    if not cls:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    instance = cls(db)
    if not instance.is_enabled(portfolio_id):
        raise HTTPException(status_code=400, detail=f"策略 {strategy_id} 未启用，请先启用")

    if spy_price is None:
        spy_price = 450.0

    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")

    result = instance.run(
        portfolio_id=portfolio_id,
        current_date=current_date,
        spy_price=spy_price,
    )

    db.save_audit_log(
        "ALPHA_RUN", user["username"],
        f"手动触发组合 {portfolio_id} 的策略 {strategy_id}: {result.get('action', 'N/A')}",
    )

    return result


# ── 策略交易记录 ──────────────────────────────────────

@router.get("/strategies/{strategy_id}/transactions")
async def get_strategy_transactions(
    strategy_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看策略交易记录"""
    return db.get_alpha_transactions(strategy_id, portfolio_id=portfolio_id, limit=limit)


# ── 策略详情 ──────────────────────────────────────────

@router.get("/strategies/{strategy_id}")
async def get_strategy_detail(
    strategy_id: str,
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看策略详情（含近期交易）"""
    cls = get_strategy_class(strategy_id)
    if not cls:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    strategy = db.get_strategy(strategy_id, portfolio_id=portfolio_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    alpha_account = db.get_alpha_account(portfolio_id)
    alpha_balance = float(alpha_account.get("cash_balance", 0.0))
    transactions = db.get_alpha_transactions(strategy_id, portfolio_id=portfolio_id, limit=10)
    return {
        **strategy,
        "capital": round(alpha_balance * float(strategy.get("allocation_pct", 0)), 2),
        "alpha_balance": round(alpha_balance, 2),
        "capital_source": "alpha_ledger",
        "formal_reporting_eligible": bool(cls.FORMAL_REPORTING_ELIGIBLE),
        "reporting_scope": cls.get_reporting_scope(),
        "reporting_note": cls.REPORTING_NOTE,
        "recent_transactions": transactions,
    }


# ── 竞技场：排行榜 ────────────────────────────────────

@router.get("/arena/leaderboard")
async def get_leaderboard(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """策略排行榜（按夏普比率排序）"""
    arena = StrategyArena(db)
    return arena.get_leaderboard(portfolio_id=portfolio_id)


# ── 竞技场：季度评估 ──────────────────────────────────

@router.post("/arena/evaluate")
async def run_evaluation(
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """手动触发季度评估（仅 admin）"""
    arena = StrategyArena(db)
    report = arena.quarterly_evaluation(portfolio_id=portfolio_id)
    return report


# ── 竞技场：运行所有策略 ──────────────────────────────

@router.post("/arena/run-all")
async def run_all_strategies(
    spy_price: float = Query(default=None, description="SPY 当前价格"),
    portfolio_id: str = Query(default="us", description="Alpha 实验仓所属组合"),
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """运行所有已启用的策略（仅 admin）"""
    if spy_price is None:
        spy_price = 450.0

    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")

    arena = StrategyArena(db)
    results = arena.run_all(
        portfolio_id=portfolio_id,
        current_date=current_date,
        spy_price=spy_price,
    )

    db.save_audit_log(
        "ARENA_RUN_ALL", user["username"],
        f"批量运行组合 {portfolio_id} 的 {len(results)} 个策略",
    )

    return {"strategies_run": len(results), "results": results}
