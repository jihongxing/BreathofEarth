"""
组合与仪表盘路由
"""

import json
from fastapi import APIRouter, Depends, HTTPException, Query

from db.database import Database
from engine.config import PORTFOLIOS, ASSETS
from api.deps import ensure_portfolio_access, get_db, get_current_user, is_admin_user, require_role
from api.models import PoolRevalueRequest

router = APIRouter(prefix="/api", tags=["组合"])


def _pool_currency(portfolio_id: str) -> str:
    symbol = PORTFOLIOS.get(portfolio_id, {}).get("currency", "USD")
    return {"$": "USD", "¥": "CNY"}.get(symbol, symbol)


def _ensure_pool_from_portfolio(db: Database, portfolio_id: str):
    try:
        portfolio = db.get_portfolio(portfolio_id)
        nav = float(portfolio["nav"])
    except ValueError:
        nav = 0.0
    return db.ensure_investment_pool(
        portfolio_id,
        portfolio_id=portfolio_id,
        nav=nav,
        currency=_pool_currency(portfolio_id),
    )


@router.get("/portfolio/{portfolio_id}")
async def get_portfolio(portfolio_id: str, db: Database = Depends(get_db), user: dict = Depends(get_current_user)):
    """获取指定组合状态（含三层结构）"""
    if portfolio_id not in PORTFOLIOS:
        raise HTTPException(status_code=404, detail=f"组合 {portfolio_id} 不存在")
    ensure_portfolio_access(db, user, portfolio_id, permission="view")
    try:
        portfolio = db.get_portfolio(portfolio_id)
        positions = json.loads(portfolio["positions"])
        portfolio["positions"] = positions
        stability = float(portfolio.get("stability_balance", 0.0))
        core_sum = sum(positions)
        portfolio["stability_balance"] = stability
        portfolio["core_balance"] = round(core_sum, 2)
        pf_config = PORTFOLIOS[portfolio_id]
        portfolio["asset_names"] = pf_config["asset_names"]
        portfolio["currency"] = pf_config["currency"]
        portfolio["portfolio_name"] = pf_config["name"]
        return portfolio
    except ValueError:
        raise HTTPException(status_code=404, detail=f"组合 {portfolio_id} 未初始化")


@router.get("/portfolios")
async def list_portfolios(db: Database = Depends(get_db), user: dict = Depends(get_current_user)):
    """列出所有组合概览"""
    result = []
    for pid, pf_config in PORTFOLIOS.items():
        if user.get("role") not in {"admin", "platform_admin", "family_principal", "investment_manager"}:
            allowed = set(
                db.list_authorized_portfolio_ids(
                    user["id"],
                    member_id=user.get("member_id"),
                    permission="view",
                )
            )
            if pid not in allowed:
                continue
        try:
            p = db.get_portfolio(pid)
            result.append({
                "id": pid,
                "name": pf_config["name"],
                "currency": pf_config["currency"],
                "state": p["state"],
                "nav": p["nav"],
                "stability_balance": float(p.get("stability_balance", 0.0)),
            })
        except ValueError:
            result.append({
                "id": pid,
                "name": pf_config["name"],
                "currency": pf_config["currency"],
                "state": "UNINITIALIZED",
                "nav": 0,
            })
    return result


@router.get("/investment-pools")
async def list_investment_pools(
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """列出投资池当前份额净值。"""
    allowed = set(PORTFOLIOS.keys())
    if not is_admin_user(user):
        allowed = set(
            db.list_authorized_portfolio_ids(
                user["id"],
                member_id=user.get("member_id"),
                permission="view",
            )
        )

    pools = []
    for portfolio_id in sorted(allowed):
        if portfolio_id not in PORTFOLIOS:
            continue
        pools.append(_ensure_pool_from_portfolio(db, portfolio_id))
    return pools


@router.get("/investment-pools/{pool_id}")
async def get_investment_pool(
    pool_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看单个投资池。"""
    portfolio_id = pool_id
    if pool_id not in PORTFOLIOS:
        pool = db.get_investment_pool(pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail="投资池不存在")
        portfolio_id = pool["portfolio_id"]
    ensure_portfolio_access(db, user, portfolio_id, permission="view")
    return _ensure_pool_from_portfolio(db, portfolio_id) if pool_id == portfolio_id else db.get_investment_pool(pool_id)


@router.post("/investment-pools/{pool_id}/revalue")
async def revalue_investment_pool(
    pool_id: str,
    req: PoolRevalueRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "investment_manager")),
):
    """锁定投资池最新 NAV，并更新当前份额净值。"""
    if pool_id not in PORTFOLIOS and db.get_investment_pool(pool_id) is None:
        raise HTTPException(status_code=404, detail="投资池不存在")
    portfolio_id = pool_id
    if pool_id not in PORTFOLIOS:
        portfolio_id = db.get_investment_pool(pool_id)["portfolio_id"]
    if req.nav is None:
        try:
            nav = float(db.get_portfolio(portfolio_id)["nav"])
        except ValueError:
            raise HTTPException(status_code=404, detail=f"组合 {portfolio_id} 未初始化")
    else:
        nav = req.nav
    pool = db.revalue_investment_pool(
        pool_id=pool_id,
        nav=nav,
        actor=user["username"],
        source=req.source,
        snapshot_date=req.snapshot_date,
    )
    db.save_audit_log(
        "NAV_REVALUED",
        user["username"],
        f"投资池 {pool_id} NAV={nav:,.2f}, share_price={pool['share_price']}",
    )
    return pool


@router.get("/admin/aum")
async def get_family_aum(
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "family_principal", "investment_manager")),
):
    """管理端全局 AUM。"""
    for portfolio_id in PORTFOLIOS:
        _ensure_pool_from_portfolio(db, portfolio_id)
    return db.get_family_aum_summary()
