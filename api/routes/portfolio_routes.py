"""
组合与仪表盘路由
"""

import json
from fastapi import APIRouter, Depends, HTTPException, Query

from db.database import Database
from engine.config import PORTFOLIOS, ASSETS
from api.deps import get_db, get_current_user

router = APIRouter(prefix="/api", tags=["组合"])


@router.get("/portfolio/{portfolio_id}")
async def get_portfolio(portfolio_id: str, db: Database = Depends(get_db), user: dict = Depends(get_current_user)):
    """获取指定组合状态（含三层结构）"""
    if portfolio_id not in PORTFOLIOS:
        raise HTTPException(status_code=404, detail=f"组合 {portfolio_id} 不存在")
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
