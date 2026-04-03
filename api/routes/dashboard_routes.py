"""
仪表盘路由 — 聚合数据供前端渲染
"""

import json
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi import status

from db.database import Database
from engine.config import PORTFOLIOS
from api.deps import get_db, get_current_user

router = APIRouter(prefix="/api", tags=["仪表盘"])


@router.get("/dashboard/{portfolio_id}")
async def get_dashboard(
    portfolio_id: str,
    days: int = Query(default=90, ge=7, le=365),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取仪表盘数据（NAV 曲线、权重、回撤、调仓）"""
    pf_config = PORTFOLIOS.get(portfolio_id)
    if not pf_config:
        return {"error": f"组合 {portfolio_id} 不存在"}

    # 快照（NAV 曲线 + 回撤 + 权重）
    snapshots = db.get_snapshots(portfolio_id, limit=days)
    snapshots.reverse()  # 按时间正序

    nav_series = []
    drawdown_series = []
    for s in snapshots:
        nav_series.append({"date": s["date"], "nav": s["nav"]})
        drawdown_series.append({"date": s["date"], "drawdown": s["drawdown"]})

    # 当前权重
    current_weights = {}
    if snapshots:
        latest = snapshots[-1]
        weights = json.loads(latest["weights"]) if isinstance(latest["weights"], str) else latest["weights"]
        assets = pf_config["assets"]
        current_weights = {
            assets[i]: {"weight": weights[i], "name": pf_config["asset_names"][assets[i]]}
            for i in range(min(len(assets), len(weights)))
        }

    # 交易记录
    with db._conn() as conn:
        tx_rows = conn.execute(
            """SELECT date, type, turnover, friction_cost, reason
               FROM transactions WHERE portfolio_id = ?
               ORDER BY date DESC LIMIT ?""",
            (portfolio_id, 20),
        ).fetchall()
    transactions = [dict(r) for r in tx_rows]

    # 风控事件
    with db._conn() as conn:
        risk_rows = conn.execute(
            """SELECT date, event_type, severity, drawdown, action_taken
               FROM risk_events WHERE portfolio_id = ?
               ORDER BY date DESC LIMIT 10""",
            (portfolio_id,),
        ).fetchall()
    risk_events = [dict(r) for r in risk_rows]

    # 汇总
    try:
        portfolio = db.get_portfolio(portfolio_id)
        current_nav = portfolio["nav"]
        state = portfolio["state"]
        stability_balance = float(portfolio.get("stability_balance", 0.0))
    except ValueError:
        current_nav = 0
        state = "UNINITIALIZED"
        stability_balance = 0.0

    return {
        "portfolio_id": portfolio_id,
        "name": pf_config["name"],
        "currency": pf_config["currency"],
        "state": state,
        "current_nav": current_nav,
        "stability_balance": stability_balance,
        "core_balance": round(current_nav - stability_balance, 2),
        "nav_series": nav_series,
        "drawdown_series": drawdown_series,
        "current_weights": current_weights,
        "transactions": transactions,
        "risk_events": risk_events,
    }


@router.get("/snapshots/{portfolio_id}")
async def get_snapshots(
    portfolio_id: str,
    limit: int = Query(default=30, ge=1, le=365),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取历史快照"""
    snapshots = db.get_snapshots(portfolio_id, limit=limit)
    for s in snapshots:
        s["positions"] = json.loads(s["positions"]) if isinstance(s["positions"], str) else s["positions"]
        s["weights"] = json.loads(s["weights"]) if isinstance(s["weights"], str) else s["weights"]
    return snapshots


@router.get("/transactions/{portfolio_id}")
async def get_transactions(
    portfolio_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取交易记录"""
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE portfolio_id = ? ORDER BY date DESC LIMIT ?",
            (portfolio_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/report")
async def get_monthly_report(
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """获取家族月报（HTML 格式，向后兼容）"""
    from fastapi.responses import HTMLResponse
    from runner.dashboard import get_latest_report_html
    html = get_latest_report_html(lang=lang)
    return HTMLResponse(content=html)


@router.get("/reports")
async def list_reports(
    user: dict = Depends(get_current_user),
):
    """列出所有月报（按年月聚合，含组合信息）"""
    from runner.dashboard import list_reports as _list_reports
    return _list_reports()


@router.get("/reports/{year}/{month}/{portfolio_id}")
async def get_portfolio_report(
    year: int,
    month: int,
    portfolio_id: str,
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """获取指定月份、指定组合的月报 HTML"""
    from fastapi.responses import HTMLResponse
    from runner.dashboard import get_report_html
    html = get_report_html(year, month, portfolio_id, lang=lang)
    return HTMLResponse(content=html)


@router.post("/report/generate")
async def generate_report(
    days: int = Query(default=90, ge=7, le=365),
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """手动生成月报（所有组合）"""
    from runner.dashboard import generate_and_save
    push = False
    path = generate_and_save(days=days, push=push, lang=lang)
    msg = "报告已生成" if lang == "zh" else "Report generated"
    return {"message": msg, "path": str(path) if path else None}


@router.post("/reports/generate/{portfolio_id}")
async def generate_portfolio_report_api(
    portfolio_id: str,
    days: int = Query(default=90, ge=7, le=365),
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """为指定组合生成月报"""
    from runner.dashboard import generate_and_save
    from engine.config import PORTFOLIOS
    if portfolio_id not in PORTFOLIOS:
        raise HTTPException(status_code=404, detail=f"组合不存在: {portfolio_id}")
    path = generate_and_save(days=days, portfolio_id=portfolio_id, push=False, lang=lang)
    msg = f"{portfolio_id} 报告已生成" if lang == "zh" else f"{portfolio_id} report generated"
    return {"message": msg, "path": str(path) if path else None}
