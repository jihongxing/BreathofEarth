"""
息壤（Xi-Rang）API 服务（可选）

纯查询接口，不负责定时调度。定时运行由系统 cron 驱动。
启动方式：python -m uvicorn main:app --host 0.0.0.0 --port 8000
"""

import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from db.database import Database
from runner.daily_runner import DailyRunner


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    app.state.db = db
    print("息壤 API 已启动")
    yield
    print("息壤 API 已关闭")


app = FastAPI(
    title="息壤 Xi-Rang",
    description="状态机驱动的自动化资产配置系统",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {"name": "息壤 Xi-Rang", "version": "0.2.0", "status": "running"}


@app.get("/api/portfolio")
async def get_portfolio():
    """查看当前组合状态"""
    db: Database = app.state.db
    portfolio = db.get_portfolio()
    portfolio["positions"] = json.loads(portfolio["positions"])
    return portfolio


@app.get("/api/snapshots")
async def get_snapshots(limit: int = 30):
    """查看历史快照"""
    db: Database = app.state.db
    snapshots = db.get_snapshots(limit=limit)
    for s in snapshots:
        s["positions"] = json.loads(s["positions"])
        s["weights"] = json.loads(s["weights"])
    return snapshots


@app.post("/api/run")
async def trigger_run(force: bool = False):
    """手动触发一次每日运行"""
    try:
        runner = DailyRunner()
        report = runner.run(force=force)
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/risk")
async def get_risk_status():
    """查看最近的风控事件"""
    db: Database = app.state.db
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM risk_events ORDER BY date DESC LIMIT 10"
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/transactions")
async def get_transactions(limit: int = 20):
    """查看交易记录"""
    db: Database = app.state.db
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions ORDER BY date DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/health")
async def health():
    """健康检查"""
    db: Database = app.state.db
    portfolio = db.get_portfolio()
    return {
        "status": "healthy",
        "portfolio_state": portfolio["state"],
        "nav": portfolio["nav"],
    }
