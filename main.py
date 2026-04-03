"""
息壤（Xi-Rang）API 服务

FastAPI 后端，提供：
- JWT 认证（admin / member / viewer 三级权限）
- 组合状态与仪表盘查询
- 出金治理（多签 + 冷却期）
- 审计日志
- 手动触发运行

启动方式：
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from db.database import Database
from api.routes.auth_routes import router as auth_router
from api.routes.portfolio_routes import router as portfolio_router
from api.routes.dashboard_routes import router as dashboard_router
from api.routes.governance_routes import router as governance_router
from api.routes.admin_routes import router as admin_router
from api.routes.alpha_routes import router as alpha_router
from api.routes.data_routes import router as data_router
from api.routes.report_routes import router as report_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    app.state.db = db

    # 启动数据自动更新调度器
    from data.scheduler import DataScheduler
    scheduler = DataScheduler()
    scheduler.start()
    app.state.scheduler = scheduler

    print("息壤 API 已启动")
    yield

    scheduler.stop()
    print("息壤 API 已关闭")


app = FastAPI(
    title="息壤 Xi-Rang",
    description="家族财富自动化配置系统 — API 服务",
    version="0.3.0",
    lifespan=lifespan,
)

# CORS（前端开发用）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(auth_router)
app.include_router(portfolio_router)
app.include_router(dashboard_router)
app.include_router(governance_router)
app.include_router(admin_router)
app.include_router(alpha_router)
app.include_router(data_router)
app.include_router(report_router)

# 静态文件（前端）
FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# 月报文件
REPORTS_DIR = Path(__file__).parent / "frontend" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")


@app.get("/")
async def root():
    """根路径：如果有前端则返回页面，否则返回 API 信息"""
    index_file = Path(__file__).parent / "frontend" / "index.html"
    if index_file.exists():
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=index_file.read_text(encoding="utf-8"))
    return {
        "name": "息壤 Xi-Rang",
        "version": "0.3.0",
        "status": "running",
        "docs": "/docs",
    }
