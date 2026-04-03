"""
息壤 — 家��月报 API 路由
"""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse

from api.deps import get_current_user
from runner.dashboard import list_reports, get_report_html, generate_and_save

router = APIRouter(prefix="/api", tags=["reports"])


@router.get("/reports")
async def get_reports(user=Depends(get_current_user)):
    """返回按年月聚合的月报列表。"""
    return list_reports()


@router.get("/reports/{year}/{month}/{portfolio_id}")
async def get_report(
    year: int,
    month: int,
    portfolio_id: str,
    lang: str = Query("zh"),
    user=Depends(get_current_user),
):
    """获取指定月份、组合的月报 HTML。"""
    html = get_report_html(year, month, portfolio_id, lang=lang)
    return HTMLResponse(content=html)


@router.post("/report/generate")
async def generate_report(
    lang: str = Query("zh"),
    user=Depends(get_current_user),
):
    """生成最新月报。"""
    path = generate_and_save(days=90, lang=lang)
    if path:
        return {"message": "报告已生成" if lang == "zh" else "Report generated", "path": str(path)}
    return JSONResponse(
        status_code=500,
        content={"detail": "生成失败" if lang == "zh" else "Generation failed"},
    )
