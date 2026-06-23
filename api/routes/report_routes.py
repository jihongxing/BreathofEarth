"""
息壤 — 家��月报 API 路由
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from api.deps import ensure_portfolio_access, get_accessible_portfolio_ids, get_current_user, get_db, is_admin_user
from db.database import Database
from runner.dashboard import list_reports, get_report_html, generate_and_save

router = APIRouter(prefix="/api", tags=["reports"])


@router.get("/reports")
async def get_reports(
    db: Database = Depends(get_db),
    user=Depends(get_current_user),
):
    """返回按年月聚合的月报列表。"""
    reports = list_reports()
    if is_admin_user(user):
        return reports
    allowed = get_accessible_portfolio_ids(db, user, permission="view")
    return [
        r
        for r in reports
        if not isinstance(r, dict) or not r.get("portfolio_id") or r.get("portfolio_id") in allowed
    ]


@router.get("/reports/{year}/{month}/{portfolio_id}")
async def get_report(
    year: int,
    month: int,
    portfolio_id: str,
    lang: str = Query("zh"),
    db: Database = Depends(get_db),
    user=Depends(get_current_user),
):
    """获取指定月份、组合的月报 HTML。"""
    if portfolio_id not in get_accessible_portfolio_ids(db, user):
        raise HTTPException(status_code=403, detail="无权访问该组合报表")
    html = get_report_html(year, month, portfolio_id, lang=lang)
    return HTMLResponse(content=html)


@router.post("/report/generate")
async def generate_report(
    lang: str = Query("zh"),
    db: Database = Depends(get_db),
    user=Depends(get_current_user),
):
    """生成最新月报。"""
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="无权生成全局报表")
    path = generate_and_save(days=90, lang=lang)
    if path:
        return {"message": "报告已生成" if lang == "zh" else "Report generated", "path": str(path)}
    return JSONResponse(
        status_code=500,
        content={"detail": "生成失败" if lang == "zh" else "Generation failed"},
    )
