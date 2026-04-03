"""
数据状态与更新 API

提供：
- ticker 级别的数据新鲜度查询
- 后台数据更新（全量/live/单 ticker）
- 更新任务状态轮询
"""

import threading
import logging
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Body
from api.deps import get_current_user, require_role

logger = logging.getLogger("xirang.data_api")
router = APIRouter(prefix="/api/data", tags=["data"])

# ── 后台任务状态 ──────────────────────────────────────

_task_lock = threading.Lock()
_current_task: Optional[dict] = None


def _get_task() -> Optional[dict]:
    with _task_lock:
        return _current_task.copy() if _current_task else None


def _set_task(task: Optional[dict]):
    global _current_task
    with _task_lock:
        _current_task = task


def _update_task(**kwargs):
    with _task_lock:
        if _current_task:
            _current_task.update(kwargs)


def _is_busy() -> bool:
    t = _get_task()
    return t is not None and t.get("status") == "running"


# ── 后台更新线程 ─────────────────────────────────────

class _LogCapture(logging.Handler):
    """捕获日志到任务状态的 logs 列表。"""
    def __init__(self):
        super().__init__()
        self.logs = []

    def emit(self, record):
        msg = self.format(record)
        self.logs.append(msg)
        # 同步更新到任务状态
        _update_task(logs=list(self.logs), last_log=msg)


def _run_update_all(markets: Optional[list[str]]):
    """后台线程：更新全部/指定市场。"""
    from data.data_manager import DataManager
    handler = _LogCapture()
    handler.setFormatter(logging.Formatter("%(message)s"))
    dm_logger = logging.getLogger("xirang.data_manager")
    dm_logger.addHandler(handler)
    try:
        dm = DataManager()
        dm.update_all(markets=markets)
        _update_task(status="done", finished_at=_now())
    except Exception as e:
        logger.error(f"更新任务失败: {e}")
        _update_task(status="error", error=str(e), finished_at=_now())
    finally:
        dm_logger.removeHandler(handler)


def _run_update_live():
    """后台线程：更新 live 数据。"""
    from data.data_manager import DataManager
    handler = _LogCapture()
    handler.setFormatter(logging.Formatter("%(message)s"))
    dm_logger = logging.getLogger("xirang.data_manager")
    dm_logger.addHandler(handler)
    try:
        dm = DataManager()
        dm.update_live()
        _update_task(status="done", finished_at=_now())
    except Exception as e:
        logger.error(f"Live 更新失败: {e}")
        _update_task(status="error", error=str(e), finished_at=_now())
    finally:
        dm_logger.removeHandler(handler)


def _run_update_ticker(ticker: str):
    """后台线程：更新单个 ticker。"""
    from data.data_manager import DataManager, MARKET_CONFIGS
    handler = _LogCapture()
    handler.setFormatter(logging.Formatter("%(message)s"))
    dm_logger = logging.getLogger("xirang.data_manager")
    dm_logger.addHandler(handler)
    try:
        dm = DataManager()
        # 找到这个 ticker 的最早起始日
        start = "2005-01-01"
        for cfg in MARKET_CONFIGS.values():
            if ticker in cfg["tickers"] and cfg["start"] < start:
                start = cfg["start"]
        result = dm.update_ticker(ticker, start)
        if result is not None:
            _update_task(
                status="done", finished_at=_now(),
                result_rows=len(result),
                result_end=str(result.index.max().date()),
            )
        else:
            _update_task(status="error", error=f"{ticker} 更新失败", finished_at=_now())
    except Exception as e:
        logger.error(f"Ticker 更新失败: {e}")
        _update_task(status="error", error=str(e), finished_at=_now())
    finally:
        dm_logger.removeHandler(handler)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── API 端点 ──────────────────────────────────────────

@router.get("/status")
async def data_status(user=Depends(get_current_user)):
    """
    返回所有 ticker 和市场文件的数据状态。

    新鲜度等级：fresh（≤2天）/ stale（3-7天）/ outdated（>7天）/ missing
    """
    from data.data_manager import DataManager

    dm = DataManager()
    status = dm.status()
    now = datetime.now()

    def freshness(end_date_str: str) -> dict:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        days = (now - end_date).days
        if days <= 2:
            level = "fresh"
        elif days <= 7:
            level = "stale"
        else:
            level = "outdated"
        return {"stale_days": days, "level": level}

    tickers = []
    for ticker, info in status["raw_tickers"].items():
        f = freshness(info["end"])
        tickers.append({
            "ticker": ticker,
            "rows": info["rows"],
            "start": info["start"],
            "end": info["end"],
            "stale_days": f["stale_days"],
            "level": f["level"],
        })
    tickers.sort(key=lambda x: (-x["stale_days"], x["ticker"]))

    markets = []
    for name, info in status["markets"].items():
        if info.get("exists") is False:
            markets.append({
                "name": name, "level": "missing",
                "rows": 0, "start": None, "end": None, "stale_days": None,
            })
        else:
            f = freshness(info["end"])
            markets.append({
                "name": name, "rows": info["rows"],
                "start": info["start"], "end": info["end"],
                "stale_days": f["stale_days"], "level": f["level"],
            })

    live = []
    for name, info in status["live"].items():
        f = freshness(info["end"])
        live.append({
            "name": name, "rows": info["rows"],
            "start": info["start"], "end": info["end"],
            "stale_days": f["stale_days"], "level": f["level"],
        })

    # 附带当前任务状态
    task = _get_task()

    # 附带调度器状态
    scheduler_status = None
    try:
        from fastapi import Request
        # 通过全局 app 获取 scheduler（如果在 API 上下文中）
        from data.scheduler import DataScheduler
        import main as main_module
        if hasattr(main_module, 'app') and hasattr(main_module.app, 'state'):
            sched = getattr(main_module.app.state, 'scheduler', None)
            if sched:
                scheduler_status = sched.status
    except Exception:
        pass

    return {
        "checked_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "tickers": tickers,
        "markets": markets,
        "live": live,
        "task": task,
        "scheduler": scheduler_status,
    }


@router.post("/update")
async def trigger_update(
    body: dict = Body(default={}),
    user=Depends(require_role("admin")),
):
    """
    触发后台数据更新。

    body 参数：
    - mode: "all"（默认）| "live" | "ticker"
    - markets: ["us", "cn", ...] （mode=all 时可选，不传则全部）
    - ticker: "SPY" （mode=ticker 时必传）
    """
    if _is_busy():
        raise HTTPException(status_code=409, detail="已有更新任务在运行中")

    mode = body.get("mode", "all")
    markets = body.get("markets")
    ticker = body.get("ticker")

    if mode == "ticker" and not ticker:
        raise HTTPException(status_code=400, detail="mode=ticker 时必须指定 ticker")

    # 初始化任务状态
    task_info = {
        "status": "running",
        "mode": mode,
        "started_at": _now(),
        "finished_at": None,
        "error": None,
        "logs": [],
        "last_log": "",
    }
    if mode == "all":
        task_info["markets"] = markets
    elif mode == "ticker":
        task_info["ticker"] = ticker

    _set_task(task_info)

    # 启动后台线程
    if mode == "all":
        t = threading.Thread(target=_run_update_all, args=(markets,), daemon=True)
    elif mode == "live":
        t = threading.Thread(target=_run_update_live, daemon=True)
    elif mode == "ticker":
        t = threading.Thread(target=_run_update_ticker, args=(ticker,), daemon=True)
    else:
        _set_task(None)
        raise HTTPException(status_code=400, detail=f"未知 mode: {mode}")

    t.start()
    return {"message": "更新任务已启动", "task": _get_task()}


@router.get("/update/status")
async def update_status(user=Depends(get_current_user)):
    """查询当前更新任务的状态和日志。"""
    task = _get_task()
    if task is None:
        return {"task": None}
    return {"task": task}


@router.post("/update/dismiss")
async def dismiss_task(user=Depends(get_current_user)):
    """清除已完成/失败的任务状态。"""
    task = _get_task()
    if task and task.get("status") == "running":
        raise HTTPException(status_code=409, detail="任务仍在运行中")
    _set_task(None)
    return {"message": "已清除"}
