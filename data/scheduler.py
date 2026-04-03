"""
息壤（Xi-Rang）数据自动更新调度器

随 FastAPI 服务启动，后台自动定时更新数据，无需手动操作。

调度策略：
- 每天 18:00（北京时间）更新 live 数据（美股盘后 + A股收盘后）
- 每周六 10:00 全量更新所有市场数据
- 服务启动时检查一次，如果数据陈旧立即更新
- 所有更新通过 DataManager 执行，自带限流保护

用法：
    # 在 FastAPI lifespan 中启动
    from data.scheduler import DataScheduler
    scheduler = DataScheduler()
    scheduler.start()
    ...
    scheduler.stop()
"""

import threading
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("xirang.scheduler")


class DataScheduler:
    """
    后台数据自动更新调度器。

    使用单线程定时检查，不依赖外部库（无需 APScheduler/celery）。
    """

    def __init__(
        self,
        live_hour: int = 18,
        live_minute: int = 0,
        full_weekday: int = 5,       # 0=Mon, 5=Sat
        full_hour: int = 10,
        stale_threshold: int = 3,    # 启动时数据陈旧超过此天数则立即更新
        check_interval: int = 300,   # 调度循环间隔（秒）
    ):
        self.live_hour = live_hour
        self.live_minute = live_minute
        self.full_weekday = full_weekday
        self.full_hour = full_hour
        self.stale_threshold = stale_threshold
        self.check_interval = check_interval

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._last_live_date: Optional[str] = None
        self._last_full_date: Optional[str] = None
        self._running_task: Optional[str] = None

    def start(self):
        """启动调度器（非阻塞）。"""
        if self._thread and self._thread.is_alive():
            logger.warning("调度器已在运行")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="data-scheduler")
        self._thread.start()
        logger.info("数据自动更新调度器已启动")
        logger.info(f"  Live 更新: 每天 {self.live_hour:02d}:{self.live_minute:02d}")
        logger.info(f"  全量更新: 每周{['一','二','三','四','五','六','日'][self.full_weekday]} {self.full_hour:02d}:00")

    def stop(self):
        """停止调度器。"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("数据自动更新调度器已停止")

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def status(self) -> dict:
        """返回调度器状态（供 API 展示）。"""
        now = datetime.now()
        return {
            "enabled": self.is_running,
            "current_task": self._running_task,
            "last_live_update": self._last_live_date,
            "last_full_update": self._last_full_date,
            "next_live": self._next_live_time(now).strftime("%Y-%m-%d %H:%M"),
            "next_full": self._next_full_time(now).strftime("%Y-%m-%d %H:%M"),
            "schedule": {
                "live": f"每天 {self.live_hour:02d}:{self.live_minute:02d}",
                "full": f"每周{['一','二','三','四','五','六','日'][self.full_weekday]} {self.full_hour:02d}:00",
            },
        }

    # ── 内部逻辑 ──────────────────────────────────────

    def _loop(self):
        """主调度循环。"""
        # 启动时先检查是否需要立即更新
        self._startup_check()

        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as e:
                logger.error(f"调度器异常: {e}", exc_info=True)

            # 等待下一次检查（可被 stop 打断）
            self._stop_event.wait(timeout=self.check_interval)

    def _tick(self):
        """单次调度检查。"""
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")

        # 检查是否到了 live 更新时间
        if (
            self._last_live_date != today
            and now.hour >= self.live_hour
            and now.minute >= self.live_minute
        ):
            self._do_live_update()
            self._last_live_date = today

        # 检查是否到了全量更新时间
        if (
            self._last_full_date != today
            and now.weekday() == self.full_weekday
            and now.hour >= self.full_hour
        ):
            self._do_full_update()
            self._last_full_date = today

    def _startup_check(self):
        """启动时检查数据新鲜度，陈旧则立即更新。"""
        try:
            from data.data_manager import DataManager
            dm = DataManager()
            status = dm.status()

            # 检查 live 数据新鲜度
            max_stale = 0
            for key, info in status["live"].items():
                end_date = datetime.strptime(info["end"], "%Y-%m-%d")
                stale = (datetime.now() - end_date).days
                max_stale = max(max_stale, stale)

            if max_stale > self.stale_threshold:
                logger.info(f"启动检查: Live 数据陈旧 {max_stale} 天，立即更新")
                self._do_live_update()
            else:
                logger.info(f"启动检查: 数据新鲜度正常 (最大陈旧 {max_stale} 天)")

            # 检查是否有市场文件缺失
            missing = [m for m, info in status["markets"].items() if info.get("exists") is False]
            if missing:
                logger.info(f"启动检查: 缺失市场 {missing}，立即全量更新")
                self._do_full_update(markets=missing)

        except Exception as e:
            logger.warning(f"启动检查失败: {e}")

    def _do_live_update(self):
        """执行 live 数据更新。"""
        if self._running_task:
            logger.info(f"跳过 live 更新（{self._running_task} 正在执行）")
            return

        self._running_task = "live_update"
        logger.info("自动任务: 开始 Live 数据更新...")
        try:
            from data.data_manager import DataManager
            dm = DataManager()
            dm.update_live()
            self._last_live_date = datetime.now().strftime("%Y-%m-%d")
            logger.info("自动任务: Live 数据更新完成 ✓")
        except Exception as e:
            logger.error(f"自动任务: Live 数据更新失败: {e}")
        finally:
            self._running_task = None

    def _do_full_update(self, markets: list = None):
        """执行全量数据更新。"""
        if self._running_task:
            logger.info(f"跳过全量更新（{self._running_task} 正在执行）")
            return

        label = f"全量更新 ({', '.join(markets)})" if markets else "全量更新"
        self._running_task = label
        logger.info(f"自动任务: 开始{label}...")
        try:
            from data.data_manager import DataManager
            dm = DataManager()
            dm.update_all(markets=markets)
            self._last_full_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"自动任务: {label}完成 ✓")
        except Exception as e:
            logger.error(f"自动任务: {label}失败: {e}")
        finally:
            self._running_task = None

    def _next_live_time(self, now: datetime) -> datetime:
        """计算下次 live 更新时间。"""
        target = now.replace(hour=self.live_hour, minute=self.live_minute, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        return target

    def _next_full_time(self, now: datetime) -> datetime:
        """计算下次全量更新时间。"""
        target = now.replace(hour=self.full_hour, minute=0, second=0, microsecond=0)
        days_ahead = self.full_weekday - now.weekday()
        if days_ahead < 0 or (days_ahead == 0 and now >= target):
            days_ahead += 7
        target += timedelta(days=days_ahead)
        return target
