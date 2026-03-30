"""
息壤（Xi-Rang）每日运行器

设计为 cron 驱动的独立脚本：跑完即退出，不依赖任何常驻进程。

职责：
1. 补跑检查（前一天失败的自动补跑）
2. 幂等性检查（一天只运行一次）
3. 拉取最新市场数据（失败自动重试 3 次）
4. 数据合理性校验（Fail-safe）
5. 计算风控指标
6. 驱动状态机执行一步
7. 持久化结果到 SQLite
8. 数据库备份
9. 通知推送（静默与唤醒）
10. 写入运行日志文件

用法：
    python -m runner.daily_runner          # 正常运行
    python -m runner.daily_runner --force  # 强制重跑今天
"""

import json
import shutil
import logging
import time
import sys
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from engine.config import ASSETS, STATE_PROTECTION
from engine.risk import RiskEngine
from engine.portfolio import PortfolioEngine
from engine.market_data import MarketDataService
from engine.data_validator import validate_prices, validate_returns, DataValidationError
from engine.notifier import notify
from db.database import Database

# ── 日志配置：同时输出到控制台和文件 ─────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "xirang.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("xirang.runner")

MAX_RETRIES = 3
RETRY_DELAY = 60  # 秒


class DailyRunner:
    def __init__(self, db: Database = None):
        self.db = db or Database()
        self.market = MarketDataService()
        self.risk_engine = RiskEngine()
        self.portfolio_engine = None

    def _load_portfolio_state(self):
        """从数据库恢复组合状态"""
        state = self.db.get_portfolio()

        self.portfolio_engine = PortfolioEngine(initial_capital=state["nav"])
        self.portfolio_engine.state = state["state"]
        self.portfolio_engine.nav = state["nav"]
        self.portfolio_engine.positions = np.array(json.loads(state["positions"]))
        self.portfolio_engine.cooldown_counter = state["cooldown_counter"]
        self.portfolio_engine.rebalance_count = state["rebalance_count"]
        self.portfolio_engine.protection_count = state["protection_count"]

        # 恢复风控引擎的高水位
        self.risk_engine.high_water_mark = state["high_water_mark"]

    def _save_portfolio_state(self):
        """持久化组合状态到数据库"""
        self.db.update_portfolio(
            state=self.portfolio_engine.state,
            nav=self.portfolio_engine.nav,
            positions=json.dumps(self.portfolio_engine.positions.tolist()),
            high_water_mark=self.risk_engine.high_water_mark,
            cooldown_counter=self.portfolio_engine.cooldown_counter,
            rebalance_count=self.portfolio_engine.rebalance_count,
            protection_count=self.portfolio_engine.protection_count,
        )

    def _backup_db(self):
        """数据库备份：每次运行后备份 .db 文件"""
        backup_dir = Path("db/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)

        today = datetime.now().strftime("%Y%m%d")
        backup_path = backup_dir / f"xirang_{today}.db"

        shutil.copy2(self.db.db_path, backup_path)

        # 只保留最近 30 天的备份
        backups = sorted(backup_dir.glob("xirang_*.db"))
        for old in backups[:-30]:
            old.unlink()
            logger.info(f"清理旧备份: {old.name}")

        logger.info(f"数据库已备份: {backup_path}")

    def run(self, force: bool = False) -> dict:
        """
        执行一次每日更新。

        Args:
            force: 强制运行，忽略幂等性检查

        Returns:
            当日运行报告 dict
        """
        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"{'='*50}")
        logger.info(f"息壤每日运行 - {today}")
        logger.info(f"{'='*50}")

        # ── 0. 幂等性检查 ────────────────────────────
        if not force and self.db.has_run_today(today):
            logger.info(f"⏭ 今日已运行过，跳过。")
            return {"date": today, "status": "SKIPPED", "reason": "已运行过"}

        # ── 1. 加载组合状态 ──────────────────────────
        self._load_portfolio_state()
        logger.info(f"组合状态: {self.portfolio_engine.state}, NAV: ${self.portfolio_engine.nav:,.2f}")

        # ── 2. 拉取市场数据（带重试）─────────────────
        prices = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"拉取市场数据 (尝试 {attempt}/{MAX_RETRIES})...")
                prices = self.market.fetch_latest(lookback_days=60)
                break
            except Exception as e:
                logger.warning(f"数据拉取失败: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"等待 {RETRY_DELAY} 秒后重试...")
                    time.sleep(RETRY_DELAY)
                else:
                    error_msg = f"数据拉取连续 {MAX_RETRIES} 次失败: {e}"
                    logger.error(error_msg)
                    self.db.record_run(today, status="FAILED", report=error_msg)
                    notify({"date": today, "state": "ERROR", "nav": self.portfolio_engine.nav,
                             "action": f"⚠ {error_msg}", "weights": {}, "drawdown": 0,
                             "spy_tlt_corr": 0, "rebalance_count": 0, "protection_count": 0})
                    return {"date": today, "status": "FAILED", "reason": error_msg}

        # ── 3. 数据合理性校验（Fail-safe）────────────
        try:
            validate_prices(prices)
        except DataValidationError as e:
            error_msg = f"数据校验失败: {e}"
            logger.error(f"{error_msg} — 系统中止，等待人工确认。")
            self.db.record_run(today, status="FAILED", report=error_msg)
            notify({"date": today, "state": "ERROR", "nav": self.portfolio_engine.nav,
                     "action": f"🚨 数据异常，系统中止: {e}", "weights": {}, "drawdown": 0,
                     "spy_tlt_corr": 0, "rebalance_count": 0, "protection_count": 0})
            return {"date": today, "status": "FAILED", "reason": error_msg}

        daily_returns = self.market.get_today_returns(prices)

        try:
            validate_returns(daily_returns)
        except DataValidationError as e:
            error_msg = f"收益率校验失败: {e}"
            logger.error(error_msg)
            self.db.record_run(today, status="FAILED", report=error_msg)
            notify({"date": today, "state": "ERROR", "nav": self.portfolio_engine.nav,
                     "action": f"🚨 收益率异常，系统中止: {e}", "weights": {}, "drawdown": 0,
                     "spy_tlt_corr": 0, "rebalance_count": 0, "protection_count": 0})
            return {"date": today, "status": "FAILED", "reason": error_msg}

        risk_indicators = self.market.get_risk_indicators(prices)

        logger.info(f"今日收益: {dict(zip(ASSETS, [f'{r:+.2%}' for r in daily_returns]))}")
        logger.info(f"SPY-TLT 相关性: {risk_indicators['spy_tlt_corr']:.2f}")
        logger.info(f"✓ 数据校验通过")

        # ── 4. 风控评估 ──────────────────────────────
        simulated_positions = self.portfolio_engine.positions * (1 + daily_returns)
        simulated_nav = float(np.sum(simulated_positions))

        risk_signal = self.risk_engine.evaluate(
            nav=simulated_nav,
            spy_tlt_corr=risk_indicators["spy_tlt_corr"],
            spy_30d_ret=risk_indicators["spy_30d_ret"],
            tlt_30d_ret=risk_indicators["tlt_30d_ret"],
        )

        logger.info(f"当前回撤: {risk_signal.current_dd:.2%}")
        if risk_signal.trigger_reason:
            logger.warning(f"⚠ 风控触发: {risk_signal.trigger_reason}")

        # ── 5. 状态机执行 ────────────────────────────
        now = datetime.now()
        is_year_end = (now.month == 12 and now.day >= 28)

        order = self.portfolio_engine.step(
            current_date=now.date(),
            daily_returns=daily_returns,
            risk_signal=risk_signal,
            is_year_end=is_year_end,
        )

        # ── 6. 持久化 ───────────────────────────────
        self._save_portfolio_state()

        self.db.save_snapshot(
            date=today,
            state=self.portfolio_engine.state,
            nav=self.portfolio_engine.nav,
            positions=self.portfolio_engine.positions.tolist(),
            weights=self.portfolio_engine.weights.tolist(),
            drawdown=risk_signal.current_dd,
            spy_tlt_corr=risk_indicators["spy_tlt_corr"],
            action=order.reason if order else None,
            trigger_reason=risk_signal.trigger_reason,
        )

        if order:
            self.db.save_transaction(
                date=today,
                tx_type="REBALANCE",
                target_weights=order.target_weights,
                turnover=order.turnover,
                friction_cost=order.friction_cost,
                reason=order.reason,
            )

        if risk_signal.trigger_reason:
            severity = "HIGH" if risk_signal.is_hard_stop else "MEDIUM" if risk_signal.is_protection else "LOW"
            self.db.save_risk_event(
                date=today,
                event_type=risk_signal.trigger_reason.split(":")[0].strip(),
                severity=severity,
                drawdown=risk_signal.current_dd,
                spy_tlt_corr=risk_signal.spy_tlt_corr,
                action_taken=order.reason if order else "无操作",
            )

        # ── 7. 生成报告 ─────────────────────────────
        report = {
            "date": today,
            "state": self.portfolio_engine.state,
            "nav": round(self.portfolio_engine.nav, 2),
            "weights": {
                asset: round(w, 4)
                for asset, w in zip(ASSETS, self.portfolio_engine.weights.tolist())
            },
            "drawdown": round(risk_signal.current_dd, 4),
            "spy_tlt_corr": round(risk_indicators["spy_tlt_corr"], 4),
            "action": order.reason if order else None,
            "rebalance_count": self.portfolio_engine.rebalance_count,
            "protection_count": self.portfolio_engine.protection_count,
        }

        self.db.record_run(today, status="SUCCESS", report=json.dumps(report))

        logger.info(f"更新后 NAV: ${report['nav']:,.2f} | 状态: {report['state']}")
        if order:
            logger.info(f"操作: {order.reason} | 换手率: {order.turnover:.2%}")
        else:
            logger.info("操作: 无（持仓不变）")

        # ── 8. 备份数据库 ────────────────────────────
        self._backup_db()
        logger.info("✓ 数据库已备份")

        # ── 9. 通知推送 ─────────────────────────────
        notify(report)

        logger.info(f"{'='*50}")
        return report


def run_daily():
    """入口函数，供 cron 或命令行调用"""
    force = "--force" in sys.argv
    runner = DailyRunner()
    return runner.run(force=force)


if __name__ == "__main__":
    run_daily()
