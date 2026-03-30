"""
息壤（Xi-Rang）每日运行器（多组合版）

同时运行中美两个市场的组合，共享同一个数据库。

用法：
    python -m runner.daily_runner              # 运行所有组合
    python -m runner.daily_runner --force      # 强制重跑
    python -m runner.daily_runner --portfolio us   # 只跑美股
    python -m runner.daily_runner --portfolio cn   # 只跑中国
"""

import json
import shutil
import logging
import time
import sys
import numpy as np
from datetime import datetime
from pathlib import Path

from engine.config import PORTFOLIOS, STATE_PROTECTION
from engine.risk import RiskEngine
from engine.portfolio import PortfolioEngine
from engine.market_data import MarketDataService
from engine.data_validator import validate_prices, validate_returns, DataValidationError
from engine.notifier import notify
from db.database import Database

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
RETRY_DELAY = 60


class DailyRunner:
    def __init__(self, db: Database = None):
        self.db = db or Database()

    def run_portfolio(self, portfolio_id: str, force: bool = False) -> dict:
        """运行单个组合"""
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]
        name = pf_config["name"]
        currency = pf_config["currency"]

        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"── {name} ({portfolio_id}) ──")

        # 幂等性
        if not force and self.db.has_run_today(today, portfolio_id):
            logger.info(f"  ⏭ 今日已运行过，跳过。")
            return {"date": today, "portfolio": portfolio_id, "status": "SKIPPED"}

        # 确保组合存在
        self.db.ensure_portfolio(portfolio_id, assets)

        # 加载状态
        state = self.db.get_portfolio(portfolio_id)
        engine = PortfolioEngine(initial_capital=state["nav"])
        engine.state = state["state"]
        engine.nav = state["nav"]
        engine.positions = np.array(json.loads(state["positions"]))
        engine.cooldown_counter = state["cooldown_counter"]
        engine.rebalance_count = state["rebalance_count"]
        engine.protection_count = state["protection_count"]

        risk = RiskEngine()
        risk.high_water_mark = state["high_water_mark"]

        logger.info(f"  状态: {engine.state}, NAV: {currency}{engine.nav:,.2f}")

        # 拉取数据
        market = MarketDataService(assets=assets, data_source=pf_config.get("data_source"))
        prices = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"  拉取数据 (尝试 {attempt}/{MAX_RETRIES})...")
                prices = market.fetch_latest(lookback_days=60)
                break
            except Exception as e:
                logger.warning(f"  数据拉取失败: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"  等待 {RETRY_DELAY} 秒后重试...")
                    time.sleep(RETRY_DELAY)
                else:
                    msg = f"数据拉取连续 {MAX_RETRIES} 次失败: {e}"
                    logger.error(f"  {msg}")
                    self.db.record_run(today, "FAILED", msg, portfolio_id)
                    return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": msg}

        # 数据校验
        try:
            validate_prices(prices)
        except DataValidationError as e:
            msg = f"数据校验失败: {e}"
            logger.error(f"  {msg}")
            self.db.record_run(today, "FAILED", msg, portfolio_id)
            return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": msg}

        daily_returns = market.get_today_returns(prices)
        try:
            validate_returns(daily_returns)
        except DataValidationError as e:
            msg = f"收益率校验失败: {e}"
            logger.error(f"  {msg}")
            self.db.record_run(today, "FAILED", msg, portfolio_id)
            return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": msg}

        indicators = market.get_risk_indicators(prices)
        logger.info(f"  今日收益: {dict(zip(assets, [f'{r:+.2%}' for r in daily_returns]))}")
        logger.info(f"  ✓ 数据校验通过")

        # 风控
        sim_nav = float(np.sum(engine.positions * (1 + daily_returns)))
        risk_signal = risk.evaluate(
            nav=sim_nav,
            spy_tlt_corr=indicators["spy_tlt_corr"],
            spy_30d_ret=indicators["spy_30d_ret"],
            tlt_30d_ret=indicators["tlt_30d_ret"],
        )
        logger.info(f"  回撤: {risk_signal.current_dd:.2%}")
        if risk_signal.trigger_reason:
            logger.warning(f"  ⚠ 风控: {risk_signal.trigger_reason}")

        # 状态机
        now = datetime.now()
        order = engine.step(
            current_date=now.date(),
            daily_returns=daily_returns,
            risk_signal=risk_signal,
            is_year_end=(now.month == 12 and now.day >= 28),
        )

        # 持久化
        self.db.update_portfolio(
            portfolio_id=portfolio_id,
            state=engine.state, nav=engine.nav,
            positions=json.dumps(engine.positions.tolist()),
            high_water_mark=risk.high_water_mark,
            cooldown_counter=engine.cooldown_counter,
            rebalance_count=engine.rebalance_count,
            protection_count=engine.protection_count,
        )
        self.db.save_snapshot(
            date=today, state=engine.state, nav=engine.nav,
            positions=engine.positions.tolist(),
            weights=engine.weights.tolist(),
            drawdown=risk_signal.current_dd,
            spy_tlt_corr=indicators["spy_tlt_corr"],
            action=order.reason if order else None,
            trigger_reason=risk_signal.trigger_reason,
            portfolio_id=portfolio_id,
        )
        if order:
            self.db.save_transaction(
                date=today, tx_type="REBALANCE",
                target_weights=order.target_weights,
                turnover=order.turnover, friction_cost=order.friction_cost,
                reason=order.reason, portfolio_id=portfolio_id,
            )
        if risk_signal.trigger_reason:
            sev = "HIGH" if risk_signal.is_hard_stop else "MEDIUM" if risk_signal.is_protection else "LOW"
            self.db.save_risk_event(
                date=today, event_type=risk_signal.trigger_reason.split(":")[0].strip(),
                severity=sev, drawdown=risk_signal.current_dd,
                spy_tlt_corr=risk_signal.spy_tlt_corr,
                action_taken=order.reason if order else "无操作",
                portfolio_id=portfolio_id,
            )

        report = {
            "date": today, "portfolio": portfolio_id, "name": name,
            "state": engine.state,
            "nav": round(engine.nav, 2), "currency": currency,
            "weights": {a: round(w, 4) for a, w in zip(assets, engine.weights.tolist())},
            "drawdown": round(risk_signal.current_dd, 4),
            "spy_tlt_corr": round(indicators["spy_tlt_corr"], 4),
            "action": order.reason if order else None,
            "rebalance_count": engine.rebalance_count,
            "protection_count": engine.protection_count,
        }
        self.db.record_run(today, "SUCCESS", json.dumps(report), portfolio_id)

        logger.info(f"  NAV: {currency}{report['nav']:,.2f} | {report['state']}")
        if order:
            logger.info(f"  操作: {order.reason}")
        else:
            logger.info(f"  操作: 无")

        notify(report)
        return report

    def run_all(self, force: bool = False, only: str = None):
        """运行所有组合（或指定组合）"""
        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"{'='*56}")
        logger.info(f"息壤每日运行 - {today}")
        logger.info(f"{'='*56}")

        portfolios = {only: PORTFOLIOS[only]} if only and only in PORTFOLIOS else PORTFOLIOS
        results = {}

        for pid in portfolios:
            try:
                results[pid] = self.run_portfolio(pid, force=force)
            except Exception as e:
                logger.error(f"  ✗ {pid} 运行异常: {e}")
                results[pid] = {"date": today, "portfolio": pid, "status": "ERROR", "reason": str(e)}

        # 备份（所有组合跑完后统一备份一次）
        backup_dir = Path("db/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"xirang_{today.replace('-','')}.db"
        shutil.copy2(self.db.db_path, backup_path)
        for old in sorted(backup_dir.glob("xirang_*.db"))[:-30]:
            old.unlink()
        logger.info(f"✓ 数据库已备份")
        logger.info(f"{'='*56}")

        return results


def run_daily():
    force = "--force" in sys.argv
    only = None
    if "--portfolio" in sys.argv:
        idx = sys.argv.index("--portfolio")
        if idx + 1 < len(sys.argv):
            only = sys.argv[idx + 1]
    runner = DailyRunner()
    return runner.run_all(force=force, only=only)


if __name__ == "__main__":
    run_daily()
