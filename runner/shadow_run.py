"""
息壤（Xi-Rang）Phase 2 影子运行 runner

只生成拟执行结果与对账摘要，不触发真实下单。
"""

from __future__ import annotations

import json
import logging
import sys

import numpy as np

from db.database import Database
from engine.config import PORTFOLIOS
from engine.execution.factory import create_shadow_runner
from engine.market_data import MarketDataService
from engine.portfolio import PortfolioEngine
from engine.risk import RiskEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("xirang.shadow_run")


def observe_shadow_run(
    *,
    db: Database,
    portfolio_id: str,
    market,
    assets: list[str],
    market_day: str,
    current_positions: dict[str, float],
    target_weights: list[float],
    total_nav: float,
    current_prices: dict[str, float],
    broker_role: str = "sandbox",
) -> dict:
    shadow = create_shadow_runner(market_data_service=market, assets=assets)
    orders, report = shadow.run(
        current_positions=current_positions,
        target_weights=target_weights,
        total_nav=total_nav,
        current_prices=current_prices,
        local_cash=0.0,
    )

    result = {
        "date": market_day,
        "portfolio": portfolio_id,
        "broker_name": report.broker_name,
        "order_count": report.order_count,
        "dry_run": report.dry_run,
        "warnings": report.warnings,
        "reconciliation_status": report.reconciliation.status.value if report.reconciliation else None,
        "orders": [
            {
                "symbol": order_item.symbol,
                "side": order_item.side.value,
                "quantity": order_item.quantity,
                "estimated_price": order_item.estimated_price,
                "estimated_amount": order_item.estimated_amount,
            }
            for order_item in orders
        ],
    }

    db.save_shadow_run_report(
        portfolio_id=portfolio_id,
        broker_role=broker_role,
        broker_name=report.broker_name,
        checked_at=report.reconciliation.checked_at.isoformat() if report.reconciliation else str(market_day),
        dry_run=report.dry_run,
        order_count=report.order_count,
        reconciliation_status=report.reconciliation.status.value if report.reconciliation else "",
        requires_attention=bool(report.warnings or (report.reconciliation and report.reconciliation.requires_manual_intervention)),
        warnings_json=json.dumps(report.warnings, ensure_ascii=False),
        report_json=json.dumps(result, ensure_ascii=False),
    )
    logger.info("影子运行完成: %s", result)
    return result


def run_shadow(portfolio_id: str = "us") -> dict:
    db = Database()
    pf_config = PORTFOLIOS[portfolio_id]
    assets = pf_config["assets"]

    db.ensure_portfolio(portfolio_id, assets)
    state = db.get_portfolio(portfolio_id)

    stored_positions = np.array(json.loads(state["positions"]), dtype=float)
    engine = PortfolioEngine(initial_capital=float(np.sum(stored_positions)))
    engine.state = state["state"]
    engine.positions = stored_positions
    engine.stability_balance = float(state.get("stability_balance", 0.0))
    engine.cooldown_counter = state["cooldown_counter"]
    engine.rebalance_count = state["rebalance_count"]
    engine.protection_count = state["protection_count"]
    engine.refresh_nav()

    market = MarketDataService(assets=assets, data_source=pf_config.get("data_source"))
    prices = market.fetch_latest(lookback_days=60)
    daily_returns = market.get_today_returns(prices)
    indicators = market.get_risk_indicators(prices)
    current_prices = {asset: float(prices[asset].iloc[-1]) for asset in assets}
    market_date = prices.index[-1]
    market_day = market_date.strftime("%Y-%m-%d") if hasattr(market_date, "strftime") else str(market_date)

    risk = RiskEngine()
    risk.high_water_mark = state["high_water_mark"]
    sim_core_nav = float(np.sum(engine.positions * (1 + daily_returns)))
    sim_nav = sim_core_nav + float(engine.stability_balance)
    risk_signal = risk.evaluate(
        nav=sim_nav,
        spy_tlt_corr=indicators["spy_tlt_corr"],
        spy_30d_ret=indicators["spy_30d_ret"],
        tlt_30d_ret=indicators["tlt_30d_ret"],
    )

    engine.apply_daily_returns(daily_returns)
    order = engine.evaluate_rebalance(risk_signal=risk_signal, is_year_end=False)
    target_weights = order.target_weights if order else engine.weights.tolist()
    current_positions = {asset: float(value) for asset, value in zip(assets, engine.positions.tolist())}

    return observe_shadow_run(
        db=db,
        portfolio_id=portfolio_id,
        market=market,
        assets=assets,
        market_day=market_day,
        current_positions=current_positions,
        target_weights=target_weights,
        total_nav=engine.core_nav,
        current_prices=current_prices,
    )


if __name__ == "__main__":
    portfolio_id = "us"
    if "--portfolio" in sys.argv:
        idx = sys.argv.index("--portfolio")
        if idx + 1 < len(sys.argv):
            portfolio_id = sys.argv[idx + 1]
    print(json.dumps(run_shadow(portfolio_id), ensure_ascii=False, indent=2))
