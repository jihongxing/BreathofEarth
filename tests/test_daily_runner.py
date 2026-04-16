import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from db.database import Database
from engine.execution.base import ExecutionResult, OrderSide, OrderStatus, TradeOrder
from engine.risk import RiskSignal
from runner import daily_runner as runner_module


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


def _make_market_service(end_date: str):
    class StubMarketDataService:
        market_date = end_date

        def __init__(self, assets=None, data_source=None):
            self.assets = assets or []
            self.data_source = data_source

        def fetch_latest(self, lookback_days=60):
            index = pd.to_datetime(["2026-12-29", self.market_date])
            data = {asset: [100.0, 100.0] for asset in self.assets}
            return pd.DataFrame(data, index=index)

        def get_today_returns(self, prices=None):
            return np.zeros(len(self.assets))

        def get_risk_indicators(self, prices=None):
            return {
                "spy_tlt_corr": 0.1,
                "spy_30d_ret": 0.01,
                "tlt_30d_ret": 0.01,
            }

    return StubMarketDataService


class PendingExecutor:
    def translate_orders(self, current_positions, target_weights, total_nav, current_prices):
        return [
            TradeOrder(
                symbol=list(current_positions.keys())[0],
                side=OrderSide.BUY,
                quantity=1,
                estimated_price=100.0,
                estimated_amount=100.0,
            )
        ]

    def execute(self, orders):
        for order in orders:
            order.status = OrderStatus.AWAITING_CONFIRM
        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=100.0,
            total_sold=0.0,
            total_commission=0.0,
            message="待人工执行",
        )


class FilledExecutor:
    def translate_orders(self, current_positions, target_weights, total_nav, current_prices):
        return [
            TradeOrder(
                symbol=list(current_positions.keys())[0],
                side=OrderSide.BUY,
                quantity=1,
                estimated_price=100.0,
                estimated_amount=100.0,
            )
        ]

    def execute(self, orders):
        for order in orders:
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.filled_price = order.estimated_price
        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=100.0,
            total_sold=0.0,
            total_commission=5.0,
            message="paper filled",
        )


class BrokerReceiptMissingExecutor:
    def translate_orders(self, current_positions, target_weights, total_nav, current_prices):
        return [
            TradeOrder(
                symbol=list(current_positions.keys())[0],
                side=OrderSide.BUY,
                quantity=1,
                estimated_price=100.0,
                estimated_amount=100.0,
            )
        ]

    def execute(self, orders):
        for order in orders:
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.filled_price = order.estimated_price
            order.broker_order_id = None
        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=100.0,
            total_sold=0.0,
            total_commission=1.0,
            message="filled without broker receipt",
        )


class HighSlippageExecutor:
    def translate_orders(self, current_positions, target_weights, total_nav, current_prices):
        return [
            TradeOrder(
                symbol=list(current_positions.keys())[0],
                side=OrderSide.BUY,
                quantity=1,
                estimated_price=100.0,
                estimated_amount=100.0,
            )
        ]

    def execute(self, orders):
        for order in orders:
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.filled_price = 101.0
            order.broker_order_id = "broker-1"
        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=101.0,
            total_sold=0.0,
            total_commission=1.0,
            message="filled with high slippage",
        )


class CaptureRiskEngine:
    seen_navs = []

    def __init__(self):
        self.high_water_mark = 0.0

    def evaluate(self, nav, spy_tlt_corr, spy_30d_ret, tlt_30d_ret):
        self.seen_navs.append(nav)
        self.high_water_mark = max(self.high_water_mark, nav)
        return RiskSignal(
            current_dd=0.0,
            spy_tlt_corr=spy_tlt_corr,
            spy_30d_ret=spy_30d_ret,
            tlt_30d_ret=tlt_30d_ret,
            is_hard_stop=False,
            is_protection=False,
            is_corr_breakdown=False,
            trigger_reason=None,
        )


def test_non_whitelisted_pending_execution_fails_closed(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    temp_db.update_portfolio("us", stability_balance=10_000.0)

    CaptureRiskEngine.seen_navs = []
    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: PendingExecutor())
    monkeypatch.setattr(runner_module, "RiskEngine", CaptureRiskEngine)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert CaptureRiskEngine.seen_navs[-1] == pytest.approx(110_000.0)
    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["action"] == "执行失败: 年末强制再平衡 未获得最终成交回执"

    portfolio = temp_db.get_portfolio("us")
    assert portfolio["state"] == "IDLE"
    assert portfolio["nav"] == pytest.approx(110_000.0)
    assert json.loads(portfolio["positions"]) == pytest.approx([25_000.0, 25_000.0, 25_000.0, 25_000.0])

    with temp_db._conn() as conn:
        run_row = conn.execute(
            "SELECT status FROM daily_runs WHERE portfolio_id = ? AND date = ?",
            ("us", "2026-12-30"),
        ).fetchone()
        tx_row = conn.execute(
            "SELECT type, reason FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()

    assert run_row["status"] == "FAILED_EXECUTION"
    assert tx_row["type"] == "REBALANCE_FAILED"
    assert "年末强制再平衡" in tx_row["reason"]


def test_stale_data_enters_manual_intervention_whitelist(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    called = {"executor": 0}

    def _executor_factory(**kwargs):
        called["executor"] += 1
        return FilledExecutor()

    stale_date = (datetime.now().date() - timedelta(days=10)).isoformat()
    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service(stale_date))
    monkeypatch.setattr(runner_module, "create_executor", _executor_factory)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "MANUAL_INTERVENTION_REQUIRED"
    assert result["manual_intervention_required"] is True
    assert result["manual_intervention_reasons"][0]["code"] == "STALE_DATA"
    assert called["executor"] == 0


def test_missing_broker_receipt_enters_manual_intervention_whitelist(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: BrokerReceiptMissingExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "MANUAL_INTERVENTION_REQUIRED"
    assert result["manual_intervention_reasons"][0]["code"] == "MISSING_BROKER_RECEIPT"
    assert "券商回执缺失" in result["action"]


def test_excessive_slippage_enters_manual_intervention_whitelist(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: HighSlippageExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "MANUAL_INTERVENTION_REQUIRED"
    assert result["manual_intervention_reasons"][0]["code"] == "EXCESSIVE_SLIPPAGE"
    assert "成交偏差过大" in result["action"]


def test_core_runner_blocks_manual_executor_as_normal_mode(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "manual")
    monkeypatch.delenv("XIRANG_ALLOW_CORE_INTERACTIVE", raising=False)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["action"] == "策略拦截: Core 常规调仓不允许人工确认模式"

    with temp_db._conn() as conn:
        tx_row = conn.execute(
            "SELECT type, reason FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()

    assert tx_row["type"] == "REBALANCE_BLOCKED"
    assert "人工确认模式" in tx_row["reason"]


def test_year_end_rebalance_only_triggers_once_per_year(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    market_cls = _make_market_service("2026-12-30")
    monkeypatch.setattr(runner_module, "MarketDataService", market_cls)
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "RiskEngine", CaptureRiskEngine)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    runner = runner_module.DailyRunner(temp_db)
    first = runner.run_portfolio("us")
    assert first["action"] == "年末强制再平衡"

    market_cls.market_date = "2026-12-31"
    second = runner.run_portfolio("us")

    assert second["run_status"] == "SUCCESS"
    assert second["action"] is None

    with temp_db._conn() as conn:
        count = conn.execute(
            """SELECT COUNT(*) AS cnt FROM transactions
               WHERE portfolio_id = ? AND type = 'REBALANCE' AND reason = ?""",
            ("us", "年末强制再平衡"),
        ).fetchone()["cnt"]

    assert count == 1
