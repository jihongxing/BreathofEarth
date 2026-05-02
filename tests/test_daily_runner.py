import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from db.database import Database
from engine.config import PORTFOLIOS, validate_config
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


def _seed_broker_sync(
    db: Database,
    *,
    portfolio_id: str = "us",
    checked_day: str = "2026-12-30",
    status: str = "MATCHED",
    items_json: str = "[]",
    report_json: str = '{"requires_manual_intervention": false}',
    broker_name: str = "ibkr",
    currency: str = "USD",
):
    db.save_broker_account_snapshot(
        portfolio_id=portfolio_id,
        broker_role="primary",
        broker_name=broker_name,
        broker_mode="read_only",
        account_id="U1234567",
        currency=currency,
        cash=0.0,
        total_value=100000.0,
        positions_json="{}",
        raw_json="{}",
        snapshot_time=f"{checked_day}T09:30:00+00:00",
    )
    db.save_broker_reconciliation_run(
        portfolio_id=portfolio_id,
        broker_role="primary",
        broker_name=broker_name,
        status=status,
        checked_at=f"{checked_day}T09:31:00+00:00",
        items_json=items_json,
        report_json=report_json,
    )


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


class FilledBrokerReceiptExecutor(FilledExecutor):
    def execute(self, orders):
        result = super().execute(orders)
        for index, order in enumerate(result.orders):
            order.broker_order_id = f"broker-{index + 1}"
            order.broker_reference = f"perm-{index + 1}"
        return result


class FilledBrokerAuditExecutor(FilledBrokerReceiptExecutor):
    def execute(self, orders):
        result = super().execute(orders)
        result.broker_events = [
            {
                "event_type": "SUBMIT_RESULT",
                "event_time": "2026-12-30T09:31:00+00:00",
                "broker_name": "ibkr",
                "broker_role": "primary",
                "broker_mode": "live",
                "order_id": "broker-1",
                "client_order_id": "xirang-spy-1",
                "broker_reference": "perm-1",
                "symbol": result.orders[0].symbol,
                "side": result.orders[0].side.value,
                "requested_quantity": result.orders[0].quantity,
                "filled_quantity": 0,
                "avg_fill_price": None,
                "commission": None,
                "status": "SUBMITTED",
                "message": "submitted",
                "raw": {"stage": "submit"},
            },
            {
                "event_type": "STATUS_POLL",
                "event_time": "2026-12-30T09:32:00+00:00",
                "broker_name": "ibkr",
                "broker_role": "primary",
                "broker_mode": "live",
                "order_id": "broker-1",
                "client_order_id": "xirang-spy-1",
                "broker_reference": "perm-1",
                "symbol": result.orders[0].symbol,
                "side": result.orders[0].side.value,
                "requested_quantity": result.orders[0].quantity,
                "filled_quantity": result.orders[0].filled_quantity,
                "avg_fill_price": result.orders[0].filled_price,
                "commission": result.total_commission,
                "status": "FILLED",
                "message": "filled",
                "raw": {"stage": "poll"},
            },
        ]
        return result


class LargeOrderExecutor:
    executed = False

    def translate_orders(self, current_positions, target_weights, total_nav, current_prices):
        return [
            TradeOrder(
                symbol=list(current_positions.keys())[0],
                side=OrderSide.BUY,
                quantity=100,
                estimated_price=200.0,
                estimated_amount=20000.0,
            )
        ]

    def execute(self, orders):
        LargeOrderExecutor.executed = True
        for order in orders:
            order.status = OrderStatus.FILLED
            order.filled_quantity = order.quantity
            order.filled_price = order.estimated_price
            order.broker_order_id = "broker-large"
        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=20000.0,
            total_sold=0.0,
            total_commission=10.0,
            message="large filled",
        )


class StubPostExecutionSyncService:
    result = {
        "status": "MATCHED",
        "broker_name": "ibkr",
        "broker_role": "primary",
        "checked_at": "2026-12-30T09:35:00+00:00",
        "difference_count": 0,
        "requires_manual_intervention": False,
        "local_state_source": "expected_post_execution",
    }

    def __init__(self, db):
        self.db = db

    def reconcile_expected_state(self, **kwargs):
        result = dict(self.result)
        result["kwargs"] = kwargs
        return result


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

    def to_insurance_signal(self, signal):
        from engine.insurance import InsuranceSignal, SignalSeverity

        return InsuranceSignal(
            source="market",
            severity=SignalSeverity.INFO,
            score=0.0,
            weight=0.40,
            hard_veto=False,
            reason="market risk normal",
            evidence={"drawdown": signal.current_dd},
        )


def test_non_whitelisted_pending_execution_fails_closed(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    temp_db.update_portfolio("us", stability_balance=10_000.0)
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

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
    _seed_broker_sync(temp_db, checked_day=stale_date)
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
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

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
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

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
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

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


def test_insurance_locked_blocks_core_rebalance(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    class LockedInsuranceLayer:
        def __init__(self, current_state=None):
            pass

        def evaluate(self, signals, approved_recovery=False):
            from engine.insurance import InsuranceAssessment, InsuranceDecision, InsuranceState
            assessment = InsuranceAssessment(
                state=InsuranceState.LOCKED,
                risk_score=1.0,
                weighted_signals=[],
                hard_blocks=["AUTHORITY_BYPASS_ATTEMPT"],
                reasons=["test locked"],
            )
            decision = InsuranceDecision(
                state=InsuranceState.LOCKED,
                allow_observation=True,
                allow_suggestions=True,
                allow_core_rebalance=False,
                allow_risk_reducing_rebalance=False,
                allow_live_execution=False,
                allow_alpha_execution=False,
                allow_withdrawal_request=False,
                allow_withdrawal_approval=False,
                allow_withdrawal_execution=False,
                allow_deposit=False,
                allow_tax_harvest=False,
                force_de_risk=False,
                force_cash_floor=False,
                block_trading=True,
                freeze_execution=True,
                require_manual_review=True,
                require_recovery_proposal=True,
                reasons=["test locked"],
            )
            return assessment, decision

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "InsuranceLayer", LockedInsuranceLayer)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["insurance"]["state"] == "LOCKED"
    assert "Insurance Layer blocked Core rebalance" in result["action"]


def test_year_end_rebalance_only_triggers_once_per_year(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    market_cls = _make_market_service("2026-12-30")
    monkeypatch.setattr(runner_module, "MarketDataService", market_cls)
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "RiskEngine", CaptureRiskEngine)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    runner = runner_module.DailyRunner(temp_db)
    first = runner.run_portfolio("us")
    assert first["action"] == "年末强制再平衡"

    market_cls.market_date = "2026-12-31"
    _seed_broker_sync(temp_db, checked_day="2026-12-31")
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


def test_daily_runner_executes_shadow_sidecar_without_blocking_report(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    shadow_calls = {}

    def _shadow_observer(**kwargs):
        shadow_calls.update(kwargs)
        temp_db.save_shadow_run_report(
            portfolio_id=kwargs["portfolio_id"],
            broker_role="sandbox",
            broker_name="paper",
            checked_at="2026-12-30T10:00:00+00:00",
            dry_run=True,
            order_count=1,
            reconciliation_status="MATCHED",
            requires_attention=False,
            warnings_json="[]",
            report_json='{"ok":true}',
        )
        return {"broker_name": "paper", "order_count": 1, "dry_run": True}

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "observe_shadow_run", _shadow_observer)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "SUCCESS"
    assert result["shadow_run"]["order_count"] == 1
    assert shadow_calls["portfolio_id"] == "us"
    assert shadow_calls["total_nav"] > 0
    latest_shadow = temp_db.get_latest_shadow_run_report("us")
    assert latest_shadow is not None
    assert latest_shadow["broker_name"] == "paper"


def test_daily_runner_shadow_sidecar_fails_open(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "observe_shadow_run", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("shadow down")))
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "SUCCESS"
    assert result["shadow_run"]["status"] == "FAILED"
    assert "shadow down" in result["shadow_run"]["message"]


def test_missing_primary_broker_sync_fails_closed_before_execution(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    called = {"executor": 0}

    def _executor_factory(**kwargs):
        called["executor"] += 1
        return FilledExecutor()

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", _executor_factory)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["broker_sync_gate"]["code"] == "BROKER_SYNC_MISSING"
    assert called["executor"] == 0


def test_stale_primary_broker_sync_fails_closed_before_execution(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-29")

    called = {"executor": 0}

    def _executor_factory(**kwargs):
        called["executor"] += 1
        return FilledExecutor()

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", _executor_factory)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["broker_sync_gate"]["code"] == "BROKER_SYNC_STALE"
    assert called["executor"] == 0


def test_primary_broker_drift_fails_closed_before_execution(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(
        temp_db,
        checked_day="2026-12-30",
        status="DRIFT",
        items_json='[{"category":"position","key":"SPY","delta":200,"threshold":50}]',
        report_json='{"requires_manual_intervention": true}',
    )

    called = {"executor": 0}

    def _executor_factory(**kwargs):
        called["executor"] += 1
        return FilledExecutor()

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", _executor_factory)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["broker_sync_gate"]["code"] == "BROKER_RECONCILIATION_DRIFT"
    assert called["executor"] == 0


def test_primary_broker_broken_fails_closed_before_execution(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(
        temp_db,
        checked_day="2026-12-30",
        status="BROKEN",
        items_json='[{"category":"nav","key":"total_value","delta":2000,"threshold":1}]',
        report_json='{"requires_manual_intervention": true}',
    )

    called = {"executor": 0}

    def _executor_factory(**kwargs):
        called["executor"] += 1
        return FilledExecutor()

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", _executor_factory)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["broker_sync_gate"]["code"] == "BROKER_RECONCILIATION_BROKEN"
    assert called["executor"] == 0


def test_broker_sync_time_policy_is_configurable_per_market(temp_db, monkeypatch):
    temp_db.ensure_portfolio("cn", ["510300.SS", "511010.SS", "518880.SS", "MONEY"])
    _seed_broker_sync(
        temp_db,
        portfolio_id="cn",
        checked_day="2026-12-29",
        broker_name="futu",
        currency="CNY",
    )

    monkeypatch.setitem(PORTFOLIOS["cn"]["broker_sync_policy"], "require_snapshot_cover_market_date", False)
    monkeypatch.setitem(PORTFOLIOS["cn"]["broker_sync_policy"], "require_reconciliation_cover_market_date", False)
    monkeypatch.setitem(PORTFOLIOS["cn"]["broker_sync_policy"], "max_snapshot_lag_days", 1)
    monkeypatch.setitem(PORTFOLIOS["cn"]["broker_sync_policy"], "max_reconciliation_lag_days", 1)
    validate_config()

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("cn")

    assert result["run_status"] == "SUCCESS"
    assert result["action"] == "年末强制再平衡"


def test_live_execution_market_must_be_whitelisted_per_portfolio(temp_db, monkeypatch):
    temp_db.ensure_portfolio("cn", ["510300.SS", "511010.SS", "518880.SS", "MONEY"])
    _seed_broker_sync(
        temp_db,
        portfolio_id="cn",
        checked_day="2026-12-30",
        broker_name="futu",
        currency="CNY",
    )

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledBrokerReceiptExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("cn")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["execution_policy_gate"]["code"] == "LIVE_EXECUTION_MARKET_DISABLED"
    assert result["execution"]["status"] == "FAILED"
    assert "尚未纳入真实执行白名单" in result["action"]

    with temp_db._conn() as conn:
        tx_row = conn.execute(
            "SELECT type FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("cn",),
        ).fetchone()
    assert tx_row["type"] == "REBALANCE_BLOCKED"


def test_live_execution_blocks_single_order_notional_above_limit(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    LargeOrderExecutor.executed = False
    monkeypatch.setitem(PORTFOLIOS["us"]["live_execution_policy"], "max_single_order_notional", 10000.0)
    validate_config()
    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: LargeOrderExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["execution_policy_gate"]["code"] == "LIVE_EXECUTION_SINGLE_ORDER_TOO_LARGE"
    assert result["execution"]["status"] == "FAILED"
    assert LargeOrderExecutor.executed is False
    assert "单笔金额过大" in result["action"]

    with temp_db._conn() as conn:
        tx_row = conn.execute(
            "SELECT type FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()
    assert tx_row["type"] == "REBALANCE_BLOCKED"


def test_post_execution_reconciliation_must_match_before_local_rebalance(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledBrokerReceiptExecutor())
    monkeypatch.setattr(runner_module, "BrokerSyncService", StubPostExecutionSyncService)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "SUCCESS"
    assert result["post_execution_reconciliation"]["status"] == "MATCHED"
    assert result["post_execution_reconciliation"]["local_state_source"] == "expected_post_execution"


def test_successful_live_execution_persists_broker_audit_events(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledBrokerAuditExecutor())
    monkeypatch.setattr(runner_module, "BrokerSyncService", StubPostExecutionSyncService)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "SUCCESS"
    assert result["execution"]["broker_event_count"] == 2
    assert result["execution"]["orders"][0]["broker_order_id"] == "broker-1"
    assert result["execution"]["orders"][0]["broker_reference"] == "perm-1"

    rows = temp_db.list_broker_execution_events("us", run_date="2026-12-30")

    assert len(rows) == 2
    assert rows[0]["event_type"] in {"STATUS_POLL", "SUBMIT_RESULT"}
    assert {row["event_type"] for row in rows} == {"SUBMIT_RESULT", "STATUS_POLL"}


def test_post_execution_reconciliation_drift_enters_manual_review(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    class DriftSyncService(StubPostExecutionSyncService):
        result = {
            "status": "DRIFT",
            "broker_name": "ibkr",
            "broker_role": "primary",
            "checked_at": "2026-12-30T09:35:00+00:00",
            "difference_count": 1,
            "requires_manual_intervention": True,
            "local_state_source": "expected_post_execution",
        }

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledBrokerReceiptExecutor())
    monkeypatch.setattr(runner_module, "BrokerSyncService", DriftSyncService)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "MANUAL_INTERVENTION_REQUIRED"
    assert result["manual_intervention_reasons"][0]["code"] == "POST_EXECUTION_RECONCILIATION_DRIFT"
    assert result["post_execution_reconciliation"]["status"] == "DRIFT"

    with temp_db._conn() as conn:
        tx_row = conn.execute(
            "SELECT type FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()
    assert tx_row["type"] == "REBALANCE_MANUAL_REVIEW"


def test_post_execution_reconciliation_broken_fails_closed(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    class BrokenSyncService(StubPostExecutionSyncService):
        result = {
            "status": "BROKEN",
            "broker_name": "ibkr",
            "broker_role": "primary",
            "checked_at": "2026-12-30T09:35:00+00:00",
            "difference_count": 2,
            "requires_manual_intervention": True,
            "local_state_source": "expected_post_execution",
        }

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledBrokerReceiptExecutor())
    monkeypatch.setattr(runner_module, "BrokerSyncService", BrokenSyncService)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["action"] == "执行后对账失败: 券商实仓与预期账本严重不一致"
    assert result["post_execution_reconciliation"]["status"] == "BROKEN"

    with temp_db._conn() as conn:
        tx_row = conn.execute(
            "SELECT type FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()
    assert tx_row["type"] == "REBALANCE_FAILED"
