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
import logging
import os
import shutil
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np

from db.database import Database
from engine.cashflow import build_stability_signal
from engine.config import MAX_EXECUTION_SLIPPAGE_PCT, PORTFOLIOS
from engine.data_validator import DataValidationError, validate_prices, validate_returns
from engine.execution.base import ExecutionResult, OrderSide, OrderStatus
from engine.execution.factory import create_executor, get_broker_topology
from engine.execution.sync import BrokerSyncService
from engine.insurance import InsuranceLayer, InsuranceState
from engine.market_data import MarketDataService
from engine.notifier import notify
from engine.portfolio import PortfolioEngine
from engine.risk import RiskEngine
from engine.tax_optimizer import TaxLossHarvester
from runner.shadow_run import observe_shadow_run

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


class DailyRunner:
    def __init__(self, db: Database = None):
        self.db = db or Database()

    def _get_executor_mode(self) -> str:
        return os.environ.get("XIRANG_EXECUTOR", "paper").lower()

    def _interactive_core_mode_allowed(self) -> bool:
        return os.environ.get("XIRANG_ALLOW_CORE_INTERACTIVE", "").lower() in {"1", "true", "yes"}

    def _shadow_run_enabled(self) -> bool:
        return os.environ.get("XIRANG_ENABLE_SHADOW_RUN", "1").lower() not in {"0", "false", "no"}

    def _get_market_date(self, prices) -> date:
        latest = prices.index[-1]
        return latest.date() if hasattr(latest, "date") else latest

    def _should_trigger_year_end_rebalance(self, portfolio_id: str, market_date: date) -> bool:
        if market_date.month != 12 or market_date.day < 28:
            return False
        return not self.db.has_year_end_rebalance(market_date.year, portfolio_id)

    def _should_run_tax_harvest(self, portfolio_id: str) -> bool:
        if portfolio_id != "us":
            return False
        return os.environ.get("XIRANG_ENABLE_TAX_HARVEST", "").lower() in {"1", "true", "yes"}

    def _build_manual_intervention(self, code: str, message: str) -> dict:
        return {"code": code, "message": message}

    def _build_fail_closed(self, code: str, message: str, **extra) -> dict:
        issue = {"code": code, "message": message}
        issue.update(extra)
        return issue

    def _get_broker_sync_policy(self, portfolio_id: str) -> dict:
        policy = PORTFOLIOS[portfolio_id].get("broker_sync_policy", {})
        return {
            "required_role": str(policy.get("required_role", "primary")).lower(),
            "require_snapshot_cover_market_date": bool(policy.get("require_snapshot_cover_market_date", True)),
            "require_reconciliation_cover_market_date": bool(policy.get("require_reconciliation_cover_market_date", True)),
            "max_snapshot_lag_days": int(policy.get("max_snapshot_lag_days", 0)),
            "max_reconciliation_lag_days": int(policy.get("max_reconciliation_lag_days", 0)),
        }

    def _get_live_execution_policy(self, portfolio_id: str) -> dict:
        policy = PORTFOLIOS[portfolio_id].get("live_execution_policy", {})
        return {
            "enabled": bool(policy.get("enabled", False)),
            "allowed_assets": list(policy.get("allowed_assets", [])),
            "allowed_order_sides": [str(side).upper() for side in policy.get("allowed_order_sides", ["BUY", "SELL"])],
            "max_single_order_notional": float(policy.get("max_single_order_notional", 0.0)),
            "max_daily_order_count": int(policy.get("max_daily_order_count", 0)),
            "max_daily_turnover_ratio": float(policy.get("max_daily_turnover_ratio", 0.0)),
        }

    def _parse_timestamp(self, value: str | None) -> datetime | None:
        if not value:
            return None
        text = str(value).replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _check_data_freshness(self, portfolio_id: str, market_date: date) -> dict | None:
        max_lag = int(PORTFOLIOS[portfolio_id].get("max_data_lag_days", 3))
        lag_days = (datetime.now().date() - market_date).days
        if lag_days > max_lag:
            return self._build_manual_intervention(
                "STALE_DATA",
                f"数据过期: 最新交易日 {market_date.isoformat()}，距今 {lag_days} 天，超过阈值 {max_lag} 天",
            )
        return None

    def _check_primary_broker_sync_gate(self, portfolio_id: str, market_date: date) -> dict | None:
        topology = get_broker_topology()
        policy = self._get_broker_sync_policy(portfolio_id)
        broker_role = policy["required_role"]
        broker_name = topology.get(broker_role, topology.get("primary", "ibkr")).upper()
        snapshot = self.db.get_latest_broker_account_snapshot(portfolio_id, broker_role)
        reconciliation = self.db.get_latest_broker_reconciliation_run(portfolio_id, broker_role)

        if not snapshot or not reconciliation:
            return self._build_fail_closed(
                "BROKER_SYNC_MISSING",
                f"主券商 {broker_name} 尚无完整的只读同步与对账记录，停止本次 Core 调仓",
                broker_role=broker_role,
                broker_name=broker_name,
                status="MISSING",
            )

        snapshot_at = self._parse_timestamp(snapshot.get("snapshot_time") or snapshot.get("created_at"))
        checked_at = self._parse_timestamp(reconciliation.get("checked_at") or reconciliation.get("created_at"))
        if snapshot_at is None or checked_at is None:
            return self._build_fail_closed(
                "BROKER_SYNC_INVALID_TIMESTAMP",
                f"主券商 {broker_name} 的同步时间戳不可解析，停止本次 Core 调仓",
                broker_role=broker_role,
                broker_name=broker_name,
                status=reconciliation.get("status") or "UNKNOWN",
            )

        snapshot_date = snapshot_at.date()
        checked_date = checked_at.date()
        snapshot_lag_days = max(0, (market_date - snapshot_date).days)
        reconciliation_lag_days = max(0, (market_date - checked_date).days)

        if policy["require_snapshot_cover_market_date"] and snapshot_date < market_date:
            return self._build_fail_closed(
                "BROKER_SYNC_STALE",
                (
                    f"主券商 {broker_name} 最新账户同步未覆盖交易日 {market_date.isoformat()} "
                    f"(snapshot={snapshot_date.isoformat()})，停止本次 Core 调仓"
                ),
                broker_role=broker_role,
                broker_name=broker_name,
                status=reconciliation.get("status") or "UNKNOWN",
                snapshot_time=snapshot_at.isoformat(),
                checked_at=checked_at.isoformat(),
                gate_source="snapshot",
                lag_days=snapshot_lag_days,
            )
        if snapshot_lag_days > policy["max_snapshot_lag_days"]:
            return self._build_fail_closed(
                "BROKER_SYNC_STALE",
                (
                    f"主券商 {broker_name} 最新账户同步落后 {snapshot_lag_days} 天，超过组合配置阈值 "
                    f"{policy['max_snapshot_lag_days']} 天，停止本次 Core 调仓"
                ),
                broker_role=broker_role,
                broker_name=broker_name,
                status=reconciliation.get("status") or "UNKNOWN",
                snapshot_time=snapshot_at.isoformat(),
                checked_at=checked_at.isoformat(),
                gate_source="snapshot",
                lag_days=snapshot_lag_days,
            )
        if policy["require_reconciliation_cover_market_date"] and checked_date < market_date:
            return self._build_fail_closed(
                "BROKER_SYNC_STALE",
                (
                    f"主券商 {broker_name} 最新对账未覆盖交易日 {market_date.isoformat()} "
                    f"(reconciliation={checked_date.isoformat()})，停止本次 Core 调仓"
                ),
                broker_role=broker_role,
                broker_name=broker_name,
                status=reconciliation.get("status") or "UNKNOWN",
                snapshot_time=snapshot_at.isoformat(),
                checked_at=checked_at.isoformat(),
                gate_source="reconciliation",
                lag_days=reconciliation_lag_days,
            )
        if reconciliation_lag_days > policy["max_reconciliation_lag_days"]:
            return self._build_fail_closed(
                "BROKER_SYNC_STALE",
                (
                    f"主券商 {broker_name} 最新对账落后 {reconciliation_lag_days} 天，超过组合配置阈值 "
                    f"{policy['max_reconciliation_lag_days']} 天，停止本次 Core 调仓"
                ),
                broker_role=broker_role,
                broker_name=broker_name,
                status=reconciliation.get("status") or "UNKNOWN",
                snapshot_time=snapshot_at.isoformat(),
                checked_at=checked_at.isoformat(),
                gate_source="reconciliation",
                lag_days=reconciliation_lag_days,
            )

        status_value = str(reconciliation.get("status") or "MISSING").upper()
        if status_value == "MATCHED":
            return None
        if status_value == "DRIFT":
            return self._build_fail_closed(
                "BROKER_RECONCILIATION_DRIFT",
                f"主券商 {broker_name} 对账仍存在漂移，系统停止本次 Core 调仓",
                broker_role=broker_role,
                broker_name=broker_name,
                status=status_value,
                checked_at=checked_at.isoformat(),
            )
        return self._build_fail_closed(
            "BROKER_RECONCILIATION_BROKEN",
            f"主券商 {broker_name} 对账已破裂，系统停止本次 Core 调仓",
            broker_role=broker_role,
                broker_name=broker_name,
                status=status_value,
                checked_at=checked_at.isoformat(),
            )

    def _build_blocked_execution_result(self, orders: list, message: str) -> ExecutionResult:
        total_bought = sum(order.estimated_amount for order in orders if order.side == OrderSide.BUY)
        total_sold = sum(order.estimated_amount for order in orders if order.side == OrderSide.SELL)
        return ExecutionResult(
            success=False,
            orders=orders,
            total_bought=float(total_bought),
            total_sold=float(total_sold),
            total_commission=0.0,
            message=message,
        )

    def _check_live_execution_policy(
        self,
        *,
        portfolio_id: str,
        executor_mode: str,
        order,
        trade_orders: list,
    ) -> dict | None:
        if executor_mode not in {"semi_auto", "auto"}:
            return None

        policy = self._get_live_execution_policy(portfolio_id)
        market_name = PORTFOLIOS[portfolio_id]["name"]

        if not policy["enabled"]:
            return self._build_fail_closed(
                "LIVE_EXECUTION_MARKET_DISABLED",
                f"{market_name} 尚未纳入真实执行白名单，停止本次 Core 调仓",
                portfolio_id=portfolio_id,
            )

        disallowed_assets = sorted({item.symbol for item in trade_orders if item.symbol not in policy["allowed_assets"]})
        if disallowed_assets:
            return self._build_fail_closed(
                "LIVE_EXECUTION_ASSET_NOT_ALLOWED",
                f"真实执行资产未纳入白名单: {', '.join(disallowed_assets)}",
                assets=disallowed_assets,
            )

        disallowed_sides = sorted({item.side.value for item in trade_orders if item.side.value not in policy["allowed_order_sides"]})
        if disallowed_sides:
            return self._build_fail_closed(
                "LIVE_EXECUTION_ACTION_NOT_ALLOWED",
                f"真实执行动作未纳入白名单: {', '.join(disallowed_sides)}",
                order_sides=disallowed_sides,
            )

        order_count = len(trade_orders)
        if order_count > policy["max_daily_order_count"]:
            return self._build_fail_closed(
                "LIVE_EXECUTION_ORDER_COUNT_EXCEEDED",
                (
                    f"真实执行订单数 {order_count} 超过当日上限 "
                    f"{policy['max_daily_order_count']}，停止本次 Core 调仓"
                ),
                order_count=order_count,
                max_daily_order_count=policy["max_daily_order_count"],
            )

        largest_order = None
        largest_notional = 0.0
        for item in trade_orders:
            notional = float(item.estimated_amount or 0.0)
            if notional > largest_notional:
                largest_notional = notional
                largest_order = item

        if largest_order and largest_notional > policy["max_single_order_notional"]:
            return self._build_fail_closed(
                "LIVE_EXECUTION_SINGLE_ORDER_TOO_LARGE",
                (
                    f"真实执行单笔金额过大: {largest_order.symbol} 预估 {largest_notional:,.2f}，"
                    f"超过上限 {policy['max_single_order_notional']:,.2f}"
                ),
                symbol=largest_order.symbol,
                estimated_amount=round(largest_notional, 4),
                max_single_order_notional=policy["max_single_order_notional"],
            )

        turnover = float(getattr(order, "turnover", 0.0) or 0.0)
        if turnover > policy["max_daily_turnover_ratio"]:
            return self._build_fail_closed(
                "LIVE_EXECUTION_TURNOVER_TOO_LARGE",
                (
                    f"真实执行当日换手 {turnover:.2%} 超过上限 "
                    f"{policy['max_daily_turnover_ratio']:.2%}，停止本次 Core 调仓"
                ),
                turnover=round(turnover, 6),
                max_daily_turnover_ratio=policy["max_daily_turnover_ratio"],
            )

        return None

    def _check_execution_slippage(self, execution_result) -> dict | None:
        if execution_result is None:
            return None

        worst_order = None
        worst_slippage = 0.0
        for order in execution_result.orders:
            if order.status != OrderStatus.FILLED:
                continue
            if order.filled_price is None or order.estimated_price <= 0:
                continue
            slippage = abs(order.filled_price - order.estimated_price) / order.estimated_price
            if slippage > worst_slippage:
                worst_slippage = slippage
                worst_order = order

        if worst_order and worst_slippage > MAX_EXECUTION_SLIPPAGE_PCT:
            return self._build_manual_intervention(
                "EXCESSIVE_SLIPPAGE",
                f"成交偏差过大: {worst_order.symbol} 偏差 {worst_slippage:.2%}，超过阈值 {MAX_EXECUTION_SLIPPAGE_PCT:.2%}",
            )
        return None

    def _check_broker_receipts(self, executor_mode: str, execution_result) -> dict | None:
        if execution_result is None or executor_mode not in {"semi_auto", "auto"}:
            return None

        missing = []
        for order in execution_result.orders:
            if not order.broker_order_id:
                missing.append(order.symbol)
                continue
            if order.status == OrderStatus.FILLED and (order.filled_price is None or order.filled_quantity is None):
                missing.append(order.symbol)

        if missing:
            return self._build_manual_intervention(
                "MISSING_BROKER_RECEIPT",
                f"券商回执缺失: {', '.join(missing)} 未返回完整订单号或成交回执",
            )
        return None

    def _detect_manual_intervention(
        self,
        portfolio_id: str,
        market_date: date,
        executor_mode: str,
        execution_result,
    ) -> list[dict]:
        issues = []
        stale = self._check_data_freshness(portfolio_id, market_date)
        if stale:
            issues.append(stale)
        receipt = self._check_broker_receipts(executor_mode, execution_result)
        if receipt:
            issues.append(receipt)
        slippage = self._check_execution_slippage(execution_result)
        if slippage:
            issues.append(slippage)
        return issues

    def _classify_execution(self, execution_result) -> str:
        if execution_result is None:
            return "FILLED"
        if not execution_result.success:
            return "FAILED"
        if not execution_result.orders:
            return "FILLED"

        statuses = {order.status for order in execution_result.orders}
        if statuses == {OrderStatus.FILLED}:
            return "FILLED"
        if OrderStatus.FAILED in statuses or OrderStatus.CANCELLED in statuses:
            return "FAILED"
        return "PENDING"

    def _run_post_execution_reconciliation(
        self,
        *,
        portfolio_id: str,
        engine,
        order,
        actual_friction_cost: float,
    ) -> dict:
        policy = self._get_broker_sync_policy(portfolio_id)
        assets = PORTFOLIOS[portfolio_id]["assets"]
        expected_core_after_cost = max(float(engine.core_nav) - float(actual_friction_cost), 0.0)
        expected_positions = {
            asset: float(order.target_weights[index]) * expected_core_after_cost
            for index, asset in enumerate(assets)
        }
        sync_service = BrokerSyncService(self.db)
        result = sync_service.reconcile_expected_state(
            portfolio_id=portfolio_id,
            local_positions=expected_positions,
            local_cash=0.0,
            broker_role=policy["required_role"],
        )
        result["expected_local_nav"] = round(expected_core_after_cost, 4)
        return result

    def _run_shadow_sidecar(
        self,
        *,
        portfolio_id: str,
        market,
        assets: list[str],
        today: str,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> dict | None:
        if not self._shadow_run_enabled():
            return None

        try:
            return observe_shadow_run(
                db=self.db,
                portfolio_id=portfolio_id,
                market=market,
                assets=assets,
                market_day=today,
                current_positions=current_positions,
                target_weights=target_weights,
                total_nav=total_nav,
                current_prices=current_prices,
            )
        except Exception as exc:
            logger.warning("  影子运行旁路失败，不影响主链路: %s: %s", type(exc).__name__, exc)
            return {
                "status": "FAILED",
                "message": f"{type(exc).__name__}: {exc}",
            }

    def _build_early_exit_report(
        self,
        *,
        today: str,
        portfolio_id: str,
        name: str,
        currency: str,
        engine,
        action: str,
        run_status: str,
        manual_intervention_reasons: list[dict],
        broker_sync_gate: dict | None = None,
    ) -> dict:
        report = {
            "date": today,
            "portfolio": portfolio_id,
            "name": name,
            "state": engine.state,
            "nav": round(engine.nav, 2),
            "core_nav": round(engine.core_nav, 2),
            "stability_balance": round(float(engine.stability_balance), 2),
            "currency": currency,
            "action": action,
            "run_status": run_status,
            "manual_intervention_required": bool(manual_intervention_reasons),
            "manual_intervention_reasons": manual_intervention_reasons,
            "broker_sync_policy": self._get_broker_sync_policy(portfolio_id),
            "live_execution_policy": self._get_live_execution_policy(portfolio_id),
        }
        if broker_sync_gate is not None:
            report["broker_sync_gate"] = broker_sync_gate
        return report

    def run_portfolio(self, portfolio_id: str, force: bool = False) -> dict:
        """运行单个组合"""
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]
        name = pf_config["name"]
        currency = pf_config["currency"]

        logger.info(f"── {name} ({portfolio_id}) ──")

        self.db.ensure_portfolio(portfolio_id, assets)
        state = self.db.get_portfolio(portfolio_id)

        stored_positions = np.array(json.loads(state["positions"]), dtype=float)
        engine = PortfolioEngine(initial_capital=float(np.sum(stored_positions)))
        engine.state = state["state"]
        engine.positions = stored_positions
        engine.stability_balance = float(state.get("stability_balance", 0.0))
        engine.cooldown_counter = state["cooldown_counter"]
        engine.rebalance_count = state["rebalance_count"]
        engine.protection_count = state["protection_count"]
        engine.refresh_nav()

        risk = RiskEngine()
        risk.high_water_mark = state["high_water_mark"]

        logger.info(f"  状态: {engine.state}, NAV: {currency}{engine.nav:,.2f}")

        market = MarketDataService(assets=assets, data_source=pf_config.get("data_source"))
        try:
            logger.info("  拉取数据...")
            prices = market.fetch_latest(lookback_days=60)
        except Exception as e:
            today = datetime.now().strftime("%Y-%m-%d")
            msg = f"数据拉取失败: {e}"
            logger.error(f"  {msg}")
            self.db.record_run(today, "FAILED", msg, portfolio_id)
            return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": msg}

        market_date = self._get_market_date(prices)
        today = market_date.strftime("%Y-%m-%d")

        if not force and self.db.has_run_today(today, portfolio_id):
            logger.info(f"  ⏭ {today} 已运行过，跳过。")
            return {"date": today, "portfolio": portfolio_id, "status": "SKIPPED"}

        try:
            validate_prices(prices, assets=assets)
        except DataValidationError as e:
            msg = f"数据校验失败: {e}"
            logger.error(f"  {msg}")
            self.db.record_run(today, "FAILED", msg, portfolio_id)
            return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": msg}

        manual_intervention = self._check_data_freshness(portfolio_id, market_date)
        if manual_intervention:
            report = self._build_early_exit_report(
                today=today,
                portfolio_id=portfolio_id,
                name=name,
                currency=currency,
                engine=engine,
                action=f"人工介入: {manual_intervention['message']}",
                run_status="MANUAL_INTERVENTION_REQUIRED",
                manual_intervention_reasons=[manual_intervention],
            )
            self.db.record_run(today, "MANUAL_INTERVENTION_REQUIRED", json.dumps(report), portfolio_id)
            notify(report)
            return report

        broker_sync_gate = self._check_primary_broker_sync_gate(portfolio_id, market_date)
        if broker_sync_gate:
            report = self._build_early_exit_report(
                today=today,
                portfolio_id=portfolio_id,
                name=name,
                currency=currency,
                engine=engine,
                action=f"系统拦截: {broker_sync_gate['message']}",
                run_status="FAILED_EXECUTION",
                manual_intervention_reasons=[],
                broker_sync_gate=broker_sync_gate,
            )
            self.db.record_run(today, "FAILED_EXECUTION", json.dumps(report), portfolio_id)
            notify(report)
            return report

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
        logger.info("  ✓ 数据校验通过")

        sim_core_nav = float(np.sum(engine.positions * (1 + daily_returns)))
        sim_nav = sim_core_nav + float(engine.stability_balance)
        risk_signal = risk.evaluate(
            nav=sim_nav,
            spy_tlt_corr=indicators["spy_tlt_corr"],
            spy_30d_ret=indicators["spy_30d_ret"],
            tlt_30d_ret=indicators["tlt_30d_ret"],
        )
        logger.info(f"  回撤: {risk_signal.current_dd:.2%}")
        if risk_signal.trigger_reason:
            logger.warning(f"  ⚠ 风控: {risk_signal.trigger_reason}")

        insurance_signals = [
            risk.to_insurance_signal(risk_signal),
            build_stability_signal(
                stability_balance=float(engine.stability_balance),
                nav=sim_nav,
            ),
        ]
        insurance = InsuranceLayer(current_state=InsuranceState.SAFE)
        insurance_assessment, insurance_decision = insurance.evaluate(insurance_signals)

        current_prices = {asset: float(prices[asset].iloc[-1]) for asset in assets}

        engine.apply_daily_returns(daily_returns)
        pre_order_state = {
            "state": engine.state,
            "cooldown_counter": engine.cooldown_counter,
            "protection_count": engine.protection_count,
        }

        order = engine.evaluate_rebalance(
            risk_signal=risk_signal,
            is_year_end=self._should_trigger_year_end_rebalance(portfolio_id, market_date),
        )

        execution_result = None
        execution_status = "FILLED"
        action = None
        tx_type = None
        friction_cost = 0.0
        executor_mode = self._get_executor_mode()
        manual_intervention_reasons = []
        shadow_run_report = None
        post_execution_reconciliation = None
        execution_policy_gate = None

        if order:
            current_positions = {asset: float(value) for asset, value in zip(assets, engine.positions.tolist())}
            shadow_run_report = self._run_shadow_sidecar(
                portfolio_id=portfolio_id,
                market=market,
                assets=assets,
                today=today,
                current_positions=current_positions,
                target_weights=order.target_weights,
                total_nav=engine.core_nav,
                current_prices=current_prices,
            )
            if not insurance_decision.allow_core_rebalance:
                execution_status = "FAILED"
                action = "Insurance Layer blocked Core rebalance"
                tx_type = "REBALANCE_BLOCKED"
                execution_policy_gate = {
                    "code": "INSURANCE_CORE_REBALANCE_BLOCKED",
                    "message": action,
                    "insurance_state": insurance_decision.state.value,
                    "reasons": insurance_decision.reasons,
                }
            elif executor_mode == "manual" and not self._interactive_core_mode_allowed():
                execution_status = "FAILED"
                action = "策略拦截: Core 常规调仓不允许人工确认模式"
                tx_type = "REBALANCE_BLOCKED"
            else:
                try:
                    executor = create_executor(market_data_service=market, use_twap=False, assets=assets)
                    trade_orders = executor.translate_orders(
                        current_positions=current_positions,
                        target_weights=order.target_weights,
                        total_nav=engine.core_nav,
                        current_prices=current_prices,
                    )
                    execution_policy_gate = self._check_live_execution_policy(
                        portfolio_id=portfolio_id,
                        executor_mode=executor_mode,
                        order=order,
                        trade_orders=trade_orders,
                    )
                    if execution_policy_gate:
                        execution_result = self._build_blocked_execution_result(
                            trade_orders,
                            execution_policy_gate["message"],
                        )
                        execution_status = "FAILED"
                    else:
                        execution_result = executor.execute(trade_orders)
                        execution_status = self._classify_execution(execution_result)
                except Exception as e:
                    execution_status = "FAILED"
                    action = f"执行异常: {type(e).__name__}"
                    tx_type = "REBALANCE_FAILED"
                    logger.error(f"  执行层异常: {e}")

                if execution_policy_gate:
                    action = f"系统拦截: {execution_policy_gate['message']}"
                    tx_type = "REBALANCE_BLOCKED"
                else:
                    manual_intervention_reasons = self._detect_manual_intervention(
                        portfolio_id=portfolio_id,
                        market_date=market_date,
                        executor_mode=executor_mode,
                        execution_result=execution_result,
                    )

                if execution_policy_gate:
                    pass
                elif execution_status == "FILLED" and not manual_intervention_reasons:
                    friction_cost = float(execution_result.total_commission)
                    if executor_mode in {"semi_auto", "auto"}:
                        try:
                            post_execution_reconciliation = self._run_post_execution_reconciliation(
                                portfolio_id=portfolio_id,
                                engine=engine,
                                order=order,
                                actual_friction_cost=friction_cost,
                            )
                        except Exception as exc:
                            manual_intervention_reasons.append(
                                self._build_manual_intervention(
                                    "POST_EXECUTION_RECONCILIATION_UNAVAILABLE",
                                    f"执行后对账不可用: {type(exc).__name__}: {exc}",
                                )
                            )
                            execution_status = "MANUAL_REVIEW"
                            action = f"人工介入: {manual_intervention_reasons[0]['message']}"
                            tx_type = "REBALANCE_MANUAL_REVIEW"
                        else:
                            recon_status = str(post_execution_reconciliation.get("status") or "MISSING").upper()
                            if recon_status == "MATCHED":
                                engine.apply_rebalance(order, actual_friction_cost=friction_cost)
                                action = order.reason
                                tx_type = "REBALANCE"
                            elif recon_status == "DRIFT":
                                manual_intervention_reasons.append(
                                    self._build_manual_intervention(
                                        "POST_EXECUTION_RECONCILIATION_DRIFT",
                                        "执行后对账未闭环: 券商实仓与预期账本仍存在漂移",
                                    )
                                )
                                execution_status = "MANUAL_REVIEW"
                                action = f"人工介入: {manual_intervention_reasons[0]['message']}"
                                tx_type = "REBALANCE_MANUAL_REVIEW"
                            else:
                                execution_status = "FAILED"
                                action = "执行后对账失败: 券商实仓与预期账本严重不一致"
                                tx_type = "REBALANCE_FAILED"
                    else:
                        engine.apply_rebalance(order, actual_friction_cost=friction_cost)
                        action = order.reason
                        tx_type = "REBALANCE"
                elif manual_intervention_reasons:
                    execution_status = "MANUAL_REVIEW"
                    action = f"人工介入: {manual_intervention_reasons[0]['message']}"
                    tx_type = "REBALANCE_MANUAL_REVIEW"
                elif execution_status == "PENDING":
                    action = f"执行失败: {order.reason} 未获得最终成交回执"
                    tx_type = "REBALANCE_FAILED"
                elif action is None:
                    action = f"执行失败: {order.reason}"
                    tx_type = "REBALANCE_FAILED"

            if execution_status != "FILLED":
                engine.state = pre_order_state["state"]
                engine.cooldown_counter = pre_order_state["cooldown_counter"]
                engine.protection_count = pre_order_state["protection_count"]
                engine.refresh_nav()
        else:
            shadow_run_report = self._run_shadow_sidecar(
                portfolio_id=portfolio_id,
                market=market,
                assets=assets,
                today=today,
                current_positions={asset: float(value) for asset, value in zip(assets, engine.positions.tolist())},
                target_weights=engine.weights.tolist(),
                total_nav=engine.core_nav,
                current_prices=current_prices,
            )
            engine.refresh_nav()

        tax_harvest_result = None
        if self._should_run_tax_harvest(portfolio_id):
            logger.info("  执行年末税务优化...")
            harvester = TaxLossHarvester(self.db, min_loss_pct=0.05)
            tax_harvest_result = harvester.run_year_end_harvest(
                portfolio_id=portfolio_id,
                current_prices=current_prices,
                current_date=today,
            )

            if tax_harvest_result.total_loss_harvested > 0:
                logger.info(
                    f"  ✓ 税损收割: {len(tax_harvest_result.harvested_positions)} 个持仓, "
                    f"收割 ${tax_harvest_result.total_loss_harvested:,.2f}, "
                    f"预估节税 ${tax_harvest_result.estimated_tax_saved:,.2f}"
                )
            else:
                logger.info(f"  ✓ 税务优化: {tax_harvest_result.message}")

            reversed_count = harvester.check_and_reverse_harvests(today, portfolio_id)
            if reversed_count > 0:
                logger.info(f"  ✓ 换回税损收割: {reversed_count} 个")
        elif portfolio_id == "us":
            logger.info("  税务优化默认关闭，待真实账本闭环后再启用。")

        engine.record_snapshot(market_date, risk_signal, action=action)

        run_status = "SUCCESS"
        if execution_status == "MANUAL_REVIEW":
            run_status = "MANUAL_INTERVENTION_REQUIRED"
        elif execution_status in {"FAILED", "PENDING"}:
            run_status = "FAILED_EXECUTION"

        report = {
            "date": today,
            "portfolio": portfolio_id,
            "name": name,
            "state": engine.state,
            "nav": round(engine.nav, 2),
            "core_nav": round(engine.core_nav, 2),
            "stability_balance": round(float(engine.stability_balance), 2),
            "currency": currency,
            "weights": {a: round(w, 4) for a, w in zip(assets, engine.weights.tolist())},
            "drawdown": round(risk_signal.current_dd, 4),
            "spy_tlt_corr": round(indicators["spy_tlt_corr"], 4),
            "action": action,
            "rebalance_count": engine.rebalance_count,
            "protection_count": engine.protection_count,
            "run_status": run_status,
            "manual_intervention_required": bool(manual_intervention_reasons),
            "manual_intervention_reasons": manual_intervention_reasons,
            "broker_sync_policy": self._get_broker_sync_policy(portfolio_id),
            "live_execution_policy": self._get_live_execution_policy(portfolio_id),
            "insurance": {
                "state": insurance_decision.state.value,
                "risk_score": round(float(insurance_assessment.risk_score), 6),
                "hard_blocks": insurance_assessment.hard_blocks,
                "reasons": insurance_decision.reasons,
            },
        }

        if execution_result is not None:
            report["execution"] = {
                "status": execution_status,
                "message": execution_result.message,
                "orders": [
                    {
                        "symbol": order_item.symbol,
                        "side": order_item.side.value,
                        "quantity": order_item.quantity,
                        "status": order_item.status.value,
                        "filled_quantity": order_item.filled_quantity,
                        "filled_price": order_item.filled_price,
                        "broker_order_id": order_item.broker_order_id,
                        "broker_reference": order_item.broker_reference,
                        "error_message": order_item.error_message,
                    }
                    for order_item in execution_result.orders
                ],
                "total_commission": round(float(execution_result.total_commission), 4),
            }
            if execution_result.broker_events:
                report["execution"]["broker_event_count"] = len(execution_result.broker_events)

        if execution_policy_gate is not None:
            report["execution_policy_gate"] = execution_policy_gate

        if post_execution_reconciliation is not None:
            report["post_execution_reconciliation"] = {
                "status": post_execution_reconciliation.get("status"),
                "broker_name": post_execution_reconciliation.get("broker_name"),
                "broker_role": post_execution_reconciliation.get("broker_role"),
                "checked_at": post_execution_reconciliation.get("checked_at"),
                "difference_count": int(post_execution_reconciliation.get("difference_count") or 0),
                "requires_manual_intervention": bool(post_execution_reconciliation.get("requires_manual_intervention")),
                "local_state_source": post_execution_reconciliation.get("local_state_source"),
                "expected_local_nav": post_execution_reconciliation.get("expected_local_nav"),
            }

        if shadow_run_report is not None:
            report["shadow_run"] = shadow_run_report

        if tax_harvest_result:
            report["tax_harvest"] = {
                "harvested_count": len(tax_harvest_result.harvested_positions),
                "total_loss": round(tax_harvest_result.total_loss_harvested, 2),
                "estimated_tax_saved": round(tax_harvest_result.estimated_tax_saved, 2),
            }

        try:
            with self.db.transaction() as conn:
                self.db.update_portfolio(
                    portfolio_id=portfolio_id,
                    state=engine.state,
                    nav=engine.nav,
                    positions=json.dumps(engine.positions.tolist()),
                    stability_balance=engine.stability_balance,
                    high_water_mark=risk.high_water_mark,
                    cooldown_counter=engine.cooldown_counter,
                    rebalance_count=engine.rebalance_count,
                    protection_count=engine.protection_count,
                    conn=conn,
                )
                self.db.save_snapshot(
                    date=today,
                    state=engine.state,
                    nav=engine.nav,
                    positions=engine.positions.tolist(),
                    weights=engine.weights.tolist(),
                    drawdown=risk_signal.current_dd,
                    spy_tlt_corr=indicators["spy_tlt_corr"],
                    action=action,
                    trigger_reason=risk_signal.trigger_reason,
                    portfolio_id=portfolio_id,
                    conn=conn,
                )
                if order:
                    self.db.save_transaction(
                        date=today,
                        tx_type=tx_type,
                        target_weights=order.target_weights,
                        turnover=order.turnover,
                        friction_cost=friction_cost if execution_status == "FILLED" else 0.0,
                        reason=action,
                        portfolio_id=portfolio_id,
                        conn=conn,
                    )
                if execution_result is not None and execution_result.broker_events:
                    self.db.save_broker_execution_events(
                        portfolio_id=portfolio_id,
                        run_date=today,
                        events=execution_result.broker_events,
                        conn=conn,
                    )
                if risk_signal.trigger_reason:
                    sev = "HIGH" if risk_signal.is_hard_stop else "MEDIUM" if risk_signal.is_protection else "LOW"
                    self.db.save_risk_event(
                        date=today,
                        event_type=risk_signal.trigger_reason.split(":")[0].strip(),
                        severity=sev,
                        drawdown=risk_signal.current_dd,
                        spy_tlt_corr=risk_signal.spy_tlt_corr,
                        action_taken=action or "无操作",
                        portfolio_id=portfolio_id,
                        conn=conn,
                    )
                self.db.record_run(today, run_status, json.dumps(report), portfolio_id, conn=conn)
        except Exception as e:
            logger.error(f"  数据库事务失败: {e}")
            return {"date": today, "portfolio": portfolio_id, "status": "FAILED", "reason": f"数据库错误: {e}"}

        logger.info(f"  NAV: {currency}{report['nav']:,.2f} | {report['state']}")
        logger.info(f"  操作: {action or '无'}")

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

        backup_dir = Path("db/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"xirang_{today.replace('-', '')}.db"
        shutil.copy2(self.db.db_path, backup_path)
        for old in sorted(backup_dir.glob("xirang_*.db"))[:-30]:
            old.unlink()
        logger.info("✓ 数据库已备份")
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
