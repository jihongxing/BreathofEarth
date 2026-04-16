"""
息壤（Xi-Rang）组合状态机

职责：
1. 维护组合状态（IDLE / PROTECTION）
2. 根据风控信号执行状态转移
3. 计算再平衡指令
4. 记录每一步决策（可审计）
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import date
import numpy as np

from engine.config import (
    ASSETS,
    WEIGHTS_IDLE,
    WEIGHTS_PROTECT,
    WEIGHTS_EMERGENCY,
    DRIFT_THRESHOLD,
    FEE_RATE,
    COOLDOWN_DAYS,
    STATE_IDLE,
    STATE_PROTECTION,
)
from engine.risk import RiskSignal


@dataclass
class PortfolioSnapshot:
    """组合每日快照，用于审计和回放"""
    date: str
    state: str
    nav: float
    positions: list[float]
    weights: list[float]
    drawdown: float
    action: Optional[str] = None
    trigger_reason: Optional[str] = None


@dataclass
class RebalanceOrder:
    """再平衡指令"""
    target_weights: list[float]
    turnover: float
    friction_cost: float
    reason: str


class PortfolioEngine:
    """
    组合状态机引擎。

    状态转移：
        IDLE → PROTECTION（风控触发）
        PROTECTION → IDLE（风控解除 + 冷却期满）

    双层防线：
        常规保护（-12%）→ WEIGHTS_PROTECT（SHV 50%）
        紧急避险（-14%）→ WEIGHTS_EMERGENCY（SHV 75%）
    """

    def __init__(self, initial_capital: float = 100000.0):
        self.state = STATE_IDLE
        self.cooldown_counter = 0
        self.nav = initial_capital
        self.stability_balance = 0.0
        self.positions = np.array(WEIGHTS_IDLE, dtype=float) * initial_capital
        self.rebalance_count = 0
        self.protection_count = 0
        self.snapshots: list[PortfolioSnapshot] = []

    @property
    def weights(self) -> np.ndarray:
        core_nav = self.core_nav
        return self.positions / core_nav if core_nav > 0 else np.zeros(len(ASSETS))

    @property
    def core_nav(self) -> float:
        return float(np.sum(self.positions))

    def refresh_nav(self):
        self.nav = self.core_nav + float(self.stability_balance)

    def apply_daily_returns(self, daily_returns: np.ndarray):
        """仅让 Core 层自然生长，Stability 保持不变。"""
        self.positions = self.positions * (1 + daily_returns)
        self.refresh_nav()

    def evaluate_rebalance(
        self,
        risk_signal: RiskSignal,
        is_year_end: bool = False,
    ) -> Optional[RebalanceOrder]:
        """基于当前状态评估是否需要再平衡。"""
        if self.state == STATE_IDLE:
            return self._handle_idle(risk_signal, is_year_end)
        if self.state == STATE_PROTECTION:
            return self._handle_protection(risk_signal)
        return None

    def apply_rebalance(
        self,
        order: RebalanceOrder,
        actual_friction_cost: Optional[float] = None,
    ):
        """
        执行再平衡。

        如果执行层返回了真实手续费/滑点成本，则优先使用执行结果，
        否则退回到状态机内部估算值。
        """
        friction_cost = order.friction_cost if actual_friction_cost is None else actual_friction_cost
        core_after_cost = max(self.core_nav - friction_cost, 0.0)
        self.positions = np.array(order.target_weights) * core_after_cost
        self.refresh_nav()
        self.rebalance_count += 1

    def record_snapshot(
        self,
        current_date: date,
        risk_signal: RiskSignal,
        action: Optional[str] = None,
    ):
        """记录当前组合快照。"""
        self.snapshots.append(PortfolioSnapshot(
            date=str(current_date),
            state=self.state,
            nav=self.nav,
            positions=self.positions.tolist(),
            weights=self.weights.tolist(),
            drawdown=risk_signal.current_dd,
            action=action,
            trigger_reason=risk_signal.trigger_reason,
        ))

    def step(
        self,
        current_date: date,
        daily_returns: np.ndarray,
        risk_signal: RiskSignal,
        is_year_end: bool = False,
    ) -> Optional[RebalanceOrder]:
        """
        执行一步：资产生长 → 风控判断 → 状态转移 → 再平衡。

        Args:
            current_date: 当前日期
            daily_returns: 各资产当日收益率 [SPY, TLT, GLD, SHV]
            risk_signal: 风控引擎输出的信号
            is_year_end: 是否年末最后一个交易日

        Returns:
            RebalanceOrder 如果执行了再平衡，否则 None
        """
        self.apply_daily_returns(daily_returns)
        order = self.evaluate_rebalance(risk_signal, is_year_end)

        action = None
        if order is not None:
            self.apply_rebalance(order)
            action = order.reason

        self.record_snapshot(current_date, risk_signal, action=action)

        return order

    def _handle_idle(self, signal: RiskSignal, is_year_end: bool) -> Optional[RebalanceOrder]:
        """IDLE 状态下的逻辑"""

        # 硬止损：最高优先级
        if signal.is_hard_stop:
            self.state = STATE_PROTECTION
            self.cooldown_counter = COOLDOWN_DAYS * 2
            self.protection_count += 1
            return self._make_order(WEIGHTS_EMERGENCY, "紧急避险: 硬止损触发")

        # 常规保护
        if signal.is_protection:
            self.state = STATE_PROTECTION
            self.cooldown_counter = COOLDOWN_DAYS
            self.protection_count += 1
            return self._make_order(WEIGHTS_PROTECT, "常规保护: 风控触发")

        # 日常再平衡（阈值或年末）
        max_drift = float(np.max(np.abs(self.weights - np.array(WEIGHTS_IDLE))))
        if max_drift > DRIFT_THRESHOLD:
            return self._make_order(WEIGHTS_IDLE, f"阈值再平衡: 偏离 {max_drift:.2%}")
        if is_year_end:
            return self._make_order(WEIGHTS_IDLE, "年末强制再平衡")

        return None

    def _handle_protection(self, signal: RiskSignal) -> Optional[RebalanceOrder]:
        """PROTECTION 状态下的逻辑"""

        # 回撤继续恶化，升级到紧急避险
        if signal.is_hard_stop:
            current_shv_weight = self.weights[3] if len(self.weights) > 3 else 0
            if current_shv_weight < WEIGHTS_EMERGENCY[3] - 0.05:
                self.cooldown_counter = COOLDOWN_DAYS * 2
                return self._make_order(WEIGHTS_EMERGENCY, "升级紧急避险: 回撤继续恶化")

        # 冷却期倒计时
        if self.cooldown_counter > 0:
            self.cooldown_counter -= 1

        # 解除条件：风控恢复 + 冷却期满
        if not signal.is_protection and self.cooldown_counter == 0:
            self.state = STATE_IDLE
            return self._make_order(WEIGHTS_IDLE, "解除保护: 风控恢复正常")

        return None

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        """生成再平衡指令"""
        target = np.array(target_weights)
        turnover = float(np.sum(np.abs(self.weights - target)) / 2)
        friction_cost = self.core_nav * turnover * FEE_RATE
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=friction_cost,
            reason=reason,
        )
