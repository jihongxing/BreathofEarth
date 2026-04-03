"""
组合状态机引擎测试

测试场景：
1. 状态转移完整性（IDLE ↔ PROTECTION）
2. 冷却期机制
3. 紧急避险升级
4. 年末强制再平衡
5. 漂移阈值再平衡
6. 边界条件和异常情况
"""

import pytest
import numpy as np
from datetime import date

from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskSignal
from engine.config import (
    WEIGHTS_IDLE,
    WEIGHTS_PROTECT,
    WEIGHTS_EMERGENCY,
    DRIFT_THRESHOLD,
    COOLDOWN_DAYS,
    STATE_IDLE,
    STATE_PROTECTION,
    RISK_DD_THRESHOLD,
    HARD_STOP_DD,
)


class TestPortfolioEngine:
    """组合状态机引擎测试套件"""

    def setup_method(self):
        """每个测试前初始化"""
        self.engine = PortfolioEngine(initial_capital=100000.0)
        self.today = date(2025, 1, 15)

    def _make_signal(
        self,
        current_dd=0.0,
        is_protection=False,
        is_hard_stop=False,
        is_corr_breakdown=False,
    ) -> RiskSignal:
        """创建测试用风控信号"""
        trigger_reason = None
        if is_hard_stop:
            trigger_reason = f"硬止损: 回撤 {current_dd:.2%}"
        elif is_protection:
            trigger_reason = f"回撤预警: {current_dd:.2%}"
        elif is_corr_breakdown:
            trigger_reason = "相关性崩溃"

        return RiskSignal(
            current_dd=current_dd,
            spy_tlt_corr=0.2,
            spy_30d_ret=0.01,
            tlt_30d_ret=0.01,
            is_hard_stop=is_hard_stop,
            is_protection=is_protection,
            is_corr_breakdown=is_corr_breakdown,
            trigger_reason=trigger_reason,
        )

    # ── 初始化测试 ──────────────────────────────────────

    def test_initial_state(self):
        """初始状态：IDLE，等权配置"""
        assert self.engine.state == STATE_IDLE
        assert self.engine.nav == 100000.0
        assert self.engine.cooldown_counter == 0
        assert self.engine.rebalance_count == 0
        assert self.engine.protection_count == 0
        np.testing.assert_array_almost_equal(self.engine.weights, WEIGHTS_IDLE)

    # ── 状态转移测试 ──────────────────────────────────────

    def test_idle_to_protection_on_drawdown(self):
        """IDLE → PROTECTION：回撤触发"""
        signal = self._make_signal(current_dd=-0.12, is_protection=True)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert self.engine.state == STATE_PROTECTION
        assert order is not None
        assert order.reason == "常规保护: 风控触发"
        np.testing.assert_array_almost_equal(order.target_weights, WEIGHTS_PROTECT)
        assert self.engine.cooldown_counter == COOLDOWN_DAYS
        assert self.engine.protection_count == 1

    def test_idle_to_protection_on_hard_stop(self):
        """IDLE → PROTECTION：硬止损触发"""
        signal = self._make_signal(current_dd=-0.14, is_hard_stop=True, is_protection=True)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert self.engine.state == STATE_PROTECTION
        assert order is not None
        assert order.reason == "紧急避险: 硬止损触发"
        np.testing.assert_array_almost_equal(order.target_weights, WEIGHTS_EMERGENCY)
        assert self.engine.cooldown_counter == COOLDOWN_DAYS * 2
        assert self.engine.protection_count == 1

    def test_protection_to_idle_after_cooldown(self):
        """PROTECTION → IDLE：风控恢复 + 冷却期满"""
        self.engine.state = STATE_PROTECTION
        self.engine.cooldown_counter = 1
        self.engine.positions = np.array(WEIGHTS_PROTECT) * 100000.0

        signal = self._make_signal(current_dd=-0.05, is_protection=False)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert self.engine.state == STATE_IDLE
        assert order is not None
        assert order.reason == "解除保护: 风控恢复正常"
        np.testing.assert_array_almost_equal(order.target_weights, WEIGHTS_IDLE)

    def test_protection_stays_during_cooldown(self):
        """PROTECTION 状态：冷却期未满，保持不变"""
        self.engine.state = STATE_PROTECTION
        self.engine.cooldown_counter = 10
        self.engine.positions = np.array(WEIGHTS_PROTECT) * 100000.0

        signal = self._make_signal(current_dd=-0.05, is_protection=False)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert self.engine.state == STATE_PROTECTION
        assert self.engine.cooldown_counter == 9
        assert order is None

    def test_protection_upgrade_to_emergency(self):
        """PROTECTION → EMERGENCY：回撤继续恶化"""
        self.engine.state = STATE_PROTECTION
        self.engine.cooldown_counter = 5
        self.engine.positions = np.array(WEIGHTS_PROTECT) * 100000.0

        signal = self._make_signal(current_dd=-0.14, is_hard_stop=True, is_protection=True)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert self.engine.state == STATE_PROTECTION
        assert order is not None
        assert order.reason == "升级紧急避险: 回撤继续恶化"
        np.testing.assert_array_almost_equal(order.target_weights, WEIGHTS_EMERGENCY)
        assert self.engine.cooldown_counter == COOLDOWN_DAYS * 2

    # ── 再平衡测试 ──────────────────────────────────────

    def test_drift_rebalance(self):
        """漂移阈值再平衡"""
        # 模拟权重漂移（超过 5% 阈值）
        self.engine.positions = np.array([32000, 25000, 25000, 18000])  # 权重 [0.32, 0.25, 0.25, 0.18]
        self.engine.nav = float(np.sum(self.engine.positions))

        signal = self._make_signal()
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert order is not None
        assert "阈值再平衡" in order.reason
        np.testing.assert_array_almost_equal(order.target_weights, WEIGHTS_IDLE)

    def test_year_end_rebalance(self):
        """年末强制再平衡"""
        signal = self._make_signal()
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal, is_year_end=True)

        assert order is not None
        assert order.reason == "年末强制再平衡"

    def test_no_rebalance_when_drift_small(self):
        """漂移小于阈值，不再平衡"""
        # 权重略微偏离但未超过阈值
        self.engine.positions = np.array([25500, 25000, 24500, 25000])

        signal = self._make_signal()
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert order is None

    # ── 资产生长测试 ──────────────────────────────────────

    def test_asset_growth(self):
        """资产自然生长"""
        initial_nav = self.engine.nav
        daily_returns = np.array([0.01, 0.02, -0.01, 0.005])

        signal = self._make_signal()
        self.engine.step(self.today, daily_returns, signal)

        expected_nav = initial_nav * (1 + np.mean(daily_returns))
        assert abs(self.engine.nav - expected_nav) < 100

    def test_friction_cost_deduction(self):
        """摩擦成本扣除"""
        self.engine.positions = np.array([32000, 25000, 25000, 18000])
        self.engine.nav = float(np.sum(self.engine.positions))
        initial_nav = self.engine.nav

        signal = self._make_signal()
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        order = self.engine.step(self.today, daily_returns, signal)

        assert order is not None
        assert order.friction_cost > 0
        assert self.engine.nav < initial_nav

    # ── 边界条件测试 ──────────────────────────────────────

    def test_zero_returns(self):
        """零收益率"""
        initial_nav = self.engine.nav
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        signal = self._make_signal()
        self.engine.step(self.today, daily_returns, signal)

        assert self.engine.nav == initial_nav

    def test_extreme_positive_returns(self):
        """极端正收益"""
        daily_returns = np.array([0.10, 0.10, 0.10, 0.10])

        signal = self._make_signal()
        self.engine.step(self.today, daily_returns, signal)

        assert self.engine.nav > 100000.0

    def test_extreme_negative_returns(self):
        """极端负收益"""
        daily_returns = np.array([-0.10, -0.10, -0.10, -0.10])

        signal = self._make_signal(current_dd=-0.10)
        self.engine.step(self.today, daily_returns, signal)

        assert self.engine.nav < 100000.0

    def test_snapshot_recording(self):
        """快照记录"""
        signal = self._make_signal()
        daily_returns = np.array([0.01, 0.01, 0.01, 0.01])

        self.engine.step(self.today, daily_returns, signal)

        assert len(self.engine.snapshots) == 1
        snapshot = self.engine.snapshots[0]
        assert snapshot.date == str(self.today)
        assert snapshot.state == STATE_IDLE
        assert snapshot.nav > 0

    def test_cooldown_counter_decrements(self):
        """冷却期倒计时"""
        self.engine.state = STATE_PROTECTION
        self.engine.cooldown_counter = 5
        self.engine.positions = np.array(WEIGHTS_PROTECT) * 100000.0

        signal = self._make_signal(is_protection=True)
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        for i in range(5):
            self.engine.step(self.today, daily_returns, signal)
            if i < 4:
                assert self.engine.cooldown_counter == 4 - i

    def test_weights_property(self):
        """权重属性计算"""
        self.engine.positions = np.array([25000, 25000, 25000, 25000])
        self.engine.nav = 100000.0

        weights = self.engine.weights
        np.testing.assert_array_almost_equal(weights, [0.25, 0.25, 0.25, 0.25])

    def test_rebalance_count_increments(self):
        """再平衡计数递增"""
        initial_count = self.engine.rebalance_count
        self.engine.positions = np.array([32000, 25000, 25000, 18000])
        self.engine.nav = float(np.sum(self.engine.positions))

        signal = self._make_signal()
        daily_returns = np.array([0.0, 0.0, 0.0, 0.0])

        self.engine.step(self.today, daily_returns, signal)

        assert self.engine.rebalance_count == initial_count + 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
