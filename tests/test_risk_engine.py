"""
风控引擎测试

测试场景：
1. 正常市场（无风控触发）
2. 回撤触发常规保护（-12%）
3. 回撤触发硬止损（-14%）
4. 相关性崩溃（股债双杀）
5. 高水位更新逻辑
6. 边界条件测试
"""

import pytest
from engine.risk import RiskEngine, RiskSignal
from engine.config import RISK_DD_THRESHOLD, HARD_STOP_DD, RISK_CORR_THRESHOLD


class TestRiskEngine:
    """风控引擎测试套件"""

    def setup_method(self):
        """每个测试前初始化"""
        self.engine = RiskEngine()
        self.engine.high_water_mark = 100000.0

    def test_normal_market_no_trigger(self):
        """正常市场：NAV 上涨，无风控触发"""
        signal = self.engine.evaluate(
            nav=105000.0,
            spy_tlt_corr=0.2,
            spy_30d_ret=0.05,
            tlt_30d_ret=0.02,
        )

        assert signal.current_dd == 0.0  # 无回撤
        assert not signal.is_protection
        assert not signal.is_hard_stop
        assert not signal.is_corr_breakdown
        assert signal.trigger_reason is None
        assert self.engine.high_water_mark == 105000.0  # 高水位更新

    def test_drawdown_triggers_protection(self):
        """回撤 -12%：触发常规保护"""
        nav = 100000.0 * (1 - 0.12)  # -12%
        signal = self.engine.evaluate(
            nav=nav,
            spy_tlt_corr=0.2,
            spy_30d_ret=0.01,
            tlt_30d_ret=0.01,
        )

        assert signal.current_dd == pytest.approx(-0.12, abs=1e-6)
        assert signal.is_protection
        assert not signal.is_hard_stop
        assert not signal.is_corr_breakdown
        assert "回撤预警" in signal.trigger_reason

    def test_hard_stop_triggers(self):
        """回撤 -14%：触发硬止损"""
        nav = 100000.0 * (1 - 0.14)
        signal = self.engine.evaluate(
            nav=nav,
            spy_tlt_corr=0.2,
            spy_30d_ret=0.01,
            tlt_30d_ret=0.01,
        )

        assert signal.current_dd == pytest.approx(-0.14, abs=1e-6)
        assert signal.is_hard_stop
        assert signal.is_protection  # 硬止损也是保护的一种
        assert "硬止损" in signal.trigger_reason

    def test_correlation_breakdown_both_negative(self):
        """相关性崩溃：高正相关 + 股债双杀"""
        signal = self.engine.evaluate(
            nav=95000.0,  # -5% 回撤，未达阈值
            spy_tlt_corr=0.6,  # 高正相关
            spy_30d_ret=-0.08,  # SPY 跌 8%
            tlt_30d_ret=-0.05,  # TLT 跌 5%
        )

        assert signal.is_corr_breakdown
        assert signal.is_protection  # 相关性崩溃触发保护
        assert not signal.is_hard_stop
        assert "相关性崩溃" in signal.trigger_reason

    def test_correlation_high_but_not_breakdown(self):
        """高相关性但非双杀：不触发"""
        signal = self.engine.evaluate(
            nav=98000.0,
            spy_tlt_corr=0.6,  # 高正相关
            spy_30d_ret=0.05,  # SPY 涨
            tlt_30d_ret=-0.02,  # TLT 跌
        )

        assert not signal.is_corr_breakdown  # 不是双杀
        assert not signal.is_protection

    def test_both_negative_but_low_correlation(self):
        """股债双跌但相关性低：不触发"""
        signal = self.engine.evaluate(
            nav=98000.0,
            spy_tlt_corr=0.3,  # 低相关性
            spy_30d_ret=-0.03,
            tlt_30d_ret=-0.02,
        )

        assert not signal.is_corr_breakdown
        assert not signal.is_protection

    def test_high_water_mark_updates_on_new_peak(self):
        """高水位在创新高时更新"""
        self.engine.high_water_mark = 100000.0

        self.engine.evaluate(105000.0, 0.2, 0.01, 0.01)
        assert self.engine.high_water_mark == 105000.0

        self.engine.evaluate(110000.0, 0.2, 0.01, 0.01)
        assert self.engine.high_water_mark == 110000.0

    def test_high_water_mark_not_updated_on_drawdown(self):
        """高水位在回撤时不更新"""
        self.engine.high_water_mark = 100000.0

        self.engine.evaluate(95000.0, 0.2, 0.01, 0.01)
        assert self.engine.high_water_mark == 100000.0  # 保持不变

    def test_zero_nav_edge_case(self):
        """边界条件：NAV 为 0"""
        self.engine.high_water_mark = 0.0
        signal = self.engine.evaluate(0.0, 0.2, 0.01, 0.01)

        assert signal.current_dd == 0.0
        assert not signal.is_protection

    def test_exact_threshold_boundary(self):
        """边界条件：精确触达阈值"""
        nav = 100000.0 * (1 + RISK_DD_THRESHOLD)  # 精确 -12%
        signal = self.engine.evaluate(nav, 0.2, 0.01, 0.01)

        assert signal.is_protection

    def test_exact_hard_stop_boundary(self):
        """边界条件：精确触达硬止损"""
        nav = 100000.0 * (1 + HARD_STOP_DD)  # 精确 -14%
        signal = self.engine.evaluate(nav, 0.2, 0.01, 0.01)

        assert signal.is_hard_stop

    def test_exact_correlation_threshold(self):
        """边界条件：精确触达相关性阈值"""
        signal = self.engine.evaluate(
            nav=95000.0,
            spy_tlt_corr=RISK_CORR_THRESHOLD,  # 精确 0.5
            spy_30d_ret=-0.01,
            tlt_30d_ret=-0.01,
        )

        # 注意：> 0.5 才触发，= 0.5 不触发
        assert not signal.is_corr_breakdown

    def test_multiple_triggers_priority(self):
        """多重触发：硬止损优先级最高"""
        signal = self.engine.evaluate(
            nav=86000.0,  # -14% 硬止损
            spy_tlt_corr=0.7,  # 同时相关性崩溃
            spy_30d_ret=-0.10,
            tlt_30d_ret=-0.08,
        )

        assert signal.is_hard_stop
        assert signal.is_corr_breakdown
        assert signal.is_protection
        assert "硬止损" in signal.trigger_reason  # 硬止损优先显示

    def test_signal_dataclass_fields(self):
        """验证 RiskSignal 数据结构完整性"""
        signal = self.engine.evaluate(95000.0, 0.3, -0.02, 0.01)

        assert hasattr(signal, "current_dd")
        assert hasattr(signal, "spy_tlt_corr")
        assert hasattr(signal, "spy_30d_ret")
        assert hasattr(signal, "tlt_30d_ret")
        assert hasattr(signal, "is_hard_stop")
        assert hasattr(signal, "is_protection")
        assert hasattr(signal, "is_corr_breakdown")
        assert hasattr(signal, "trigger_reason")

        assert signal.spy_tlt_corr == 0.3
        assert signal.spy_30d_ret == -0.02
        assert signal.tlt_30d_ret == 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
