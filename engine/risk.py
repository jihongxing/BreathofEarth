"""
息壤（Xi-Rang）风控引擎

职责：
1. 计算当前回撤
2. 计算 SPY-TLT 滚动相关性
3. 判断是否触发风控（常规保护 / 紧急避险 / 相关性崩溃）
"""

from dataclasses import dataclass
from typing import Optional
import numpy as np

from engine.config import (
    RISK_DD_THRESHOLD,
    HARD_STOP_DD,
    RISK_CORR_THRESHOLD,
)


@dataclass
class RiskSignal:
    """风控信号"""
    current_dd: float           # 当前回撤
    spy_tlt_corr: float         # SPY-TLT 相关性
    spy_30d_ret: float          # SPY 30日收益
    tlt_30d_ret: float          # TLT 30日收益
    is_hard_stop: bool          # 是否触发硬止损
    is_protection: bool         # 是否触发常规保护
    is_corr_breakdown: bool     # 是否相关性崩溃
    trigger_reason: Optional[str] = None  # 触发原因


class RiskEngine:
    """风控引擎：每日嗅探市场风险"""

    def __init__(self):
        self.high_water_mark = 0.0

    def update_hwm(self, nav: float):
        """更新高水位"""
        if nav > self.high_water_mark:
            self.high_water_mark = nav

    def evaluate(
        self,
        nav: float,
        spy_tlt_corr: float,
        spy_30d_ret: float,
        tlt_30d_ret: float,
    ) -> RiskSignal:
        """
        评估当前风险状态。

        Returns:
            RiskSignal 包含所有风控判断结果
        """
        self.update_hwm(nav)

        current_dd = (nav - self.high_water_mark) / self.high_water_mark if self.high_water_mark > 0 else 0.0

        # 相关性崩溃 = 高正相关 + 双杀
        is_corr_breakdown = (
            spy_tlt_corr > RISK_CORR_THRESHOLD
            and spy_30d_ret < 0
            and tlt_30d_ret < 0
        )

        is_hard_stop = current_dd <= HARD_STOP_DD
        is_protection = (current_dd <= RISK_DD_THRESHOLD) or is_corr_breakdown

        # 确定触发原因
        trigger_reason = None
        if is_hard_stop:
            trigger_reason = f"硬止损: 回撤 {current_dd:.2%} <= {HARD_STOP_DD:.2%}"
        elif current_dd <= RISK_DD_THRESHOLD:
            trigger_reason = f"回撤预警: {current_dd:.2%} <= {RISK_DD_THRESHOLD:.2%}"
        elif is_corr_breakdown:
            trigger_reason = f"相关性崩溃: corr={spy_tlt_corr:.2f}, SPY_30d={spy_30d_ret:.2%}, TLT_30d={tlt_30d_ret:.2%}"

        return RiskSignal(
            current_dd=current_dd,
            spy_tlt_corr=spy_tlt_corr,
            spy_30d_ret=spy_30d_ret,
            tlt_30d_ret=tlt_30d_ret,
            is_hard_stop=is_hard_stop,
            is_protection=is_protection,
            is_corr_breakdown=is_corr_breakdown,
            trigger_reason=trigger_reason,
        )
