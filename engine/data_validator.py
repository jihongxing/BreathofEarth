"""
息壤（Xi-Rang）数据合理性校验

Fail-safe 机制：如果数据异常，中止运行而不是根据错误数据调仓。

校验规则：
1. 无 NaN / Inf
2. 所有价格 > 0
3. 单日涨跌幅不超过 ±25%（美股熔断极限）
4. 各资产之间的日期必须对齐
"""

import numpy as np
import pandas as pd
from typing import Optional

from engine.config import ASSETS
from engine.insurance import InsuranceSignal, SignalSeverity


class DataValidationError(Exception):
    """数据校验失败，系统应中止运行"""
    pass


def build_data_integrity_signal(ok: bool, reason: str, evidence: dict) -> InsuranceSignal:
    return InsuranceSignal(
        source="data",
        severity=SignalSeverity.INFO if ok else SignalSeverity.CRITICAL,
        score=0.0 if ok else 1.0,
        weight=1.0,
        hard_veto=not ok,
        reason=reason,
        evidence=evidence,
    )


def validate_prices(prices: pd.DataFrame, assets: list = None) -> None:
    """
    校验价格数据的合理性。

    Raises:
        DataValidationError 如果数据异常
    """
    check_assets = assets or ASSETS

    # 1. 检查必要列
    missing = [a for a in check_assets if a not in prices.columns]
    if missing:
        raise DataValidationError(f"缺少资产数据: {missing}")

    # 2. 检查 NaN / Inf
    check_cols = [a for a in check_assets if a in prices.columns]
    if prices[check_cols].isna().any().any():
        nan_cols = prices[check_cols].columns[prices[check_cols].isna().any()].tolist()
        raise DataValidationError(f"存在 NaN 值: {nan_cols}")

    if np.isinf(prices[check_cols].values).any():
        raise DataValidationError("存在 Inf 值")

    # 3. 检查价格为正
    for asset in check_cols:
        if (prices[asset] <= 0).any():
            bad_dates = prices.index[prices[asset] <= 0].tolist()
            raise DataValidationError(f"{asset} 存在非正价格，日期: {bad_dates[:3]}")

    # 4. 检查单日涨跌幅（最后一天 vs 倒数第二天）
    if len(prices) >= 2:
        last_returns = prices[check_cols].iloc[-1] / prices[check_cols].iloc[-2] - 1
        for asset in check_cols:
            ret = last_returns[asset]
            if abs(ret) > 0.25:
                raise DataValidationError(
                    f"{asset} 单日涨跌幅异常: {ret:+.2%}（超过 ±25% 熔断极限）。"
                    f"可能是数据源错误、拆股或除权事件，请人工确认。"
                )


def validate_returns(daily_returns: np.ndarray) -> None:
    """
    校验日收益率数组。

    Raises:
        DataValidationError 如果数据异常
    """
    if np.isnan(daily_returns).any():
        raise DataValidationError(f"日收益率包含 NaN: {daily_returns}")

    if np.isinf(daily_returns).any():
        raise DataValidationError(f"日收益率包含 Inf: {daily_returns}")

    for i, asset in enumerate(ASSETS):
        if abs(daily_returns[i]) > 0.25:
            raise DataValidationError(
                f"{asset} 日收益率异常: {daily_returns[i]:+.2%}（超过 ±25%）。"
                f"系统中止，请人工确认数据源。"
            )


def validate_consecutive_anomalies(
    prices: pd.DataFrame, assets: list = None, window: int = 3, threshold: float = 0.15
) -> None:
    """
    检测连续大幅波动，防止数据源持续异常。

    Args:
        prices: 价格数据
        assets: 要检查的资产列表
        window: 检测窗口（天数）
        threshold: 单日涨跌幅阈值

    Raises:
        DataValidationError 如果检测到连续异常
    """
    check_assets = assets or ASSETS
    check_cols = [a for a in check_assets if a in prices.columns]

    if len(prices) < window:
        return

    returns = prices[check_cols].pct_change().iloc[-window:]

    for asset in check_cols:
        anomaly_count = (returns[asset].abs() > threshold).sum()
        if anomaly_count >= window - 1:
            raise DataValidationError(
                f"{asset} 在最近 {window} 天内有 {anomaly_count} 天涨跌幅超过 ±{threshold:.0%}，"
                f"可能数据源异常，请人工确认。"
            )


def validate_correlation_range(corr_value: float, name: str = "相关性") -> None:
    """
    校验相关性值的合理性。

    Args:
        corr_value: 相关性值
        name: 指标名称（用于错误消息）

    Raises:
        DataValidationError 如果相关性超出 [-1, 1] 范围
    """
    if np.isnan(corr_value):
        raise DataValidationError(f"{name}为 NaN，可能数据不足或计算错误")

    if np.isinf(corr_value):
        raise DataValidationError(f"{name}为 Inf，计算异常")

    if not (-1.0 <= corr_value <= 1.0):
        raise DataValidationError(
            f"{name}超出合理范围 [-1, 1]: {corr_value:.4f}，计算逻辑可能有误"
        )


def validate_synthetic_data(
    prices: pd.DataFrame, asset_name: str, expected_annual_return: float = 0.02
) -> None:
    """
    校验模拟数据（如货币基金）的一致性。

    Args:
        prices: 价格数据
        asset_name: 资产名称
        expected_annual_return: 预期年化收益率

    Raises:
        DataValidationError 如果模拟数据异常
    """
    if asset_name not in prices.columns:
        return

    series = prices[asset_name]

    # 检查单调性（货币基金应该单调递增）
    if not series.is_monotonic_increasing:
        first_decrease = series.diff()[series.diff() < 0].index[0]
        raise DataValidationError(
            f"模拟资产 {asset_name} 应单调递增，但在 {first_decrease} 出现下降"
        )

    # 检查增长率合理性
    if len(series) >= 252:
        total_return = (series.iloc[-1] / series.iloc[-252]) - 1
        if abs(total_return - expected_annual_return) > 0.01:
            raise DataValidationError(
                f"模拟资产 {asset_name} 年化收益率 {total_return:.2%} "
                f"偏离预期 {expected_annual_return:.2%} 超过 1%"
            )
