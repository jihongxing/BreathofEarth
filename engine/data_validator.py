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


class DataValidationError(Exception):
    """数据校验失败，系统应中止运行"""
    pass


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
