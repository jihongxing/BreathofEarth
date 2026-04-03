"""
数据校验器测试

测试场景：
1. 价格数据校验（NaN、Inf、负价格、单日异常）
2. 收益率校验
3. 连续异常检测
4. 相关性范围校验
5. 模拟数据一致性检查
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from engine.data_validator import (
    validate_prices,
    validate_returns,
    validate_consecutive_anomalies,
    validate_correlation_range,
    validate_synthetic_data,
    DataValidationError,
)


class TestValidatePrices:
    """价格数据校验测试"""

    def setup_method(self):
        """创建测试数据"""
        dates = pd.date_range("2025-01-01", periods=10, freq="D")
        self.valid_prices = pd.DataFrame({
            "SPY": np.linspace(400, 410, 10),
            "TLT": np.linspace(100, 102, 10),
            "GLD": np.linspace(180, 182, 10),
            "SHV": np.linspace(110, 110.2, 10),
        }, index=dates)
        self.assets = ["SPY", "TLT", "GLD", "SHV"]

    def test_valid_prices_pass(self):
        """正常价格数据通过校验"""
        validate_prices(self.valid_prices, self.assets)

    def test_missing_columns_fail(self):
        """缺少必要列"""
        prices = self.valid_prices.drop(columns=["SPY"])
        with pytest.raises(DataValidationError, match="缺少资产数据"):
            validate_prices(prices, self.assets)

    def test_nan_values_fail(self):
        """包含 NaN 值"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[5], "SPY"] = np.nan
        with pytest.raises(DataValidationError, match="存在 NaN 值"):
            validate_prices(prices, self.assets)

    def test_inf_values_fail(self):
        """包含 Inf 值"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[3], "TLT"] = np.inf
        with pytest.raises(DataValidationError, match="存在 Inf 值"):
            validate_prices(prices, self.assets)

    def test_negative_prices_fail(self):
        """负价格"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[2], "GLD"] = -10.0
        with pytest.raises(DataValidationError, match="存在非正价格"):
            validate_prices(prices, self.assets)

    def test_zero_price_fail(self):
        """零价格"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[4], "SHV"] = 0.0
        with pytest.raises(DataValidationError, match="存在非正价格"):
            validate_prices(prices, self.assets)

    def test_extreme_single_day_return_fail(self):
        """单日涨跌幅超过 ±25%"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[-1], "SPY"] = prices.loc[prices.index[-2], "SPY"] * 1.30
        with pytest.raises(DataValidationError, match="单日涨跌幅异常"):
            validate_prices(prices, self.assets)

    def test_extreme_negative_return_fail(self):
        """单日暴跌超过 -25%"""
        prices = self.valid_prices.copy()
        prices.loc[prices.index[-1], "TLT"] = prices.loc[prices.index[-2], "TLT"] * 0.70
        with pytest.raises(DataValidationError, match="单日涨跌幅异常"):
            validate_prices(prices, self.assets)


class TestValidateReturns:
    """收益率校验测试"""

    def test_valid_returns_pass(self):
        """正常收益率通过校验"""
        returns = np.array([0.01, -0.02, 0.015, 0.005])
        validate_returns(returns)

    def test_nan_in_returns_fail(self):
        """收益率包含 NaN"""
        returns = np.array([0.01, np.nan, 0.015, 0.005])
        with pytest.raises(DataValidationError, match="日收益率包含 NaN"):
            validate_returns(returns)

    def test_inf_in_returns_fail(self):
        """收益率包含 Inf"""
        returns = np.array([0.01, 0.02, np.inf, 0.005])
        with pytest.raises(DataValidationError, match="日收益率包含 Inf"):
            validate_returns(returns)

    def test_extreme_positive_return_fail(self):
        """极端正收益率"""
        returns = np.array([0.30, 0.02, 0.015, 0.005])
        with pytest.raises(DataValidationError, match="日收益率异常"):
            validate_returns(returns)

    def test_extreme_negative_return_fail(self):
        """极端负收益率"""
        returns = np.array([0.01, -0.28, 0.015, 0.005])
        with pytest.raises(DataValidationError, match="日收益率异常"):
            validate_returns(returns)

    def test_boundary_25_percent_pass(self):
        """边界值 ±25% 应该通过"""
        returns = np.array([0.25, -0.25, 0.0, 0.0])
        validate_returns(returns)


class TestValidateConsecutiveAnomalies:
    """连续异常检测测试"""

    def setup_method(self):
        dates = pd.date_range("2025-01-01", periods=10, freq="D")
        self.prices = pd.DataFrame({
            "SPY": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            "TLT": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        }, index=dates)
        self.assets = ["SPY", "TLT"]

    def test_no_anomalies_pass(self):
        """无异常通过"""
        validate_consecutive_anomalies(self.prices, self.assets, window=3, threshold=0.15)

    def test_consecutive_anomalies_fail(self):
        """连续大幅波动"""
        prices = self.prices.copy()
        prices.iloc[-3:, 0] = [100, 120, 95]  # 连续 3 天大幅波动
        with pytest.raises(DataValidationError, match="可能数据源异常"):
            validate_consecutive_anomalies(prices, self.assets, window=3, threshold=0.15)

    def test_single_anomaly_pass(self):
        """单次异常不触发"""
        prices = self.prices.copy()
        prices.iloc[-1, 0] = 130  # 只有最后一天异常
        validate_consecutive_anomalies(prices, self.assets, window=3, threshold=0.15)

    def test_insufficient_data_pass(self):
        """数据不足，跳过检查"""
        prices = self.prices.iloc[:2]
        validate_consecutive_anomalies(prices, self.assets, window=3, threshold=0.15)


class TestValidateCorrelationRange:
    """相关性范围校验测试"""

    def test_valid_correlation_pass(self):
        """正常相关性值"""
        validate_correlation_range(0.5, "SPY-TLT相关性")
        validate_correlation_range(-0.3, "SPY-TLT相关性")
        validate_correlation_range(1.0, "SPY-TLT相关性")
        validate_correlation_range(-1.0, "SPY-TLT相关性")

    def test_nan_correlation_fail(self):
        """NaN 相关性"""
        with pytest.raises(DataValidationError, match="为 NaN"):
            validate_correlation_range(np.nan, "相关性")

    def test_inf_correlation_fail(self):
        """Inf 相关性"""
        with pytest.raises(DataValidationError, match="为 Inf"):
            validate_correlation_range(np.inf, "相关性")

    def test_out_of_range_positive_fail(self):
        """超出范围（正）"""
        with pytest.raises(DataValidationError, match="超出合理范围"):
            validate_correlation_range(1.5, "相关性")

    def test_out_of_range_negative_fail(self):
        """超出范围（负）"""
        with pytest.raises(DataValidationError, match="超出合理范围"):
            validate_correlation_range(-1.2, "相关性")


class TestValidateSyntheticData:
    """模拟数据一致性检查测试"""

    def setup_method(self):
        dates = pd.date_range("2025-01-01", periods=252, freq="D")
        # 模拟年化 2% 的货币基金
        daily_rate = (1.02 ** (1/252)) - 1
        values = [100.0]
        for _ in range(251):
            values.append(values[-1] * (1 + daily_rate))
        self.prices = pd.DataFrame({"MONEY": values}, index=dates)

    def test_valid_synthetic_data_pass(self):
        """正常模拟数据通过"""
        validate_synthetic_data(self.prices, "MONEY", expected_annual_return=0.02)

    def test_non_monotonic_fail(self):
        """非单调递增"""
        prices = self.prices.copy()
        prices.loc[prices.index[100], "MONEY"] = prices.loc[prices.index[99], "MONEY"] * 0.99
        with pytest.raises(DataValidationError, match="应单调递增"):
            validate_synthetic_data(prices, "MONEY")

    def test_wrong_annual_return_fail(self):
        """年化收益率偏离预期"""
        dates = pd.date_range("2025-01-01", periods=252, freq="D")
        # 模拟年化 5% 的数据（偏离预期 2%）
        daily_rate = (1.05 ** (1/252)) - 1
        values = [100.0]
        for _ in range(251):
            values.append(values[-1] * (1 + daily_rate))
        prices = pd.DataFrame({"MONEY": values}, index=dates)

        with pytest.raises(DataValidationError, match="偏离预期"):
            validate_synthetic_data(prices, "MONEY", expected_annual_return=0.02)

    def test_missing_asset_skip(self):
        """资产不存在，跳过检查"""
        validate_synthetic_data(self.prices, "NONEXISTENT")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
