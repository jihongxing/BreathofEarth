import numpy as np
import pandas as pd
import pytest

from engine.stress_model import StressSlippageConfig, StressSlippageModel


def test_base_side_rates_match_existing_turnover_fee():
    dates = pd.date_range("2025-01-01", periods=1)
    anomaly = pd.DataFrame({"SPY": [1.0], "TLT": [1.0]}, index=dates)
    model = StressSlippageModel(anomaly)

    estimate = model.estimate_rebalance_cost(
        current_positions=np.array([60_000.0, 40_000.0]),
        target_weights=np.array([0.5, 0.5]),
        current_date=dates[0],
        assets=["SPY", "TLT"],
    )

    turnover_notional = 10_000.0
    assert estimate.buy_notional == pytest.approx(turnover_notional)
    assert estimate.sell_notional == pytest.approx(turnover_notional)
    assert estimate.total_cost == pytest.approx(turnover_notional * 0.001)
    assert estimate.stressed_assets == ()


def test_stress_asset_gets_extra_side_slippage():
    dates = pd.date_range("2025-01-01", periods=1)
    anomaly = pd.DataFrame({"SPY": [1.0], "TLT": [3.5]}, index=dates)
    config = StressSlippageConfig(
        anomaly_threshold=3.0,
        stress_extra_side_rates={"TLT": 0.0025},
    )
    model = StressSlippageModel(anomaly, config)

    estimate = model.estimate_rebalance_cost(
        current_positions=np.array([60_000.0, 40_000.0]),
        target_weights=np.array([0.5, 0.5]),
        current_date=dates[0],
        assets=["SPY", "TLT"],
    )

    expected = 10_000.0 * 0.0005 + 10_000.0 * 0.0030
    assert estimate.total_cost == pytest.approx(expected)
    assert estimate.stressed_assets == ("TLT",)
    assert estimate.max_anomaly == pytest.approx(3.5)


def test_from_prices_flags_large_volatility_anomaly_without_future_data():
    dates = pd.date_range("2025-01-01", periods=120)
    calm = np.tile([0.001, -0.001], 50)
    shock = np.tile([0.02, -0.02], 10)
    prices = pd.DataFrame(
        {"SPY": 100.0 * np.cumprod(1 + np.r_[calm, shock])},
        index=dates,
    )
    config = StressSlippageConfig(
        volatility_window=5,
        baseline_window=30,
        baseline_min_periods=10,
        anomaly_threshold=3.0,
    )

    model = StressSlippageModel.from_prices(prices, config)

    assert model.side_rate("SPY", dates[106]) > config.base_side_rate
