import numpy as np
import pandas as pd

from backtest.macro_regime_backtest import DualEngineConfig, asymmetric_config, run_scenario
from engine.config import ASSETS
from engine.macro_filter import MacroRegimeConfig


def _slow_breakdown_prices():
    dates = pd.date_range("2022-01-01", periods=360, freq="B")
    down = np.full(len(dates), -0.001)
    prices = pd.DataFrame(index=dates)
    prices["SPY"] = 100.0 * np.cumprod(1 + down)
    prices["TLT"] = 100.0 * np.cumprod(1 + down * 1.2)
    prices["GLD"] = 100.0 * np.cumprod(1 + np.full(len(dates), 0.0002))
    prices["SHV"] = 100.0 * np.cumprod(1 + np.full(len(dates), 0.00005))
    return prices[ASSETS]


def test_macro_regime_backtest_triggers_macro_filter():
    prices = _slow_breakdown_prices()
    config = DualEngineConfig(
        name="test",
        asymmetric=asymmetric_config(8),
        macro=MacroRegimeConfig(name="macro", ma_window=30),
    )

    run = run_scenario(prices, "test", config)

    assert run.macro_triggers >= 1
    assert run.final > 0
