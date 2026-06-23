import numpy as np
import pandas as pd

from backtest.asymmetric_shift_backtest import asymmetric_config, run_scenario
from engine.config import ASSETS


def _synthetic_prices():
    dates = pd.date_range("2020-01-01", periods=360, freq="B")
    shock = np.r_[np.full(120, 0.001), np.tile([0.03, -0.03], 60), np.full(120, 0.001)]
    prices = pd.DataFrame(index=dates)
    prices["SPY"] = 100.0 * np.cumprod(1 + shock)
    prices["TLT"] = 100.0 * np.cumprod(1 + shock * 0.5)
    prices["GLD"] = 100.0 * np.cumprod(1 + np.full(len(dates), 0.0002))
    prices["SHV"] = 100.0 * np.cumprod(1 + np.full(len(dates), 0.00005))
    return prices[ASSETS]


def test_asymmetric_backtest_applies_panic_penalty():
    prices = _synthetic_prices()
    cfg = asymmetric_config(4, crisis_lock_days=5)

    clean = run_scenario(prices, "clean", cfg, panic_sell_bps=0.0)
    penalized = run_scenario(prices, "penalized", cfg, panic_sell_bps=0.01)

    assert penalized.trigger_count >= 1
    assert penalized.total_cost > clean.total_cost
    assert penalized.final < clean.final
