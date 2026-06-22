import numpy as np
import pandas as pd

from backtest.walk_forward_audit import AuditConfig, cash_lock_config, run_audit_scenario
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


def test_panic_penalty_increases_cost_on_defense_entry():
    prices = _synthetic_prices()
    config = cash_lock_config(vol_threshold=1.5, cooldown_days=30)

    clean = run_audit_scenario(prices, "clean", config)
    penalized = run_audit_scenario(
        prices,
        "penalized",
        config,
        AuditConfig(panic_entry_sell_bps=0.01),
    )

    assert penalized.shift_triggers >= 1
    assert penalized.total_cost > clean.total_cost
    assert penalized.final < clean.final
