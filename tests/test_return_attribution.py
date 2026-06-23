import numpy as np

from backtest.return_attribution import run_return_attribution
from engine.config import ASSETS


def test_return_attribution_reconciles_daily_pnl_and_costs():
    audit = run_return_attribution()
    history = audit.history

    expected_net_pnl = history["asset_pnl_total"] - history["cost"]

    assert not history.empty
    assert np.allclose(history["net_pnl"], expected_net_pnl)
    assert np.isclose(audit.metrics.final, history["nav_end"].iloc[-1])


def test_return_attribution_summaries_reconcile_to_history():
    audit = run_return_attribution()
    history = audit.history

    asset_total = audit.asset_summary["total_pnl"].sum()
    regime_days = audit.regime_summary["days"].sum()
    regime_net = audit.regime_summary["net_pnl"].sum()
    expected_asset_total = history[[f"{asset}_pnl" for asset in ASSETS]].sum().sum()
    expected_net = history["asset_pnl_total"].sum() - history["cost"].sum()

    assert np.isclose(asset_total, expected_asset_total)
    assert regime_days == len(history)
    assert np.isclose(regime_net, expected_net)
