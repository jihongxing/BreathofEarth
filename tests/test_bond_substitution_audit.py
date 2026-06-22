import numpy as np

from backtest.bond_substitution_audit import (
    build_substitution_prices,
    run_bond_substitution_audit,
)


def test_bond_substitution_baseline_matches_locked_etf_daily_policy():
    results = run_bond_substitution_audit()
    baseline = results["TLT baseline"].audit.metrics

    assert np.isclose(baseline.final, 365897.47, atol=0.05)
    assert np.isclose(baseline.cagr, 0.0627, atol=0.0001)
    assert np.isclose(baseline.mdd, -0.1129, atol=0.0001)


def test_bond_substitution_prices_are_positive_and_aligned():
    prices = build_substitution_prices("IEF")

    assert list(prices.columns) == ["SPY", "TLT", "GLD", "SHV"]
    assert str(prices.index.min().date()) == "2005-01-03"
    assert str(prices.index.max().date()) == "2026-04-30"
    assert (prices > 0).all().all()


def test_duration_cut_reduces_drawdown_but_not_cash_lock():
    results = run_bond_substitution_audit()
    baseline = results["TLT baseline"]
    duration_cut = results["IEF duration cut"]

    assert duration_cut.audit.metrics.mdd > baseline.audit.metrics.mdd
    assert duration_cut.audit.metrics.cagr < baseline.audit.metrics.cagr
    assert duration_cut.high_cash_pct >= baseline.high_cash_pct
