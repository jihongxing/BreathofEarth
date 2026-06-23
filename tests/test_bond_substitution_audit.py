import numpy as np
import pandas as pd

import backtest.bond_substitution_audit as bond_audit
from backtest.bond_substitution_audit import (
    build_substitution_prices,
    load_raw_series,
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


def test_load_raw_series_falls_back_to_frozen_audit_snapshot(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    snapshot_dir = tmp_path / "snapshot"
    raw_dir.mkdir()
    snapshot_dir.mkdir()
    frame = pd.DataFrame(
        {"adj_close": [100.0, 101.0]},
        index=pd.to_datetime(["2026-01-02", "2026-01-05"]),
    )
    frame.index.name = "date"
    frame.to_csv(snapshot_dir / "IEF.csv")

    monkeypatch.setattr(bond_audit, "RAW_DIR", raw_dir)
    monkeypatch.setattr(bond_audit, "AUDIT_SNAPSHOT_DIR", snapshot_dir)

    series = load_raw_series("IEF")

    assert series.tolist() == [100.0, 101.0]
