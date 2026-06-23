import pandas as pd

import backtest.portfolio_aggregation_audit as aggregation_audit
from backtest.portfolio_aggregation_audit import (
    load_raw_series,
    run_allocation_grid,
    run_portfolio_aggregation_audit,
)


def test_aggregation_50_50_fails_mdd_limit():
    results = run_portfolio_aggregation_audit()

    assert results["qqq_spy_gld"].aggregate.cagr > 0.075
    assert results["qqq_spy_gld"].aggregate.mdd < -0.16


def test_allocation_grid_finds_small_beta_passing_configuration():
    rows = run_allocation_grid()
    qqq_spy_10 = [
        row
        for row in rows
        if row.scenario == "qqq_spy_gld" and abs(row.beta_weight - 0.10) < 1e-9
    ][0]
    qqq_spy_20 = [
        row
        for row in rows
        if row.scenario == "qqq_spy_gld" and abs(row.beta_weight - 0.20) < 1e-9
    ][0]

    assert qqq_spy_10.pass_target is True
    assert qqq_spy_20.pass_target is False


def test_load_raw_series_falls_back_to_frozen_audit_snapshot(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    snapshot_dir = tmp_path / "snapshot"
    raw_dir.mkdir()
    snapshot_dir.mkdir()
    frame = pd.DataFrame(
        {"adj_close": [200.0, 202.0]},
        index=pd.to_datetime(["2026-01-02", "2026-01-05"]),
    )
    frame.index.name = "date"
    frame.to_csv(snapshot_dir / "QQQ.csv")

    monkeypatch.setattr(aggregation_audit, "RAW_DIR", raw_dir)
    monkeypatch.setattr(aggregation_audit, "AUDIT_SNAPSHOT_DIR", snapshot_dir)

    series = load_raw_series("QQQ")

    assert series.tolist() == [200.0, 202.0]
