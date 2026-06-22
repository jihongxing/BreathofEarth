from backtest.portfolio_aggregation_audit import (
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
