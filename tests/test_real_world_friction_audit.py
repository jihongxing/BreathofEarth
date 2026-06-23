import pandas as pd

from backtest.real_world_friction_audit import (
    RealWorldFrictionScenario,
    apply_real_world_friction,
    run_real_world_friction_audit,
)
from engine.portfolio_aggregator import calculate_cagr, calculate_mdd


def test_extra_drag_reduces_cagr_without_changing_research_nav_shape():
    nav = pd.Series(
        [100.0, 101.0, 103.0, 106.0],
        index=pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"]),
    )

    clean = apply_real_world_friction(nav, RealWorldFrictionScenario(name="clean"))
    dragged = apply_real_world_friction(
        nav,
        RealWorldFrictionScenario(name="dragged", tax_drag_bps=500),
    )

    assert calculate_cagr(dragged) < calculate_cagr(clean)
    assert dragged.iloc[-1] < clean.iloc[-1]


def test_tail_failure_shock_worsens_drawdown():
    nav = pd.Series(
        [100.0, 110.0, 105.0, 115.0, 112.0, 120.0],
        index=pd.to_datetime(
            ["2020-01-02", "2020-01-03", "2020-01-06", "2021-01-04", "2021-01-05", "2021-01-06"]
        ),
    )

    clean = apply_real_world_friction(nav, RealWorldFrictionScenario(name="clean"))
    shocked = apply_real_world_friction(
        nav,
        RealWorldFrictionScenario(name="shocked", tail_failure_shock_bps=1000),
    )

    assert calculate_mdd(shocked) < calculate_mdd(clean)


def test_default_real_world_audit_orders_scenarios_by_friction():
    results = run_real_world_friction_audit()

    assert results["research_current"].cagr > results["unlevered_base_case"].cagr
    assert results["unlevered_base_case"].cagr > results["unlevered_harsh_case"].cagr
    assert results["unlevered_harsh_case"].mdd < results["research_current"].mdd
    assert results["levered_1_15_reference"].scenario.leverage == 1.15
