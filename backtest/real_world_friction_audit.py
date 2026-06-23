"""
Real-world friction audit for the 90/10 production candidate.

The portfolio aggregation audit reports a research NAV after clean-price data,
rebalance friction, stress slippage, and panic sell penalties. This module adds
an explicit second layer for costs that are not present in that research NAV:

- dividend withholding drag
- realized tax drag
- broker cash / financing spread drag
- operational failure drag
- deterministic tail shock for execution or system failures

The model is intentionally parameterized. The local price cache only stores
adjusted-close series, not dividend cashflows or tax lots, so this audit must not
pretend to be a precise tax-lot simulator.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from backtest.portfolio_aggregation_audit import (
    INITIAL_CAPITAL,
    fixed_policy_sleeve,
    run_static_beta_sleeve,
)
from engine.portfolio_aggregator import AggregatedPortfolio, aggregate_sleeves


PRODUCTION_BETA_SCENARIO = "qqq_spy_gld"
PRODUCTION_BETA_WEIGHTS = {"QQQ": 0.40, "SPY": 0.30, "GLD": 0.30}
PRODUCTION_SLEEVE_WEIGHTS = {"defensive": 0.90, "beta": 0.10}
TRADING_DAYS = 252


@dataclass(frozen=True)
class RealWorldFrictionScenario:
    name: str
    dividend_withholding_drag_bps: float = 0.0
    tax_drag_bps: float = 0.0
    broker_cash_financing_drag_bps: float = 0.0
    operational_failure_drag_bps: float = 0.0
    tail_failure_shock_bps: float = 0.0
    leverage: float = 1.0
    margin_rate: float = 0.0

    @property
    def extra_annual_drag_bps(self) -> float:
        operating_drag = (
            self.dividend_withholding_drag_bps
            + self.tax_drag_bps
            + self.broker_cash_financing_drag_bps
            + self.operational_failure_drag_bps
        )
        borrow_drag = max(self.leverage - 1.0, 0.0) * self.margin_rate * 10_000
        return self.leverage * operating_drag + borrow_drag


@dataclass(frozen=True)
class RealWorldFrictionResult:
    scenario: RealWorldFrictionScenario
    nav: pd.Series
    cagr: float
    mdd: float
    final: float
    research_cagr: float
    research_mdd: float
    cagr_delta: float
    mdd_delta: float


def calculate_cagr(nav: pd.Series) -> float:
    years = max((nav.index[-1] - nav.index[0]).days / 365.25, 1 / 365.25)
    return float((nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1)


def calculate_mdd(nav: pd.Series) -> float:
    drawdown = (nav - nav.cummax()) / nav.cummax()
    return float(drawdown.min())


def production_candidate_nav() -> AggregatedPortfolio:
    defensive = fixed_policy_sleeve()
    beta = run_static_beta_sleeve(PRODUCTION_BETA_SCENARIO, PRODUCTION_BETA_WEIGHTS)
    return aggregate_sleeves(
        {"defensive": defensive.nav, "beta": beta.nav},
        PRODUCTION_SLEEVE_WEIGHTS,
        initial_capital=INITIAL_CAPITAL,
    )


def apply_real_world_friction(
    research_nav: pd.Series,
    scenario: RealWorldFrictionScenario,
) -> pd.Series:
    if scenario.leverage <= 0:
        raise ValueError("leverage must be positive")
    if research_nav.empty:
        raise ValueError("research_nav cannot be empty")
    if (research_nav <= 0).any():
        raise ValueError("research_nav must contain only positive values")

    returns = research_nav.sort_index().pct_change().fillna(0.0)
    adjusted_returns = returns * scenario.leverage

    annual_drag = scenario.extra_annual_drag_bps / 10_000
    daily_drag = (1.0 - annual_drag) ** (1.0 / TRADING_DAYS) - 1.0
    adjusted_returns = adjusted_returns + daily_drag

    if scenario.tail_failure_shock_bps > 0:
        shock = scenario.tail_failure_shock_bps / 10_000
        adjusted_returns = adjusted_returns.copy()
        for _, year_returns in adjusted_returns.groupby(adjusted_returns.index.year):
            if len(year_returns) <= 1:
                continue
            worst_date = year_returns.iloc[1:].idxmin()
            adjusted_returns.at[worst_date] -= shock

    nav = (1.0 + adjusted_returns).cumprod() * float(research_nav.iloc[0])
    nav.name = scenario.name
    return nav


def run_scenario(
    research: AggregatedPortfolio,
    scenario: RealWorldFrictionScenario,
) -> RealWorldFrictionResult:
    nav = apply_real_world_friction(research.nav, scenario)
    cagr = calculate_cagr(nav)
    mdd = calculate_mdd(nav)
    return RealWorldFrictionResult(
        scenario=scenario,
        nav=nav,
        cagr=cagr,
        mdd=mdd,
        final=float(nav.iloc[-1]),
        research_cagr=research.cagr,
        research_mdd=research.mdd,
        cagr_delta=cagr - research.cagr,
        mdd_delta=mdd - research.mdd,
    )


def default_scenarios() -> list[RealWorldFrictionScenario]:
    return [
        RealWorldFrictionScenario(name="research_current"),
        RealWorldFrictionScenario(
            name="unlevered_base_case",
            dividend_withholding_drag_bps=55,
            tax_drag_bps=35,
            broker_cash_financing_drag_bps=10,
            operational_failure_drag_bps=20,
            tail_failure_shock_bps=50,
        ),
        RealWorldFrictionScenario(
            name="unlevered_harsh_case",
            dividend_withholding_drag_bps=75,
            tax_drag_bps=75,
            broker_cash_financing_drag_bps=25,
            operational_failure_drag_bps=50,
            tail_failure_shock_bps=150,
        ),
        RealWorldFrictionScenario(
            name="levered_1_15_reference",
            dividend_withholding_drag_bps=55,
            tax_drag_bps=35,
            broker_cash_financing_drag_bps=10,
            operational_failure_drag_bps=20,
            tail_failure_shock_bps=50,
            leverage=1.15,
            margin_rate=0.0479,
        ),
    ]


def run_real_world_friction_audit(
    scenarios: list[RealWorldFrictionScenario] | None = None,
) -> dict[str, RealWorldFrictionResult]:
    research = production_candidate_nav()
    return {
        scenario.name: run_scenario(research, scenario)
        for scenario in (scenarios or default_scenarios())
    }


def print_report(results: dict[str, RealWorldFrictionResult]) -> None:
    print("=" * 132)
    print("  Real-World Friction Audit: 90/10 Production Candidate")
    print("=" * 132)
    print(
        "  Cost note: research_current already includes rebalance friction, stress "
        "slippage, and 50bp panic sell penalties."
    )
    print(
        "  Extra frictions below are parameterized drags because local data has "
        "adjusted-close prices, not dividend cashflows or tax lots."
    )
    print("-" * 132)
    print(
        f"  {'Scenario':<24} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Extra Drag':>11} {'Tail':>7} {'Lev':>5} {'Delta CAGR':>11} {'Delta MDD':>10}"
    )
    for result in results.values():
        scenario = result.scenario
        print(
            f"  {scenario.name:<24} {result.cagr:>7.2%} {result.mdd:>8.2%} "
            f"{result.final:>13,.2f} {scenario.extra_annual_drag_bps:>10.0f}bp "
            f"{scenario.tail_failure_shock_bps:>6.0f}bp {scenario.leverage:>5.2f} "
            f"{result.cagr_delta:>10.2%} {result.mdd_delta:>9.2%}"
        )
    print("-" * 132)
    print("  Interpretation:")
    print("  - unlevered_base_case is the first real-world reference lens, not a promise.")
    print("  - levered_1_15_reference is a financing stress lens, not live leverage approval.")
    print("  - Tax drag must be replaced by account-specific tax-lot simulation before live execution.")
    print("=" * 132)


def main() -> None:
    print_report(run_real_world_friction_audit())


if __name__ == "__main__":
    main()
