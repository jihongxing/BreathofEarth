"""
Portfolio aggregation audit.

Sleeve A is the audited fixed Permanent Portfolio policy.
Sleeve B is a simple modern-beta sleeve with annual rebalancing.

The sleeves are run independently, then combined by initial capital weights. No
cross-sleeve timing, optimization, or implicit portfolio-level rebalancing is
introduced.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from backtest.return_attribution import run_return_attribution
from engine.config import FEE_RATE
from engine.portfolio_aggregator import AggregatedPortfolio, aggregate_sleeves


INITIAL_CAPITAL = 100000.0
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
BASELINE_FILE = DATA_DIR / "etf_daily.csv"


BETA_SCENARIOS = {
    "qqq_spy_gld": {"QQQ": 0.40, "SPY": 0.30, "GLD": 0.30},
    "qqq_vti_gld": {"QQQ": 0.40, "VTI": 0.30, "GLD": 0.30},
    "qqq_smh_gld": {"QQQ": 0.40, "SMH": 0.30, "GLD": 0.30},
}
BETA_WEIGHT_GRID = [0.10, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
TARGET_AGG_CAGR = 0.075
TARGET_AGG_MDD = -0.16


@dataclass(frozen=True)
class SleeveMetrics:
    name: str
    nav: pd.Series
    cagr: float
    mdd: float
    final: float
    total_cost: float
    rebalances: int


@dataclass(frozen=True)
class AggregationScenario:
    name: str
    defensive: SleeveMetrics
    beta: SleeveMetrics
    aggregate: AggregatedPortfolio
    sleeve_weights: dict[str, float]


@dataclass(frozen=True)
class AllocationGridRow:
    scenario: str
    beta_weight: float
    aggregate: AggregatedPortfolio
    pass_target: bool


def load_raw_series(ticker: str) -> pd.Series:
    if ticker in {"SPY", "TLT", "GLD", "SHV"} and BASELINE_FILE.exists():
        df = pd.read_csv(BASELINE_FILE, index_col="date", parse_dates=True).sort_index()
        if ticker in df.columns:
            series = df[ticker].copy()
            series.name = ticker
            return _validate_positive(series, ticker, BASELINE_FILE)

    path = RAW_DIR / f"{ticker}.csv"
    if not path.exists():
        raise FileNotFoundError(f"missing raw data for {ticker}: {path}")
    df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
    col = "adj_close" if "adj_close" in df.columns else df.columns[0]
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    series.name = ticker
    return _validate_positive(series, ticker, path)


def _validate_positive(series: pd.Series, ticker: str, source: Path) -> pd.Series:
    bad = series[series <= 0]
    if not bad.empty:
        samples = ", ".join(
            f"{idx.date()}={value:.4f}" for idx, value in bad.head(5).items()
        )
        raise ValueError(f"non-positive prices for {ticker} in {source}: {samples}")
    return series


def calculate_cagr(nav: pd.Series) -> float:
    years = max((nav.index[-1] - nav.index[0]).days / 365.25, 1 / 365.25)
    return float((nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1)


def calculate_mdd(nav: pd.Series) -> float:
    drawdown = (nav - nav.cummax()) / nav.cummax()
    return float(drawdown.min())


def build_beta_prices(weights: dict[str, float]) -> pd.DataFrame:
    frames = {ticker: load_raw_series(ticker) for ticker in weights}
    prices = pd.DataFrame(frames).sort_index().ffill().dropna(how="any")
    prices = prices.loc["2005-01-03":"2026-04-30", list(weights)]
    if prices.empty:
        raise ValueError(f"empty beta price table for {list(weights)}")
    return prices


def run_static_beta_sleeve(
    name: str,
    weights: dict[str, float],
    initial_capital: float = INITIAL_CAPITAL,
    fee_rate: float = FEE_RATE,
) -> SleeveMetrics:
    if abs(sum(weights.values()) - 1.0) > 1e-9:
        raise ValueError(f"weights must sum to 1.0: {weights}")
    prices = build_beta_prices(weights)
    returns = prices.pct_change().fillna(0.0)
    tickers = list(weights)
    target = np.array([weights[t] for t in tickers], dtype=float)
    positions = target * initial_capital
    nav_history: list[float] = []
    cost_total = 0.0
    rebalances = 0

    for i, current_date in enumerate(prices.index):
        positions = positions * (1.0 + returns.iloc[i].values)
        nav = float(positions.sum())
        is_year_end = (
            i < len(prices.index) - 1
            and prices.index[i].year != prices.index[i + 1].year
        )
        if is_year_end:
            current_weights = positions / nav if nav > 0 else np.zeros(len(tickers))
            turnover = float(np.sum(np.abs(current_weights - target)) / 2.0)
            cost = nav * turnover * fee_rate
            nav = max(nav - cost, 0.0)
            positions = target * nav
            cost_total += cost
            rebalances += 1
        nav_history.append(nav)

    nav_series = pd.Series(nav_history, index=prices.index, name=name)
    return SleeveMetrics(
        name=name,
        nav=nav_series,
        cagr=calculate_cagr(nav_series),
        mdd=calculate_mdd(nav_series),
        final=float(nav_series.iloc[-1]),
        total_cost=float(cost_total),
        rebalances=rebalances,
    )


def fixed_policy_sleeve() -> SleeveMetrics:
    audit = run_return_attribution()
    nav = audit.history["nav_end"].rename("fixed_policy")
    metrics = audit.metrics
    return SleeveMetrics(
        name="fixed_policy",
        nav=nav,
        cagr=float(metrics.cagr),
        mdd=float(metrics.mdd),
        final=float(metrics.final),
        total_cost=float(metrics.total_cost),
        rebalances=int(metrics.rebalances),
    )


def run_portfolio_aggregation_audit() -> dict[str, AggregationScenario]:
    defensive = fixed_policy_sleeve()
    results: dict[str, AggregationScenario] = {}
    sleeve_weights = {"defensive": 0.50, "beta": 0.50}
    for name, weights in BETA_SCENARIOS.items():
        beta = run_static_beta_sleeve(name, weights)
        aggregate = aggregate_sleeves(
            {"defensive": defensive.nav, "beta": beta.nav},
            sleeve_weights,
            initial_capital=INITIAL_CAPITAL,
        )
        results[name] = AggregationScenario(
            name=name,
            defensive=defensive,
            beta=beta,
            aggregate=aggregate,
            sleeve_weights=sleeve_weights,
        )
    return results


def run_allocation_grid(
    defensive: SleeveMetrics | None = None,
) -> list[AllocationGridRow]:
    defensive = defensive or fixed_policy_sleeve()
    rows: list[AllocationGridRow] = []
    for scenario_name, beta_weights in BETA_SCENARIOS.items():
        beta = run_static_beta_sleeve(scenario_name, beta_weights)
        for beta_weight in BETA_WEIGHT_GRID:
            aggregate = aggregate_sleeves(
                {"defensive": defensive.nav, "beta": beta.nav},
                {"defensive": 1.0 - beta_weight, "beta": beta_weight},
                initial_capital=INITIAL_CAPITAL,
            )
            rows.append(
                AllocationGridRow(
                    scenario=scenario_name,
                    beta_weight=beta_weight,
                    aggregate=aggregate,
                    pass_target=(
                        aggregate.cagr >= TARGET_AGG_CAGR
                        and aggregate.mdd >= TARGET_AGG_MDD
                    ),
                )
            )
    return rows


def best_passing_grid_rows(rows: list[AllocationGridRow]) -> dict[str, AllocationGridRow]:
    best: dict[str, AllocationGridRow] = {}
    for row in rows:
        if not row.pass_target:
            continue
        current = best.get(row.scenario)
        if current is None or row.aggregate.cagr > current.aggregate.cagr:
            best[row.scenario] = row
    return best


def print_report(results: dict[str, AggregationScenario]) -> None:
    grid_rows = run_allocation_grid(next(iter(results.values())).defensive)
    best_rows = best_passing_grid_rows(grid_rows)
    print("=" * 132)
    print("  Portfolio Aggregation Audit: 50% Fixed Policy + 50% Modern Beta")
    print("=" * 132)
    print(
        f"  {'Scenario':<14} {'Agg CAGR':>9} {'Agg MDD':>9} {'Agg Final':>13} "
        f"{'Beta CAGR':>10} {'Beta MDD':>9} {'Beta Cost':>11} {'Verdict':>9}"
    )
    for name, scenario in results.items():
        agg = scenario.aggregate
        beta = scenario.beta
        verdict = "PASS" if agg.cagr >= TARGET_AGG_CAGR and agg.mdd >= TARGET_AGG_MDD else "FAIL"
        print(
            f"  {name:<14} {agg.cagr:>8.2%} {agg.mdd:>8.2%} {agg.final:>13,.2f} "
            f"{beta.cagr:>9.2%} {beta.mdd:>8.2%} {beta.total_cost:>11,.2f} {verdict:>9}"
        )
    print("-" * 132)
    defensive = next(iter(results.values())).defensive
    print(
        f"  Defensive sleeve: CAGR {defensive.cagr:.2%} | MDD {defensive.mdd:.2%} | "
        f"Final {defensive.final:,.2f} | Cost {defensive.total_cost:,.2f}"
    )
    print("  Verdict lens: aggregate CAGR >= 7.50%, aggregate MDD >= -16.00%")
    print("-" * 132)
    print("  Allocation grid: best passing beta sleeve weight by scenario")
    print(f"  {'Scenario':<14} {'Beta Weight':>11} {'Agg CAGR':>9} {'Agg MDD':>9} {'Agg Final':>13}")
    for scenario in BETA_SCENARIOS:
        row = best_rows.get(scenario)
        if row is None:
            print(f"  {scenario:<14} {'none':>11} {'--':>9} {'--':>9} {'--':>13}")
            continue
        print(
            f"  {scenario:<14} {row.beta_weight:>10.0%} {row.aggregate.cagr:>8.2%} "
            f"{row.aggregate.mdd:>8.2%} {row.aggregate.final:>13,.2f}"
        )
    print("=" * 132)


def plot_results(results: dict[str, AggregationScenario]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "portfolio_aggregation_audit.png"

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    for name, scenario in results.items():
        nav = scenario.aggregate.nav
        drawdown = (nav - nav.cummax()) / nav.cummax()
        axes[0].plot(nav.index, nav, linewidth=1.0, label=name)
        axes[1].plot(drawdown.index, drawdown, linewidth=0.9, label=name)

    axes[0].set_title("Aggregated NAV")
    axes[0].set_ylabel("NAV")
    axes[1].set_title("Aggregated Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].axhline(-0.16, color="black", linestyle="--", alpha=0.7)
    for ax in axes:
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    results = run_portfolio_aggregation_audit()
    print_report(results)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
