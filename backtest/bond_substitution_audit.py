"""
Bond substitution audit.

Compares the fixed policy with three bond sleeves:
- TLT: current long-duration Treasury baseline
- IEF: 7-10Y Treasury duration reduction
- AGG: aggregate bond market duration/credit mix

The production data files and strategy code are not modified. Each substitute is
renamed into the internal TLT slot so the existing fixed-policy state machine is
reused exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from backtest.return_attribution import (
    EXTREME_CASH_THRESHOLD,
    HIGH_CASH_THRESHOLD,
    PANIC_SELL_BPS,
    AttributionAudit,
    run_return_attribution_from_prices,
)
from engine.config import ASSETS


DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
BASELINE_FILE = DATA_DIR / "etf_daily.csv"
BOND_SCENARIOS = {
    "TLT baseline": "TLT",
    "IEF duration cut": "IEF",
    "AGG aggregate": "AGG",
}


@dataclass(frozen=True)
class BondScenarioResult:
    name: str
    bond_asset: str
    audit: AttributionAudit
    high_cash_days: int
    extreme_cash_days: int
    high_cash_pct: float
    extreme_cash_pct: float
    bond_pnl: float
    bond_avg_weight: float
    bond_buy_hold_return: float


def load_raw_series(ticker: str) -> pd.Series:
    path = RAW_DIR / f"{ticker}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing raw data for {ticker}: {path}. Run data_manager/Yahoo fetch first."
        )
    df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
    col = "adj_close" if "adj_close" in df.columns else df.columns[0]
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    bad = series[series <= 0]
    if not bad.empty:
        samples = ", ".join(
            f"{idx.date()}={value:.4f}" for idx, value in bad.head(5).items()
        )
        raise ValueError(f"non-positive raw prices for {ticker}: {samples}")
    series.name = ticker
    return series


def build_substitution_prices(bond_ticker: str) -> pd.DataFrame:
    baseline = pd.read_csv(BASELINE_FILE, index_col="date", parse_dates=True).sort_index()
    baseline = baseline.loc["2005-01-03":"2026-04-30", ASSETS]
    if bond_ticker == "TLT":
        prices = baseline.copy()
    else:
        prices = baseline.copy()
        prices["TLT"] = load_raw_series(bond_ticker)
    prices = prices.sort_index().ffill().dropna(how="any")
    prices = prices.loc["2005-01-03":"2026-04-30", ASSETS]
    if prices.empty:
        raise ValueError(f"empty scenario prices for {bond_ticker}")
    return prices


def run_bond_substitution_audit() -> dict[str, BondScenarioResult]:
    results: dict[str, BondScenarioResult] = {}
    for scenario, bond_ticker in BOND_SCENARIOS.items():
        prices = build_substitution_prices(bond_ticker)
        audit = run_return_attribution_from_prices(prices, panic_sell_bps=PANIC_SELL_BPS)
        history = audit.history
        high_cash_days = int((history["SHV_end_weight"] >= HIGH_CASH_THRESHOLD).sum())
        extreme_cash_days = int(
            (history["SHV_end_weight"] >= EXTREME_CASH_THRESHOLD).sum()
        )
        bond_row = audit.asset_summary.loc["TLT"]
        results[scenario] = BondScenarioResult(
            name=scenario,
            bond_asset=bond_ticker,
            audit=audit,
            high_cash_days=high_cash_days,
            extreme_cash_days=extreme_cash_days,
            high_cash_pct=high_cash_days / len(history),
            extreme_cash_pct=extreme_cash_days / len(history),
            bond_pnl=float(bond_row["total_pnl"]),
            bond_avg_weight=float(bond_row["avg_start_weight"]),
            bond_buy_hold_return=float(bond_row["buy_hold_return"]),
        )
    return results


def print_report(results: dict[str, BondScenarioResult]) -> None:
    print("=" * 128)
    print("  Bond Substitution Audit: Fixed MA150 + 8w + 50bp Panic")
    print("=" * 128)
    print(
        f"  {'Scenario':<18} {'Bond':<5} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Cost':>11} {'SHV>=50':>10} {'SHV>=70':>10} {'Bond PnL':>12} {'Bond B/H':>10}"
    )
    for scenario, result in results.items():
        metrics = result.audit.metrics
        print(
            f"  {scenario:<18} {result.bond_asset:<5} {metrics.cagr:>7.2%} "
            f"{metrics.mdd:>8.2%} {metrics.final:>13,.2f} {metrics.total_cost:>11,.2f} "
            f"{result.high_cash_pct:>9.2%} {result.extreme_cash_pct:>9.2%} "
            f"{result.bond_pnl:>12,.2f} {result.bond_buy_hold_return:>9.2%}"
        )
    print("-" * 128)
    print("  Asset attribution by scenario")
    print(
        f"  {'Scenario':<18} {'SPY':>12} {'BondSlot':>12} {'GLD':>12} {'SHV':>12} "
        f"{'Rebal':>7} {'Acute':>7} {'Macro':>7}"
    )
    for scenario, result in results.items():
        summary = result.audit.asset_summary
        metrics = result.audit.metrics
        print(
            f"  {scenario:<18} {summary.loc['SPY','total_pnl']:>12,.2f} "
            f"{summary.loc['TLT','total_pnl']:>12,.2f} "
            f"{summary.loc['GLD','total_pnl']:>12,.2f} "
            f"{summary.loc['SHV','total_pnl']:>12,.2f} "
            f"{metrics.rebalances:>7d} {metrics.acute_triggers:>7d} {metrics.macro_triggers:>7d}"
        )
    print("-" * 128)
    print("  Verdict lens: target CAGR >= 7.50%, MDD >= -13.00%")
    for scenario, result in results.items():
        metrics = result.audit.metrics
        verdict = "PASS" if metrics.cagr >= 0.075 and metrics.mdd >= -0.13 else "FAIL"
        print(f"  {scenario:<18} {verdict}")
    print("=" * 128)


def plot_results(results: dict[str, BondScenarioResult]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "bond_substitution_audit.png"

    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    for scenario, result in results.items():
        history = result.audit.history
        nav = history["nav_end"]
        drawdown = (nav - nav.cummax()) / nav.cummax()
        axes[0].plot(history.index, nav, linewidth=1.0, label=scenario)
        axes[1].plot(history.index, drawdown, linewidth=0.9, label=scenario)
        axes[2].plot(
            history.index,
            history["SHV_end_weight"],
            linewidth=0.9,
            label=scenario,
        )

    axes[0].set_title("NAV by Bond Sleeve")
    axes[0].set_ylabel("NAV")
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].axhline(-0.13, color="black", linestyle="--", alpha=0.7)
    axes[2].set_title("SHV End Weight")
    axes[2].set_ylabel("SHV Weight")
    axes[2].axhline(HIGH_CASH_THRESHOLD, color="black", linestyle="--", alpha=0.5)
    for ax in axes:
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    results = run_bond_substitution_audit()
    print_report(results)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
