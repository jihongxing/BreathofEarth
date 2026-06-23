"""
Return attribution audit for the fixed policy.

This is a read-only bypass audit. It replays the same MA150 + 8w + both-recovery
policy used by fixed_policy_audit, but records daily start weights, asset PnL,
end weights, regime labels, and rebalance costs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from backtest.fixed_policy_audit import fixed_policy_config, load_prices
from backtest.macro_regime_backtest import (
    INITIAL_CAPITAL,
    DualEngineConfig,
    DualEnginePortfolio,
    build_metrics,
    build_signals,
)
from engine.asymmetric_shifter import AsymmetricShiftSignal, AsymmetricShifter
from engine.config import ASSETS, CORR_WINDOW, STATE_PROTECTION
from engine.macro_filter import MacroRegimeFilter, MacroRegimeSignal
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel


PANIC_SELL_BPS = 0.005
HIGH_CASH_THRESHOLD = 0.50
EXTREME_CASH_THRESHOLD = 0.70


@dataclass(frozen=True)
class AttributionAudit:
    history: pd.DataFrame
    asset_summary: pd.DataFrame
    regime_summary: pd.DataFrame
    metrics: object


def classify_regime(portfolio: DualEnginePortfolio) -> str:
    """Return the dominant portfolio regime at the current point in the replay."""
    asym = getattr(portfolio, "asymmetric", None)
    macro = getattr(portfolio, "macro_filter", None)

    if asym is not None and asym.state == AsymmetricShifter.CRISIS:
        return "ACUTE_CRISIS"
    if asym is not None and asym.state == AsymmetricShifter.RECOVERY:
        return "ACUTE_RECOVERY"
    if macro is not None and getattr(macro, "state", None) == "RECOVERY":
        return "MACRO_RECOVERY"
    if macro is not None and macro.active:
        return "MACRO_DEFENSE"
    if portfolio.state == STATE_PROTECTION:
        return "CORE_PROTECTION"
    return "NORMAL"


def run_return_attribution(
    file_path: str = "data/etf_daily.csv",
    panic_sell_bps: float = PANIC_SELL_BPS,
    config: DualEngineConfig | None = None,
    macro_filter_factory: Callable[[object], object] | None = None,
) -> AttributionAudit:
    """Load local prices, replay the fixed policy, and return attribution tables."""
    df = load_prices(file_path)
    prices = df[ASSETS].sort_index()
    return run_return_attribution_from_prices(
        prices,
        panic_sell_bps=panic_sell_bps,
        config=config,
        macro_filter_factory=macro_filter_factory,
    )


def run_return_attribution_from_prices(
    prices: pd.DataFrame,
    panic_sell_bps: float = PANIC_SELL_BPS,
    config: DualEngineConfig | None = None,
    macro_filter_factory: Callable[[object], object] | None = None,
) -> AttributionAudit:
    """Replay the fixed policy from an already prepared SPY/TLT/GLD/SHV table."""
    prices = prices[ASSETS].sort_index()
    config = config or fixed_policy_config()

    stress_model = StressSlippageModel.from_prices(prices)
    signals = build_signals(prices, stress_model, config.macro.ma_window)
    returns = prices.pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)

    portfolio = DualEnginePortfolio(
        stress_model,
        AsymmetricShifter(config.asymmetric),
        (
            macro_filter_factory(config.macro)
            if macro_filter_factory is not None
            else MacroRegimeFilter(config.macro)
        ),
        panic_sell_bps=panic_sell_bps,
        initial_capital=INITIAL_CAPITAL,
    )
    risk = RiskEngine()
    risk.high_water_mark = INITIAL_CAPITAL

    rows: list[dict] = []
    for i, current_date in enumerate(prices.index):
        daily_ret = returns.iloc[i].values
        start_positions = portfolio.positions.copy()
        start_nav = float(portfolio.nav)
        start_weights = portfolio.weights.copy()
        return_regime = classify_regime(portfolio)

        asset_pnl = start_positions * daily_ret
        simulated_nav = float(np.sum(start_positions * (1 + daily_ret)))
        risk_signal = risk.evaluate(
            simulated_nav,
            float(spy_tlt_corr.iloc[i]),
            float(spy_30d_ret.iloc[i]),
            float(tlt_30d_ret.iloc[i]),
        )

        row = signals.loc[current_date]
        portfolio.acute_signal = AsymmetricShiftSignal(
            spy_vol_anomaly=float(row["spy_vol_anomaly"]),
            tlt_vol_anomaly=float(row["tlt_vol_anomaly"]),
            rolling_corr=float(row["rolling_corr"]),
            spy_trend=float(row["spy_trend"]),
            tlt_trend=float(row["tlt_trend"]),
            is_weekly_rebalance_day=bool(row["is_weekly_rebalance_day"]),
        )
        portfolio.macro_signal = MacroRegimeSignal(
            spy_price=float(prices.at[current_date, "SPY"]),
            tlt_price=float(prices.at[current_date, "TLT"]),
            spy_ma=float(row["spy_ma"]),
            tlt_ma=float(row["tlt_ma"]),
        )
        is_year_end = (
            i < len(prices.index) - 1
            and prices.index[i].year != prices.index[i + 1].year
        )

        order = portfolio.step(current_date.date(), daily_ret, risk_signal, is_year_end)
        cost = float(portfolio.last_order_cost if order is not None else 0.0)
        extra_cost = float(portfolio.last_extra_cost if order is not None else 0.0)
        end_weights = portfolio.weights.copy()
        end_regime = classify_regime(portfolio)

        record = {
            "date": current_date,
            "nav_start": start_nav,
            "nav_end": float(portfolio.nav),
            "return_regime": return_regime,
            "end_regime": end_regime,
            "action": order.reason if order is not None else None,
            "cost": cost,
            "extra_cost": extra_cost,
            "net_pnl": float(portfolio.nav - start_nav),
            "asset_pnl_total": float(np.sum(asset_pnl)),
        }
        for asset, weight, end_weight, ret, pnl in zip(
            ASSETS, start_weights, end_weights, daily_ret, asset_pnl
        ):
            record[f"{asset}_start_weight"] = float(weight)
            record[f"{asset}_end_weight"] = float(end_weight)
            record[f"{asset}_return"] = float(ret)
            record[f"{asset}_pnl"] = float(pnl)
            record[f"{asset}_daily_contribution"] = float(pnl / start_nav) if start_nav else 0.0
        rows.append(record)

    history = pd.DataFrame(rows).set_index("date")
    result = history[["nav_end"]].rename(columns={"nav_end": "NAV"})
    result["State"] = (history["end_regime"] != "NORMAL").astype(int)
    metrics = build_metrics(
        "return attribution fixed policy",
        result,
        pd.DataFrame(portfolio.action_rows),
        portfolio,
        INITIAL_CAPITAL,
    )
    asset_summary = summarize_assets(history, prices)
    regime_summary = summarize_regimes(history)
    return AttributionAudit(history, asset_summary, regime_summary, metrics)


def summarize_assets(history: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Summarize time-weighted exposure and dollar PnL by asset."""
    rows = []
    gross_asset_pnl = sum(float(history[f"{asset}_pnl"].sum()) for asset in ASSETS)
    for asset in ASSETS:
        pnl = float(history[f"{asset}_pnl"].sum())
        rows.append(
            {
                "asset": asset,
                "avg_start_weight": float(history[f"{asset}_start_weight"].mean()),
                "avg_end_weight": float(history[f"{asset}_end_weight"].mean()),
                "total_pnl": pnl,
                "pct_initial": pnl / INITIAL_CAPITAL,
                "gross_pnl_share": pnl / gross_asset_pnl if gross_asset_pnl else 0.0,
                "sum_daily_contribution": float(
                    history[f"{asset}_daily_contribution"].sum()
                ),
                "buy_hold_return": float(prices[asset].iloc[-1] / prices[asset].iloc[0] - 1),
            }
        )
    return pd.DataFrame(rows).set_index("asset")


def summarize_regimes(history: pd.DataFrame) -> pd.DataFrame:
    """Summarize days, cash exposure, costs, and PnL by return regime."""
    rows = []
    total_days = len(history)
    for regime, group in history.groupby("return_regime", sort=False):
        gross_pnl = float(group["asset_pnl_total"].sum())
        cost = float(group["cost"].sum())
        rows.append(
            {
                "regime": regime,
                "days": int(len(group)),
                "day_pct": len(group) / total_days if total_days else 0.0,
                "avg_shv_start_weight": float(group["SHV_start_weight"].mean()),
                "gross_pnl": gross_pnl,
                "cost": cost,
                "net_pnl": gross_pnl - cost,
                "net_pct_initial": (gross_pnl - cost) / INITIAL_CAPITAL,
                "spy_pnl": float(group["SPY_pnl"].sum()),
                "tlt_pnl": float(group["TLT_pnl"].sum()),
                "gld_pnl": float(group["GLD_pnl"].sum()),
                "shv_pnl": float(group["SHV_pnl"].sum()),
            }
        )
    return pd.DataFrame(rows).set_index("regime")


def annual_attribution(history: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year, group in history.groupby(history.index.year):
        start_nav = float(group["nav_start"].iloc[0])
        end_nav = float(group["nav_end"].iloc[-1])
        rows.append(
            {
                "year": int(year),
                "return": end_nav / start_nav - 1,
                "cost": float(group["cost"].sum()),
                "avg_shv_weight": float(group["SHV_start_weight"].mean()),
                "spy_pnl": float(group["SPY_pnl"].sum()),
                "tlt_pnl": float(group["TLT_pnl"].sum()),
                "gld_pnl": float(group["GLD_pnl"].sum()),
                "shv_pnl": float(group["SHV_pnl"].sum()),
            }
        )
    return pd.DataFrame(rows).set_index("year")


def print_report(audit: AttributionAudit) -> None:
    history = audit.history
    metrics = audit.metrics
    high_cash_days = int((history["SHV_end_weight"] >= HIGH_CASH_THRESHOLD).sum())
    extreme_cash_days = int((history["SHV_end_weight"] >= EXTREME_CASH_THRESHOLD).sum())
    total_days = len(history)
    total_asset_pnl = float(history["asset_pnl_total"].sum())
    total_cost = float(history["cost"].sum())
    annual = annual_attribution(history)
    worst_years = annual.sort_values("return").head(5)

    print("=" * 118)
    print("  Return Attribution Audit: Fixed MA150 + 8w + Both-Recovery")
    print("=" * 118)
    print(
        f"  CAGR {metrics.cagr:.2%} | MDD {metrics.mdd:.2%} | "
        f"Final NAV {metrics.final:,.2f} | Rebalances {metrics.rebalances} | Cost {metrics.total_cost:,.2f}"
    )
    print(
        f"  Asset PnL {total_asset_pnl:,.2f} - Cost {total_cost:,.2f} "
        f"= Net {total_asset_pnl - total_cost:,.2f}"
    )
    print(
        f"  SHV >= 50%: {high_cash_days} days ({high_cash_days / total_days:.2%}) | "
        f"SHV >= 70%: {extreme_cash_days} days ({extreme_cash_days / total_days:.2%})"
    )
    print("-" * 118)
    print("  Asset attribution")
    print(
        f"  {'Asset':<6} {'Avg Wt':>8} {'End Wt':>8} {'PnL':>13} "
        f"{'% Initial':>10} {'Gross Share':>12} {'Buy/Hold':>10}"
    )
    for asset, row in audit.asset_summary.iterrows():
        print(
            f"  {asset:<6} {row['avg_start_weight']:>7.2%} {row['avg_end_weight']:>7.2%} "
            f"{row['total_pnl']:>13,.2f} {row['pct_initial']:>9.2%} "
            f"{row['gross_pnl_share']:>11.2%} {row['buy_hold_return']:>9.2%}"
        )
    print("-" * 118)
    print("  Regime attribution")
    print(
        f"  {'Regime':<16} {'Days':>6} {'Day %':>8} {'Avg SHV':>9} "
        f"{'Gross PnL':>13} {'Cost':>11} {'Net PnL':>13} {'Net %':>9}"
    )
    for regime, row in audit.regime_summary.iterrows():
        print(
            f"  {regime:<16} {int(row['days']):>6d} {row['day_pct']:>7.2%} "
            f"{row['avg_shv_start_weight']:>8.2%} {row['gross_pnl']:>13,.2f} "
            f"{row['cost']:>11,.2f} {row['net_pnl']:>13,.2f} {row['net_pct_initial']:>8.2%}"
        )
    print("-" * 118)
    print("  Worst annual slices")
    print(
        f"  {'Year':<6} {'Return':>9} {'Avg SHV':>9} {'Cost':>11} "
        f"{'SPY':>11} {'TLT':>11} {'GLD':>11} {'SHV':>11}"
    )
    for year, row in worst_years.iterrows():
        print(
            f"  {year:<6d} {row['return']:>8.2%} {row['avg_shv_weight']:>8.2%} "
            f"{row['cost']:>11,.2f} {row['spy_pnl']:>11,.2f} {row['tlt_pnl']:>11,.2f} "
            f"{row['gld_pnl']:>11,.2f} {row['shv_pnl']:>11,.2f}"
        )
    print("=" * 118)


def plot_attribution(audit: AttributionAudit) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "return_attribution.png"

    history = audit.history
    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    axes[0].plot(history.index, history["nav_end"], color="#1f2933", linewidth=1.0)
    axes[0].set_title("Fixed Policy NAV")
    axes[0].set_ylabel("NAV")
    axes[0].grid(True, alpha=0.3)

    for asset in ASSETS:
        axes[1].plot(
            history.index,
            history[f"{asset}_pnl"].cumsum(),
            linewidth=0.9,
            label=asset,
        )
    axes[1].set_title("Cumulative Asset PnL Before Costs")
    axes[1].set_ylabel("PnL")
    axes[1].legend(fontsize=8)
    axes[1].grid(True, alpha=0.3)

    weight_cols = [f"{asset}_end_weight" for asset in ASSETS]
    axes[2].stackplot(
        history.index,
        [history[col] for col in weight_cols],
        labels=ASSETS,
        alpha=0.85,
    )
    axes[2].set_title("End-of-Day Weights")
    axes[2].set_ylabel("Weight")
    axes[2].legend(fontsize=8, loc="upper left")
    axes[2].grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    audit = run_return_attribution()
    print_report(audit)
    output_path = plot_attribution(audit)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
