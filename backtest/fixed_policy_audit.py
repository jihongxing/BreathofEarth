"""
Fixed policy audit.

No grid search. No walk-forward parameter selection. This script audits one
governance candidate:

    acute asymmetric shifter: 8-week re-entry
    macro filter: MA150, release only when both SPY and TLT recover

It then applies panic-slippage breakpoints and checks whether the local data has
enough OHLCV information to run a capacity/market-impact audit.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from backtest.macro_regime_backtest import (
    DualEngineConfig,
    MacroRegimeMetrics,
    asymmetric_config,
    run_scenario,
)
from engine.config import ASSETS
from engine.macro_filter import MacroRegimeConfig


PANIC_BPS_LEVELS = [0, 50, 100, 200]
TARGET_CAGR = 0.075
TARGET_MDD = -0.13
LIVE_AUM = 2_000_000.0
MAX_ADV_PARTICIPATION = 0.005


@dataclass(frozen=True)
class CapacityAuditResult:
    status: str
    reason: str
    rows_checked: int = 0
    max_participation: float | None = None


def fixed_policy_config() -> DualEngineConfig:
    macro = MacroRegimeConfig(
        name="macro_ma150_both",
        ma_window=150,
        defense_weights=(0.10, 0.05, 0.35, 0.50),
        release_on_any_recovery=False,
    )
    return DualEngineConfig(
        name="fixed_ma150_8w_both",
        asymmetric=asymmetric_config(8),
        macro=macro,
    )


def load_prices(file_path: str = "data/etf_daily.csv") -> pd.DataFrame:
    df = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()
    validate_positive_prices(df[ASSETS])
    return df


def validate_positive_prices(prices: pd.DataFrame) -> None:
    flattened = prices.stack()
    bad = flattened[flattened <= 0]
    if bad.empty:
        return
    samples = [
        f"{idx[0].date()} {idx[1]}={value:.4f}"
        for idx, value in bad.head(8).items()
    ]
    raise ValueError(
        "non-positive prices found; audit cannot proceed: "
        + ", ".join(samples)
    )


def run_fixed_policy_audit(file_path: str = "data/etf_daily.csv") -> dict[int, MacroRegimeMetrics]:
    df = load_prices(file_path)
    prices = df[ASSETS]
    config = fixed_policy_config()
    results: dict[int, MacroRegimeMetrics] = {}
    for bps in PANIC_BPS_LEVELS:
        results[bps] = run_scenario(
            prices,
            f"fixed MA150+8w panic {bps}bp",
            config,
            panic_sell_bps=bps / 10_000,
        )
    return results


def crisis_slice(run: MacroRegimeMetrics, start: str, end: str) -> dict:
    result = run.result.loc[start:end]
    period_return = result["NAV"].iloc[-1] / result["NAV"].iloc[0] - 1
    drawdown = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    actions = run.actions
    if not actions.empty:
        actions = actions[(actions["date"] >= pd.Timestamp(start)) & (actions["date"] <= pd.Timestamp(end))]
    return {
        "return": float(period_return),
        "mdd": float(drawdown.min()),
        "actions": int(len(actions)),
        "acute_entries": int(actions["action"].isin(["进入非对称防御", "恢复期再触发防御"]).sum()) if not actions.empty else 0,
        "macro_entries": int((actions["action"] == "进入宏观慢熊防御").sum()) if not actions.empty else 0,
    }


def capacity_audit(df: pd.DataFrame, run: MacroRegimeMetrics, live_aum: float = LIVE_AUM) -> CapacityAuditResult:
    required = []
    for asset in ["SPY", "TLT", "GLD", "SHV"]:
        required.extend([f"{asset}_Volume", f"{asset}_High", f"{asset}_Low"])
    missing = [col for col in required if col not in df.columns]
    if missing:
        return CapacityAuditResult(
            status="SKIPPED",
            reason=(
                "local dataset has adjusted-close prices only; missing OHLCV columns "
                f"such as {', '.join(missing[:4])}"
            ),
        )

    if run.actions.empty:
        return CapacityAuditResult(status="OK", reason="no rebalance actions to inspect")

    # Conservative proxy: assume the largest rebalance may trade up to 85% of live AUM.
    trade_notional = live_aum * 0.85
    max_participation = 0.0
    checked = 0
    for _, action in run.actions.iterrows():
        ts = pd.Timestamp(action["date"])
        if ts not in df.index:
            continue
        daily_dollar_volume = 0.0
        for asset in ["SPY", "TLT", "GLD"]:
            daily_dollar_volume += float(df.at[ts, asset]) * float(df.at[ts, f"{asset}_Volume"])
        if daily_dollar_volume <= 0:
            continue
        max_participation = max(max_participation, trade_notional / daily_dollar_volume)
        checked += 1

    if max_participation > MAX_ADV_PARTICIPATION:
        return CapacityAuditResult(
            status="FAIL",
            reason=f"estimated participation {max_participation:.3%} exceeds {MAX_ADV_PARTICIPATION:.3%}",
            rows_checked=checked,
            max_participation=max_participation,
        )
    return CapacityAuditResult(
        status="PASS",
        reason=f"estimated participation {max_participation:.3%} within {MAX_ADV_PARTICIPATION:.3%}",
        rows_checked=checked,
        max_participation=max_participation,
    )


def print_report(results: dict[int, MacroRegimeMetrics], capacity: CapacityAuditResult) -> None:
    print("=" * 112)
    print("  Fixed Policy Audit: MA150 + 8w + Both-Recovery")
    print("=" * 112)
    print(f"  Target: CAGR >= {TARGET_CAGR:.2%}, MDD >= {TARGET_MDD:.2%}")
    print("-" * 112)
    print(f"  {'Panic Slippage':<18} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} {'Rebal':>7} {'Acute':>7} {'Macro':>7} {'Cost':>11} {'Verdict':>9}")
    for bps, run in results.items():
        verdict = "PASS" if run.cagr >= TARGET_CAGR and run.mdd >= TARGET_MDD else "FAIL"
        print(
            f"  {bps:>4d} bp{'':<10} {run.cagr:>7.2%} {run.mdd:>8.2%} {run.final:>13,.2f} "
            f"{run.rebalances:>7d} {run.acute_triggers:>7d} {run.macro_triggers:>7d} {run.total_cost:>11,.2f} {verdict:>9}"
        )

    print("-" * 112)
    periods = {
        "2008": ("2008-01-01", "2008-12-31"),
        "2020": ("2020-01-01", "2020-12-31"),
        "2022": ("2022-01-01", "2022-12-31"),
    }
    base_run = results[50]
    print("  Crisis slices under 50bp panic slippage:")
    print(f"  {'Period':<8} {'Return':>9} {'MDD':>9} {'Actions':>8} {'Acute':>7} {'Macro':>7}")
    for period, (start, end) in periods.items():
        item = crisis_slice(base_run, start, end)
        print(
            f"  {period:<8} {item['return']:>8.2%} {item['mdd']:>8.2%} "
            f"{item['actions']:>8d} {item['acute_entries']:>7d} {item['macro_entries']:>7d}"
        )

    print("-" * 112)
    print(f"  Capacity audit: {capacity.status}")
    print(f"  Reason: {capacity.reason}")
    if capacity.max_participation is not None:
        print(f"  Rows checked: {capacity.rows_checked}, max participation: {capacity.max_participation:.3%}")
    print("=" * 112)


def plot_results(results: dict[int, MacroRegimeMetrics]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "fixed_policy_audit.png"
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    colors = {0: "#2c3e50", 50: "#1f8a70", 100: "#c0392b", 200: "#8e44ad"}
    for bps, run in results.items():
        drawdown = (run.result["NAV"] - run.result["NAV"].cummax()) / run.result["NAV"].cummax()
        axes[0].plot(run.result.index, run.result["NAV"], label=f"{bps}bp panic", color=colors[bps], linewidth=1.0)
        axes[1].plot(drawdown.index, drawdown, label=f"{bps}bp panic", color=colors[bps], linewidth=0.9)
    axes[1].axhline(y=TARGET_MDD, color="black", linestyle="--", alpha=0.7)
    axes[0].set_title("Fixed Policy Audit: MA150 + 8w + Both-Recovery")
    axes[0].set_ylabel("NAV")
    axes[0].legend(fontsize=8)
    axes[0].grid(True, alpha=0.3)
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].legend(fontsize=8)
    axes[1].grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    df = load_prices()
    results = run_fixed_policy_audit()
    capacity = capacity_audit(df, results[50])
    print_report(results, capacity)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
