"""
Asymmetric macro-defense audit.

This bypass test keeps the fixed MA150 + 8w + 50bp panic policy intact and
changes only the MacroRegimeFilter defense weights. The goal is to see whether
selectively cutting the bond sleeve preserves SPY/GLD compounding without
loosening the crisis trigger.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.fixed_policy_audit import TARGET_CAGR, TARGET_MDD, fixed_policy_config
from backtest.macro_regime_backtest import DualEngineConfig, asymmetric_config
from backtest.return_attribution import (
    HIGH_CASH_THRESHOLD,
    PANIC_SELL_BPS,
    AttributionAudit,
    run_return_attribution,
)
from engine.macro_filter import MacroRegimeConfig


DEFENSE_SCENARIOS = {
    "baseline_10_05_35_50": (0.10, 0.05, 0.35, 0.50),
    "cut_tlt_keep_spy_gld": (0.25, 0.00, 0.25, 0.50),
    "cut_tlt_tilt_gld": (0.20, 0.00, 0.30, 0.50),
}


@dataclass(frozen=True)
class DefenseScenarioResult:
    name: str
    defense_weights: tuple[float, float, float, float]
    audit: AttributionAudit
    high_cash_pct: float
    macro_days: int
    macro_net_pnl: float
    macro_spy_pnl: float
    macro_tlt_pnl: float
    macro_gld_pnl: float


def scenario_config(name: str, weights: tuple[float, float, float, float]) -> DualEngineConfig:
    if name == "baseline_10_05_35_50":
        return fixed_policy_config()
    macro = MacroRegimeConfig(
        name=f"macro_ma150_{name}",
        ma_window=150,
        defense_weights=weights,
        release_on_any_recovery=False,
    )
    return DualEngineConfig(
        name=f"fixed_8w_{name}",
        asymmetric=asymmetric_config(8),
        macro=macro,
    )


def run_asymmetric_defense_audit() -> dict[str, DefenseScenarioResult]:
    results: dict[str, DefenseScenarioResult] = {}
    for name, weights in DEFENSE_SCENARIOS.items():
        audit = run_return_attribution(
            panic_sell_bps=PANIC_SELL_BPS,
            config=scenario_config(name, weights),
        )
        history = audit.history
        high_cash_pct = float((history["SHV_end_weight"] >= HIGH_CASH_THRESHOLD).mean())
        macro = audit.regime_summary.loc["MACRO_DEFENSE"]
        results[name] = DefenseScenarioResult(
            name=name,
            defense_weights=weights,
            audit=audit,
            high_cash_pct=high_cash_pct,
            macro_days=int(macro["days"]),
            macro_net_pnl=float(macro["net_pnl"]),
            macro_spy_pnl=float(macro["spy_pnl"]),
            macro_tlt_pnl=float(macro["tlt_pnl"]),
            macro_gld_pnl=float(macro["gld_pnl"]),
        )
    return results


def print_report(results: dict[str, DefenseScenarioResult]) -> None:
    print("=" * 132)
    print("  Asymmetric Defense Audit: MA150 + 8w + 50bp Panic")
    print("=" * 132)
    print(
        f"  {'Scenario':<24} {'Weights SPY/TLT/GLD/SHV':<24} {'CAGR':>8} {'MDD':>9} "
        f"{'Final NAV':>13} {'Cost':>11} {'SHV>=50':>9} {'Rebal':>7} {'Macro':>7} {'Verdict':>9}"
    )
    for name, result in results.items():
        metrics = result.audit.metrics
        verdict = "PASS" if metrics.cagr >= TARGET_CAGR and metrics.mdd >= TARGET_MDD else "FAIL"
        weights = "/".join(f"{w:.0%}" for w in result.defense_weights)
        print(
            f"  {name:<24} {weights:<24} {metrics.cagr:>7.2%} {metrics.mdd:>8.2%} "
            f"{metrics.final:>13,.2f} {metrics.total_cost:>11,.2f} {result.high_cash_pct:>8.2%} "
            f"{metrics.rebalances:>7d} {metrics.macro_triggers:>7d} {verdict:>9}"
        )
    print("-" * 132)
    print("  Macro-defense attribution only")
    print(
        f"  {'Scenario':<24} {'Days':>6} {'Macro Net':>13} {'SPY PnL':>13} "
        f"{'TLT PnL':>13} {'GLD PnL':>13}"
    )
    for name, result in results.items():
        print(
            f"  {name:<24} {result.macro_days:>6d} {result.macro_net_pnl:>13,.2f} "
            f"{result.macro_spy_pnl:>13,.2f} {result.macro_tlt_pnl:>13,.2f} "
            f"{result.macro_gld_pnl:>13,.2f}"
        )
    print("=" * 132)


def plot_results(results: dict[str, DefenseScenarioResult]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "asymmetric_defense_audit.png"

    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    for name, result in results.items():
        history = result.audit.history
        nav = history["nav_end"]
        drawdown = (nav - nav.cummax()) / nav.cummax()
        axes[0].plot(history.index, nav, linewidth=1.0, label=name)
        axes[1].plot(history.index, drawdown, linewidth=0.9, label=name)
        axes[2].plot(history.index, history["SHV_end_weight"], linewidth=0.9, label=name)

    axes[0].set_title("NAV by Macro Defense Weights")
    axes[0].set_ylabel("NAV")
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].axhline(TARGET_MDD, color="black", linestyle="--", alpha=0.7)
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
    results = run_asymmetric_defense_audit()
    print_report(results)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
