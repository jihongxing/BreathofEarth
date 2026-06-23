"""
Stepped macro-recovery audit.

The fixed policy keeps:
- MA150 macro trigger/release logic
- 8-week acute recovery
- 10/5/35/50 macro-defense weights
- 50bp panic slippage

This bypass experiment changes only the exit path after MacroRegimeFilter
releases: instead of jumping straight back to 25/25/25/25, it linearly blends
from defense weights to baseline weights over a fixed number of trading days.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.fixed_policy_audit import TARGET_CAGR, TARGET_MDD, fixed_policy_config
from backtest.return_attribution import (
    HIGH_CASH_THRESHOLD,
    PANIC_SELL_BPS,
    AttributionAudit,
    run_return_attribution,
)
from engine.config import WEIGHTS_IDLE
from engine.macro_filter import MacroRegimeConfig, MacroRegimeDecision, MacroRegimeSignal


RECOVERY_SCENARIOS = {
    "baseline_instant": 0,
    "stepped_20d": 20,
    "stepped_45d": 45,
}


class SteppedMacroRegimeFilter:
    """Macro filter with linear re-entry after defense release."""

    NORMAL = "NORMAL"
    DEFENSE = "DEFENSE"
    RECOVERY = "RECOVERY"

    def __init__(self, config: MacroRegimeConfig, recovery_days: int):
        if recovery_days < 1:
            raise ValueError("recovery_days must be positive")
        if abs(sum(config.defense_weights) - 1.0) > 1e-9:
            raise ValueError("defense_weights must sum to 1.0")
        self.config = config
        self.recovery_days = recovery_days
        self.state = self.NORMAL
        self.active = False
        self.trigger_count = 0
        self.release_count = 0
        self.active_days = 0
        self.recovery_days_count = 0
        self.recovery_step = 0

    def decide(self, signal: MacroRegimeSignal) -> MacroRegimeDecision:
        breakdown = self._is_breakdown(signal)
        recovered = self._is_recovered(signal)

        if self.state == self.DEFENSE:
            if recovered:
                self.state = self.RECOVERY
                self.active = False
                self.release_count += 1
                self.recovery_step = 1
                self.recovery_days_count += 1
                return MacroRegimeDecision(
                    self._recovery_weights(),
                    "非对称分批复位",
                    False,
                )
            self.active_days += 1
            return MacroRegimeDecision(self.config.defense_weights, None, True)

        if self.state == self.RECOVERY:
            if breakdown:
                self.state = self.DEFENSE
                self.active = True
                self.trigger_count += 1
                self.active_days += 1
                self.recovery_step = 0
                return MacroRegimeDecision(
                    self.config.defense_weights,
                    "进入宏观慢熊防御",
                    True,
                )

            self.recovery_days_count += 1
            self.recovery_step += 1
            if self.recovery_step >= self.recovery_days:
                weights = tuple(float(w) for w in WEIGHTS_IDLE)
                self.state = self.NORMAL
                self.active = False
                return MacroRegimeDecision(weights, "非对称分批复位", False)
            return MacroRegimeDecision(
                self._recovery_weights(),
                "非对称分批复位",
                False,
            )

        if breakdown:
            self.state = self.DEFENSE
            self.active = True
            self.trigger_count += 1
            self.active_days += 1
            return MacroRegimeDecision(
                self.config.defense_weights,
                "进入宏观慢熊防御",
                True,
            )

        return MacroRegimeDecision(None, None, False)

    def _recovery_weights(self) -> tuple[float, float, float, float]:
        alpha = min(self.recovery_step / self.recovery_days, 1.0)
        return tuple(
            defense + alpha * (baseline - defense)
            for defense, baseline in zip(self.config.defense_weights, WEIGHTS_IDLE)
        )

    def _is_breakdown(self, signal: MacroRegimeSignal) -> bool:
        if signal.spy_ma <= 0 or signal.tlt_ma <= 0:
            return False
        return signal.spy_price < signal.spy_ma and signal.tlt_price < signal.tlt_ma

    def _is_recovered(self, signal: MacroRegimeSignal) -> bool:
        if signal.spy_ma <= 0 or signal.tlt_ma <= 0:
            return False
        spy_recovered = signal.spy_price >= signal.spy_ma
        tlt_recovered = signal.tlt_price >= signal.tlt_ma
        if self.config.release_on_any_recovery:
            return spy_recovered or tlt_recovered
        return spy_recovered and tlt_recovered


@dataclass(frozen=True)
class SteppedRecoveryResult:
    name: str
    recovery_days: int
    audit: AttributionAudit
    high_cash_pct: float
    macro_recovery_days: int
    macro_recovery_net_pnl: float
    macro_recovery_cost: float


def run_stepped_recovery_audit() -> dict[str, SteppedRecoveryResult]:
    results: dict[str, SteppedRecoveryResult] = {}
    config = fixed_policy_config()
    for name, recovery_days in RECOVERY_SCENARIOS.items():
        if recovery_days == 0:
            audit = run_return_attribution(panic_sell_bps=PANIC_SELL_BPS, config=config)
        else:
            audit = run_return_attribution(
                panic_sell_bps=PANIC_SELL_BPS,
                config=config,
                macro_filter_factory=lambda macro_config, days=recovery_days: SteppedMacroRegimeFilter(
                    macro_config,
                    days,
                ),
            )
        history = audit.history
        high_cash_pct = float((history["SHV_end_weight"] >= HIGH_CASH_THRESHOLD).mean())
        if "MACRO_RECOVERY" in audit.regime_summary.index:
            recovery = audit.regime_summary.loc["MACRO_RECOVERY"]
            recovery_regime_days = int(recovery["days"])
            recovery_net_pnl = float(recovery["net_pnl"])
            recovery_cost = float(recovery["cost"])
        else:
            recovery_regime_days = 0
            recovery_net_pnl = 0.0
            recovery_cost = 0.0
        results[name] = SteppedRecoveryResult(
            name=name,
            recovery_days=recovery_days,
            audit=audit,
            high_cash_pct=high_cash_pct,
            macro_recovery_days=recovery_regime_days,
            macro_recovery_net_pnl=recovery_net_pnl,
            macro_recovery_cost=recovery_cost,
        )
    return results


def print_report(results: dict[str, SteppedRecoveryResult]) -> None:
    print("=" * 130)
    print("  Stepped Macro-Recovery Audit: MA150 + 8w + 10/5/35/50 + 50bp Panic")
    print("=" * 130)
    print(
        f"  {'Scenario':<18} {'Steps':>7} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Cost':>11} {'SHV>=50':>9} {'Rebal':>7} {'MacroRec Days':>13} {'Verdict':>9}"
    )
    for name, result in results.items():
        metrics = result.audit.metrics
        verdict = "PASS" if metrics.cagr >= TARGET_CAGR and metrics.mdd >= TARGET_MDD else "FAIL"
        steps = "instant" if result.recovery_days == 0 else f"{result.recovery_days}d"
        print(
            f"  {name:<18} {steps:>7} {metrics.cagr:>7.2%} {metrics.mdd:>8.2%} "
            f"{metrics.final:>13,.2f} {metrics.total_cost:>11,.2f} {result.high_cash_pct:>8.2%} "
            f"{metrics.rebalances:>7d} {result.macro_recovery_days:>13d} {verdict:>9}"
        )
    print("-" * 130)
    print("  Macro-recovery attribution only")
    print(f"  {'Scenario':<18} {'Days':>7} {'Net PnL':>13} {'Cost':>11}")
    for name, result in results.items():
        print(
            f"  {name:<18} {result.macro_recovery_days:>7d} "
            f"{result.macro_recovery_net_pnl:>13,.2f} {result.macro_recovery_cost:>11,.2f}"
        )
    print("=" * 130)


def plot_results(results: dict[str, SteppedRecoveryResult]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "stepped_recovery_audit.png"

    fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
    for name, result in results.items():
        history = result.audit.history
        nav = history["nav_end"]
        drawdown = (nav - nav.cummax()) / nav.cummax()
        axes[0].plot(history.index, nav, linewidth=1.0, label=name)
        axes[1].plot(history.index, drawdown, linewidth=0.9, label=name)
        axes[2].plot(history.index, history["SHV_end_weight"], linewidth=0.9, label=name)

    axes[0].set_title("NAV by Macro Recovery Path")
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
    results = run_stepped_recovery_audit()
    print_report(results)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
