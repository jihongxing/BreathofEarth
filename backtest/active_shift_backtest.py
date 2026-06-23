"""
Active shift backtest.

This is a bypass experiment. It keeps the production PortfolioEngine intact and
tests whether earlier, more aggressive defensive weight shifts can reduce MDD
without destroying CAGR.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.config import (
    ASSETS,
    CORR_WINDOW,
    DRIFT_THRESHOLD,
    STATE_IDLE,
    STATE_PROTECTION,
    WEIGHTS_IDLE,
)
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel
from engine.weight_shifter import MarketShiftSignal, WeightShiftConfig, WeightShifter


INITIAL_CAPITAL = 100000.0


@dataclass
class ActiveShiftMetrics:
    label: str
    result: pd.DataFrame
    drawdown: pd.Series
    final: float
    cagr: float
    mdd: float
    mdd_date: pd.Timestamp
    vol: float
    sharpe: float
    rebalances: int
    protection_count: int
    protection_days: int
    shift_triggers: int
    shift_days: int
    total_cost: float


class ActiveShiftPortfolio(PortfolioEngine):
    """Portfolio engine variant that lets WeightShifter override target weights."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        shifter: WeightShifter | None = None,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.shifter = shifter
        self.current_date = None
        self.current_shift_signal: MarketShiftSignal | None = None
        self.total_cost = 0.0
        self.shift_triggers = 0
        self.shift_days = 0

    def step(self, current_date, daily_returns, risk_signal, is_year_end=False):
        self.current_date = current_date
        self.apply_daily_returns(daily_returns)

        target_override = None
        shift_action = None
        if self.shifter is not None and self.current_shift_signal is not None:
            before_triggers = self.shifter.trigger_count
            decision = self.shifter.decide(self.current_shift_signal)
            if self.shifter.trigger_count > before_triggers:
                self.shift_triggers += 1
            if decision.in_defense_mode:
                self.shift_days += 1
            target_override = decision.target_weights
            shift_action = decision.action

        order = self.evaluate_rebalance_with_override(
            risk_signal=risk_signal,
            is_year_end=is_year_end,
            target_override=target_override,
            shift_action=shift_action,
        )

        action = shift_action
        if order is not None:
            self.apply_rebalance(order)
            action = order.reason

        self.record_snapshot(current_date, risk_signal, action=action)
        return order

    def evaluate_rebalance_with_override(
        self,
        risk_signal,
        is_year_end: bool,
        target_override: tuple[float, float, float, float] | None,
        shift_action: str | None,
    ) -> RebalanceOrder | None:
        if target_override is not None:
            if self.state == STATE_IDLE and risk_signal.is_hard_stop:
                self.state = STATE_PROTECTION
                self.cooldown_counter = 40
                self.protection_count += 1
            elif self.state == STATE_IDLE and risk_signal.is_protection:
                self.state = STATE_PROTECTION
                self.cooldown_counter = 20
                self.protection_count += 1

            max_drift = float(np.max(np.abs(self.weights - np.array(target_override))))
            if max_drift > DRIFT_THRESHOLD or shift_action == "进入主动防御":
                return self._make_order(list(target_override), shift_action or "主动防御再平衡")
            return None

        if self.shifter is not None and shift_action == "解除主动防御":
            self.state = STATE_IDLE
            return self._make_order(WEIGHTS_IDLE, "解除主动防御: 恢复默认权重")

        return self.evaluate_rebalance(risk_signal, is_year_end)

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        estimate = self.slippage_model.estimate_rebalance_cost(
            current_positions=self.positions,
            target_weights=np.array(target_weights, dtype=float),
            current_date=self.current_date,
            assets=ASSETS,
        )
        self.total_cost += estimate.total_cost
        turnover = float(np.sum(np.abs(self.weights - np.array(target_weights, dtype=float))) / 2)
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=estimate.total_cost,
            reason=reason,
        )


def build_shift_signals(prices: pd.DataFrame, stress_model: StressSlippageModel, trend_window: int) -> pd.DataFrame:
    returns = prices[ASSETS].pct_change().fillna(0)
    return pd.DataFrame(
        {
            "spy_vol_anomaly": stress_model.volatility_anomaly["SPY"],
            "tlt_vol_anomaly": stress_model.volatility_anomaly["TLT"],
            "rolling_corr": returns["SPY"].rolling(CORR_WINDOW).corr(returns["TLT"]).fillna(0),
            "spy_trend": prices["SPY"].pct_change(trend_window).fillna(0),
            "tlt_trend": prices["TLT"].pct_change(trend_window).fillna(0),
        },
        index=prices.index,
    ).fillna(0)


def run_scenario(
    prices: pd.DataFrame,
    stress_model: StressSlippageModel,
    label: str,
    shift_config: WeightShiftConfig | None = None,
) -> ActiveShiftMetrics:
    returns = prices[ASSETS].pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)
    dates = returns.index

    shifter = WeightShifter(shift_config) if shift_config is not None else None
    trend_window = shift_config.trend_window if shift_config is not None else CORR_WINDOW
    shift_signals = build_shift_signals(prices, stress_model, trend_window)
    portfolio = ActiveShiftPortfolio(stress_model, shifter, initial_capital=INITIAL_CAPITAL)
    risk = RiskEngine()
    risk.high_water_mark = INITIAL_CAPITAL
    nav_history = []
    state_history = []

    for i, current_date in enumerate(dates):
        daily_ret = returns.iloc[i].values
        simulated_nav = float(np.sum(portfolio.positions * (1 + daily_ret)))
        risk_signal = risk.evaluate(
            simulated_nav,
            spy_tlt_corr.iloc[i],
            spy_30d_ret.iloc[i],
            tlt_30d_ret.iloc[i],
        )
        row = shift_signals.loc[current_date]
        portfolio.current_shift_signal = MarketShiftSignal(
            spy_vol_anomaly=float(row["spy_vol_anomaly"]),
            tlt_vol_anomaly=float(row["tlt_vol_anomaly"]),
            rolling_corr=float(row["rolling_corr"]),
            spy_trend=float(row["spy_trend"]),
            tlt_trend=float(row["tlt_trend"]),
        )
        is_year_end = i < len(dates) - 1 and dates[i].year != dates[i + 1].year
        portfolio.step(current_date.date(), daily_ret, risk_signal, is_year_end)
        nav_history.append(portfolio.nav)
        state_history.append(1 if portfolio.state == STATE_PROTECTION else 0)

    result = pd.DataFrame({"NAV": nav_history, "State": state_history}, index=dates)
    years = (dates[-1] - dates[0]).days / 365.25
    final = float(result["NAV"].iloc[-1])
    cagr = (final / INITIAL_CAPITAL) ** (1 / years) - 1
    drawdown = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    vol = float(result["NAV"].pct_change().std() * np.sqrt(252))
    sharpe = (cagr - 0.02) / vol if vol > 0 else 0.0

    return ActiveShiftMetrics(
        label=label,
        result=result,
        drawdown=drawdown,
        final=final,
        cagr=float(cagr),
        mdd=float(drawdown.min()),
        mdd_date=drawdown.idxmin(),
        vol=vol,
        sharpe=float(sharpe),
        rebalances=portfolio.rebalance_count,
        protection_count=portfolio.protection_count,
        protection_days=sum(state_history),
        shift_triggers=portfolio.shift_triggers,
        shift_days=portfolio.shift_days,
        total_cost=float(portfolio.total_cost),
    )


def scenario_configs() -> list[WeightShiftConfig]:
    configs: list[WeightShiftConfig] = []
    defense_sets = [
        ("half_cash", (0.10, 0.10, 0.25, 0.55)),
        ("extreme_cash", (0.00, 0.00, 0.30, 0.70)),
        ("cash_lock", (0.00, 0.00, 0.00, 1.00)),
    ]
    for defense_name, weights in defense_sets:
        for vol_threshold in [1.5, 2.0, 2.5, 3.0]:
            for cooldown in [30, 45, 60, 90]:
                configs.append(
                    WeightShiftConfig(
                        name=f"vol{vol_threshold:g}_{defense_name}_{cooldown}d",
                        defense_weights=weights,
                        vol_anomaly_threshold=vol_threshold,
                        corr_threshold=None,
                        cooldown_days=cooldown,
                        use_vol_trigger=True,
                        use_corr_trigger=False,
                    )
                )
        if defense_name != "cash_lock":
            for corr_threshold in [0.3, 0.4, 0.5]:
                for trend_window in [15, 20, 30]:
                    for cooldown in [20, 30, 45]:
                        configs.append(
                            WeightShiftConfig(
                                name=f"corr{corr_threshold:g}_{trend_window}d_{defense_name}_{cooldown}d",
                                defense_weights=weights,
                                vol_anomaly_threshold=None,
                                corr_threshold=corr_threshold,
                                trend_window=trend_window,
                                cooldown_days=cooldown,
                                use_vol_trigger=False,
                                use_corr_trigger=True,
                            )
                        )
    return configs


def run_active_shift_backtest(file_path: str = "data/etf_daily.csv") -> dict[str, ActiveShiftMetrics]:
    prices = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()[ASSETS]
    stress_model = StressSlippageModel.from_prices(prices)

    results = {
        "baseline_stress": run_scenario(
            prices,
            stress_model,
            "baseline stress slippage",
            shift_config=None,
        )
    }
    for config in scenario_configs():
        results[config.name] = run_scenario(
            prices,
            stress_model,
            config.name,
            shift_config=config,
        )
    return results


def print_report(results: dict[str, ActiveShiftMetrics], top_n: int = 12) -> None:
    baseline = results["baseline_stress"]
    ranked = sorted(
        [m for key, m in results.items() if key != "baseline_stress"],
        key=lambda m: (-m.mdd, -m.cagr),
    )
    print("=" * 104)
    print("  Xi-Rang Active Shift Backtest")
    print("=" * 104)
    print(f"  Window: {baseline.result.index[0].date()} ~ {baseline.result.index[-1].date()}")
    print(f"  Baseline stress: CAGR {baseline.cagr:.2%}, MDD {baseline.mdd:.2%}, Final {baseline.final:,.2f}")
    print("-" * 104)
    print(
        f"  {'Scenario':<40} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Rebal':>7} {'Shift':>7} {'Days':>7} {'Cost':>10}"
    )
    for row in ranked[:top_n]:
        print(
            f"  {row.label:<40} {row.cagr:>7.2%} {row.mdd:>8.2%} {row.final:>13,.2f} "
            f"{row.rebalances:>7d} {row.shift_triggers:>7d} {row.shift_days:>7d} {row.total_cost:>10,.2f}"
        )

    viable = [
        row
        for row in ranked
        if row.mdd >= -0.12 and row.cagr >= baseline.cagr - 0.005
    ]
    print("-" * 104)
    if viable:
        best = sorted(viable, key=lambda m: (-m.cagr, -m.mdd))[0]
        print(f"  Viable target found: {best.label} | CAGR {best.cagr:.2%} | MDD {best.mdd:.2%}")
    else:
        print("  No scenario met target: MDD >= -12% and CAGR drawdown <= 0.50%.")
    print("=" * 104)


def plot_results(results: dict[str, ActiveShiftMetrics]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "active_shift_backtest.png"

    baseline = results["baseline_stress"]
    ranked = sorted(
        [m for key, m in results.items() if key != "baseline_stress"],
        key=lambda m: (-m.mdd, -m.cagr),
    )
    selected = [baseline] + ranked[:3]
    colors = ["#2c3e50", "#1f8a70", "#c0392b", "#8e44ad"]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    for row, color in zip(selected, colors):
        axes[0].plot(row.result.index, row.result["NAV"], label=row.label, color=color, linewidth=1.1)
        axes[1].plot(row.drawdown.index, row.drawdown, label=row.label, color=color, linewidth=1.0)

    axes[0].set_title("Xi-Rang Active Shift Top Scenarios")
    axes[0].set_ylabel("NAV")
    axes[0].legend(fontsize=8)
    axes[0].grid(True, alpha=0.3)

    axes[1].axhline(y=-0.12, color="black", linestyle="--", alpha=0.7, label="-12% target")
    axes[1].axhline(y=-0.15, color="gray", linestyle=":", alpha=0.7, label="-15%")
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].legend(fontsize=8)
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    results = run_active_shift_backtest()
    print_report(results)
    output_path = plot_results(results)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
