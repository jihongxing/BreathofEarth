"""
Circuit breaker backtest.

Compares the existing engine baseline, stress slippage, and stress slippage
with a hard-hold circuit breaker that freezes automatic rebalances after deep
drawdown.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.circuit_breaker import CircuitBreakerConfig, CircuitBreakerState
from engine.config import ASSETS, CORR_WINDOW, STATE_PROTECTION
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel


INITIAL_CAPITAL = 100000.0


@dataclass
class ScenarioMetrics:
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
    total_cost: float
    stress_rebalances: int = 0
    breaker_triggers: int = 0
    breaker_days: int = 0


class AttributedFixedCostPortfolio(PortfolioEngine):
    """Baseline engine with cost attribution."""

    def __init__(self, initial_capital: float = INITIAL_CAPITAL):
        super().__init__(initial_capital=initial_capital)
        self.total_cost = 0.0

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        order = super()._make_order(target_weights, reason)
        self.total_cost += order.friction_cost
        return order


class StressCostPortfolio(PortfolioEngine):
    """Portfolio engine that uses StressSlippageModel for rebalance costs."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.current_date = None
        self.total_cost = 0.0
        self.stress_rebalances = 0

    def step(self, current_date, daily_returns, risk_signal, is_year_end=False):
        self.current_date = current_date
        return super().step(current_date, daily_returns, risk_signal, is_year_end)

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        estimate = self.slippage_model.estimate_rebalance_cost(
            current_positions=self.positions,
            target_weights=np.array(target_weights, dtype=float),
            current_date=self.current_date,
            assets=ASSETS,
        )
        self.total_cost += estimate.total_cost
        if estimate.stressed_assets:
            self.stress_rebalances += 1
        turnover = float(np.sum(np.abs(self.weights - np.array(target_weights, dtype=float))) / 2)
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=estimate.total_cost,
            reason=reason,
        )


class CircuitBreakerStressPortfolio(StressCostPortfolio):
    """Stress-cost engine with a hard-hold circuit breaker."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        macro_anomaly: pd.Series,
        breaker_config: CircuitBreakerConfig | None = None,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(slippage_model=slippage_model, initial_capital=initial_capital)
        self.macro_anomaly = macro_anomaly.sort_index()
        self.breaker = CircuitBreakerState(breaker_config or CircuitBreakerConfig())
        self.breaker_days = 0

    def step(self, current_date, daily_returns, risk_signal, is_year_end=False):
        self.current_date = current_date
        self.apply_daily_returns(daily_returns)

        macro_stress = self._macro_stress(current_date)
        breaker_action = self.breaker.update(risk_signal.current_dd, macro_stress)

        action = breaker_action
        order = None
        if self.breaker.active:
            self.breaker_days += 1
            if action is None:
                action = "熔断保持: Hard Hold"
        else:
            order = self.evaluate_rebalance(risk_signal, is_year_end)
            if order is not None:
                self.apply_rebalance(order)
                action = order.reason

        self.record_snapshot(current_date, risk_signal, action=action)
        return order

    def _macro_stress(self, current_date) -> float:
        ts = pd.Timestamp(current_date)
        if ts not in self.macro_anomaly.index:
            loc = self.macro_anomaly.index.searchsorted(ts, side="right") - 1
            if loc < 0:
                return 0.0
            ts = self.macro_anomaly.index[loc]
        value = self.macro_anomaly.at[ts]
        return 0.0 if pd.isna(value) else float(value)


def run_scenario(
    portfolio: PortfolioEngine,
    prices: pd.DataFrame,
    label: str,
) -> ScenarioMetrics:
    returns = prices[ASSETS].pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)
    dates = returns.index

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
    breaker = getattr(portfolio, "breaker", None)

    return ScenarioMetrics(
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
        total_cost=float(getattr(portfolio, "total_cost", 0.0)),
        stress_rebalances=int(getattr(portfolio, "stress_rebalances", 0)),
        breaker_triggers=int(getattr(breaker, "trigger_count", 0)),
        breaker_days=int(getattr(portfolio, "breaker_days", 0)),
    )


def run_circuit_breaker_backtest(file_path: str = "data/etf_daily.csv") -> dict[str, ScenarioMetrics]:
    prices = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()[ASSETS]
    stress_model = StressSlippageModel.from_prices(prices)
    macro_anomaly = stress_model.volatility_anomaly[ASSETS].max(axis=1)

    baseline = run_scenario(
        AttributedFixedCostPortfolio(initial_capital=INITIAL_CAPITAL),
        prices,
        "baseline fixed 10 bps turnover",
    )
    stress = run_scenario(
        StressCostPortfolio(stress_model, initial_capital=INITIAL_CAPITAL),
        prices,
        "stress slippage",
    )
    breaker = run_scenario(
        CircuitBreakerStressPortfolio(
            stress_model,
            macro_anomaly=macro_anomaly,
            breaker_config=CircuitBreakerConfig(),
            initial_capital=INITIAL_CAPITAL,
        ),
        prices,
        "stress + hard-hold circuit breaker",
    )
    return {"baseline": baseline, "stress": stress, "breaker": breaker}


def print_report(metrics: dict[str, ScenarioMetrics]) -> None:
    rows = [metrics["baseline"], metrics["stress"], metrics["breaker"]]
    print("=" * 86)
    print("  Xi-Rang Circuit Breaker Backtest")
    print("=" * 86)
    print(f"  Window: {rows[0].result.index[0].date()} ~ {rows[0].result.index[-1].date()}")
    print("  Breaker: trigger DD <= -15%, release DD >= -10%, vol anomaly <= 1.5x, min hold 20d")
    print("-" * 86)
    print(
        f"  {'Scenario':<34} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Rebal':>7} {'Cost':>10} {'CB Days':>8}"
    )
    for row in rows:
        print(
            f"  {row.label:<34} {row.cagr:>7.2%} {row.mdd:>8.2%} "
            f"{row.final:>13,.2f} {row.rebalances:>7d} {row.total_cost:>10,.2f} "
            f"{row.breaker_days:>8d}"
        )
    print("-" * 86)
    print(f"  Baseline MDD date: {metrics['baseline'].mdd_date.date()}")
    print(f"  Breaker MDD date:  {metrics['breaker'].mdd_date.date()}")
    print(f"  Breaker triggers:  {metrics['breaker'].breaker_triggers}")
    print("=" * 86)


def plot_results(metrics: dict[str, ScenarioMetrics]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "circuit_breaker_backtest.png"

    rows = [metrics["baseline"], metrics["stress"], metrics["breaker"]]
    colors = ["#2c3e50", "#c0392b", "#1f8a70"]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    for row, color in zip(rows, colors):
        axes[0].plot(row.result.index, row.result["NAV"], label=row.label, color=color, linewidth=1.1)
        axes[1].plot(row.drawdown.index, row.drawdown, label=row.label, color=color, linewidth=1.0)

    axes[0].set_title("Xi-Rang Circuit Breaker Scenarios")
    axes[0].set_ylabel("NAV")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    axes[1].axhline(y=-0.15, color="black", linestyle="--", alpha=0.7, label="-15%")
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    metrics = run_circuit_breaker_backtest()
    print_report(metrics)
    output_path = plot_results(metrics)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
