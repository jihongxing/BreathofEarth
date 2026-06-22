"""
Stress slippage backtest.

Runs the existing engine logic twice:
1. Baseline fixed friction cost from PortfolioEngine.
2. Dynamic stress slippage with asset-specific bid/ask widening.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.config import ASSETS, CORR_WINDOW, STATE_PROTECTION
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel


INITIAL_CAPITAL = 100000.0


@dataclass
class BacktestMetrics:
    label: str
    result: pd.DataFrame
    drawdown: pd.Series
    final: float
    total_return: float
    cagr: float
    mdd: float
    mdd_date: pd.Timestamp
    vol: float
    sharpe: float
    rebalances: int
    protection_count: int
    protection_days: int
    total_cost: float
    buy_notional: float
    sell_notional: float
    stress_rebalances: int = 0


class FixedCostPortfolioEngine(PortfolioEngine):
    """PortfolioEngine with cost attribution for baseline reporting."""

    def __init__(self, initial_capital: float = INITIAL_CAPITAL):
        super().__init__(initial_capital=initial_capital)
        self.total_cost = 0.0
        self.buy_notional = 0.0
        self.sell_notional = 0.0

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        order = super()._make_order(target_weights, reason)
        target = np.array(target_weights, dtype=float)
        deltas = (target - self.weights) * self.core_nav
        self.buy_notional += float(np.maximum(deltas, 0).sum())
        self.sell_notional += float(np.maximum(-deltas, 0).sum())
        self.total_cost += order.friction_cost
        return order


class StressSlippagePortfolioEngine(PortfolioEngine):
    """PortfolioEngine that prices rebalances through StressSlippageModel."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.current_date = None
        self.total_cost = 0.0
        self.buy_notional = 0.0
        self.sell_notional = 0.0
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
        self.buy_notional += estimate.buy_notional
        self.sell_notional += estimate.sell_notional
        if estimate.stressed_assets:
            self.stress_rebalances += 1

        turnover = float(np.sum(np.abs(self.weights - np.array(target_weights, dtype=float))) / 2)
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=estimate.total_cost,
            reason=reason,
        )


def run_backtest(
    portfolio: PortfolioEngine,
    prices: pd.DataFrame,
    label: str,
) -> BacktestMetrics:
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
    daily_vol = result["NAV"].pct_change().std()
    vol = float(daily_vol * np.sqrt(252))
    sharpe = (cagr - 0.02) / vol if vol > 0 else 0.0

    return BacktestMetrics(
        label=label,
        result=result,
        drawdown=drawdown,
        final=final,
        total_return=final / INITIAL_CAPITAL - 1,
        cagr=float(cagr),
        mdd=float(drawdown.min()),
        mdd_date=drawdown.idxmin(),
        vol=vol,
        sharpe=float(sharpe),
        rebalances=portfolio.rebalance_count,
        protection_count=portfolio.protection_count,
        protection_days=sum(state_history),
        total_cost=float(getattr(portfolio, "total_cost", 0.0)),
        buy_notional=float(getattr(portfolio, "buy_notional", 0.0)),
        sell_notional=float(getattr(portfolio, "sell_notional", 0.0)),
        stress_rebalances=int(getattr(portfolio, "stress_rebalances", 0)),
    )


def run_stress_slippage_backtest(file_path: str = "data/etf_daily.csv") -> dict[str, BacktestMetrics]:
    prices = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()
    prices = prices[ASSETS]

    baseline = run_backtest(
        FixedCostPortfolioEngine(initial_capital=INITIAL_CAPITAL),
        prices,
        "baseline fixed 10 bps turnover",
    )
    stress_model = StressSlippageModel.from_prices(prices)
    stress = run_backtest(
        StressSlippagePortfolioEngine(
            stress_model,
            initial_capital=INITIAL_CAPITAL,
        ),
        prices,
        "stress dynamic bid/ask widening",
    )

    return {"baseline": baseline, "stress": stress}


def print_report(metrics: dict[str, BacktestMetrics]) -> None:
    baseline = metrics["baseline"]
    stress = metrics["stress"]

    print("=" * 72)
    print("  Xi-Rang Stress Slippage Backtest")
    print("=" * 72)
    print(f"  Window: {baseline.result.index[0].date()} ~ {baseline.result.index[-1].date()}")
    print(f"  Assets: {', '.join(ASSETS)}")
    print("-" * 72)
    print(f"  {'Metric':<18} {'Baseline':>18} {'Stress':>18} {'Delta':>14}")
    print(f"  {'CAGR':<18} {baseline.cagr:>17.2%} {stress.cagr:>17.2%} {stress.cagr - baseline.cagr:>+13.2%}")
    print(f"  {'MDD':<18} {baseline.mdd:>17.2%} {stress.mdd:>17.2%} {stress.mdd - baseline.mdd:>+13.2%}")
    print(f"  {'Final NAV':<18} {baseline.final:>18,.2f} {stress.final:>18,.2f} {stress.final - baseline.final:>+14,.2f}")
    print(f"  {'Sharpe':<18} {baseline.sharpe:>18.2f} {stress.sharpe:>18.2f} {stress.sharpe - baseline.sharpe:>+14.2f}")
    print(f"  {'Rebalances':<18} {baseline.rebalances:>18d} {stress.rebalances:>18d} {stress.rebalances - baseline.rebalances:>+14d}")
    print(f"  {'Total cost':<18} {baseline.total_cost:>18,.2f} {stress.total_cost:>18,.2f} {stress.total_cost - baseline.total_cost:>+14,.2f}")
    print(f"  {'Stress rebalances':<18} {'-':>18} {stress.stress_rebalances:>18d} {'':>14}")
    print("-" * 72)
    print(f"  Baseline MDD date: {baseline.mdd_date.date()}")
    print(f"  Stress MDD date:   {stress.mdd_date.date()}")
    print("=" * 72)


def plot_results(metrics: dict[str, BacktestMetrics]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "stress_slippage_backtest.png"

    baseline = metrics["baseline"]
    stress = metrics["stress"]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    axes[0].plot(baseline.result.index, baseline.result["NAV"], label="Baseline", color="#2c3e50", linewidth=1.2)
    axes[0].plot(stress.result.index, stress.result["NAV"], label="Stress slippage", color="#c0392b", linewidth=1.2)
    axes[0].set_title("Xi-Rang Baseline vs Stress Slippage")
    axes[0].set_ylabel("NAV")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(baseline.drawdown.index, baseline.drawdown, label="Baseline DD", color="#2c3e50", linewidth=1.0)
    axes[1].plot(stress.drawdown.index, stress.drawdown, label="Stress DD", color="#c0392b", linewidth=1.0)
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
    metrics = run_stress_slippage_backtest()
    print_report(metrics)
    output_path = plot_results(metrics)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
