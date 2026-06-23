"""
Asymmetric shift backtest.

Tests fast-exit / slow-re-entry policies with a mandatory 50 bps panic sell
penalty. This is a bypass experiment and does not change production behavior.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.asymmetric_shifter import AsymmetricShiftConfig, AsymmetricShiftSignal, AsymmetricShifter
from engine.config import ASSETS, CORR_WINDOW, DRIFT_THRESHOLD, STATE_IDLE, STATE_PROTECTION, WEIGHTS_IDLE
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel


INITIAL_CAPITAL = 100000.0
PANIC_SELL_BPS = 0.005
TARGET_CAGR = 0.075
TARGET_MDD = -0.13


@dataclass
class AsymmetricMetrics:
    label: str
    result: pd.DataFrame
    actions: pd.DataFrame
    cagr: float
    mdd: float
    mdd_date: pd.Timestamp
    final: float
    rebalances: int
    total_cost: float
    trigger_count: int
    crisis_days: int
    recovery_days: int


class AsymmetricPortfolio(PortfolioEngine):
    """Portfolio variant driven by AsymmetricShifter target weights."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        shifter: AsymmetricShifter | None = None,
        panic_sell_bps: float = PANIC_SELL_BPS,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.shifter = shifter
        self.panic_sell_bps = panic_sell_bps
        self.current_date = None
        self.current_signal: AsymmetricShiftSignal | None = None
        self.total_cost = 0.0
        self.action_rows: list[dict] = []
        self.last_order_cost = 0.0
        self.last_extra_cost = 0.0

    def step(self, current_date, daily_returns, risk_signal, is_year_end=False):
        self.current_date = current_date
        self.apply_daily_returns(daily_returns)

        target_override = None
        shift_action = None
        if self.shifter is not None and self.current_signal is not None:
            decision = self.shifter.decide(self.current_signal)
            target_override = decision.target_weights
            shift_action = decision.action

        order = self._evaluate_with_override(risk_signal, is_year_end, target_override, shift_action)
        action = shift_action
        if order is not None:
            self.apply_rebalance(order)
            action = order.reason
            self.action_rows.append(
                {
                    "date": pd.Timestamp(current_date),
                    "action": action,
                    "nav": self.nav,
                    "cost": self.last_order_cost,
                    "extra_cost": self.last_extra_cost,
                    "state": self.state,
                    "weights": tuple(round(float(w), 6) for w in self.weights),
                }
            )

        self.record_snapshot(current_date, risk_signal, action=action)
        return order

    def _evaluate_with_override(
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
            if max_drift > DRIFT_THRESHOLD or shift_action in {"进入非对称防御", "恢复期再触发防御", "非对称分批复位"}:
                return self._make_order(list(target_override), shift_action or "非对称防御维持")
            return None

        if self.shifter is not None and shift_action == "完成非对称复位":
            self.state = STATE_IDLE
            return self._make_order(WEIGHTS_IDLE, "完成非对称复位: 恢复默认权重")

        return self.evaluate_rebalance(risk_signal, is_year_end)

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        estimate = self.slippage_model.estimate_rebalance_cost(
            current_positions=self.positions,
            target_weights=np.array(target_weights, dtype=float),
            current_date=self.current_date,
            assets=ASSETS,
        )
        extra_cost = estimate.sell_notional * self.panic_sell_bps if reason in {"进入非对称防御", "恢复期再触发防御"} else 0.0
        total_cost = estimate.total_cost + extra_cost
        self.total_cost += total_cost
        self.last_order_cost = total_cost
        self.last_extra_cost = extra_cost
        turnover = float(np.sum(np.abs(self.weights - np.array(target_weights, dtype=float))) / 2)
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=total_cost,
            reason=reason,
        )


def asymmetric_config(recovery_weeks: int, crisis_lock_days: int = 15) -> AsymmetricShiftConfig:
    return AsymmetricShiftConfig(
        name=f"asym_{recovery_weeks}w_reentry",
        defense_weights=(0.05, 0.05, 0.20, 0.70),
        vol_anomaly_threshold=2.0,
        corr_threshold=0.5,
        crisis_lock_days=crisis_lock_days,
        recovery_weeks=recovery_weeks,
        use_vol_trigger=True,
        use_corr_trigger=True,
    )


def candidate_configs() -> list[AsymmetricShiftConfig]:
    return [asymmetric_config(weeks) for weeks in [4, 8, 12]]


def build_signals(prices: pd.DataFrame, stress_model: StressSlippageModel) -> pd.DataFrame:
    returns = prices[ASSETS].pct_change().fillna(0)
    return pd.DataFrame(
        {
            "spy_vol_anomaly": stress_model.volatility_anomaly["SPY"],
            "tlt_vol_anomaly": stress_model.volatility_anomaly["TLT"],
            "rolling_corr": returns["SPY"].rolling(CORR_WINDOW).corr(returns["TLT"]).fillna(0),
            "spy_trend": prices["SPY"].pct_change(CORR_WINDOW).fillna(0),
            "tlt_trend": prices["TLT"].pct_change(CORR_WINDOW).fillna(0),
            "is_weekly_rebalance_day": pd.Series(prices.index.weekday == 4, index=prices.index),
        },
        index=prices.index,
    ).fillna(0)


def run_scenario(
    prices: pd.DataFrame,
    label: str,
    config: AsymmetricShiftConfig | None,
    initial_capital: float = INITIAL_CAPITAL,
    panic_sell_bps: float = PANIC_SELL_BPS,
) -> AsymmetricMetrics:
    prices = prices.sort_index()[ASSETS]
    stress_model = StressSlippageModel.from_prices(prices)
    returns = prices.pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)
    signals = build_signals(prices, stress_model)
    shifter = AsymmetricShifter(config) if config is not None else None
    portfolio = AsymmetricPortfolio(stress_model, shifter, panic_sell_bps, initial_capital)
    risk = RiskEngine()
    risk.high_water_mark = initial_capital
    nav_history = []
    state_history = []

    for i, current_date in enumerate(prices.index):
        daily_ret = returns.iloc[i].values
        simulated_nav = float(np.sum(portfolio.positions * (1 + daily_ret)))
        risk_signal = risk.evaluate(
            simulated_nav,
            spy_tlt_corr.iloc[i],
            spy_30d_ret.iloc[i],
            tlt_30d_ret.iloc[i],
        )
        row = signals.loc[current_date]
        portfolio.current_signal = AsymmetricShiftSignal(
            spy_vol_anomaly=float(row["spy_vol_anomaly"]),
            tlt_vol_anomaly=float(row["tlt_vol_anomaly"]),
            rolling_corr=float(row["rolling_corr"]),
            spy_trend=float(row["spy_trend"]),
            tlt_trend=float(row["tlt_trend"]),
            is_weekly_rebalance_day=bool(row["is_weekly_rebalance_day"]),
        )
        is_year_end = i < len(prices.index) - 1 and prices.index[i].year != prices.index[i + 1].year
        portfolio.step(current_date.date(), daily_ret, risk_signal, is_year_end)
        nav_history.append(portfolio.nav)
        state_history.append(1 if portfolio.state == STATE_PROTECTION else 0)

    result = pd.DataFrame({"NAV": nav_history, "State": state_history}, index=prices.index)
    return build_metrics(label, result, pd.DataFrame(portfolio.action_rows), portfolio, initial_capital)


def build_metrics(label: str, result: pd.DataFrame, actions: pd.DataFrame, portfolio, initial_capital: float) -> AsymmetricMetrics:
    years = max((result.index[-1] - result.index[0]).days / 365.25, 1 / 365.25)
    final = float(result["NAV"].iloc[-1])
    cagr = (final / initial_capital) ** (1 / years) - 1
    drawdown = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    shifter = getattr(portfolio, "shifter", None)
    return AsymmetricMetrics(
        label=label,
        result=result,
        actions=actions,
        cagr=float(cagr),
        mdd=float(drawdown.min()),
        mdd_date=drawdown.idxmin(),
        final=final,
        rebalances=int(portfolio.rebalance_count),
        total_cost=float(getattr(portfolio, "total_cost", 0.0)),
        trigger_count=int(getattr(shifter, "trigger_count", 0) or 0),
        crisis_days=int(getattr(shifter, "crisis_days", 0) or 0),
        recovery_days=int(getattr(shifter, "recovery_days", 0) or 0),
    )


def crisis_slice(run: AsymmetricMetrics, start: str, end: str) -> dict:
    result = run.result.loc[start:end]
    period_return = result["NAV"].iloc[-1] / result["NAV"].iloc[0] - 1
    dd = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    actions = run.actions
    if not actions.empty:
        actions = actions[(actions["date"] >= pd.Timestamp(start)) & (actions["date"] <= pd.Timestamp(end))]
    entries = actions[actions["action"].isin(["进入非对称防御", "恢复期再触发防御"])] if not actions.empty else actions
    return {
        "return": float(period_return),
        "mdd": float(dd.min()),
        "entries": len(entries),
        "first_entry": None if entries.empty else entries["date"].iloc[0].date().isoformat(),
        "extra_cost": 0.0 if actions.empty else float(actions["extra_cost"].sum()),
    }


def full_sample_audit(prices: pd.DataFrame) -> dict[str, AsymmetricMetrics]:
    runs = {"baseline": run_scenario(prices, "baseline stress + 50bp", None)}
    for config in candidate_configs():
        runs[config.name] = run_scenario(prices, config.name, config)
    return runs


def select_best_config(train_prices: pd.DataFrame) -> AsymmetricShiftConfig:
    scored = []
    for config in candidate_configs():
        run = run_scenario(train_prices, config.name, config)
        scored.append((run.mdd, run.cagr, run.final, config))
    return sorted(scored, key=lambda item: (-item[0], -item[1], -item[2]))[0][3]


def walk_forward(prices: pd.DataFrame) -> tuple[AsymmetricMetrics, list[dict]]:
    rows: list[dict] = []
    frames: list[pd.DataFrame] = []
    stitched_nav = INITIAL_CAPITAL
    for train_start in range(prices.index[0].year, prices.index[-1].year - 5):
        train_end = train_start + 4
        test_year = train_start + 5
        train = prices.loc[f"{train_start}-01-01":f"{train_end}-12-31"]
        test = prices.loc[f"{test_year}-01-01":f"{test_year}-12-31"]
        if len(train) < 252 * 3 or len(test) < 100:
            continue
        best = select_best_config(train)
        oos = run_scenario(test, best.name, best, initial_capital=stitched_nav)
        baseline = run_scenario(test, "baseline", None, initial_capital=stitched_nav)
        stitched_nav = oos.final
        frames.append(oos.result.assign(TestYear=test_year, Config=best.name))
        rows.append(
            {
                "train": f"{train_start}-{train_end}",
                "test_year": test_year,
                "config": best.name,
                "oos_return": oos.final / oos.result["NAV"].iloc[0] - 1,
                "oos_mdd": ((oos.result["NAV"] - oos.result["NAV"].cummax()) / oos.result["NAV"].cummax()).min(),
                "baseline_return": baseline.final / baseline.result["NAV"].iloc[0] - 1,
                "baseline_mdd": ((baseline.result["NAV"] - baseline.result["NAV"].cummax()) / baseline.result["NAV"].cummax()).min(),
                "triggers": oos.trigger_count,
            }
        )
    stitched = pd.concat(frames).sort_index()
    stitched = stitched[~stitched.index.duplicated(keep="last")]
    dummy = type(
        "StitchedPortfolio",
        (),
        {
            "rebalance_count": 0,
            "total_cost": 0.0,
            "shifter": type(
                "DummyShifter",
                (),
                {
                    "trigger_count": int(sum(row["triggers"] for row in rows)),
                    "crisis_days": 0,
                    "recovery_days": 0,
                },
            )(),
        },
    )()
    return build_metrics("asymmetric walk-forward OOS", stitched[["NAV", "State"]], pd.DataFrame(), dummy, INITIAL_CAPITAL), rows


def print_report(runs: dict[str, AsymmetricMetrics], oos: AsymmetricMetrics, oos_rows: list[dict]) -> None:
    print("=" * 112)
    print("  Asymmetric Shift Backtest (50bp panic sell penalty)")
    print("=" * 112)
    print(f"  {'Scenario':<24} {'CAGR':>8} {'MDD':>9} {'Final':>12} {'Rebal':>7} {'Trig':>6} {'Crisis':>7} {'Recovery':>8} {'Cost':>10}")
    for run in runs.values():
        print(
            f"  {run.label:<24} {run.cagr:>7.2%} {run.mdd:>8.2%} {run.final:>12,.2f} "
            f"{run.rebalances:>7d} {run.trigger_count:>6d} {run.crisis_days:>7d} {run.recovery_days:>8d} {run.total_cost:>10,.2f}"
        )
    print("-" * 112)
    periods = {"2008": ("2008-01-01", "2008-12-31"), "2020": ("2020-01-01", "2020-12-31"), "2022": ("2022-01-01", "2022-12-31")}
    baseline = runs["baseline"]
    print(f"  {'Period':<8} {'Scenario':<24} {'Return':>9} {'MDD':>9} {'Ret vs Base':>12} {'MDD vs Base':>12} {'Entries':>8} {'First':>12} {'Extra':>10}")
    for period, (start, end) in periods.items():
        base = crisis_slice(baseline, start, end)
        for key, run in runs.items():
            if key == "baseline":
                continue
            item = crisis_slice(run, start, end)
            print(
                f"  {period:<8} {run.label:<24} {item['return']:>8.2%} {item['mdd']:>8.2%} "
                f"{item['return'] - base['return']:>+11.2%} {item['mdd'] - base['mdd']:>+11.2%} "
                f"{item['entries']:>8d} {(item['first_entry'] or '-'):>12} {item['extra_cost']:>10,.2f}"
            )
    print("\n" + "=" * 112)
    print("  Walk-forward OOS")
    print("=" * 112)
    verdict = "PASS" if oos.cagr >= TARGET_CAGR and oos.mdd >= TARGET_MDD else "FAIL"
    print(f"  OOS CAGR {oos.cagr:.2%} | MDD {oos.mdd:.2%} | Final {oos.final:,.2f} | Target CAGR {TARGET_CAGR:.2%}, MDD {TARGET_MDD:.2%} | {verdict}")
    print(f"  {'Train':<11} {'Test':>6} {'Config':<18} {'OOS Ret':>9} {'OOS MDD':>9} {'Base Ret':>9} {'Base MDD':>9} {'Trig':>5}")
    for row in oos_rows:
        print(
            f"  {row['train']:<11} {row['test_year']:>6d} {row['config']:<18} "
            f"{row['oos_return']:>8.2%} {row['oos_mdd']:>8.2%} {row['baseline_return']:>8.2%} {row['baseline_mdd']:>8.2%} {row['triggers']:>5d}"
        )


def plot_results(runs: dict[str, AsymmetricMetrics], oos: AsymmetricMetrics) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "asymmetric_shift_backtest.png"
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    colors = ["#2c3e50", "#1f8a70", "#c0392b", "#8e44ad"]
    for run, color in zip(runs.values(), colors):
        dd = (run.result["NAV"] - run.result["NAV"].cummax()) / run.result["NAV"].cummax()
        axes[0].plot(run.result.index, run.result["NAV"], label=run.label, color=color, linewidth=1.0)
        axes[1].plot(dd.index, dd, label=run.label, color=color, linewidth=0.9)
    axes[0].plot(oos.result.index, oos.result["NAV"], label="OOS", color="#f39c12", linewidth=1.1)
    oos_dd = (oos.result["NAV"] - oos.result["NAV"].cummax()) / oos.result["NAV"].cummax()
    axes[1].plot(oos_dd.index, oos_dd, label="OOS", color="#f39c12", linewidth=1.0)
    axes[1].axhline(y=-0.13, color="black", linestyle="--", alpha=0.7)
    axes[0].set_title("Asymmetric Shift Backtest")
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
    prices = pd.read_csv("data/etf_daily.csv", index_col="date", parse_dates=True).sort_index()[ASSETS]
    runs = full_sample_audit(prices)
    oos, oos_rows = walk_forward(prices)
    print_report(runs, oos, oos_rows)
    output_path = plot_results(runs, oos)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
