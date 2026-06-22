"""
Macro regime backtest.

Dual-engine bypass experiment:
- AsymmetricShifter handles acute volatility/correlation shocks.
- MacroRegimeFilter handles slow stock/bond trend breakdowns.

The production PortfolioEngine remains unchanged.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.asymmetric_shifter import AsymmetricShiftConfig, AsymmetricShiftSignal, AsymmetricShifter
from engine.config import ASSETS, CORR_WINDOW, DRIFT_THRESHOLD, STATE_IDLE, STATE_PROTECTION, WEIGHTS_IDLE
from engine.macro_filter import MacroRegimeConfig, MacroRegimeFilter, MacroRegimeSignal
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel


INITIAL_CAPITAL = 100000.0
PANIC_SELL_BPS = 0.005
TARGET_CAGR = 0.075
TARGET_MDD = -0.13


@dataclass(frozen=True)
class DualEngineConfig:
    name: str
    asymmetric: AsymmetricShiftConfig
    macro: MacroRegimeConfig


@dataclass
class MacroRegimeMetrics:
    label: str
    result: pd.DataFrame
    actions: pd.DataFrame
    cagr: float
    mdd: float
    mdd_date: pd.Timestamp
    final: float
    rebalances: int
    total_cost: float
    acute_triggers: int
    macro_triggers: int
    acute_days: int
    macro_days: int


class DualEnginePortfolio(PortfolioEngine):
    """Portfolio variant driven by acute and macro target-weight overlays."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        asymmetric: AsymmetricShifter | None = None,
        macro_filter: MacroRegimeFilter | None = None,
        panic_sell_bps: float = PANIC_SELL_BPS,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.asymmetric = asymmetric
        self.macro_filter = macro_filter
        self.panic_sell_bps = panic_sell_bps
        self.current_date = None
        self.acute_signal: AsymmetricShiftSignal | None = None
        self.macro_signal: MacroRegimeSignal | None = None
        self.total_cost = 0.0
        self.action_rows: list[dict] = []
        self.last_order_cost = 0.0
        self.last_extra_cost = 0.0

    def step(self, current_date, daily_returns, risk_signal, is_year_end=False):
        self.current_date = current_date
        self.apply_daily_returns(daily_returns)

        macro_target = None
        macro_action = None
        if self.macro_filter is not None and self.macro_signal is not None:
            macro_decision = self.macro_filter.decide(self.macro_signal)
            macro_target = macro_decision.target_weights
            macro_action = macro_decision.action

        acute_target = None
        acute_action = None
        if self.asymmetric is not None and self.acute_signal is not None:
            acute_decision = self.asymmetric.decide(self.acute_signal)
            acute_target = acute_decision.target_weights
            acute_action = acute_decision.action

        # Acute crisis has priority over chronic macro defense.
        target_override = acute_target if acute_target is not None else macro_target
        overlay_action = acute_action if acute_target is not None else macro_action
        if target_override is None and acute_action == "完成非对称复位" and macro_target is not None:
            target_override = macro_target
            overlay_action = "完成非对称复位: 回到宏观慢熊防御"

        order = self._evaluate_with_override(risk_signal, is_year_end, target_override, overlay_action)
        action = overlay_action
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
        overlay_action: str | None,
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
            forced_actions = {
                "进入非对称防御",
                "恢复期再触发防御",
                "非对称分批复位",
                "进入宏观慢熊防御",
                "完成非对称复位: 回到宏观慢熊防御",
            }
            if max_drift > DRIFT_THRESHOLD or overlay_action in forced_actions:
                return self._make_order(list(target_override), overlay_action or "双引擎防御维持")
            return None

        if overlay_action in {"完成非对称复位", "解除宏观慢熊防御"}:
            self.state = STATE_IDLE
            return self._make_order(WEIGHTS_IDLE, f"{overlay_action}: 恢复默认权重")

        return self.evaluate_rebalance(risk_signal, is_year_end)

    def _make_order(self, target_weights: list[float], reason: str) -> RebalanceOrder:
        estimate = self.slippage_model.estimate_rebalance_cost(
            current_positions=self.positions,
            target_weights=np.array(target_weights, dtype=float),
            current_date=self.current_date,
            assets=ASSETS,
        )
        extra_cost = (
            estimate.sell_notional * self.panic_sell_bps
            if reason in {"进入非对称防御", "恢复期再触发防御"}
            else 0.0
        )
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


def asymmetric_config(recovery_weeks: int = 8) -> AsymmetricShiftConfig:
    return AsymmetricShiftConfig(
        name=f"acute_{recovery_weeks}w",
        defense_weights=(0.05, 0.05, 0.20, 0.70),
        vol_anomaly_threshold=2.0,
        corr_threshold=0.5,
        crisis_lock_days=15,
        recovery_weeks=recovery_weeks,
        use_vol_trigger=True,
        use_corr_trigger=True,
    )


def dual_configs() -> list[DualEngineConfig]:
    configs: list[DualEngineConfig] = []
    for ma_window in [120, 150]:
        for recovery_weeks in [4, 8, 12]:
            for release_any in [True, False]:
                asym = asymmetric_config(recovery_weeks)
                macro = MacroRegimeConfig(
                    name=f"macro_ma{ma_window}_{'any' if release_any else 'both'}",
                    ma_window=ma_window,
                    defense_weights=(0.10, 0.05, 0.35, 0.50),
                    release_on_any_recovery=release_any,
                )
                configs.append(
                    DualEngineConfig(
                        name=f"dual_ma{ma_window}_{recovery_weeks}w_{'any' if release_any else 'both'}",
                        asymmetric=asym,
                        macro=macro,
                    )
                )
    return configs


def build_signals(prices: pd.DataFrame, stress_model: StressSlippageModel, ma_window: int) -> pd.DataFrame:
    returns = prices[ASSETS].pct_change().fillna(0)
    return pd.DataFrame(
        {
            "spy_vol_anomaly": stress_model.volatility_anomaly["SPY"],
            "tlt_vol_anomaly": stress_model.volatility_anomaly["TLT"],
            "rolling_corr": returns["SPY"].rolling(CORR_WINDOW).corr(returns["TLT"]).fillna(0),
            "spy_trend": prices["SPY"].pct_change(CORR_WINDOW).fillna(0),
            "tlt_trend": prices["TLT"].pct_change(CORR_WINDOW).fillna(0),
            "is_weekly_rebalance_day": pd.Series(prices.index.weekday == 4, index=prices.index),
            "spy_ma": prices["SPY"].rolling(ma_window, min_periods=ma_window).mean(),
            "tlt_ma": prices["TLT"].rolling(ma_window, min_periods=ma_window).mean(),
        },
        index=prices.index,
    ).fillna(0)


def run_scenario(
    prices: pd.DataFrame,
    label: str,
    config: DualEngineConfig | None,
    initial_capital: float = INITIAL_CAPITAL,
    panic_sell_bps: float = PANIC_SELL_BPS,
) -> MacroRegimeMetrics:
    prices = prices.sort_index()[ASSETS]
    stress_model = StressSlippageModel.from_prices(prices)
    ma_window = config.macro.ma_window if config is not None else 120
    signals = build_signals(prices, stress_model, ma_window)
    returns = prices.pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)

    asym = AsymmetricShifter(config.asymmetric) if config is not None else None
    macro = MacroRegimeFilter(config.macro) if config is not None else None
    portfolio = DualEnginePortfolio(stress_model, asym, macro, panic_sell_bps, initial_capital)
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
        is_year_end = i < len(prices.index) - 1 and prices.index[i].year != prices.index[i + 1].year
        portfolio.step(current_date.date(), daily_ret, risk_signal, is_year_end)
        nav_history.append(portfolio.nav)
        state_history.append(1 if portfolio.state == STATE_PROTECTION else 0)

    result = pd.DataFrame({"NAV": nav_history, "State": state_history}, index=prices.index)
    return build_metrics(label, result, pd.DataFrame(portfolio.action_rows), portfolio, initial_capital)


def build_metrics(label: str, result: pd.DataFrame, actions: pd.DataFrame, portfolio, initial_capital: float) -> MacroRegimeMetrics:
    years = max((result.index[-1] - result.index[0]).days / 365.25, 1 / 365.25)
    final = float(result["NAV"].iloc[-1])
    cagr = (final / initial_capital) ** (1 / years) - 1
    drawdown = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    return MacroRegimeMetrics(
        label=label,
        result=result,
        actions=actions,
        cagr=float(cagr),
        mdd=float(drawdown.min()),
        mdd_date=drawdown.idxmin(),
        final=final,
        rebalances=int(portfolio.rebalance_count),
        total_cost=float(getattr(portfolio, "total_cost", 0.0)),
        acute_triggers=int(getattr(getattr(portfolio, "asymmetric", None), "trigger_count", 0) or 0),
        macro_triggers=int(getattr(getattr(portfolio, "macro_filter", None), "trigger_count", 0) or 0),
        acute_days=int(getattr(getattr(portfolio, "asymmetric", None), "crisis_days", 0) or 0),
        macro_days=int(getattr(getattr(portfolio, "macro_filter", None), "active_days", 0) or 0),
    )


def full_sample(prices: pd.DataFrame) -> dict[str, MacroRegimeMetrics]:
    runs = {"baseline": run_scenario(prices, "baseline stress + 50bp", None)}
    for config in dual_configs():
        runs[config.name] = run_scenario(prices, config.name, config)
    return runs


def select_best_config(train_prices: pd.DataFrame) -> DualEngineConfig:
    scored = []
    for config in dual_configs():
        run = run_scenario(train_prices, config.name, config)
        # Favor OOS-safe drawdown, then CAGR.
        scored.append((run.mdd, run.cagr, run.final, config))
    return sorted(scored, key=lambda item: (-item[0], -item[1], -item[2]))[0][3]


def walk_forward(prices: pd.DataFrame) -> tuple[MacroRegimeMetrics, list[dict]]:
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
                "acute_triggers": oos.acute_triggers,
                "macro_triggers": oos.macro_triggers,
            }
        )
    stitched = pd.concat(frames).sort_index()
    stitched = stitched[~stitched.index.duplicated(keep="last")]
    dummy = type(
        "DummyPortfolio",
        (),
        {
            "rebalance_count": 0,
            "total_cost": 0.0,
            "asymmetric": type("DummyAsym", (), {"trigger_count": int(sum(r["acute_triggers"] for r in rows)), "crisis_days": 0})(),
            "macro_filter": type("DummyMacro", (), {"trigger_count": int(sum(r["macro_triggers"] for r in rows)), "active_days": 0})(),
        },
    )()
    return build_metrics("macro walk-forward OOS", stitched[["NAV", "State"]], pd.DataFrame(), dummy, INITIAL_CAPITAL), rows


def crisis_slice(run: MacroRegimeMetrics, start: str, end: str) -> dict:
    result = run.result.loc[start:end]
    period_return = result["NAV"].iloc[-1] / result["NAV"].iloc[0] - 1
    dd = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    actions = run.actions
    if not actions.empty:
        actions = actions[(actions["date"] >= pd.Timestamp(start)) & (actions["date"] <= pd.Timestamp(end))]
    return {
        "return": float(period_return),
        "mdd": float(dd.min()),
        "macro_entries": int((actions["action"] == "进入宏观慢熊防御").sum()) if not actions.empty else 0,
        "acute_entries": int(actions["action"].isin(["进入非对称防御", "恢复期再触发防御"]).sum()) if not actions.empty else 0,
    }


def print_report(runs: dict[str, MacroRegimeMetrics], oos: MacroRegimeMetrics, oos_rows: list[dict]) -> None:
    ranked = sorted(runs.values(), key=lambda row: (-row.mdd, -row.cagr))
    print("=" * 120)
    print("  Macro Regime Dual-Engine Backtest")
    print("=" * 120)
    print(f"  {'Scenario':<26} {'CAGR':>8} {'MDD':>9} {'Final':>12} {'Rebal':>7} {'Acute':>6} {'Macro':>6} {'Cost':>10}")
    for run in ranked[:10]:
        print(
            f"  {run.label:<26} {run.cagr:>7.2%} {run.mdd:>8.2%} {run.final:>12,.2f} "
            f"{run.rebalances:>7d} {run.acute_triggers:>6d} {run.macro_triggers:>6d} {run.total_cost:>10,.2f}"
        )
    print("-" * 120)
    periods = {"2008": ("2008-01-01", "2008-12-31"), "2020": ("2020-01-01", "2020-12-31"), "2022": ("2022-01-01", "2022-12-31")}
    baseline = runs["baseline"]
    best = ranked[0]
    print(f"  Best full-sample scenario: {best.label}")
    print(f"  {'Period':<8} {'Return':>9} {'MDD':>9} {'Ret vs Base':>12} {'MDD vs Base':>12} {'Acute':>7} {'Macro':>7}")
    for period, (start, end) in periods.items():
        base = crisis_slice(baseline, start, end)
        item = crisis_slice(best, start, end)
        print(
            f"  {period:<8} {item['return']:>8.2%} {item['mdd']:>8.2%} "
            f"{item['return'] - base['return']:>+11.2%} {item['mdd'] - base['mdd']:>+11.2%} "
            f"{item['acute_entries']:>7d} {item['macro_entries']:>7d}"
        )
    print("\n" + "=" * 120)
    print("  Walk-forward OOS")
    print("=" * 120)
    verdict = "PASS" if oos.cagr >= TARGET_CAGR and oos.mdd >= TARGET_MDD else "FAIL"
    print(f"  OOS CAGR {oos.cagr:.2%} | MDD {oos.mdd:.2%} | Final {oos.final:,.2f} | Target {TARGET_CAGR:.2%}/{TARGET_MDD:.2%} | {verdict}")
    print(f"  {'Train':<11} {'Test':>6} {'Config':<24} {'OOS Ret':>9} {'OOS MDD':>9} {'Base Ret':>9} {'Base MDD':>9} {'A':>3} {'M':>3}")
    for row in oos_rows:
        print(
            f"  {row['train']:<11} {row['test_year']:>6d} {row['config']:<24} "
            f"{row['oos_return']:>8.2%} {row['oos_mdd']:>8.2%} {row['baseline_return']:>8.2%} {row['baseline_mdd']:>8.2%} "
            f"{row['acute_triggers']:>3d} {row['macro_triggers']:>3d}"
        )


def plot_results(runs: dict[str, MacroRegimeMetrics], oos: MacroRegimeMetrics) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "macro_regime_backtest.png"
    ranked = sorted(runs.values(), key=lambda row: (-row.mdd, -row.cagr))
    selected = [runs["baseline"], ranked[0], ranked[1], oos]
    colors = ["#2c3e50", "#1f8a70", "#c0392b", "#8e44ad"]
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    for run, color in zip(selected, colors):
        dd = (run.result["NAV"] - run.result["NAV"].cummax()) / run.result["NAV"].cummax()
        axes[0].plot(run.result.index, run.result["NAV"], label=run.label, color=color, linewidth=1.0)
        axes[1].plot(dd.index, dd, label=run.label, color=color, linewidth=0.9)
    axes[1].axhline(y=-0.13, color="black", linestyle="--", alpha=0.7)
    axes[0].set_title("Macro Regime Dual-Engine Backtest")
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
    runs = full_sample(prices)
    oos, rows = walk_forward(prices)
    print_report(runs, oos, rows)
    output_path = plot_results(runs, oos)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
