"""
Walk-forward audit for active-shift experiments.

This script is intentionally a bypass audit. It does not change production
PortfolioEngine behavior. It stress-tests the best active-shift candidate from
the prior experiment against:

1. Crisis-period slices: 2008, 2020, 2022.
2. Panic execution penalties on defense-entry rebalances.
3. 5-year train / 1-year out-of-sample rolling selection.
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from engine.config import ASSETS, CORR_WINDOW, DRIFT_THRESHOLD, STATE_IDLE, STATE_PROTECTION, WEIGHTS_IDLE
from engine.portfolio import PortfolioEngine, RebalanceOrder
from engine.risk import RiskEngine
from engine.stress_model import StressSlippageModel
from engine.weight_shifter import MarketShiftSignal, WeightShiftConfig, WeightShifter


INITIAL_CAPITAL = 100000.0
TARGET_CAGR = 0.082
TARGET_MDD = -0.13


@dataclass(frozen=True)
class AuditConfig:
    """Execution-audit knobs."""

    panic_entry_sell_bps: float = 0.0


@dataclass
class AuditRun:
    label: str
    result: pd.DataFrame
    actions: pd.DataFrame
    cagr: float
    mdd: float
    mdd_date: pd.Timestamp
    final: float
    rebalances: int
    total_cost: float
    shift_triggers: int
    shift_days: int


class AuditPortfolio(PortfolioEngine):
    """Active-shift portfolio with action logging and optional panic penalty."""

    def __init__(
        self,
        slippage_model: StressSlippageModel,
        shifter: WeightShifter | None = None,
        audit_config: AuditConfig | None = None,
        initial_capital: float = INITIAL_CAPITAL,
    ):
        super().__init__(initial_capital=initial_capital)
        self.slippage_model = slippage_model
        self.shifter = shifter
        self.audit_config = audit_config or AuditConfig()
        self.current_date = None
        self.current_shift_signal: MarketShiftSignal | None = None
        self.total_cost = 0.0
        self.shift_triggers = 0
        self.shift_days = 0
        self.action_rows: list[dict] = []
        self.last_order_cost = 0.0
        self.last_extra_cost = 0.0
        self.last_sell_notional = 0.0

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
                    "turnover": order.turnover,
                    "cost": self.last_order_cost,
                    "extra_cost": self.last_extra_cost,
                    "sell_notional": self.last_sell_notional,
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
        extra_cost = 0.0
        if reason == "进入主动防御" and self.audit_config.panic_entry_sell_bps > 0:
            extra_cost = estimate.sell_notional * self.audit_config.panic_entry_sell_bps

        total_cost = estimate.total_cost + extra_cost
        self.total_cost += total_cost
        self.last_order_cost = total_cost
        self.last_extra_cost = extra_cost
        self.last_sell_notional = estimate.sell_notional
        turnover = float(np.sum(np.abs(self.weights - np.array(target_weights, dtype=float))) / 2)
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=total_cost,
            reason=reason,
        )


def cash_lock_config(vol_threshold: float = 2.0, cooldown_days: int = 90) -> WeightShiftConfig:
    return WeightShiftConfig(
        name=f"vol{vol_threshold:g}_cash_lock_{cooldown_days}d",
        defense_weights=(0.0, 0.0, 0.0, 1.0),
        vol_anomaly_threshold=vol_threshold,
        corr_threshold=None,
        cooldown_days=cooldown_days,
        use_vol_trigger=True,
        use_corr_trigger=False,
    )


def candidate_configs() -> list[WeightShiftConfig]:
    configs: list[WeightShiftConfig] = []
    for vol_threshold in [1.5, 1.75, 2.0, 2.25, 2.5, 3.0]:
        for cooldown in [30, 45, 60, 90]:
            configs.append(cash_lock_config(vol_threshold, cooldown))
    return configs


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


def run_audit_scenario(
    prices: pd.DataFrame,
    label: str,
    shift_config: WeightShiftConfig | None,
    audit_config: AuditConfig | None = None,
    initial_capital: float = INITIAL_CAPITAL,
) -> AuditRun:
    prices = prices.sort_index()[ASSETS]
    stress_model = StressSlippageModel.from_prices(prices)
    returns = prices.pct_change().fillna(0)
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW).fillna(0)
    dates = returns.index

    shifter = WeightShifter(shift_config) if shift_config is not None else None
    trend_window = shift_config.trend_window if shift_config is not None else CORR_WINDOW
    shift_signals = build_shift_signals(prices, stress_model, trend_window)
    portfolio = AuditPortfolio(stress_model, shifter, audit_config, initial_capital=initial_capital)
    risk = RiskEngine()
    risk.high_water_mark = initial_capital

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
    return build_audit_run(label, result, pd.DataFrame(portfolio.action_rows), portfolio, initial_capital)


def build_audit_run(
    label: str,
    result: pd.DataFrame,
    actions: pd.DataFrame,
    portfolio,
    initial_capital: float,
) -> AuditRun:
    years = max((result.index[-1] - result.index[0]).days / 365.25, 1 / 365.25)
    final = float(result["NAV"].iloc[-1])
    cagr = (final / initial_capital) ** (1 / years) - 1
    drawdown = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    return AuditRun(
        label=label,
        result=result,
        actions=actions,
        cagr=float(cagr),
        mdd=float(drawdown.min()),
        mdd_date=drawdown.idxmin(),
        final=final,
        rebalances=int(portfolio.rebalance_count),
        total_cost=float(getattr(portfolio, "total_cost", 0.0)),
        shift_triggers=int(getattr(portfolio, "shift_triggers", 0)),
        shift_days=int(getattr(portfolio, "shift_days", 0)),
    )


def slice_period(run: AuditRun, start: str, end: str) -> dict:
    result = run.result.loc[start:end]
    if result.empty:
        return {"return": np.nan, "mdd": np.nan, "final": np.nan}
    period_return = result["NAV"].iloc[-1] / result["NAV"].iloc[0] - 1
    period_dd = (result["NAV"] - result["NAV"].cummax()) / result["NAV"].cummax()
    actions = run.actions
    if not actions.empty:
        actions = actions[(actions["date"] >= pd.Timestamp(start)) & (actions["date"] <= pd.Timestamp(end))]
    entry_actions = actions[actions["action"] == "进入主动防御"] if not actions.empty else actions
    return {
        "return": float(period_return),
        "mdd": float(period_dd.min()),
        "final": float(result["NAV"].iloc[-1]),
        "actions": len(actions),
        "entries": len(entry_actions),
        "defense_days": int(run.result.loc[start:end, "State"].sum()),
        "first_entry": None if entry_actions.empty else entry_actions["date"].iloc[0].date().isoformat(),
        "extra_cost": 0.0 if actions.empty else float(actions["extra_cost"].sum()),
    }


def crisis_audit(prices: pd.DataFrame) -> tuple[list[dict], dict[str, AuditRun]]:
    cfg = cash_lock_config(2.0, 90)
    runs = {
        "baseline": run_audit_scenario(prices, "baseline stress", None),
        "cash_lock": run_audit_scenario(prices, cfg.name, cfg),
        "cash_lock_50bp": run_audit_scenario(prices, f"{cfg.name} + 50bp panic", cfg, AuditConfig(0.005)),
        "cash_lock_100bp": run_audit_scenario(prices, f"{cfg.name} + 100bp panic", cfg, AuditConfig(0.010)),
    }

    periods = {
        "2008": ("2008-01-01", "2008-12-31"),
        "2020": ("2020-01-01", "2020-12-31"),
        "2022": ("2022-01-01", "2022-12-31"),
    }
    rows: list[dict] = []
    for period, (start, end) in periods.items():
        base = slice_period(runs["baseline"], start, end)
        for key in ["cash_lock", "cash_lock_50bp", "cash_lock_100bp"]:
            item = slice_period(runs[key], start, end)
            rows.append(
                {
                    "period": period,
                    "scenario": key,
                    "return": item["return"],
                    "mdd": item["mdd"],
                    "vs_baseline_return": item["return"] - base["return"],
                    "vs_baseline_mdd": item["mdd"] - base["mdd"],
                    "entries": item["entries"],
                    "first_entry": item["first_entry"],
                    "extra_cost": item["extra_cost"],
                }
            )
    return rows, runs


def select_best_config(train_prices: pd.DataFrame, configs: list[WeightShiftConfig]) -> WeightShiftConfig:
    scored = []
    for config in configs:
        run = run_audit_scenario(train_prices, config.name, config)
        scored.append((run.mdd, run.cagr, run.final, config))
    return sorted(scored, key=lambda item: (-item[0], -item[1], -item[2]))[0][3]


def walk_forward_audit(prices: pd.DataFrame) -> tuple[AuditRun, list[dict]]:
    configs = candidate_configs()
    rows: list[dict] = []
    oos_frames: list[pd.DataFrame] = []
    start_year = prices.index[0].year
    end_year = prices.index[-1].year

    stitched_nav = INITIAL_CAPITAL
    for train_start in range(start_year, end_year - 5):
        train_end = train_start + 4
        test_year = train_start + 5
        train = prices.loc[f"{train_start}-01-01":f"{train_end}-12-31"]
        test = prices.loc[f"{test_year}-01-01":f"{test_year}-12-31"]
        if len(train) < 252 * 3 or len(test) < 100:
            continue

        best_config = select_best_config(train, configs)
        oos = run_audit_scenario(test, best_config.name, best_config, initial_capital=stitched_nav)
        baseline = run_audit_scenario(test, "baseline", None, initial_capital=stitched_nav)
        stitched_nav = oos.final
        oos_frames.append(oos.result.assign(TrainStart=train_start, TestYear=test_year, Config=best_config.name))
        rows.append(
            {
                "train": f"{train_start}-{train_end}",
                "test_year": test_year,
                "config": best_config.name,
                "oos_return": oos.final / oos.result["NAV"].iloc[0] - 1,
                "oos_mdd": ((oos.result["NAV"] - oos.result["NAV"].cummax()) / oos.result["NAV"].cummax()).min(),
                "baseline_return": baseline.final / baseline.result["NAV"].iloc[0] - 1,
                "baseline_mdd": ((baseline.result["NAV"] - baseline.result["NAV"].cummax()) / baseline.result["NAV"].cummax()).min(),
                "shift_triggers": oos.shift_triggers,
                "shift_days": oos.shift_days,
            }
        )

    stitched = pd.concat(oos_frames).sort_index()
    stitched = stitched[~stitched.index.duplicated(keep="last")]
    dummy = type(
        "StitchedPortfolio",
        (),
        {
            "rebalance_count": 0,
            "total_cost": 0.0,
            "shift_triggers": int(sum(row["shift_triggers"] for row in rows)),
            "shift_days": int(sum(row["shift_days"] for row in rows)),
        },
    )()
    run = build_audit_run(
        "walk-forward OOS stitched",
        stitched[["NAV", "State"]],
        pd.DataFrame(),
        dummy,
        INITIAL_CAPITAL,
    )
    return run, rows


def print_crisis_report(rows: list[dict], runs: dict[str, AuditRun]) -> None:
    print("=" * 112)
    print("  Crisis Slice Audit")
    print("=" * 112)
    for key, run in runs.items():
        print(
            f"  {key:<16} CAGR {run.cagr:>7.2%} | MDD {run.mdd:>8.2%} "
            f"| Final {run.final:>11,.2f} | Cost {run.total_cost:>9,.2f} | Shifts {run.shift_triggers:>3d}"
        )
    print("-" * 112)
    print(
        f"  {'Period':<8} {'Scenario':<18} {'Return':>9} {'MDD':>9} "
        f"{'Ret vs Base':>12} {'MDD vs Base':>12} {'Entries':>8} {'First Entry':>12} {'Extra Cost':>11}"
    )
    for row in rows:
        first_entry = row["first_entry"] or "-"
        print(
            f"  {row['period']:<8} {row['scenario']:<18} {row['return']:>8.2%} {row['mdd']:>8.2%} "
            f"{row['vs_baseline_return']:>+11.2%} {row['vs_baseline_mdd']:>+11.2%} "
            f"{row['entries']:>8d} {first_entry:>12} {row['extra_cost']:>11,.2f}"
        )


def print_walk_forward_report(run: AuditRun, rows: list[dict]) -> None:
    print("\n" + "=" * 112)
    print("  Walk-forward Cross Validation")
    print("=" * 112)
    print(
        f"  OOS stitched CAGR {run.cagr:.2%} | MDD {run.mdd:.2%} | Final {run.final:,.2f} "
        f"| Shift triggers {run.shift_triggers} | Shift days {run.shift_days}"
    )
    print(f"  Target: CAGR >= {TARGET_CAGR:.2%}, MDD >= {TARGET_MDD:.2%}")
    status = "PASS" if run.cagr >= TARGET_CAGR and run.mdd >= TARGET_MDD else "FAIL"
    print(f"  Verdict: {status}")
    print("-" * 112)
    print(f"  {'Train':<11} {'Test':>6} {'Config':<24} {'OOS Ret':>9} {'OOS MDD':>9} {'Base Ret':>9} {'Base MDD':>9}")
    for row in rows:
        print(
            f"  {row['train']:<11} {row['test_year']:>6d} {row['config']:<24} "
            f"{row['oos_return']:>8.2%} {row['oos_mdd']:>8.2%} "
            f"{row['baseline_return']:>8.2%} {row['baseline_mdd']:>8.2%}"
        )


def plot_audit(runs: dict[str, AuditRun], oos: AuditRun) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "walk_forward_audit.png"

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    for key, color in [("baseline", "#2c3e50"), ("cash_lock", "#1f8a70"), ("cash_lock_100bp", "#c0392b")]:
        run = runs[key]
        dd = (run.result["NAV"] - run.result["NAV"].cummax()) / run.result["NAV"].cummax()
        axes[0].plot(run.result.index, run.result["NAV"], label=key, color=color, linewidth=1.0)
        axes[1].plot(dd.index, dd, label=key, color=color, linewidth=0.9)

    axes[0].plot(oos.result.index, oos.result["NAV"], label="walk-forward OOS", color="#8e44ad", linewidth=1.1)
    oos_dd = (oos.result["NAV"] - oos.result["NAV"].cummax()) / oos.result["NAV"].cummax()
    axes[1].plot(oos_dd.index, oos_dd, label="walk-forward OOS", color="#8e44ad", linewidth=1.0)

    axes[0].set_title("Walk-forward Audit")
    axes[0].set_ylabel("NAV")
    axes[0].legend(fontsize=8)
    axes[0].grid(True, alpha=0.3)

    axes[1].axhline(y=-0.13, color="black", linestyle="--", alpha=0.7, label="-13% OOS target")
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
    crisis_rows, crisis_runs = crisis_audit(prices)
    print_crisis_report(crisis_rows, crisis_runs)
    oos_run, oos_rows = walk_forward_audit(prices)
    print_walk_forward_report(oos_run, oos_rows)
    output_path = plot_audit(crisis_runs, oos_run)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
