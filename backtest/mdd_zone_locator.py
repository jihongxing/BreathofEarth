"""
MDD zone locator for the fixed policy.

This audit locates the deepest drawdown episodes for the fixed governance
candidate:

    acute asymmetric shifter: 8-week re-entry
    macro filter: MA150, release only when both SPY and TLT recover
    panic sell penalty: 50 bps
"""

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from backtest.fixed_policy_audit import fixed_policy_config, load_prices
from backtest.macro_regime_backtest import MacroRegimeMetrics, run_scenario
from engine.config import ASSETS


PANIC_SELL_BPS = 0.005


@dataclass(frozen=True)
class DrawdownEpisode:
    peak_date: pd.Timestamp
    valley_date: pd.Timestamp
    recovery_date: pd.Timestamp | None
    peak_nav: float
    valley_nav: float
    mdd: float

    @property
    def duration_days(self) -> int:
        end = self.recovery_date or self.valley_date
        return int((end - self.peak_date).days)


def locate_drawdown_episodes(nav: pd.Series) -> list[DrawdownEpisode]:
    """Return completed and open drawdown episodes sorted by depth."""
    if nav.empty:
        return []

    hwm = float(nav.iloc[0])
    peak_date = nav.index[0]
    in_drawdown = False
    episode_peak_date = peak_date
    episode_peak_nav = hwm
    valley_date = peak_date
    valley_nav = hwm
    episodes: list[DrawdownEpisode] = []

    for current_date, raw_value in nav.iloc[1:].items():
        value = float(raw_value)
        if value >= hwm:
            if in_drawdown:
                episodes.append(
                    DrawdownEpisode(
                        peak_date=episode_peak_date,
                        valley_date=valley_date,
                        recovery_date=current_date,
                        peak_nav=episode_peak_nav,
                        valley_nav=valley_nav,
                        mdd=valley_nav / episode_peak_nav - 1,
                    )
                )
                in_drawdown = False
            hwm = value
            peak_date = current_date
            continue

        if not in_drawdown:
            in_drawdown = True
            episode_peak_date = peak_date
            episode_peak_nav = hwm
            valley_date = current_date
            valley_nav = value
        elif value < valley_nav:
            valley_date = current_date
            valley_nav = value

    if in_drawdown:
        episodes.append(
            DrawdownEpisode(
                peak_date=episode_peak_date,
                valley_date=valley_date,
                recovery_date=None,
                peak_nav=episode_peak_nav,
                valley_nav=valley_nav,
                mdd=valley_nav / episode_peak_nav - 1,
            )
        )

    return sorted(episodes, key=lambda item: item.mdd)


def run_fixed_policy() -> MacroRegimeMetrics:
    prices = load_prices()[ASSETS]
    return run_scenario(
        prices,
        "fixed MA150+8w both + 50bp",
        fixed_policy_config(),
        panic_sell_bps=PANIC_SELL_BPS,
    )


def asset_returns(prices: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, float]:
    window = prices.loc[start:end, ASSETS]
    if window.empty:
        return {asset: 0.0 for asset in ASSETS}
    return {
        asset: float(window[asset].iloc[-1] / window[asset].iloc[0] - 1)
        for asset in ASSETS
    }


def summarize_episode(
    episode: DrawdownEpisode,
    run: MacroRegimeMetrics,
    prices: pd.DataFrame,
) -> dict:
    end = episode.recovery_date or episode.valley_date
    actions = run.actions
    if not actions.empty:
        actions = actions[
            (actions["date"] >= episode.peak_date)
            & (actions["date"] <= end)
        ]
    acute_entries = (
        int(actions["action"].isin(["进入非对称防御", "恢复期再触发防御"]).sum())
        if not actions.empty
        else 0
    )
    macro_entries = (
        int((actions["action"] == "进入宏观慢熊防御").sum())
        if not actions.empty
        else 0
    )
    returns = asset_returns(prices, episode.peak_date, episode.valley_date)
    return {
        "peak": episode.peak_date.date().isoformat(),
        "valley": episode.valley_date.date().isoformat(),
        "recovery": "-" if episode.recovery_date is None else episode.recovery_date.date().isoformat(),
        "mdd": episode.mdd,
        "duration_days": episode.duration_days,
        "peak_nav": episode.peak_nav,
        "valley_nav": episode.valley_nav,
        "actions": len(actions),
        "acute_entries": acute_entries,
        "macro_entries": macro_entries,
        "cost": 0.0 if actions.empty else float(actions["cost"].sum()),
        "extra_cost": 0.0 if actions.empty else float(actions["extra_cost"].sum()),
        **{f"{asset}_return": value for asset, value in returns.items()},
    }


def print_report(run: MacroRegimeMetrics, summaries: list[dict]) -> None:
    print("=" * 122)
    print("  Fixed Policy MDD Zone Locator")
    print("=" * 122)
    print(
        f"  Policy: MA150 + 8w + both recovery + 50bp panic | "
        f"CAGR {run.cagr:.2%} | MDD {run.mdd:.2%} | Final {run.final:,.2f}"
    )
    print("-" * 122)
    print(
        f"  {'Rank':>4} {'Peak':>12} {'Valley':>12} {'Recovery':>12} "
        f"{'MDD':>9} {'Days':>6} {'Actions':>8} {'Acute':>6} {'Macro':>6} {'Cost':>10} {'Extra':>10}"
    )
    for idx, item in enumerate(summaries, 1):
        print(
            f"  {idx:>4d} {item['peak']:>12} {item['valley']:>12} {item['recovery']:>12} "
            f"{item['mdd']:>8.2%} {item['duration_days']:>6d} {item['actions']:>8d} "
            f"{item['acute_entries']:>6d} {item['macro_entries']:>6d} "
            f"{item['cost']:>10,.2f} {item['extra_cost']:>10,.2f}"
        )
    print("-" * 122)
    print("  Asset returns from peak to valley:")
    print(f"  {'Rank':>4} {'SPY':>9} {'TLT':>9} {'GLD':>9} {'SHV':>9}")
    for idx, item in enumerate(summaries, 1):
        print(
            f"  {idx:>4d} {item['SPY_return']:>8.2%} {item['TLT_return']:>8.2%} "
            f"{item['GLD_return']:>8.2%} {item['SHV_return']:>8.2%}"
        )
    print("=" * 122)


def plot_zones(run: MacroRegimeMetrics, episodes: list[DrawdownEpisode]) -> Path:
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "mdd_zone_locator.png"

    nav = run.result["NAV"]
    drawdown = (nav - nav.cummax()) / nav.cummax()
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    axes[0].plot(nav.index, nav, color="#2c3e50", linewidth=1.0, label="Fixed policy NAV")
    axes[1].plot(drawdown.index, drawdown, color="#c0392b", linewidth=0.9, label="Drawdown")

    colors = ["#f39c12", "#8e44ad", "#1f8a70"]
    for episode, color in zip(episodes[:3], colors):
        end = episode.recovery_date or episode.valley_date
        axes[0].axvspan(episode.peak_date, end, color=color, alpha=0.12)
        axes[1].axvspan(episode.peak_date, end, color=color, alpha=0.12)
        axes[1].scatter([episode.valley_date], [episode.mdd], color=color, s=30)

    axes[0].set_title("Fixed Policy Top Drawdown Zones")
    axes[0].set_ylabel("NAV")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[1].axhline(y=-0.13, color="black", linestyle="--", alpha=0.7, label="-13%")
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def main() -> None:
    prices = load_prices()
    run = run_fixed_policy()
    episodes = locate_drawdown_episodes(run.result["NAV"])
    summaries = [summarize_episode(ep, run, prices) for ep in episodes[:3]]
    print_report(run, summaries)
    output_path = plot_zones(run, episodes)
    print(f"\nChart saved: {output_path}")


if __name__ == "__main__":
    main()
