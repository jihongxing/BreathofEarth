"""
息壤（Xi-Rang）引擎回测

用 Phase 3 的正式引擎模块（engine/portfolio.py + engine/risk.py）
跑 2005-2025 的历史数据，验证引擎代码与 Phase 2 回测结果一致。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from engine.config import ASSETS, STATE_IDLE, STATE_PROTECTION, CORR_WINDOW
from engine.risk import RiskEngine
from engine.portfolio import PortfolioEngine


def run_engine_backtest(file_path="data/etf_daily.csv"):
    # 1. 加载数据
    print("加载数据...")
    df = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()
    returns = df[ASSETS].pct_change().fillna(0)

    # 风控前置计算
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = df["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = df["TLT"].pct_change(CORR_WINDOW).fillna(0)

    dates = returns.index

    # 2. 初始化引擎
    initial_capital = 100000.0
    portfolio = PortfolioEngine(initial_capital=initial_capital)
    risk = RiskEngine()
    risk.high_water_mark = initial_capital

    nav_history = []
    state_history = []

    # 3. 逐日驱动
    print(f"开始回测: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} 个交易日)")
    for i in range(len(dates)):
        daily_ret = returns.iloc[i].values
        corr_val = spy_tlt_corr.iloc[i]
        spy_30d = spy_30d_ret.iloc[i]
        tlt_30d = tlt_30d_ret.iloc[i]

        # 模拟资产生长以获取准确 NAV 供风控评估
        simulated_nav = float(np.sum(portfolio.positions * (1 + daily_ret)))
        risk_signal = risk.evaluate(simulated_nav, corr_val, spy_30d, tlt_30d)

        # 年末判断
        is_year_end = (i < len(dates) - 1 and dates[i].year != dates[i + 1].year)

        # 状态机执行一步
        portfolio.step(
            current_date=dates[i].date(),
            daily_returns=daily_ret,
            risk_signal=risk_signal,
            is_year_end=is_year_end,
        )

        nav_history.append(portfolio.nav)
        state_history.append(1 if portfolio.state == STATE_PROTECTION else 0)

    # 4. 计算指标
    result = pd.DataFrame({"NAV": nav_history, "State": state_history}, index=dates)

    total_return = result["NAV"].iloc[-1] / initial_capital - 1
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result["NAV"].iloc[-1] / initial_capital) ** (1 / years) - 1

    running_max = result["NAV"].cummax()
    drawdown = (result["NAV"] - running_max) / running_max
    max_drawdown = drawdown.min()
    max_dd_date = drawdown.idxmin()

    daily_vol = result["NAV"].pct_change().std()
    annual_vol = daily_vol * np.sqrt(252)
    sharpe = (cagr - 0.02) / annual_vol if annual_vol > 0 else 0

    protection_days = sum(state_history)

    # CPI
    cpi_df = pd.read_csv("data/cpi_monthly.csv", index_col="date", parse_dates=True)
    avg_inflation = cpi_df["cpi_yoy"].dropna().mean()

    # 5. 打印报告
    print()
    print("=" * 60)
    print("  息壤（Xi-Rang）引擎回测报告  2005-2025")
    print("=" * 60)
    print(f"  初始资金:         ${initial_capital:,.2f}")
    print(f"  最终资金:         ${result['NAV'].iloc[-1]:,.2f}")
    print(f"  总收益率:         {total_return:.2%}")
    print(f"  年化收益(CAGR):   {cagr:.2%}")
    print(f"  最大回撤(MDD):    {max_drawdown:.2%}  (发生于 {max_dd_date.date()})")
    print(f"  年化波动率:       {annual_vol:.2%}")
    print(f"  夏普比率:         {sharpe:.2f}")
    print(f"  总调仓次数:       {portfolio.rebalance_count} 次")
    print(f"  保护模式触发:     {portfolio.protection_count} 次")
    print(f"  保护模式天数:     {protection_days} 天 ({protection_days/len(dates)*100:.1f}%)")
    print(f"  回测年数:         {years:.1f} 年")
    print(f"  平均年化通胀:     {avg_inflation:.2%}")
    print("-" * 60)

    # 成功标准
    print("  ── 成功标准判定 ──")
    checks = [
        ("CAGR ≥ 通胀+2%", cagr >= avg_inflation + 0.02, f"{cagr:.2%} vs {avg_inflation+0.02:.2%}"),
        ("MDD ≤ -15%", max_drawdown >= -0.15, f"{max_drawdown:.2%}"),
        ("夏普 > 0.5", sharpe > 0.5, f"{sharpe:.2f}"),
    ]
    all_pass = True
    for name, passed, detail in checks:
        mark = "✓" if passed else "✗"
        print(f"    {mark} {name}: {detail}")
        if not passed:
            all_pass = False

    if all_pass:
        print("\n  🎉 全部通过！息壤引擎验证成功。")
    else:
        print("\n  ⚠ 存在未通过项，需要进一步调优。")
    print("=" * 60)

    # 6. 找出所有保护模式触发的时间点
    print("\n  ── 保护模式触发记录 ──")
    actions = [s for s in portfolio.snapshots if s.action and "保护" in s.action or s.action and "避险" in s.action]
    for a in actions:
        print(f"    {a.date}  {a.action}  回撤:{a.drawdown:.2%}")

    # 7. 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(3, 1, figsize=(16, 12), gridspec_kw={"height_ratios": [3, 1, 1]})

    # NAV + 保护区间
    ax1 = axes[0]
    ax1.plot(result.index, result["NAV"], color="#2c3e50", linewidth=1.2, label="Xi-Rang NAV")
    ax1.fill_between(
        result.index,
        result["NAV"].min() * 0.95, result["NAV"].max() * 1.02,
        where=(result["State"] == 1),
        color="#f1c40f", alpha=0.3, label="PROTECTION",
    )
    ax1.axhline(y=initial_capital, color="gray", linestyle="--", alpha=0.4)
    ax1.set_title("Xi-Rang Engine Backtest: NAV (2005-2025)", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 回撤
    ax2 = axes[1]
    ax2.fill_between(drawdown.index, drawdown, 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 各资产权重变化
    ax3 = axes[2]
    weight_history = pd.DataFrame(
        [s.weights for s in portfolio.snapshots],
        index=[s.date for s in portfolio.snapshots],
        columns=ASSETS,
    )
    # 每 20 天采样一次避免太密
    weight_sampled = weight_history.iloc[::20]
    ax3.stackplot(
        range(len(weight_sampled)),
        [weight_sampled[a] for a in ASSETS],
        labels=ASSETS,
        alpha=0.8,
        colors=["#3498db", "#2ecc71", "#f39c12", "#95a5a6"],
    )
    # x 轴标签
    tick_positions = list(range(0, len(weight_sampled), len(weight_sampled) // 10))
    tick_labels = [weight_sampled.index[i][:7] for i in tick_positions]
    ax3.set_xticks(tick_positions)
    ax3.set_xticklabels(tick_labels, rotation=45)
    ax3.set_title("Asset Weight Allocation Over Time", fontsize=12)
    ax3.set_ylabel("Weight")
    ax3.legend(loc="upper right")
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "engine_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'engine_backtest.png'}")
    plt.close()

    return result


if __name__ == "__main__":
    run_engine_backtest()
