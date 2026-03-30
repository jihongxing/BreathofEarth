"""
息壤（Xi-Rang）Phase 1: 最简回测

永久组合基准测试：25% SPY + 25% TLT + 25% GLD + 25% SHV
双轨再平衡：阈值触发（偏离>5%）+ 时间触发（年末强制）
逐日遍历写法，为 Phase 2 状态机打地基。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def run_simple_backtest(file_path="data/etf_daily.csv"):
    # 1. 加载数据
    print("加载数据中...")
    df = pd.read_csv(file_path, index_col="date", parse_dates=True)
    df = df.sort_index()

    assets = ["SPY", "TLT", "GLD", "SHV"]

    # 计算每日收益率
    returns = df[assets].pct_change().fillna(0)

    # 2. 初始化回测参数
    initial_capital = 100000.0
    target_weights = np.array([0.25, 0.25, 0.25, 0.25])

    # 阈值：偏离绝对值 > 5% (即某资产占比超过 30% 或低于 20%)
    drift_threshold = 0.05
    trading_fee_rate = 0.001  # 万分之十的综合摩擦成本(滑点+手续费)

    # 状态记录
    nav_history = []

    # 初始化当前状态
    current_value = initial_capital
    current_positions = current_value * target_weights  # 各资产绝对金额
    rebalance_count = 0

    # 3. 逐日演进 (为 Phase 2 的状态机打地基)
    print("开始执行逐日回测...")
    dates = returns.index

    for i in range(len(dates)):
        current_date = dates[i]
        daily_ret = returns.iloc[i].values

        # 资产跟随市场自然生长
        current_positions = current_positions * (1 + daily_ret)
        current_value = np.sum(current_positions)
        current_weights = current_positions / current_value

        # 检查是否触发再平衡 (阈值触发)
        max_drift = np.max(np.abs(current_weights - target_weights))

        # 检查是否是每年的最后一个交易日 (时间触发)
        is_year_end = False
        if i < len(dates) - 1:
            if current_date.year != dates[i + 1].year:
                is_year_end = True

        # 执行再平衡
        if max_drift > drift_threshold or is_year_end:
            # 计算需要买卖的差额 (此处简化：直接扣除总盘子的摩擦成本)
            turnover = np.sum(np.abs(current_weights - target_weights)) / 2
            friction_cost = current_value * turnover * trading_fee_rate

            # 扣除摩擦成本后，重置持仓
            current_value -= friction_cost
            current_positions = current_value * target_weights
            rebalance_count += 1

        nav_history.append(current_value)

    # 4. 生成结果数据框
    result_df = pd.DataFrame({"NAV": nav_history}, index=dates)

    # 5. 计算核心指标
    total_return = result_df["NAV"].iloc[-1] / initial_capital - 1
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result_df["NAV"].iloc[-1] / initial_capital) ** (1 / years) - 1

    # 计算回撤
    running_max = result_df["NAV"].cummax()
    drawdown = (result_df["NAV"] - running_max) / running_max
    max_drawdown = drawdown.min()

    # 年化波动率与夏普比率 (假设无风险利率为 2%)
    daily_vol = result_df["NAV"].pct_change().std()
    annual_vol = daily_vol * np.sqrt(252)
    sharpe_ratio = (cagr - 0.02) / annual_vol if annual_vol > 0 else 0

    # 6. 打印报告
    print("-" * 50)
    print("息壤 Phase 1: 永久组合基准测试 (2005-2025)")
    print("-" * 50)
    print(f"  初始资金:       ${initial_capital:,.2f}")
    print(f"  最终资金:       ${result_df['NAV'].iloc[-1]:,.2f}")
    print(f"  总收益率:       {total_return:.2%}")
    print(f"  年化收益(CAGR): {cagr:.2%}")
    print(f"  最大回撤(MDD):  {max_drawdown:.2%}")
    print(f"  年化波动率:     {annual_vol:.2%}")
    print(f"  夏普比率:       {sharpe_ratio:.2f}")
    print(f"  调仓次数:       {rebalance_count} 次")
    print(f"  回测年数:       {years:.1f} 年")
    print("-" * 50)

    # 成功标准判定
    print("\n── 成功标准判定 ──")
    cpi_df = pd.read_csv("data/cpi_monthly.csv", index_col="date", parse_dates=True)
    avg_inflation = cpi_df["cpi_yoy"].dropna().mean()
    print(f"  平均年化通胀:   {avg_inflation:.2%}")
    print(
        f"  CAGR ≥ 通胀+2%: {'✓ 通过' if cagr >= avg_inflation + 0.02 else '✗ 未通过'}"
        f" ({cagr:.2%} vs {avg_inflation + 0.02:.2%})"
    )
    print(
        f"  MDD ≤ -15%:     {'✓ 通过' if max_drawdown >= -0.15 else '✗ 未通过'}"
        f" ({max_drawdown:.2%})"
    )
    print(
        f"  夏普 > 0.5:     {'✓ 通过' if sharpe_ratio > 0.5 else '✗ 未通过'}"
        f" ({sharpe_ratio:.2f})"
    )

    # 7. 绘制净值与回撤图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]}
    )

    ax1.plot(result_df.index, result_df["NAV"], label="Xi-Rang Portfolio NAV", color="#2c3e50", linewidth=1.2)
    ax1.axhline(y=initial_capital, color="gray", linestyle="--", alpha=0.5, label="Initial Capital")
    ax1.set_title("Xi-Rang Permanent Portfolio NAV (2005-2025)", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    ax2.fill_between(drawdown.index, drawdown, 0, color="#e74c3c", alpha=0.5)
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Percentage")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "phase1_nav_drawdown.png", dpi=150, bbox_inches="tight")
    print(f"\n图表已保存: {output_dir / 'phase1_nav_drawdown.png'}")
    plt.close()

    return result_df


if __name__ == "__main__":
    run_simple_backtest()
