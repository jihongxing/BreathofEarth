"""
息壤（Xi-Rang）Phase 2: 状态机风控回测

在 Phase 1 永久组合基础上引入：
- 状态机：IDLE / PROTECTION 双模式
- 回撤预警：动态回撤触及 -12% 切入保护模式
- 相关性崩溃检测：SPY-TLT 30日滚动相关性 > 0.5 切入保护模式
- 保护模式：现金(SHV)仓位拉至 50%，冷却期 20 个交易日
- 目标：把最大回撤压制在 -15% 以内
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def run_stateful_backtest(file_path="data/etf_daily.csv"):
    # ---------------------------------------------------------
    # 1. 加载数据与预处理
    # ---------------------------------------------------------
    print("加载数据并初始化风控引擎...")
    df = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()

    assets = ["SPY", "TLT", "GLD", "SHV"]
    returns = df[assets].pct_change().fillna(0)

    # 【风控前置计算】SPY 和 TLT 的 30天滚动相关系数
    spy_tlt_corr = returns["SPY"].rolling(window=30).corr(returns["TLT"]).fillna(0)

    # 【风控前置计算】SPY 和 TLT 的 30天滚动收益，用于判断"双杀"
    spy_30d_ret = df["SPY"].pct_change(30).fillna(0)
    tlt_30d_ret = df["TLT"].pct_change(30).fillna(0)

    # ---------------------------------------------------------
    # 2. 状态机与组合参数设置
    # ---------------------------------------------------------
    initial_capital = 100000.0
    fee_rate = 0.001  # 万分之十的综合摩擦成本
    drift_threshold = 0.05  # 5% 的偏离阈值

    # 状态 A：正常模式 (IDLE) - 25% 平配
    WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])

    # 状态 B：保护模式 (PROTECTION) - 现金大幅提升至 50%
    WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])

    # 风控触发阈值
    RISK_DD_THRESHOLD = -0.12  # 回撤触及 -12% 触发保护
    RISK_CORR_THRESHOLD = 0.5  # 股债相关性 > 0.5 触发保护
    COOLDOWN_DAYS = 20  # 保护模式至少维持 20 个交易日
    HARD_STOP_DD = -0.14  # 硬止损：回撤触及 -14% 紧急避险

    # 紧急避险权重：几乎全现金
    WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])

    # ---------------------------------------------------------
    # 3. 逐日演进与状态转移
    # ---------------------------------------------------------
    print("开始执行状态机回测...")
    dates = returns.index
    returns_arr = returns.values
    corr_arr = spy_tlt_corr.values
    spy_30d_arr = spy_30d_ret.values
    tlt_30d_arr = tlt_30d_ret.values

    nav_history = []
    state_history = []  # 0=IDLE, 1=PROTECTION

    current_value = initial_capital
    current_positions = current_value * WEIGHTS_IDLE
    max_nav = initial_capital

    current_state = "IDLE"
    cooldown_counter = 0
    rebalance_count = 0
    protection_trigger_count = 0

    for i in range(len(dates)):
        current_date = dates[i]
        daily_ret = returns_arr[i]
        current_corr = corr_arr[i]
        spy_30d = spy_30d_arr[i]
        tlt_30d = tlt_30d_arr[i]

        # 资产自然生长
        current_positions = current_positions * (1 + daily_ret)
        current_value = np.sum(current_positions)
        current_weights = current_positions / current_value

        # 更新高水位与当前回撤
        if current_value > max_nav:
            max_nav = current_value
        current_dd = (current_value - max_nav) / max_nav

        # 【风控嗅探】
        # 相关性崩溃 = 股债高度正相关 且 两者都在跌（真正的双杀）
        corr_breakdown = (
            current_corr > RISK_CORR_THRESHOLD
            and spy_30d < 0
            and tlt_30d < 0
        )
        is_risk_detected = (current_dd <= RISK_DD_THRESHOLD) or corr_breakdown

        # ---------------------------------------
        # 【状态机核心逻辑】
        # ---------------------------------------
        if current_state == "IDLE":
            if current_dd <= HARD_STOP_DD:
                # 紧急避险：硬止损触发，直接进入最高级别保护
                current_state = "PROTECTION"
                cooldown_counter = COOLDOWN_DAYS * 2  # 双倍冷却期
                protection_trigger_count += 1

                turnover = np.sum(np.abs(current_weights - WEIGHTS_EMERGENCY)) / 2
                current_value -= current_value * turnover * fee_rate
                current_positions = current_value * WEIGHTS_EMERGENCY
                rebalance_count += 1
            elif is_risk_detected:
                # 触发警报，切入 PROTECTION 模式
                current_state = "PROTECTION"
                cooldown_counter = COOLDOWN_DAYS
                protection_trigger_count += 1

                # 强行调仓至保护权重
                turnover = np.sum(np.abs(current_weights - WEIGHTS_PROTECT)) / 2
                current_value -= current_value * turnover * fee_rate
                current_positions = current_value * WEIGHTS_PROTECT
                rebalance_count += 1
            else:
                # 正常模式下的日常维护（阈值触发或年末时间触发）
                max_drift = np.max(np.abs(current_weights - WEIGHTS_IDLE))
                is_year_end = (
                    i < len(dates) - 1 and dates[i].year != dates[i + 1].year
                )

                if max_drift > drift_threshold or is_year_end:
                    turnover = np.sum(np.abs(current_weights - WEIGHTS_IDLE)) / 2
                    current_value -= current_value * turnover * fee_rate
                    current_positions = current_value * WEIGHTS_IDLE
                    rebalance_count += 1

        elif current_state == "PROTECTION":
            # 如果已在保护模式但回撤继续恶化到硬止损线，升级到紧急避险权重
            if current_dd <= HARD_STOP_DD:
                current_weights_check = current_positions / current_value
                if current_weights_check[3] < WEIGHTS_EMERGENCY[3] - 0.05:
                    turnover = np.sum(np.abs(current_weights_check - WEIGHTS_EMERGENCY)) / 2
                    current_value -= current_value * turnover * fee_rate
                    current_positions = current_value * WEIGHTS_EMERGENCY
                    cooldown_counter = COOLDOWN_DAYS * 2
                    rebalance_count += 1

            if cooldown_counter > 0:
                cooldown_counter -= 1

            # 解除警报条件：风控指标恢复正常，且冷却期已满
            if not is_risk_detected and cooldown_counter == 0:
                current_state = "IDLE"
                # 危险解除，切回正常权重
                turnover = np.sum(np.abs(current_weights - WEIGHTS_IDLE)) / 2
                current_value -= current_value * turnover * fee_rate
                current_positions = current_value * WEIGHTS_IDLE
                rebalance_count += 1

        # 记录每日数据
        nav_history.append(current_value)
        state_history.append(1 if current_state == "PROTECTION" else 0)

    # ---------------------------------------------------------
    # 4. 生成指标与报告
    # ---------------------------------------------------------
    result_df = pd.DataFrame(
        {"NAV": nav_history, "State": state_history}, index=dates
    )

    total_return = result_df["NAV"].iloc[-1] / initial_capital - 1
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result_df["NAV"].iloc[-1] / initial_capital) ** (1 / years) - 1

    running_max = result_df["NAV"].cummax()
    drawdown = (result_df["NAV"] - running_max) / running_max
    max_drawdown = drawdown.min()

    daily_vol = result_df["NAV"].pct_change().std()
    annual_vol = daily_vol * np.sqrt(252)
    sharpe_ratio = (cagr - 0.02) / annual_vol if annual_vol > 0 else 0

    # 保护模式总天数
    protection_days = sum(state_history)

    print("-" * 50)
    print("息壤 Phase 2: 状态机风控回测 (2005-2025)")
    print("-" * 50)
    print(f"  初始资金:       ${initial_capital:,.2f}")
    print(f"  最终资金:       ${result_df['NAV'].iloc[-1]:,.2f}")
    print(f"  总收益率:       {total_return:.2%}")
    print(f"  年化收益(CAGR): {cagr:.2%}")
    print(f"  最大回撤(MDD):  {max_drawdown:.2%}  ◄── 风控核心指标")
    print(f"  年化波动率:     {annual_vol:.2%}")
    print(f"  夏普比率:       {sharpe_ratio:.2f}")
    print(f"  总调仓次数:     {rebalance_count} 次")
    print(f"  保护模式触发:   {protection_trigger_count} 次")
    print(f"  保护模式天数:   {protection_days} 天 ({protection_days/len(dates)*100:.1f}%)")
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

    # Phase 1 vs Phase 2 对比
    print("\n── Phase 1 vs Phase 2 对比 ──")
    print(f"  {'指标':<16} {'Phase 1':>12} {'Phase 2':>12} {'变化':>10}")
    print(f"  {'─'*50}")
    # Phase 1 硬编码结果（来自上一次运行）
    p1_cagr, p1_mdd, p1_sharpe, p1_rebal = 0.0742, -0.1720, 0.75, 31
    print(f"  {'CAGR':<16} {p1_cagr:>11.2%} {cagr:>11.2%} {cagr-p1_cagr:>+9.2%}")
    print(f"  {'MDD':<16} {p1_mdd:>11.2%} {max_drawdown:>11.2%} {max_drawdown-p1_mdd:>+9.2%}")
    print(f"  {'夏普比率':<14} {p1_sharpe:>11.2f} {sharpe_ratio:>11.2f} {sharpe_ratio-p1_sharpe:>+9.2f}")
    print(f"  {'调仓次数':<14} {p1_rebal:>11d} {rebalance_count:>11d} {rebalance_count-p1_rebal:>+9d}")

    # ---------------------------------------------------------
    # 5. 可视化
    # ---------------------------------------------------------
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]}
    )

    # 净值图
    ax1.plot(
        result_df.index, result_df["NAV"],
        label="Xi-Rang NAV (With Risk Engine)", color="#2c3e50", linewidth=1.2,
    )
    ax1.set_title("Xi-Rang Portfolio NAV with PROTECTION State (2005-2025)", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.grid(True, alpha=0.3)

    # 黄色高亮 PROTECTION 区间
    ax1.fill_between(
        result_df.index,
        result_df["NAV"].min() * 0.95,
        result_df["NAV"].max() * 1.02,
        where=(result_df["State"] == 1),
        color="#f1c40f", alpha=0.3, label="PROTECTION State (Cash 50%)",
    )
    ax1.legend()

    # 回撤图
    ax2.fill_between(drawdown.index, drawdown, 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Hard Limit")
    ax2.set_title("Drawdown Profile", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "phase2_stateful_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n图表已保存: {output_dir / 'phase2_stateful_backtest.png'}")
    plt.close()

    return result_df


if __name__ == "__main__":
    run_stateful_backtest()
