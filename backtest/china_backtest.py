"""
息壤（Xi-Rang）中国市场回测

用中国 A 股市场的 ETF 替代美股标的，验证策略在中国金融环境下的表现。

标的映射：
  SPY → 510300.SS  沪深300 ETF（2012年上市）
  TLT → 511010.SS  国债 ETF（2013年上市）
  GLD → 518880.SS  黄金 ETF（2013年上市）
  SHV → 511880.SS  货币 ETF（银华日利，2013年上市）

限制：
  中国 ETF 历史较短，只能覆盖约 2013-2025（约 12 年），
  无法像美股那样覆盖 2005-2025 的完整周期。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from pathlib import Path


# ── 中国市场标的 ──────────────────────────────────────

CN_ASSETS = {
    "510300.SS": "沪深300 ETF",
    "511010.SS": "国债 ETF",
    "518880.SS": "黄金 ETF",
    "511880.SS": "货币 ETF",
}

CN_TICKERS = list(CN_ASSETS.keys())
CN_NAMES = list(CN_ASSETS.values())

# 风控参数（与美股版完全一致）
WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])
WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])
WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])
RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
COOLDOWN_DAYS = 20
DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001


def fetch_china_data():
    """拉取中国 ETF 数据"""
    print("正在拉取中国市场 ETF 数据...")

    frames = {}
    for ticker, name in CN_ASSETS.items():
        print(f"  {ticker} ({name})...", end=" ")
        df = yf.download(ticker, start="2012-01-01", end="2025-12-31", auto_adjust=False, progress=False)
        if df.empty:
            print("⚠ 无数据")
            continue

        if isinstance(df.columns, pd.MultiIndex):
            adj_close = df["Adj Close"]
            if isinstance(adj_close, pd.DataFrame):
                adj_close = adj_close.iloc[:, 0]
        else:
            adj_close = df["Adj Close"]

        frames[ticker] = adj_close
        print(f"✓ {len(adj_close)} 条 ({adj_close.index[0].date()} ~ {adj_close.index[-1].date()})")

    if len(frames) < 4:
        print("  ⚠ 部分标的无数据，用模拟数据补充...")
        # 货币 ETF 拉不到时，用固定年化 2% 模拟（日收益 = 2%/252）
        if "511880.SS" not in frames:
            # 用已有数据的日期索引
            ref_index = list(frames.values())[0].index
            daily_rate = (1.02 ** (1/252)) - 1
            money_fund = pd.Series(index=ref_index, dtype=float)
            money_fund.iloc[0] = 100.0
            for j in range(1, len(money_fund)):
                money_fund.iloc[j] = money_fund.iloc[j-1] * (1 + daily_rate)
            frames["511880.SS"] = money_fund
            print(f"  511880.SS (货币 ETF): 用年化 2% 模拟，{len(money_fund)} 条")

    combined = pd.DataFrame(frames).ffill().bfill().dropna()
    combined.index.name = "date"
    return combined


def run_china_backtest():
    # 1. 拉取数据
    prices = fetch_china_data()

    if prices.empty or len(prices.columns) < 4:
        print("✗ 数据不足，无法运行中国市场回测。")
        print("  可能原因：Yahoo Finance 对中国 ETF 的数据覆盖有限。")
        print("  建议：手动从 Wind/Choice/Tushare 导出 CSV 数据。")
        return

    assets = list(prices.columns)
    returns = prices.pct_change().fillna(0)

    # 风控前置计算（沪深300 vs 国债 的相关性）
    stock_col = assets[0]  # 沪深300
    bond_col = assets[1]   # 国债
    corr_rolling = returns[stock_col].rolling(window=30).corr(returns[bond_col]).fillna(0)
    stock_30d = prices[stock_col].pct_change(30).fillna(0)
    bond_30d = prices[bond_col].pct_change(30).fillna(0)

    # 2. 回测
    print(f"\n开始中国市场回测: {prices.index[0].date()} ~ {prices.index[-1].date()} ({len(prices)} 个交易日)")

    initial_capital = 100000.0
    current_value = initial_capital
    current_positions = current_value * WEIGHTS_IDLE
    max_nav = initial_capital

    state = "IDLE"
    cooldown = 0
    rebalance_count = 0
    protection_count = 0

    nav_history = []
    state_history = []
    dates = returns.index

    for i in range(len(dates)):
        daily_ret = returns.iloc[i].values
        corr_val = corr_rolling.iloc[i]
        s30d = stock_30d.iloc[i]
        b30d = bond_30d.iloc[i]

        # 资产生长
        current_positions = current_positions * (1 + daily_ret)
        current_value = np.sum(current_positions)
        current_weights = current_positions / current_value

        if current_value > max_nav:
            max_nav = current_value
        current_dd = (current_value - max_nav) / max_nav

        # 风控
        corr_breakdown = corr_val > RISK_CORR_THRESHOLD and s30d < 0 and b30d < 0
        is_risk = (current_dd <= RISK_DD_THRESHOLD) or corr_breakdown

        if state == "IDLE":
            if current_dd <= HARD_STOP_DD:
                state = "PROTECTION"
                cooldown = COOLDOWN_DAYS * 2
                protection_count += 1
                turnover = np.sum(np.abs(current_weights - WEIGHTS_EMERGENCY)) / 2
                current_value -= current_value * turnover * FEE_RATE
                current_positions = current_value * WEIGHTS_EMERGENCY
                rebalance_count += 1
            elif is_risk:
                state = "PROTECTION"
                cooldown = COOLDOWN_DAYS
                protection_count += 1
                turnover = np.sum(np.abs(current_weights - WEIGHTS_PROTECT)) / 2
                current_value -= current_value * turnover * FEE_RATE
                current_positions = current_value * WEIGHTS_PROTECT
                rebalance_count += 1
            else:
                max_drift = np.max(np.abs(current_weights - WEIGHTS_IDLE))
                is_year_end = i < len(dates) - 1 and dates[i].year != dates[i + 1].year
                if max_drift > DRIFT_THRESHOLD or is_year_end:
                    turnover = np.sum(np.abs(current_weights - WEIGHTS_IDLE)) / 2
                    current_value -= current_value * turnover * FEE_RATE
                    current_positions = current_value * WEIGHTS_IDLE
                    rebalance_count += 1
        elif state == "PROTECTION":
            if current_dd <= HARD_STOP_DD:
                cw = current_positions / current_value
                if cw[3] < WEIGHTS_EMERGENCY[3] - 0.05:
                    turnover = np.sum(np.abs(cw - WEIGHTS_EMERGENCY)) / 2
                    current_value -= current_value * turnover * FEE_RATE
                    current_positions = current_value * WEIGHTS_EMERGENCY
                    cooldown = COOLDOWN_DAYS * 2
                    rebalance_count += 1
            if cooldown > 0:
                cooldown -= 1
            if not is_risk and cooldown == 0:
                state = "IDLE"
                turnover = np.sum(np.abs(current_weights - WEIGHTS_IDLE)) / 2
                current_value -= current_value * turnover * FEE_RATE
                current_positions = current_value * WEIGHTS_IDLE
                rebalance_count += 1

        nav_history.append(current_value)
        state_history.append(1 if state == "PROTECTION" else 0)

    # 3. 计算指标
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

    # 4. 报告
    print()
    print("=" * 60)
    print("  息壤 · 中国市场回测报告")
    print("=" * 60)
    print(f"  标的: {' / '.join(CN_NAMES)}")
    print(f"  时间: {dates[0].date()} ~ {dates[-1].date()} ({years:.1f} 年)")
    print(f"  初始资金:       ¥{initial_capital:,.2f}")
    print(f"  最终资金:       ¥{result['NAV'].iloc[-1]:,.2f}")
    print(f"  总收益率:       {total_return:.2%}")
    print(f"  年化收益(CAGR): {cagr:.2%}")
    print(f"  最大回撤(MDD):  {max_drawdown:.2%}  (发生于 {max_dd_date.date()})")
    print(f"  年化波动率:     {annual_vol:.2%}")
    print(f"  夏普比率:       {sharpe:.2f}")
    print(f"  调仓次数:       {rebalance_count} 次")
    print(f"  保护模式触发:   {protection_count} 次")
    print(f"  保护模式天数:   {protection_days} 天 ({protection_days/len(dates)*100:.1f}%)")
    print("-" * 60)

    # 成功标准
    print("  ── 成功标准判定 ──")
    avg_cn_inflation = 0.025  # 中国近年 CPI 约 2-3%
    checks = [
        ("CAGR ≥ 通胀+2%", cagr >= avg_cn_inflation + 0.02, f"{cagr:.2%} vs {avg_cn_inflation+0.02:.2%}"),
        ("MDD ≤ -15%", max_drawdown >= -0.15, f"{max_drawdown:.2%}"),
        ("夏普 > 0.5", sharpe > 0.5, f"{sharpe:.2f}"),
    ]
    for name, passed, detail in checks:
        mark = "✓" if passed else "✗"
        print(f"    {mark} {name}: {detail}")

    # 美股 vs 中国对比
    print(f"\n  ── 美股 vs 中国市场对比 ──")
    print(f"  {'指标':<16} {'美股(21年)':>12} {'中国('+str(round(years))+'年)':>12}")
    print(f"  {'─'*40}")
    print(f"  {'CAGR':<16} {'7.34%':>12} {cagr:>11.2%}")
    print(f"  {'MDD':<16} {'-14.81%':>12} {max_drawdown:>11.2%}")
    print(f"  {'夏普比率':<14} {'0.77':>12} {sharpe:>11.2f}")
    print(f"  {'调仓次数':<14} {'51':>12} {rebalance_count:>11d}")
    print("=" * 60)

    # 5. 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})

    ax1.plot(result.index, result["NAV"], color="#c0392b", linewidth=1.2, label="Xi-Rang China NAV")
    ax1.fill_between(
        result.index, result["NAV"].min() * 0.95, result["NAV"].max() * 1.02,
        where=(result["State"] == 1), color="#f1c40f", alpha=0.3, label="PROTECTION",
    )
    ax1.set_title("Xi-Rang China Market Backtest", fontsize=14)
    ax1.set_ylabel("Value (CNY)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.fill_between(drawdown.index, drawdown, 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "china_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'china_backtest.png'}")
    plt.close()

    return result


if __name__ == "__main__":
    run_china_backtest()
