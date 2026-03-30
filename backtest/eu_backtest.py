"""
息壤（Xi-Rang）欧洲市场回测

验证策略在欧洲金融环境下的表现。

标的：
  EZU:  iShares MSCI Eurozone ETF（欧元区股票，2005年起）
  BWX:  SPDR International Treasury Bond ETF（国际国债，2007年起）
  GLD:  黄金 ETF（全球定价）
  SHV:  短期美债 / 现金等价物

覆盖周期：2007-2025（约 18 年），包含：
  - 2008 全球金融危机
  - 2011 欧债危机
  - 2020 疫情
  - 2022 俄乌战争 + 欧洲能源危机
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from pathlib import Path

EU_ASSETS = ["EZU", "BWX", "GLD", "SHV"]
EU_NAMES = ["欧元区股票", "国际国债", "黄金", "现金"]

WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])
WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])
WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])
RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
COOLDOWN_DAYS = 20
DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001


def fetch_eu_data():
    print("正在拉取欧洲市场 ETF 数据...")
    frames = {}
    for ticker, name in zip(EU_ASSETS, EU_NAMES):
        print(f"  {ticker} ({name})...", end=" ")
        df = yf.download(ticker, start="2005-01-01", end="2025-12-31", auto_adjust=False, progress=False)
        if df.empty:
            print("⚠ 无数据")
            continue
        if isinstance(df.columns, pd.MultiIndex):
            adj = df["Adj Close"]
            if isinstance(adj, pd.DataFrame):
                adj = adj.iloc[:, 0]
        else:
            adj = df["Adj Close"]
        frames[ticker] = adj
        print(f"✓ {len(adj)} 条 ({adj.index[0].date()} ~ {adj.index[-1].date()})")

    combined = pd.DataFrame(frames).ffill().bfill().dropna()
    combined.index.name = "date"
    return combined


def run_eu_backtest():
    prices = fetch_eu_data()
    if prices.empty or len(prices.columns) < 4:
        print("✗ 数据不足")
        return

    assets = list(prices.columns)
    returns = prices.pct_change().fillna(0)

    # 风控：欧洲股票 vs 国际国债 的相关性
    corr_rolling = returns[assets[0]].rolling(30).corr(returns[assets[1]]).fillna(0)
    stock_30d = prices[assets[0]].pct_change(30).fillna(0)
    bond_30d = prices[assets[1]].pct_change(30).fillna(0)

    dates = returns.index
    print(f"\n开始欧洲市场回测: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} 个交易日)")

    initial = 100000.0
    value = initial
    positions = value * WEIGHTS_IDLE
    max_nav = initial
    state = "IDLE"
    cooldown = 0
    rebal = 0
    prot_count = 0
    nav_hist = []
    state_hist = []

    for i in range(len(dates)):
        dr = returns.iloc[i].values
        cv = corr_rolling.iloc[i]
        s30 = stock_30d.iloc[i]
        b30 = bond_30d.iloc[i]

        positions = positions * (1 + dr)
        value = np.sum(positions)
        weights = positions / value

        if value > max_nav:
            max_nav = value
        dd = (value - max_nav) / max_nav

        corr_break = cv > RISK_CORR_THRESHOLD and s30 < 0 and b30 < 0
        is_risk = (dd <= RISK_DD_THRESHOLD) or corr_break

        if state == "IDLE":
            if dd <= HARD_STOP_DD:
                state = "PROTECTION"
                cooldown = COOLDOWN_DAYS * 2
                prot_count += 1
                t = np.sum(np.abs(weights - WEIGHTS_EMERGENCY)) / 2
                value -= value * t * FEE_RATE
                positions = value * WEIGHTS_EMERGENCY
                rebal += 1
            elif is_risk:
                state = "PROTECTION"
                cooldown = COOLDOWN_DAYS
                prot_count += 1
                t = np.sum(np.abs(weights - WEIGHTS_PROTECT)) / 2
                value -= value * t * FEE_RATE
                positions = value * WEIGHTS_PROTECT
                rebal += 1
            else:
                md = np.max(np.abs(weights - WEIGHTS_IDLE))
                ye = i < len(dates) - 1 and dates[i].year != dates[i + 1].year
                if md > DRIFT_THRESHOLD or ye:
                    t = np.sum(np.abs(weights - WEIGHTS_IDLE)) / 2
                    value -= value * t * FEE_RATE
                    positions = value * WEIGHTS_IDLE
                    rebal += 1
        elif state == "PROTECTION":
            if dd <= HARD_STOP_DD:
                cw = positions / value
                if cw[3] < WEIGHTS_EMERGENCY[3] - 0.05:
                    t = np.sum(np.abs(cw - WEIGHTS_EMERGENCY)) / 2
                    value -= value * t * FEE_RATE
                    positions = value * WEIGHTS_EMERGENCY
                    cooldown = COOLDOWN_DAYS * 2
                    rebal += 1
            if cooldown > 0:
                cooldown -= 1
            if not is_risk and cooldown == 0:
                state = "IDLE"
                t = np.sum(np.abs(weights - WEIGHTS_IDLE)) / 2
                value -= value * t * FEE_RATE
                positions = value * WEIGHTS_IDLE
                rebal += 1

        nav_hist.append(value)
        state_hist.append(1 if state == "PROTECTION" else 0)

    result = pd.DataFrame({"NAV": nav_hist, "State": state_hist}, index=dates)
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result["NAV"].iloc[-1] / initial) ** (1 / years) - 1
    rm = result["NAV"].cummax()
    dd_series = (result["NAV"] - rm) / rm
    mdd = dd_series.min()
    mdd_date = dd_series.idxmin()
    vol = result["NAV"].pct_change().std() * np.sqrt(252)
    sharpe = (cagr - 0.02) / vol if vol > 0 else 0
    prot_days = sum(state_hist)

    # 报告
    print()
    print("=" * 60)
    print("  息壤 · 欧洲市场回测报告")
    print("=" * 60)
    print(f"  标的: {' / '.join(f'{a}({n})' for a, n in zip(EU_ASSETS, EU_NAMES))}")
    print(f"  时间: {dates[0].date()} ~ {dates[-1].date()} ({years:.1f} 年)")
    print(f"  初始资金:       ${initial:,.2f}")
    print(f"  最终资金:       ${result['NAV'].iloc[-1]:,.2f}")
    print(f"  总收益率:       {result['NAV'].iloc[-1] / initial - 1:.2%}")
    print(f"  年化收益(CAGR): {cagr:.2%}")
    print(f"  最大回撤(MDD):  {mdd:.2%}  (发生于 {mdd_date.date()})")
    print(f"  年化波动率:     {vol:.2%}")
    print(f"  夏普比率:       {sharpe:.2f}")
    print(f"  调仓次数:       {rebal} 次")
    print(f"  保护模式触发:   {prot_count} 次")
    print(f"  保护模式天数:   {prot_days} 天 ({prot_days/len(dates)*100:.1f}%)")
    print("-" * 60)

    # 成功标准
    print("  ── 成功标准判定 ──")
    eu_inflation = 0.025
    checks = [
        ("CAGR ≥ 通胀+2%", cagr >= eu_inflation + 0.02, f"{cagr:.2%} vs {eu_inflation+0.02:.2%}"),
        ("MDD ≤ -15%", mdd >= -0.15, f"{mdd:.2%}"),
        ("夏普 > 0.5", sharpe > 0.5, f"{sharpe:.2f}"),
    ]
    all_pass = True
    for name, ok, detail in checks:
        mark = "✓" if ok else "✗"
        print(f"    {mark} {name}: {detail}")
        if not ok:
            all_pass = False

    # 四市场对比
    print(f"\n  ── 全球四配置对比 ──")
    print(f"  {'配置':<12} {'时间':>8} {'CAGR':>8} {'MDD':>10} {'夏普':>6}")
    print(f"  {'─'*46}")
    print(f"  {'美股原版':<10} {'21年':>8} {'7.34%':>8} {'-14.81%':>10} {'0.77':>6}")
    print(f"  {'纯中国':<10} {'14年':>8} {'5.39%':>8} {'-12.97%':>10} {'0.53':>6}")
    print(f"  {'中美混血':<10} {'13年':>8} {'5.98%':>8} {'-15.97%':>10} {'0.62':>6}")
    print(f"  {'欧洲':<10} {f'{years:.0f}年':>8} {cagr:>7.2%} {mdd:>9.2%} {sharpe:>6.2f}")

    # 定投
    n = int(years)
    principal = 100000 + 50000 * n
    init_fv = 100000 * (1 + cagr) ** n
    ann_fv = 50000 * (((1 + cagr) ** n - 1) / cagr) if cagr > 0 else 50000 * n
    total_fv = init_fv + ann_fv
    bank_init = 100000 * (1.025) ** n
    bank_ann = 50000 * (((1.025) ** n - 1) / 0.025)
    bank_total = bank_init + bank_ann

    print(f"\n  ── 定投场景（初始 $10万 + 每年 $5万，{n} 年）──")
    print(f"  总投入本金: ${principal:,.0f}")
    print(f"  息壤(欧洲): ${total_fv:>12,.0f}  (利润 ${total_fv - principal:>10,.0f})")
    print(f"  银行定期:   ${bank_total:>12,.0f}  (利润 ${bank_total - principal:>10,.0f})")
    print(f"  息壤多赚:   ${total_fv - bank_total:>12,.0f}")
    print("=" * 60)

    # 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})

    ax1.plot(result.index, result["NAV"], color="#2980b9", linewidth=1.2, label="Xi-Rang Europe NAV")
    ax1.fill_between(result.index, result["NAV"].min() * 0.95, result["NAV"].max() * 1.02,
                     where=(result["State"] == 1), color="#f1c40f", alpha=0.3, label="PROTECTION")
    ax1.set_title("Xi-Rang Europe Market Backtest", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.fill_between(dd_series.index, dd_series, 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "eu_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'eu_backtest.png'}")
    plt.close()


if __name__ == "__main__":
    run_eu_backtest()
