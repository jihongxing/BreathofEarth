"""
息壤（Xi-Rang）印度市场回测

验证策略在印度这个高速增长的新兴市场中的表现。

标的：
  EPI:  WisdomTree India Earnings Fund（印度股票，2008年起）
  EMB:  iShares JP Morgan USD EM Bond（新兴市场美元债，替代印度国债）
  GLD:  黄金 ETF（印度是全球最大黄金消费国之一）
  SHV:  短期美债 / 现金等价物

覆盖：2008-2025（约 17 年），包含：
  - 2008 全球金融危机
  - 2013 印度"缩减恐慌"（Taper Tantrum）
  - 2016 废钞令
  - 2020 疫情
  - 2022 全球加息
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from pathlib import Path

ASSETS = ["EPI", "EMB", "GLD", "SHV"]
NAMES = ["印度股票", "新兴市场债券", "黄金", "现金"]

WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])
WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])
WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])
RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
COOLDOWN_DAYS = 20
DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001


def fetch_data():
    print("正在拉取印度市场相关 ETF 数据...")
    frames = {}
    for ticker, name in zip(ASSETS, NAMES):
        print(f"  {ticker} ({name})...", end=" ")
        df = yf.download(ticker, start="2007-01-01", end="2025-12-31", auto_adjust=False, progress=False)
        if df.empty:
            print("⚠ 无数据"); continue
        if isinstance(df.columns, pd.MultiIndex):
            adj = df["Adj Close"]
            if isinstance(adj, pd.DataFrame): adj = adj.iloc[:, 0]
        else:
            adj = df["Adj Close"]
        frames[ticker] = adj
        print(f"✓ {len(adj)} 条 ({adj.index[0].date()} ~ {adj.index[-1].date()})")
    return pd.DataFrame(frames).ffill().bfill().dropna()


def run_backtest(prices):
    assets = list(prices.columns)
    returns = prices.pct_change().fillna(0)
    corr = returns[assets[0]].rolling(30).corr(returns[assets[1]]).fillna(0)
    s30 = prices[assets[0]].pct_change(30).fillna(0)
    b30 = prices[assets[1]].pct_change(30).fillna(0)

    dates = returns.index
    initial = 100000.0
    value = initial
    pos = value * WEIGHTS_IDLE
    hwm = initial
    state = "IDLE"
    cd = 0
    rebal = 0
    prot = 0
    navs = []
    states = []

    for i in range(len(dates)):
        dr = returns.iloc[i].values
        cv, s, b = corr.iloc[i], s30.iloc[i], b30.iloc[i]
        pos = pos * (1 + dr)
        value = np.sum(pos)
        w = pos / value
        if value > hwm: hwm = value
        dd = (value - hwm) / hwm
        cb = cv > RISK_CORR_THRESHOLD and s < 0 and b < 0
        risk = (dd <= RISK_DD_THRESHOLD) or cb

        if state == "IDLE":
            if dd <= HARD_STOP_DD:
                state = "PROTECTION"; cd = COOLDOWN_DAYS * 2; prot += 1
                t = np.sum(np.abs(w - WEIGHTS_EMERGENCY)) / 2
                value -= value * t * FEE_RATE; pos = value * WEIGHTS_EMERGENCY; rebal += 1
            elif risk:
                state = "PROTECTION"; cd = COOLDOWN_DAYS; prot += 1
                t = np.sum(np.abs(w - WEIGHTS_PROTECT)) / 2
                value -= value * t * FEE_RATE; pos = value * WEIGHTS_PROTECT; rebal += 1
            else:
                md = np.max(np.abs(w - WEIGHTS_IDLE))
                ye = i < len(dates) - 1 and dates[i].year != dates[i+1].year
                if md > DRIFT_THRESHOLD or ye:
                    t = np.sum(np.abs(w - WEIGHTS_IDLE)) / 2
                    value -= value * t * FEE_RATE; pos = value * WEIGHTS_IDLE; rebal += 1
        elif state == "PROTECTION":
            if dd <= HARD_STOP_DD:
                cw = pos / value
                if cw[3] < WEIGHTS_EMERGENCY[3] - 0.05:
                    t = np.sum(np.abs(cw - WEIGHTS_EMERGENCY)) / 2
                    value -= value * t * FEE_RATE; pos = value * WEIGHTS_EMERGENCY
                    cd = COOLDOWN_DAYS * 2; rebal += 1
            if cd > 0: cd -= 1
            if not risk and cd == 0:
                state = "IDLE"
                t = np.sum(np.abs(w - WEIGHTS_IDLE)) / 2
                value -= value * t * FEE_RATE; pos = value * WEIGHTS_IDLE; rebal += 1

        navs.append(value)
        states.append(1 if state == "PROTECTION" else 0)

    result = pd.DataFrame({"NAV": navs, "State": states}, index=dates)
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result["NAV"].iloc[-1] / initial) ** (1 / years) - 1
    rm = result["NAV"].cummax()
    dd_s = (result["NAV"] - rm) / rm
    mdd = dd_s.min()
    mdd_date = dd_s.idxmin()
    vol = result["NAV"].pct_change().std() * np.sqrt(252)
    sharpe = (cagr - 0.02) / vol if vol > 0 else 0
    pd_days = sum(states)

    return {
        "result": result, "dd": dd_s, "years": years,
        "initial": initial, "final": result["NAV"].iloc[-1],
        "cagr": cagr, "mdd": mdd, "mdd_date": mdd_date,
        "vol": vol, "sharpe": sharpe,
        "rebal": rebal, "prot": prot, "prot_days": pd_days, "total_days": len(dates),
    }


def main():
    print("=" * 60)
    print("  息壤 · 印度市场回测")
    print("=" * 60)

    prices = fetch_data()
    if prices.empty or len(prices.columns) < 4:
        print("✗ 数据不足"); return

    print(f"\n回测范围: {prices.index[0].date()} ~ {prices.index[-1].date()}")
    m = run_backtest(prices)

    passed_cagr = m["cagr"] >= 0.045
    passed_mdd = m["mdd"] >= -0.15
    passed_sharpe = m["sharpe"] > 0.5
    all_pass = passed_cagr and passed_mdd and passed_sharpe
    tag = "✓ 全通过" if all_pass else "✗ 未全通过"

    print(f"\n{'='*60}")
    print(f"  息壤 · 印度市场回测报告  {tag}")
    print(f"{'='*60}")
    print(f"  标的: {' / '.join(f'{a}({n})' for a, n in zip(ASSETS, NAMES))}")
    print(f"  时间: {prices.index[0].date()} ~ {prices.index[-1].date()} ({m['years']:.1f} 年)")
    print(f"  初始资金:       ${m['initial']:,.2f}")
    print(f"  最终资金:       ${m['final']:,.2f}")
    print(f"  总收益率:       {m['final']/m['initial']-1:.2%}")
    print(f"  年化收益(CAGR): {m['cagr']:.2%}")
    print(f"  最大回撤(MDD):  {m['mdd']:.2%}  (发生于 {m['mdd_date'].date()})")
    print(f"  年化波动率:     {m['vol']:.2%}")
    print(f"  夏普比率:       {m['sharpe']:.2f}")
    print(f"  调仓次数:       {m['rebal']} 次")
    print(f"  保护模式触发:   {m['prot']} 次")
    print(f"  保护模式天数:   {m['prot_days']} 天 ({m['prot_days']/m['total_days']*100:.1f}%)")
    print(f"-" * 60)

    print(f"  ── 成功标准判定 ──")
    for name, ok, detail in [
        ("CAGR ≥ 通胀+2%", passed_cagr, f"{m['cagr']:.2%} vs 4.50%"),
        ("MDD ≤ -15%", passed_mdd, f"{m['mdd']:.2%}"),
        ("夏普 > 0.5", passed_sharpe, f"{m['sharpe']:.2f}"),
    ]:
        print(f"    {'✓' if ok else '✗'} {name}: {detail}")

    # 定投
    n = int(m["years"])
    principal = 100000 + 50000 * n
    r = m["cagr"]
    fv = 100000*(1+r)**n + 50000*(((1+r)**n-1)/r) if r > 0 else principal
    bank = 100000*(1.025)**n + 50000*(((1.025)**n-1)/0.025)

    print(f"\n  ── 定投场景（初始 $10万 + 每年 $5万，{n} 年）──")
    print(f"  总投入本金: ${principal:,.0f}")
    print(f"  息壤(印度): ${fv:>12,.0f}  (利润 ${fv-principal:>10,.0f})")
    print(f"  银行定期:   ${bank:>12,.0f}  (利润 ${bank-principal:>10,.0f})")
    print(f"  息壤多赚:   ${fv-bank:>12,.0f}")

    # 全球六配置总览
    print(f"\n  ── 全球六配置总览 ──")
    print(f"  {'配置':<12} {'时间':>6} {'CAGR':>7} {'MDD':>9} {'夏普':>6} {'标准':>8}")
    print(f"  {'─'*52}")
    print(f"  {'美股原版':<10} {'21年':>6} {'7.34%':>7} {'-14.81%':>9} {'0.77':>6} {'✓全通过':>8}")
    india_years = f"{m['years']:.0f}年"
    india_tag = "✓全通过" if all_pass else "✗未全通过"
    print(f"  {'印度':<10} {india_years:>6} {m['cagr']:>6.2%} {m['mdd']:>8.2%} {m['sharpe']:>6.2f} {india_tag:>8}")
    print(f"  {'纯中国':<10} {'14年':>6} {'5.39%':>7} {'-12.97%':>9} {'0.53':>6} {'✓全通过':>8}")
    print(f"  {'中美混血':<10} {'13年':>6} {'5.98%':>7} {'-15.97%':>9} {'0.62':>6} {'✗MDD超标':>8}")
    print(f"  {'全球配置':<10} {'19年':>6} {'4.44%':>7} {'-15.55%':>9} {'0.36':>6} {'✗未全通过':>8}")
    print(f"  {'欧洲':<10} {'21年':>6} {'3.77%':>7} {'-17.06%':>9} {'0.25':>6} {'✗全未过':>8}")
    print(f"{'='*60}")

    # 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    ax1.plot(m["result"].index, m["result"]["NAV"], color="#e67e22", linewidth=1.2, label="Xi-Rang India NAV")
    ax1.fill_between(m["result"].index, m["result"]["NAV"].min()*0.95, m["result"]["NAV"].max()*1.02,
                     where=(m["result"]["State"]==1), color="#f1c40f", alpha=0.3, label="PROTECTION")
    ax1.set_title("Xi-Rang India Market Backtest", fontsize=14)
    ax1.set_ylabel("Value ($)"); ax1.legend(); ax1.grid(True, alpha=0.3)

    ax2.fill_between(m["dd"].index, m["dd"], 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown"); ax2.set_ylabel("Drawdown"); ax2.legend(); ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "india_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'india_backtest.png'}")
    plt.close()


if __name__ == "__main__":
    main()
