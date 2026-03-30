"""
息壤（Xi-Rang）全球配置回测（All-Weather Global）

真正的降维打击：不赌任何单一国家的国运。

标的：
  VT:   Vanguard Total World Stock ETF（全球股票，覆盖 49 个国家）
  BWX:  SPDR International Treasury Bond ETF（全球主权债券）
  GLD:  黄金 ETF（全球定价）
  SHV:  短期美债 / 现金等价物

覆盖：2008-2025（约 17 年），包含：
  - 2008 全球金融危机
  - 2011 欧债危机
  - 2015 中国股灾
  - 2018 全球贸易战
  - 2020 疫情熔断
  - 2022 全球加息 + 股债双杀

同时跑美股原版（SPY+TLT+GLD+SHV）作为对照组，
用相同的时间窗口做苹果对苹果的对比。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from pathlib import Path


WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])
WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])
WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])
RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
COOLDOWN_DAYS = 20
DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001


def fetch(tickers, start="2007-01-01", end="2025-12-31"):
    frames = {}
    for t in tickers:
        df = yf.download(t, start=start, end=end, auto_adjust=False, progress=False)
        if df.empty:
            print(f"  ⚠ {t}: 无数据")
            continue
        if isinstance(df.columns, pd.MultiIndex):
            adj = df["Adj Close"]
            if isinstance(adj, pd.DataFrame):
                adj = adj.iloc[:, 0]
        else:
            adj = df["Adj Close"]
        frames[t] = adj
        print(f"  ✓ {t}: {len(adj)} 条 ({adj.index[0].date()} ~ {adj.index[-1].date()})")
    return pd.DataFrame(frames).ffill().bfill().dropna()


def run_engine(prices, label):
    """运行一次完整回测"""
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
        "label": label, "result": result, "dd": dd_s,
        "years": years, "initial": initial, "final": result["NAV"].iloc[-1],
        "cagr": cagr, "mdd": mdd, "mdd_date": mdd_date,
        "vol": vol, "sharpe": sharpe,
        "rebal": rebal, "prot": prot, "prot_days": pd_days, "total_days": len(dates),
    }


def main():
    print("=" * 64)
    print("  息壤 · 全球配置 vs 美股原版 对比回测")
    print("=" * 64)

    print("\n── 全球配置（VT + BWX + GLD + SHV）──")
    global_prices = fetch(["VT", "BWX", "GLD", "SHV"])

    print("\n── 美股原版（SPY + TLT + GLD + SHV）──")
    us_prices = fetch(["SPY", "TLT", "GLD", "SHV"])

    if global_prices.empty:
        print("✗ 数据不足"); return

    # 对齐时间窗口
    start = max(global_prices.index[0], us_prices.index[0])
    end = min(global_prices.index[-1], us_prices.index[-1])
    global_prices = global_prices[start:end]
    us_prices = us_prices[start:end]
    print(f"\n对齐后: {start.date()} ~ {end.date()}")

    g = run_engine(global_prices, "全球配置 (VT+BWX+GLD+SHV)")
    u = run_engine(us_prices, "美股原版 (SPY+TLT+GLD+SHV)")

    # 报告
    print("\n" + "=" * 64)
    print("  回测结果")
    print("=" * 64)

    for m in [g, u]:
        passed = m["cagr"] >= 0.045 and m["mdd"] >= -0.15 and m["sharpe"] > 0.5
        tag = "✓ 全通过" if passed else "✗ 未全通过"
        print(f"\n  [{m['label']}]  {tag}")
        print(f"  {m['years']:.1f} 年 | ${m['initial']:,.0f} → ${m['final']:,.2f} ({m['final']/m['initial']-1:+.2%})")
        print(f"  CAGR: {m['cagr']:.2%}  |  MDD: {m['mdd']:.2%} ({m['mdd_date'].date()})  |  夏普: {m['sharpe']:.2f}")
        print(f"  波动率: {m['vol']:.2%}  |  调仓: {m['rebal']}次  |  保护: {m['prot']}次/{m['prot_days']}天({m['prot_days']/m['total_days']*100:.1f}%)")

    # 对比
    print(f"\n  ── 核心指标对比（同一时间窗口）──")
    print(f"  {'指标':<12} {'全球配置':>14} {'美股原版':>14} {'差异':>10}")
    print(f"  {'─'*52}")
    print(f"  {'CAGR':<12} {g['cagr']:>13.2%} {u['cagr']:>13.2%} {g['cagr']-u['cagr']:>+9.2%}")
    print(f"  {'MDD':<12} {g['mdd']:>13.2%} {u['mdd']:>13.2%} {g['mdd']-u['mdd']:>+9.2%}")
    print(f"  {'夏普比率':<10} {g['sharpe']:>13.2f} {u['sharpe']:>13.2f} {g['sharpe']-u['sharpe']:>+9.2f}")
    print(f"  {'波动率':<10} {g['vol']:>13.2%} {u['vol']:>13.2%} {g['vol']-u['vol']:>+9.2%}")
    print(f"  {'调仓次数':<10} {g['rebal']:>13d} {u['rebal']:>13d} {g['rebal']-u['rebal']:>+9d}")
    print(f"  {'保护触发':<10} {g['prot']:>13d} {u['prot']:>13d} {g['prot']-u['prot']:>+9d}")
    print(f"  {'保护占比':<10} {g['prot_days']/g['total_days']*100:>12.1f}% {u['prot_days']/u['total_days']*100:>12.1f}% {(g['prot_days']/g['total_days']-u['prot_days']/u['total_days'])*100:>+8.1f}%")

    # 定投
    n = int(g["years"])
    principal = 100000 + 50000 * n
    print(f"\n  ── 定投场景（初始 $10万 + 每年 $5万，{n} 年）──")
    print(f"  总投入本金: ${principal:,.0f}")
    for m in [g, u]:
        r = m["cagr"]
        fv = 100000 * (1+r)**n + 50000 * (((1+r)**n - 1)/r) if r > 0 else principal
        print(f"  {m['label'][:6]}: ${fv:>12,.0f}  (利润 ${fv-principal:>10,.0f})")
    bank = 100000*(1.025)**n + 50000*(((1.025)**n-1)/0.025)
    print(f"  {'银行定期':6}: ${bank:>12,.0f}  (利润 ${bank-principal:>10,.0f})")

    # 全球五配置总览
    print(f"\n  ── 全球五配置总览 ──")
    print(f"  {'配置':<12} {'时间':>6} {'CAGR':>7} {'MDD':>9} {'夏普':>6} {'标准':>8}")
    print(f"  {'─'*52}")
    print(f"  {'美股原版':<10} {'21年':>6} {'7.34%':>7} {'-14.81%':>9} {'0.77':>6} {'✓全通过':>8}")
    g_years_label = f"{g['years']:.0f}年"
    g_pass = "✓全通过" if g["cagr"] >= 0.045 and g["mdd"] >= -0.15 and g["sharpe"] > 0.5 else "✗未全通过"
    print(f"  {'全球配置':<10} {g_years_label:>6} {g['cagr']:>6.2%} {g['mdd']:>8.2%} {g['sharpe']:>6.2f} {g_pass:>8}")
    print(f"  {'纯中国':<10} {'14年':>6} {'5.39%':>7} {'-12.97%':>9} {'0.53':>6} {'✓全通过':>8}")
    print(f"  {'中美混血':<10} {'13年':>6} {'5.98%':>7} {'-15.97%':>9} {'0.62':>6} {'✗MDD超标':>8}")
    print(f"  {'欧洲':<10} {'21年':>6} {'3.77%':>7} {'-17.06%':>9} {'0.25':>6} {'✗全未过':>8}")
    print("=" * 64)

    # 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})

    ax1.plot(g["result"].index, g["result"]["NAV"], color="#27ae60", linewidth=1.5, label="Global (VT+BWX+GLD+SHV)")
    ax1.plot(u["result"].index, u["result"]["NAV"], color="#2c3e50", linewidth=1.2, alpha=0.7, label="US (SPY+TLT+GLD+SHV)")
    ax1.fill_between(g["result"].index, g["result"]["NAV"].min()*0.95, g["result"]["NAV"].max()*1.02,
                     where=(g["result"]["State"]==1), color="#f1c40f", alpha=0.2, label="Global PROTECTION")
    ax1.set_title("Global All-Weather vs US-Only Portfolio", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.plot(g["dd"].index, g["dd"], color="#27ae60", linewidth=1, label="Global DD")
    ax2.plot(u["dd"].index, u["dd"], color="#2c3e50", linewidth=1, alpha=0.7, label="US DD")
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown Comparison", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "global_vs_us.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'global_vs_us.png'}")
    plt.close()


if __name__ == "__main__":
    main()
