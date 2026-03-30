"""
息壤（Xi-Rang）混血配置回测（Chimerica Portfolio）

用国内券商账户实现"美股进攻 + 中国防守"的混血组合：
  股票：513500.SS  标普500 ETF（国内上市，人民币计价）
  国债：511090.SS  30年国债 ETF
  黄金：518880.SS  黄金 ETF
  现金：模拟货币基金（年化 2%）

对比：
  1. 纯中国配置（沪深300 + 国债 + 黄金 + 货币）
  2. 混血配置（标普500 ETF + 国债 + 黄金 + 货币）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from pathlib import Path


# ── 标的定义 ──────────────────────────────────────────

CHIMERICA_ASSETS = {
    "513500.SS": "标普500 ETF(国内)",
    "511010.SS": "国债 ETF",
    "518880.SS": "黄金 ETF",
    "MONEY": "货币基金(模拟)",
}

CHINA_ASSETS = {
    "510300.SS": "沪深300 ETF",
    "511010.SS": "国债 ETF",
    "518880.SS": "黄金 ETF",
    "MONEY": "货币基金(模拟)",
}

# 风控参数（与主系统一致）
WEIGHTS_IDLE = np.array([0.25, 0.25, 0.25, 0.25])
WEIGHTS_PROTECT = np.array([0.10, 0.20, 0.20, 0.50])
WEIGHTS_EMERGENCY = np.array([0.03, 0.07, 0.15, 0.75])
RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
COOLDOWN_DAYS = 20
DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001


def fetch_data(asset_map: dict) -> pd.DataFrame:
    """拉取 ETF 数据，货币基金用模拟"""
    frames = {}
    for ticker, name in asset_map.items():
        if ticker == "MONEY":
            continue
        print(f"  {ticker} ({name})...", end=" ")
        df = yf.download(ticker, start="2012-01-01", end="2025-12-31", auto_adjust=False, progress=False)
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

    if not frames:
        return pd.DataFrame()

    combined = pd.DataFrame(frames).ffill().bfill().dropna()

    # 模拟货币基金
    daily_rate = (1.02 ** (1 / 252)) - 1
    money = pd.Series(index=combined.index, dtype=float)
    money.iloc[0] = 100.0
    for j in range(1, len(money)):
        money.iloc[j] = money.iloc[j - 1] * (1 + daily_rate)
    combined["MONEY"] = money

    combined.index.name = "date"
    return combined


def run_backtest(prices: pd.DataFrame, label: str) -> dict:
    """运行一次完整回测，返回指标字典"""
    assets = list(prices.columns)
    returns = prices.pct_change().fillna(0)

    stock_col, bond_col = assets[0], assets[1]
    corr_rolling = returns[stock_col].rolling(30).corr(returns[bond_col]).fillna(0)
    stock_30d = prices[stock_col].pct_change(30).fillna(0)
    bond_30d = prices[bond_col].pct_change(30).fillna(0)

    initial = 100000.0
    value = initial
    positions = value * WEIGHTS_IDLE
    max_nav = initial
    state = "IDLE"
    cooldown = 0
    rebal_count = 0
    prot_count = 0
    nav_hist = []
    state_hist = []
    dates = returns.index

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
                rebal_count += 1
            elif is_risk:
                state = "PROTECTION"
                cooldown = COOLDOWN_DAYS
                prot_count += 1
                t = np.sum(np.abs(weights - WEIGHTS_PROTECT)) / 2
                value -= value * t * FEE_RATE
                positions = value * WEIGHTS_PROTECT
                rebal_count += 1
            else:
                md = np.max(np.abs(weights - WEIGHTS_IDLE))
                ye = i < len(dates) - 1 and dates[i].year != dates[i + 1].year
                if md > DRIFT_THRESHOLD or ye:
                    t = np.sum(np.abs(weights - WEIGHTS_IDLE)) / 2
                    value -= value * t * FEE_RATE
                    positions = value * WEIGHTS_IDLE
                    rebal_count += 1
        elif state == "PROTECTION":
            if dd <= HARD_STOP_DD:
                cw = positions / value
                if cw[3] < WEIGHTS_EMERGENCY[3] - 0.05:
                    t = np.sum(np.abs(cw - WEIGHTS_EMERGENCY)) / 2
                    value -= value * t * FEE_RATE
                    positions = value * WEIGHTS_EMERGENCY
                    cooldown = COOLDOWN_DAYS * 2
                    rebal_count += 1
            if cooldown > 0:
                cooldown -= 1
            if not is_risk and cooldown == 0:
                state = "IDLE"
                t = np.sum(np.abs(weights - WEIGHTS_IDLE)) / 2
                value -= value * t * FEE_RATE
                positions = value * WEIGHTS_IDLE
                rebal_count += 1

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

    return {
        "label": label,
        "result": result,
        "drawdown": dd_series,
        "years": years,
        "initial": initial,
        "final": result["NAV"].iloc[-1],
        "total_return": result["NAV"].iloc[-1] / initial - 1,
        "cagr": cagr,
        "mdd": mdd,
        "mdd_date": mdd_date,
        "vol": vol,
        "sharpe": sharpe,
        "rebal_count": rebal_count,
        "prot_count": prot_count,
        "prot_days": prot_days,
        "total_days": len(dates),
    }


def print_report(m: dict):
    """打印单个回测报告"""
    print(f"\n  [{m['label']}]")
    print(f"  时间: {m['result'].index[0].date()} ~ {m['result'].index[-1].date()} ({m['years']:.1f} 年)")
    print(f"  ¥{m['initial']:,.0f} → ¥{m['final']:,.2f}  ({m['total_return']:+.2%})")
    print(f"  CAGR: {m['cagr']:.2%}  |  MDD: {m['mdd']:.2%} ({m['mdd_date'].date()})  |  夏普: {m['sharpe']:.2f}")
    print(f"  波动率: {m['vol']:.2%}  |  调仓: {m['rebal_count']}次  |  保护: {m['prot_count']}次/{m['prot_days']}天({m['prot_days']/m['total_days']*100:.1f}%)")

    passed = m['cagr'] >= 0.045 and m['mdd'] >= -0.15 and m['sharpe'] > 0.5
    checks = [
        ("CAGR≥4.5%", m['cagr'] >= 0.045, f"{m['cagr']:.2%}"),
        ("MDD≤-15%", m['mdd'] >= -0.15, f"{m['mdd']:.2%}"),
        ("夏普>0.5", m['sharpe'] > 0.5, f"{m['sharpe']:.2f}"),
    ]
    for name, ok, detail in checks:
        print(f"    {'✓' if ok else '✗'} {name}: {detail}")


def main():
    print("=" * 60)
    print("  息壤 · 混血配置 vs 纯中国配置 对比回测")
    print("=" * 60)

    # 拉取数据
    print("\n── 混血配置（标普500 ETF + 国债 + 黄金 + 货币）──")
    chimerica_prices = fetch_data(CHIMERICA_ASSETS)

    print("\n── 纯中国配置（沪深300 + 国债 + 黄金 + 货币）──")
    china_prices = fetch_data(CHINA_ASSETS)

    if chimerica_prices.empty or china_prices.empty:
        print("✗ 数据不足")
        return

    # 对齐日期范围（取交集）
    common_start = max(chimerica_prices.index[0], china_prices.index[0])
    common_end = min(chimerica_prices.index[-1], china_prices.index[-1])
    chimerica_prices = chimerica_prices[common_start:common_end]
    china_prices = china_prices[common_start:common_end]

    print(f"\n对齐后日期范围: {common_start.date()} ~ {common_end.date()}")

    # 运行回测
    m_chimerica = run_backtest(chimerica_prices, "混血配置 (标普500+国债+黄金+货币)")
    m_china = run_backtest(china_prices, "纯中国配置 (沪深300+国债+黄金+货币)")

    # 报告
    print("\n" + "=" * 60)
    print("  回测结果")
    print("=" * 60)
    print_report(m_chimerica)
    print_report(m_china)

    # 对比表
    print(f"\n  ── 核心指标对比 ──")
    print(f"  {'指标':<12} {'混血配置':>14} {'纯中国配置':>14} {'差异':>10}")
    print(f"  {'─'*52}")
    print(f"  {'CAGR':<12} {m_chimerica['cagr']:>13.2%} {m_china['cagr']:>13.2%} {m_chimerica['cagr']-m_china['cagr']:>+9.2%}")
    print(f"  {'MDD':<12} {m_chimerica['mdd']:>13.2%} {m_china['mdd']:>13.2%} {m_chimerica['mdd']-m_china['mdd']:>+9.2%}")
    print(f"  {'夏普比率':<10} {m_chimerica['sharpe']:>13.2f} {m_china['sharpe']:>13.2f} {m_chimerica['sharpe']-m_china['sharpe']:>+9.2f}")
    print(f"  {'波动率':<10} {m_chimerica['vol']:>13.2%} {m_china['vol']:>13.2%} {m_chimerica['vol']-m_china['vol']:>+9.2%}")
    print(f"  {'调仓次数':<10} {m_chimerica['rebal_count']:>13d} {m_china['rebal_count']:>13d} {m_chimerica['rebal_count']-m_china['rebal_count']:>+9d}")
    print(f"  {'保护触发':<10} {m_chimerica['prot_count']:>13d} {m_china['prot_count']:>13d} {m_chimerica['prot_count']-m_china['prot_count']:>+9d}")

    # 定投对比
    print(f"\n  ── 定投场景（初始 ¥10万 + 每年 ¥5万，{m_chimerica['years']:.0f} 年）──")
    n_years = int(m_chimerica['years'])
    principal = 100000 + 50000 * n_years

    for m in [m_chimerica, m_china]:
        r = m['cagr']
        # 初始10万的终值
        init_fv = 100000 * (1 + r) ** n_years
        # 每年5万的年金终值
        annuity_fv = 50000 * (((1 + r) ** n_years - 1) / r) if r > 0 else 50000 * n_years
        total_fv = init_fv + annuity_fv
        profit = total_fv - principal
        print(f"  {m['label'][:8]}: ¥{total_fv:>12,.0f}  (本金 ¥{principal:,.0f}, 利润 ¥{profit:>10,.0f})")

    # 银行对比
    bank_r = 0.025
    bank_init = 100000 * (1 + bank_r) ** n_years
    bank_annuity = 50000 * (((1 + bank_r) ** n_years - 1) / bank_r)
    bank_total = bank_init + bank_annuity
    bank_profit = bank_total - principal
    print(f"  {'银行定期':8}: ¥{bank_total:>12,.0f}  (本金 ¥{principal:,.0f}, 利润 ¥{bank_profit:>10,.0f})")

    print("=" * 60)

    # 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})

    ax1.plot(m_chimerica["result"].index, m_chimerica["result"]["NAV"],
             color="#2c3e50", linewidth=1.2, label="Chimerica (S&P500+Bond+Gold+Cash)")
    ax1.plot(m_china["result"].index, m_china["result"]["NAV"],
             color="#c0392b", linewidth=1.2, label="China (CSI300+Bond+Gold+Cash)")
    ax1.axhline(y=100000, color="gray", linestyle="--", alpha=0.4)
    ax1.set_title("Chimerica vs Pure China Portfolio", fontsize=14)
    ax1.set_ylabel("Value (CNY)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.plot(m_chimerica["drawdown"].index, m_chimerica["drawdown"],
             color="#2c3e50", linewidth=1, label="Chimerica DD")
    ax2.plot(m_china["drawdown"].index, m_china["drawdown"],
             color="#c0392b", linewidth=1, label="China DD")
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.set_title("Drawdown Comparison", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "chimerica_vs_china.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'chimerica_vs_china.png'}")
    plt.close()


if __name__ == "__main__":
    main()
