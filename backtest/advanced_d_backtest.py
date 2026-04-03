"""
方案D：分层风控+动态阈值

基于引擎回测，增加：
1. 动态阈值逻辑（根据30日波动率调整）
2. 三档保护权重（-10%, -12%, -14%）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from engine.config import ASSETS, STATE_IDLE, STATE_PROTECTION, CORR_WINDOW
from engine.risk import RiskEngine
from engine.portfolio import PortfolioEngine


class DynamicRiskEngine(RiskEngine):
    """动态阈值风控引擎"""

    def __init__(self):
        super().__init__()
        self.volatility_window = 30

    def calculate_volatility(self, returns_series):
        """计算30日波动率（年化）"""
        if len(returns_series) < self.volatility_window:
            return 0.0
        vol = returns_series.iloc[-self.volatility_window:].std() * np.sqrt(252)
        return vol

    def get_dynamic_thresholds(self, volatility):
        """根据波动率动态调整阈值"""
        if volatility > 0.02:  # 高波动期
            return -0.10, -0.12
        else:  # 低波动期
            return -0.13, -0.15

    def evaluate_with_volatility(self, nav, corr_val, spy_30d, tlt_30d, volatility):
        """带波动率的风控评估"""
        # 更新高水位
        if nav > self.high_water_mark:
            self.high_water_mark = nav

        # 计算回撤
        drawdown = (nav - self.high_water_mark) / self.high_water_mark

        # 获取动态阈值
        risk_threshold, hard_stop = self.get_dynamic_thresholds(volatility)

        # 判断风险信号
        if drawdown <= hard_stop:
            return "HARD_STOP"
        elif drawdown <= risk_threshold:
            return "RISK"
        elif corr_val < -0.3 and spy_30d < -0.05 and tlt_30d > 0.02:
            return "RISK"
        else:
            return "SAFE"


class LayeredPortfolioEngine(PortfolioEngine):
    """分层保护权重组合引擎"""

    def __init__(self, initial_capital=100000.0):
        super().__init__(initial_capital)
        # 三档保护权重
        self.protection_weights_tier1 = np.array([0.15, 0.25, 0.25, 0.35])  # -10%
        self.protection_weights_tier2 = np.array([0.10, 0.20, 0.20, 0.50])  # -12%
        self.protection_weights_tier3 = np.array([0.03, 0.07, 0.15, 0.75])  # -14%

    def get_protection_weights(self, drawdown):
        """根据回撤程度选择保护权重"""
        if drawdown <= -0.14:
            return self.protection_weights_tier3
        elif drawdown <= -0.12:
            return self.protection_weights_tier2
        else:
            return self.protection_weights_tier1

    def step(self, current_date, daily_returns, risk_signal, is_year_end, drawdown=0.0):
        """执行一步，增加回撤参数用于选择保护权重"""
        # 先应用收益
        self.positions = self.positions * (1 + daily_returns)
        self.nav = float(np.sum(self.positions))

        # 状态机逻辑
        if self.state == STATE_IDLE:
            if risk_signal in ["RISK", "HARD_STOP"]:
                # 进入保护模式，根据回撤选择权重
                protection_weights = self.get_protection_weights(drawdown)
                self._rebalance(protection_weights, current_date, f"触发保护模式 (回撤:{drawdown:.2%})")
                self.state = STATE_PROTECTION
                self.protection_count += 1
            elif is_year_end:
                self._rebalance(self.normal_weights, current_date, "年末调仓")

        elif self.state == STATE_PROTECTION:
            if risk_signal == "SAFE":
                # 恢复正常
                self._rebalance(self.normal_weights, current_date, "风险解除，恢复正常")
                self.state = STATE_IDLE
            elif is_year_end:
                # 保护模式下也可能需要根据回撤调整权重
                protection_weights = self.get_protection_weights(drawdown)
                self._rebalance(protection_weights, current_date, f"年末调仓 (保护模式, 回撤:{drawdown:.2%})")


def run_advanced_d_backtest(file_path="data/etf_daily.csv"):
    """运行方案D回测：分层风控+动态阈值"""
    # 1. 加载数据
    print("加载数据...")
    df = pd.read_csv(file_path, index_col="date", parse_dates=True).sort_index()
    returns = df[ASSETS].pct_change().fillna(0)

    # 风控前置计算
    spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"]).fillna(0)
    spy_30d_ret = df["SPY"].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = df["TLT"].pct_change(CORR_WINDOW).fillna(0)

    # 计算SPY的30日波动率
    spy_returns = returns["SPY"]
    spy_volatility = spy_returns.rolling(window=30).std() * np.sqrt(252)
    spy_volatility = spy_volatility.fillna(0)

    dates = returns.index

    # 2. 初始化引擎
    initial_capital = 100000.0
    portfolio = LayeredPortfolioEngine(initial_capital=initial_capital)
    risk = DynamicRiskEngine()
    risk.high_water_mark = initial_capital

    nav_history = []
    state_history = []
    volatility_history = []

    # 3. 逐日驱动
    print(f"开始回测: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} 个交易日)")
    for i in range(len(dates)):
        daily_ret = returns.iloc[i].values
        corr_val = spy_tlt_corr.iloc[i]
        spy_30d = spy_30d_ret.iloc[i]
        tlt_30d = tlt_30d_ret.iloc[i]
        volatility = spy_volatility.iloc[i]

        # 模拟资产生长以获取准确 NAV 供风控评估
        simulated_nav = float(np.sum(portfolio.positions * (1 + daily_ret)))

        # 计算当前回撤
        if simulated_nav > risk.high_water_mark:
            current_drawdown = 0.0
        else:
            current_drawdown = (simulated_nav - risk.high_water_mark) / risk.high_water_mark

        # 使用动态阈值评估风险
        risk_signal = risk.evaluate_with_volatility(simulated_nav, corr_val, spy_30d, tlt_30d, volatility)

        # 年末判断
        is_year_end = (i < len(dates) - 1 and dates[i].year != dates[i + 1].year)

        # 状态机执行一步（传入回撤用于选择保护权重）
        portfolio.step(
            current_date=dates[i].date(),
            daily_returns=daily_ret,
            risk_signal=risk_signal,
            is_year_end=is_year_end,
            drawdown=current_drawdown,
        )

        nav_history.append(portfolio.nav)
        state_history.append(1 if portfolio.state == STATE_PROTECTION else 0)
        volatility_history.append(volatility)

    # 4. 计算指标
    result = pd.DataFrame({
        "NAV": nav_history,
        "State": state_history,
        "Volatility": volatility_history
    }, index=dates)

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
    print("  方案D：分层风控+动态阈值  2005-2025")
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
        print("\n  全部通过！方案D验证成功。")
    else:
        print("\n  存在未通过项，需要进一步调优。")
    print("=" * 60)

    # 6. 找出所有保护模式触发的时间点
    print("\n  ── 保护模式触发记录 ──")
    actions = [s for s in portfolio.snapshots if s.action and "保护" in s.action or s.action and "避险" in s.action]
    for a in actions:
        print(f"    {a.date}  {a.action}  回撤:{a.drawdown:.2%}")

    # 7. 绘图
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(4, 1, figsize=(16, 14), gridspec_kw={"height_ratios": [3, 1, 1, 1]})

    # NAV + 保护区间
    ax1 = axes[0]
    ax1.plot(result.index, result["NAV"], color="#2c3e50", linewidth=1.2, label="方案D NAV")
    ax1.fill_between(
        result.index,
        result["NAV"].min() * 0.95, result["NAV"].max() * 1.02,
        where=(result["State"] == 1),
        color="#f1c40f", alpha=0.3, label="PROTECTION",
    )
    ax1.axhline(y=initial_capital, color="gray", linestyle="--", alpha=0.4)
    ax1.set_title("方案D：分层风控+动态阈值 (2005-2025)", fontsize=14)
    ax1.set_ylabel("Value ($)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 回撤
    ax2 = axes[1]
    ax2.fill_between(drawdown.index, drawdown, 0, color="#e74c3c", alpha=0.6)
    ax2.axhline(y=-0.15, color="black", linestyle="--", alpha=0.8, label="-15% Limit")
    ax2.axhline(y=-0.10, color="orange", linestyle="--", alpha=0.6, label="-10% Tier1")
    ax2.axhline(y=-0.12, color="red", linestyle="--", alpha=0.6, label="-12% Tier2")
    ax2.axhline(y=-0.14, color="darkred", linestyle="--", alpha=0.6, label="-14% Tier3")
    ax2.set_title("Drawdown", fontsize=12)
    ax2.set_ylabel("Drawdown")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 波动率
    ax3 = axes[2]
    ax3.plot(result.index, result["Volatility"], color="#9b59b6", linewidth=1, label="30日波动率")
    ax3.axhline(y=0.02, color="red", linestyle="--", alpha=0.6, label="2% 阈值")
    ax3.set_title("SPY 30日波动率（年化）", fontsize=12)
    ax3.set_ylabel("Volatility")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # 各资产权重变化
    ax4 = axes[3]
    weight_history = pd.DataFrame(
        [s.weights for s in portfolio.snapshots],
        index=[s.date for s in portfolio.snapshots],
        columns=ASSETS,
    )
    # 每 20 天采样一次避免太密
    weight_sampled = weight_history.iloc[::20]
    ax4.stackplot(
        range(len(weight_sampled)),
        [weight_sampled[a] for a in ASSETS],
        labels=ASSETS,
        alpha=0.8,
        colors=["#3498db", "#2ecc71", "#f39c12", "#95a5a6"],
    )
    # x 轴标签
    tick_positions = list(range(0, len(weight_sampled), len(weight_sampled) // 10))
    tick_labels = [weight_sampled.index[i][:7] for i in tick_positions]
    ax4.set_xticks(tick_positions)
    ax4.set_xticklabels(tick_labels, rotation=45)
    ax4.set_title("Asset Weight Allocation Over Time", fontsize=12)
    ax4.set_ylabel("Weight")
    ax4.legend(loc="upper right")
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_dir / "advanced_d_backtest.png", dpi=150, bbox_inches="tight")
    print(f"\n  图表已保存: {output_dir / 'advanced_d_backtest.png'}")
    plt.close()

    return result


if __name__ == "__main__":
    run_advanced_d_backtest()

