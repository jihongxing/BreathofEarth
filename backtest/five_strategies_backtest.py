"""
息壤（Xi-Rang）五方案对比回测

对比五个策略方案：
- 方案0（基准）：当前方案（双层风控 + 等权）
- 方案1：动态风险平价权重（Risk Parity）
- 方案2：200日均线趋势过滤
- 方案3：提升股票权重至30%
- 方案4：相关性崩溃时黄金加仓

使用真实交易摩擦成本，在中美两个市场分别回测。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import json

from engine.config import STATE_IDLE, STATE_PROTECTION, CORR_WINDOW, FEE_RATE
from engine.risk import RiskEngine, RiskSignal
from engine.portfolio import PortfolioEngine, RebalanceOrder


@dataclass
class StrategyConfig:
    """策略配置"""
    name: str
    description: str
    use_risk_parity: bool = False
    use_trend_filter: bool = False
    use_higher_stock: bool = False
    use_gold_boost: bool = False
    
    # 自定义权重（如果不为None则覆盖默认）
    custom_idle_weights: Optional[list] = None
    custom_protect_weights: Optional[list] = None
    custom_emergency_weights: Optional[list] = None


class StrategyPortfolioEngine(PortfolioEngine):
    """扩展的组合引擎，支持多种策略"""
    
    def __init__(self, initial_capital: float, config: StrategyConfig, prices_df: pd.DataFrame):
        super().__init__(initial_capital)
        self.config = config
        self.prices_df = prices_df
        
        # 如果有自定义权重，覆盖默认值
        if config.custom_idle_weights:
            self.weights_idle = np.array(config.custom_idle_weights)
        else:
            self.weights_idle = np.array([0.25, 0.25, 0.25, 0.25])
            
        if config.custom_protect_weights:
            self.weights_protect = np.array(config.custom_protect_weights)
        else:
            self.weights_protect = np.array([0.10, 0.20, 0.20, 0.50])
            
        if config.custom_emergency_weights:
            self.weights_emergency = np.array(config.custom_emergency_weights)
        else:
            self.weights_emergency = np.array([0.03, 0.07, 0.15, 0.75])
    
    def _get_dynamic_idle_weights(self, current_date) -> np.ndarray:
        """计算动态权重（Risk Parity）"""
        if not self.config.use_risk_parity:
            return self.weights_idle

        # 转换日期类型
        if isinstance(current_date, pd.Timestamp):
            search_date = current_date
        else:
            search_date = pd.Timestamp(current_date)

        # 获取过去60天的数据
        idx = self.prices_df.index.get_loc(search_date)
        if idx < 60:
            return self.weights_idle
        
        window_prices = self.prices_df.iloc[idx-60:idx]
        returns = window_prices.pct_change().dropna()
        
        # 计算波动率
        vols = returns.std().values
        
        # 避免除零
        vols = np.where(vols == 0, 1e-6, vols)
        
        # 权重 ∝ 1/波动率
        inv_vols = 1 / vols
        
        # SHV 上限30%（避免过度保守）
        raw_weights = inv_vols / inv_vols.sum()
        
        if raw_weights[3] > 0.30:
            shv_weight = 0.30
            other_inv_vols = inv_vols[:3]
            other_weights = (other_inv_vols / other_inv_vols.sum()) * 0.70
            weights = np.append(other_weights, shv_weight)
        else:
            weights = raw_weights
        
        return weights
    
    def _check_trend_filter(self, current_date) -> bool:
        """检查是否触发趋势过滤（SPY < 200日均线）"""
        if not self.config.use_trend_filter:
            return False

        # 转换日期类型
        if isinstance(current_date, pd.Timestamp):
            search_date = current_date
        else:
            search_date = pd.Timestamp(current_date)

        idx = self.prices_df.index.get_loc(search_date)
        if idx < 200:
            return False
        
        spy_prices = self.prices_df.iloc[:idx+1, 0]  # SPY是第一列
        ma200 = spy_prices.rolling(200).mean().iloc[-1]
        current_price = spy_prices.iloc[-1]
        
        return current_price < ma200
    
    def step(
        self,
        current_date,
        daily_returns: np.ndarray,
        risk_signal: RiskSignal,
        is_year_end: bool = False,
    ) -> Optional[RebalanceOrder]:
        """
        执行一步：资产生长 → 风控判断 → 状态转移 → 再平衡。
        重写父类方法以支持策略扩展。
        """
        # 1. 资产自然生长
        self.positions = self.positions * (1 + daily_returns)
        self.nav = float(np.sum(self.positions))

        order = None
        action = None

        # 2. 状态机逻辑
        if self.state == STATE_IDLE:
            order = self._handle_idle_extended(risk_signal, is_year_end, current_date)
        elif self.state == STATE_PROTECTION:
            order = self._handle_protection(risk_signal)

        # 3. 执行再平衡指令
        if order is not None:
            self._execute_rebalance(order)
            action = order.reason

        # 4. 记录快照
        from engine.portfolio import PortfolioSnapshot
        self.snapshots.append(PortfolioSnapshot(
            date=str(current_date),
            state=self.state,
            nav=self.nav,
            positions=self.positions.tolist(),
            weights=self.weights.tolist(),
            drawdown=risk_signal.current_dd,
            action=action,
            trigger_reason=risk_signal.trigger_reason,
        ))

        return order

    def _handle_idle_extended(self, signal: RiskSignal, is_year_end: bool, current_date) -> Optional[RebalanceOrder]:
        """IDLE 状态下的逻辑（扩展版）"""

        # 硬止损：最高优先级
        if signal.is_hard_stop:
            self.state = STATE_PROTECTION
            self.cooldown_counter = 40  # COOLDOWN_DAYS * 2
            self.protection_count += 1
            return self._make_order(self.weights_emergency.tolist(), "紧急避险: 硬止损触发")

        # 相关性崩溃 - 方案4：黄金加仓
        if signal.is_corr_breakdown and self.config.use_gold_boost:
            self.state = STATE_PROTECTION
            self.cooldown_counter = 20
            self.protection_count += 1
            # 黄金从20%提到30%
            gold_boost_weights = np.array([0.10, 0.15, 0.30, 0.45])
            return self._make_order(gold_boost_weights.tolist(), "相关性崩溃: 黄金加仓")

        # 常规保护
        if signal.is_protection:
            self.state = STATE_PROTECTION
            self.cooldown_counter = 20
            self.protection_count += 1
            return self._make_order(self.weights_protect.tolist(), "常规保护: 风控触发")

        # 获取目标权重（可能是动态的）
        target_weights = self._get_dynamic_idle_weights(current_date)

        # 方案2：趋势过滤
        if self.config.use_trend_filter and self._check_trend_filter(current_date):
            # SPY < 200MA，降低股票权重
            target_weights = np.array([0.15, 0.25, 0.25, 0.35])

        # 日常再平衡（阈值或年末）
        max_drift = float(np.max(np.abs(self.weights - target_weights)))
        if max_drift > 0.05:  # DRIFT_THRESHOLD
            return self._make_order(target_weights.tolist(), f"阈值再平衡: 偏离 {max_drift:.2%}")
        if is_year_end:
            return self._make_order(target_weights.tolist(), "年末强制再平衡")

        return None
    
    def _handle_protection(self, signal: RiskSignal) -> Optional[RebalanceOrder]:
        """PROTECTION 状态下的逻辑"""

        # 回撤继续恶化，升级到紧急避险
        if signal.is_hard_stop:
            current_shv_weight = self.weights[3] if len(self.weights) > 3 else 0
            if current_shv_weight < self.weights_emergency[3] - 0.05:
                self.cooldown_counter = 40
                return self._make_order(self.weights_emergency.tolist(), "升级紧急避险: 回撤继续恶化")

        # 冷却期倒计时
        if self.cooldown_counter > 0:
            self.cooldown_counter -= 1

        # 解除条件：风控恢复 + 冷却期满
        if not signal.is_protection and self.cooldown_counter == 0:
            self.state = STATE_IDLE
            return self._make_order(self.weights_idle.tolist(), "解除保护: 风控恢复正常")

        return None

    def _make_order(self, target_weights: list, reason: str) -> RebalanceOrder:
        """生成再平衡指令"""
        target = np.array(target_weights)
        turnover = float(np.sum(np.abs(self.weights - target)) / 2)
        friction_cost = self.nav * turnover * FEE_RATE
        return RebalanceOrder(
            target_weights=target_weights,
            turnover=turnover,
            friction_cost=friction_cost,
            reason=reason,
        )

    def _execute_rebalance(self, order: RebalanceOrder):
        """执行再平衡：扣除摩擦成本，重置持仓"""
        self.nav -= order.friction_cost
        self.positions = np.array(order.target_weights) * self.nav
        self.rebalance_count += 1



def run_single_strategy(
    strategy_config: StrategyConfig,
    prices_df: pd.DataFrame,
    assets: list,
    initial_capital: float = 100000.0
) -> dict:
    """运行单个策略的回测"""
    
    returns = prices_df[assets].pct_change().fillna(0)
    
    # 风控前置计算
    spy_tlt_corr = returns.iloc[:, 0].rolling(window=CORR_WINDOW).corr(returns.iloc[:, 1]).fillna(0)
    spy_30d_ret = prices_df.iloc[:, 0].pct_change(CORR_WINDOW).fillna(0)
    tlt_30d_ret = prices_df.iloc[:, 1].pct_change(CORR_WINDOW).fillna(0)
    
    dates = returns.index
    
    # 初始化引擎
    portfolio = StrategyPortfolioEngine(initial_capital, strategy_config, prices_df)
    risk = RiskEngine()
    risk.high_water_mark = initial_capital
    
    nav_history = []
    state_history = []
    
    # 逐日驱动
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
    
    # 计算指标
    result_df = pd.DataFrame({"NAV": nav_history, "State": state_history}, index=dates)
    
    total_return = result_df["NAV"].iloc[-1] / initial_capital - 1
    years = (dates[-1] - dates[0]).days / 365.25
    cagr = (result_df["NAV"].iloc[-1] / initial_capital) ** (1 / years) - 1
    
    running_max = result_df["NAV"].cummax()
    drawdown = (result_df["NAV"] - running_max) / running_max
    max_drawdown = drawdown.min()
    max_dd_date = drawdown.idxmin()
    
    daily_vol = result_df["NAV"].pct_change().std()
    annual_vol = daily_vol * np.sqrt(252)
    sharpe = (cagr - 0.02) / annual_vol if annual_vol > 0 else 0
    
    protection_days = sum(state_history)
    
    return {
        "name": strategy_config.name,
        "description": strategy_config.description,
        "initial_capital": initial_capital,
        "final_nav": result_df["NAV"].iloc[-1],
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "max_dd_date": str(max_dd_date.date()),
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "rebalance_count": portfolio.rebalance_count,
        "protection_count": portfolio.protection_count,
        "protection_days": protection_days,
        "protection_pct": protection_days / len(dates) * 100,
        "years": years,
        "result_df": result_df,
        "portfolio": portfolio,
    }


def run_five_strategies_comparison(
    prices_file: str,
    assets: list,
    market_name: str,
    currency: str = "$"
) -> dict:
    """运行五个策略的对比回测"""
    
    print(f"\n{'='*70}")
    print(f"  {market_name} 五方案对比回测")
    print(f"{'='*70}")
    
    # 加载数据
    prices_df = pd.read_csv(prices_file, index_col="date", parse_dates=True).sort_index()
    print(f"  数据范围: {prices_df.index[0].date()} ~ {prices_df.index[-1].date()}")
    print(f"  交易日数: {len(prices_df)}")
    
    # 定义五个策略
    strategies = [
        StrategyConfig(
            name="方案0-基准",
            description="当前方案（双层风控 + 等权）",
        ),
        StrategyConfig(
            name="方案1-风险平价",
            description="动态风险平价权重（Risk Parity）",
            use_risk_parity=True,
        ),
        StrategyConfig(
            name="方案2-趋势过滤",
            description="200日均线趋势过滤",
            use_trend_filter=True,
        ),
        StrategyConfig(
            name="方案3-高股票",
            description="提升股票权重至30%",
            custom_idle_weights=[0.30, 0.23, 0.23, 0.24],
        ),
        StrategyConfig(
            name="方案4-黄金加仓",
            description="相关性崩溃时黄金加仓",
            use_gold_boost=True,
        ),
    ]
    
    # 运行所有策略
    results = []
    for i, strategy in enumerate(strategies):
        print(f"\n  [{i+1}/5] 运行 {strategy.name}...")
        result = run_single_strategy(strategy, prices_df, assets)
        results.append(result)
        print(f"    CAGR: {result['cagr']:.2%}, MDD: {result['max_drawdown']:.2%}, 夏普: {result['sharpe']:.2f}")
    
    return {
        "market_name": market_name,
        "currency": currency,
        "results": results,
        "prices_df": prices_df,
    }


def print_comparison_table(comparison: dict):
    """打印对比表格"""
    results = comparison["results"]
    currency = comparison["currency"]
    market_name = comparison["market_name"]
    
    print(f"\n{'='*100}")
    print(f"  {market_name} 五方案对比结果")
    print(f"{'='*100}")
    
    # 表头
    print(f"{'策略':<20} {'CAGR':>8} {'MDD':>8} {'夏普':>6} {'调仓':>6} {'保护':>6} {'保护天数':>10} {'最终NAV':>12}")
    print("-" * 100)
    
    # 数据行
    for r in results:
        print(f"{r['name']:<20} "
              f"{r['cagr']:>7.2%} "
              f"{r['max_drawdown']:>7.2%} "
              f"{r['sharpe']:>6.2f} "
              f"{r['rebalance_count']:>6} "
              f"{r['protection_count']:>6} "
              f"{r['protection_days']:>5}天({r['protection_pct']:>4.1f}%) "
              f"{currency}{r['final_nav']:>10,.0f}")
    
    print("-" * 100)
    
    # 找出最优方案
    best_cagr = max(results, key=lambda x: x['cagr'])
    best_mdd = max(results, key=lambda x: x['max_drawdown'])  # MDD越接近0越好
    best_sharpe = max(results, key=lambda x: x['sharpe'])
    
    print(f"\n  最优指标:")
    print(f"    最高CAGR: {best_cagr['name']} ({best_cagr['cagr']:.2%})")
    print(f"    最小MDD:  {best_mdd['name']} ({best_mdd['max_drawdown']:.2%})")
    print(f"    最高夏普: {best_sharpe['name']} ({best_sharpe['sharpe']:.2f})")
    
    # 成功标准判定
    print(f"\n  成功标准判定 (CAGR ≥ 通胀+2%, MDD ≤ -15%, 夏普 > 0.5):")
    for r in results:
        cagr_pass = "✓" if r['cagr'] >= 0.045 else "✗"  # 假设通胀2.5%
        mdd_pass = "✓" if r['max_drawdown'] >= -0.15 else "✗"
        sharpe_pass = "✓" if r['sharpe'] > 0.5 else "✗"
        all_pass = "✓ 全通过" if (cagr_pass == "✓" and mdd_pass == "✓" and sharpe_pass == "✓") else "✗ 未全通过"
        print(f"    {r['name']:<20} CAGR:{cagr_pass} MDD:{mdd_pass} 夏普:{sharpe_pass}  {all_pass}")
    
    print(f"{'='*100}")


def plot_comparison(comparison: dict, output_dir: Path):
    """绘制对比图表"""
    results = comparison["results"]
    market_name = comparison["market_name"]
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    
    # 1. NAV 曲线对比
    ax1 = axes[0, 0]
    colors = ['#2c3e50', '#e74c3c', '#3498db', '#2ecc71', '#f39c12']
    for i, r in enumerate(results):
        ax1.plot(r['result_df'].index, r['result_df']['NAV'], 
                label=r['name'], color=colors[i], linewidth=1.5, alpha=0.8)
    ax1.set_title(f"{market_name} - NAV 曲线对比", fontsize=14, fontweight='bold')
    ax1.set_ylabel("NAV ($)")
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # 2. 回撤对比
    ax2 = axes[0, 1]
    for i, r in enumerate(results):
        nav = r['result_df']['NAV']
        running_max = nav.cummax()
        drawdown = (nav - running_max) / running_max
        ax2.plot(drawdown.index, drawdown, label=r['name'], 
                color=colors[i], linewidth=1.5, alpha=0.8)
    ax2.axhline(y=-0.15, color='red', linestyle='--', alpha=0.6, label='-15% 红线')
    ax2.set_title(f"{market_name} - 回撤对比", fontsize=14, fontweight='bold')
    ax2.set_ylabel("Drawdown")
    ax2.legend(loc='lower left')
    ax2.grid(True, alpha=0.3)
    
    # 3. 关键指标柱状图
    ax3 = axes[1, 0]
    x = np.arange(len(results))
    width = 0.25
    
    cagrs = [r['cagr'] * 100 for r in results]
    mdds = [abs(r['max_drawdown']) * 100 for r in results]
    sharpes = [r['sharpe'] * 10 for r in results]  # 放大10倍以便显示
    
    ax3.bar(x - width, cagrs, width, label='CAGR (%)', color='#2ecc71', alpha=0.8)
    ax3.bar(x, mdds, width, label='|MDD| (%)', color='#e74c3c', alpha=0.8)
    ax3.bar(x + width, sharpes, width, label='夏普×10', color='#3498db', alpha=0.8)
    
    ax3.set_title(f"{market_name} - 关键指标对比", fontsize=14, fontweight='bold')
    ax3.set_xticks(x)
    ax3.set_xticklabels([r['name'] for r in results], rotation=15, ha='right')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    # 4. 调仓次数和保护天数
    ax4 = axes[1, 1]
    rebalances = [r['rebalance_count'] for r in results]
    protections = [r['protection_days'] for r in results]
    
    ax4.bar(x - width/2, rebalances, width, label='调仓次数', color='#9b59b6', alpha=0.8)
    ax4.bar(x + width/2, protections, width, label='保护天数', color='#f39c12', alpha=0.8)
    
    ax4.set_title(f"{market_name} - 操作频率对比", fontsize=14, fontweight='bold')
    ax4.set_xticks(x)
    ax4.set_xticklabels([r['name'] for r in results], rotation=15, ha='right')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # 保存
    output_file = output_dir / f"five_strategies_{market_name.replace(' ', '_')}.png"
    fig.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\n  图表已保存: {output_file}")
    plt.close()


def save_results_json(comparison: dict, output_dir: Path):
    """保存结果为JSON"""
    market_name = comparison["market_name"]
    
    # 准备可序列化的数据
    export_data = {
        "market_name": market_name,
        "currency": comparison["currency"],
        "strategies": []
    }
    
    for r in comparison["results"]:
        export_data["strategies"].append({
            "name": r["name"],
            "description": r["description"],
            "cagr": round(r["cagr"], 4),
            "max_drawdown": round(r["max_drawdown"], 4),
            "sharpe": round(r["sharpe"], 2),
            "annual_vol": round(r["annual_vol"], 4),
            "rebalance_count": r["rebalance_count"],
            "protection_count": r["protection_count"],
            "protection_days": r["protection_days"],
            "protection_pct": round(r["protection_pct"], 2),
            "final_nav": round(r["final_nav"], 2),
            "total_return": round(r["total_return"], 4),
            "years": round(r["years"], 2),
        })
    
    output_file = output_dir / f"five_strategies_{market_name.replace(' ', '_')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    print(f"  结果已保存: {output_file}")


def validate_local_data_or_raise(markets: list[tuple], manifest_path: Path):
    """强制本地模式：检查市场文件和 manifest 完整性。"""
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"未找到 manifest: {manifest_path}。请先运行 python -m backtest.prepare_all_markets"
        )

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    policy = manifest.get("policy", {})
    if policy.get("backtest_mode") != "local_only":
        raise RuntimeError("manifest.backtest_mode 不是 local_only，拒绝运行回测")

    for prices_file, assets, market_name, _currency in markets:
        p = Path(prices_file)
        if not p.exists():
            raise FileNotFoundError(f"{market_name} 缺少本地数据文件: {prices_file}")

        df = pd.read_csv(p, index_col="date", parse_dates=True)
        missing = [a for a in assets if a not in df.columns]
        if missing:
            raise RuntimeError(f"{market_name} 缺少列: {missing} in {prices_file}")


def build_summary_csv(all_results: dict, output_dir: Path):
    """汇总六市场×五策略结果，输出标准化 CSV。"""
    rows = []
    for market_name, comp in all_results.items():
        currency = comp["currency"]
        for r in comp["results"]:
            rows.append({
                "market": market_name,
                "strategy": r["name"],
                "description": r["description"],
                "currency": currency,
                "years": round(r["years"], 2),
                "final_nav": round(r["final_nav"], 2),
                "total_return": round(r["total_return"], 6),
                "cagr": round(r["cagr"], 6),
                "max_drawdown": round(r["max_drawdown"], 6),
                "sharpe": round(r["sharpe"], 6),
                "annual_vol": round(r["annual_vol"], 6),
                "rebalance_count": r["rebalance_count"],
                "protection_count": r["protection_count"],
                "protection_days": r["protection_days"],
                "protection_pct": round(r["protection_pct"], 4),
                "passes_criteria": (
                    r["cagr"] >= 0.045 and r["max_drawdown"] >= -0.15 and r["sharpe"] > 0.5
                ),
            })

    summary = pd.DataFrame(rows)
    summary.sort_values(["market", "strategy"], inplace=True)
    out = output_dir / "phase1_five_strategies_six_markets_summary.csv"
    summary.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n  汇总CSV已保存: {out}")


def main():
    """主函数（严格本地数据模式）"""
    output_dir = Path("backtest/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    all_markets = [
        ("data/market_us.csv",        ["SPY","TLT","GLD","SHV"],                          "美股 (2005-2025)",     "$"),
        ("data/market_cn.csv",        ["510300.SS","511010.SS","518880.SS","MONEY"],       "中国 (2012-2025)",     "¥"),
        ("data/market_chimerica.csv", ["513500.SS","511010.SS","518880.SS","MONEY"],       "中美混血 (2013-2025)", "¥"),
        ("data/market_eu.csv",        ["EZU","BWX","GLD","SHV"],                          "欧洲 (2005-2025)",     "$"),
        ("data/market_india.csv",     ["EPI","EMB","GLD","SHV"],                          "印度 (2008-2025)",     "$"),
        ("data/market_global.csv",    ["VT","BWX","GLD","SHV"],                           "全球 (2008-2025)",     "$"),
    ]

    print("\n[LOCAL-ONLY] 校验本地数据和manifest...")
    validate_local_data_or_raise(all_markets, Path("data/data_manifest.json"))
    print("[LOCAL-ONLY] 校验通过，开始回测（不会请求外部接口）")

    all_results = {}
    for prices_file, assets, market_name, currency in all_markets:
        comparison = run_five_strategies_comparison(
            prices_file=prices_file,
            assets=assets,
            market_name=market_name,
            currency=currency,
        )
        print_comparison_table(comparison)
        plot_comparison(comparison, output_dir)
        save_results_json(comparison, output_dir)
        all_results[market_name] = comparison

    if all_results:
        print("\n" + "=" * 110)
        print("  六大市场 × 五方案 汇总矩阵")
        print("=" * 110)
        strategy_names = ["方案0-基准", "方案1-风险平价", "方案2-趋势过滤", "方案3-高股票", "方案4-黄金加仓"]

        print(f"\n  ── CAGR 矩阵 ──")
        header = f"  {'市场':<16}" + "".join(f"{s:>14}" for s in strategy_names)
        print(header)
        print("  " + "-" * (16 + 14 * 5))
        for mkt_name, comp in all_results.items():
            row = f"  {mkt_name:<16}"
            for r in comp["results"]:
                row += f"{r['cagr']:>13.2%}"
            print(row)

        print(f"\n  ── MDD 矩阵 ──")
        print(header)
        print("  " + "-" * (16 + 14 * 5))
        for mkt_name, comp in all_results.items():
            row = f"  {mkt_name:<16}"
            for r in comp["results"]:
                row += f"{r['max_drawdown']:>13.2%}"
            print(row)

        print(f"\n  ── 夏普比率矩阵 ──")
        print(header)
        print("  " + "-" * (16 + 14 * 5))
        for mkt_name, comp in all_results.items():
            row = f"  {mkt_name:<16}"
            for r in comp["results"]:
                row += f"{r['sharpe']:>13.2f}"
            print(row)

        print(f"\n  ── 成功标准矩阵 (✓=全通过) ──")
        print(header)
        print("  " + "-" * (16 + 14 * 5))
        for mkt_name, comp in all_results.items():
            row = f"  {mkt_name:<16}"
            for r in comp["results"]:
                ok = r['cagr'] >= 0.045 and r['max_drawdown'] >= -0.15 and r['sharpe'] > 0.5
                row += f"{'✓':>13}" if ok else f"{'✗':>13}"
            print(row)

        print("\n" + "=" * 110)

    build_summary_csv(all_results, output_dir)
    print("\n  五方案 × 六市场对比回测完成！")


if __name__ == "__main__":
    main()
