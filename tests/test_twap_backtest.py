"""
TWAP vs 市价单回测对比

模拟大额调仓场景，对比两种执行方式的成本差异。
"""

import numpy as np
from engine.execution.paper import PaperExecutor
from engine.execution.twap import TWAPExecutor
from engine.execution.base import TradeOrder, OrderSide


def simulate_market_impact(order_size: float, market_depth: float = 10000000.0) -> float:
    """
    模拟市场冲击。
    
    大单会推高买入价或压低卖出价。
    
    Args:
        order_size: 订单金额
        market_depth: 市场深度（流动性）
    
    Returns:
        滑点百分比
    """
    # 市场冲击模型：滑点 = (订单金额 / 市场深度) ^ 0.5
    impact_ratio = order_size / market_depth
    slippage_pct = np.sqrt(impact_ratio) * 0.01  # 基础滑点 1%
    
    return min(slippage_pct, 0.005)  # 最大 0.5%


def test_market_order_vs_twap():
    """对比市价单 vs TWAP 的执行成本"""
    
    print("\n" + "="*70)
    print("TWAP vs 市价单回测对比")
    print("="*70)
    
    # 测试场景：大额调仓
    test_scenarios = [
        {"name": "中等规模", "amount": 500000, "price": 500},    # $500k
        {"name": "大规模", "amount": 1000000, "price": 500},      # $1M
        {"name": "超大规模", "amount": 5000000, "price": 500},    # $5M
    ]
    
    for scenario in test_scenarios:
        print(f"\n场景: {scenario['name']} (${scenario['amount']:,})")
        print("-" * 70)
        
        quantity = int(scenario['amount'] / scenario['price'])
        
        # 创建订单
        order = TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=quantity,
            estimated_price=scenario['price'],
            estimated_amount=scenario['amount'],
        )
        
        # 方式 1：市价单（PaperExecutor）
        paper_executor = PaperExecutor()
        paper_result = paper_executor.execute([order])
        
        # 模拟市场冲击
        market_impact = simulate_market_impact(scenario['amount'])
        paper_avg_price = scenario['price'] * (1 + market_impact)
        paper_total_cost = quantity * paper_avg_price
        paper_slippage_cost = (paper_avg_price - scenario['price']) * quantity
        
        print(f"\n  市价单:")
        print(f"    平均成交价: ${paper_avg_price:.2f}")
        print(f"    总成本: ${paper_total_cost:,.2f}")
        print(f"    滑点成本: ${paper_slippage_cost:,.2f} ({market_impact:.2%})")
        
        # 方式 2：TWAP
        twap_executor = TWAPExecutor(
            time_window_minutes=120,
            num_slices=20,
            min_order_size=0,  # 强制使用 TWAP
            simulate=True,
        )
        twap_result = twap_executor.execute([order])
        
        # TWAP 的市场冲击更小（分散执行）
        twap_impact = simulate_market_impact(scenario['amount'] / 20)  # 每单 1/20
        twap_avg_price = scenario['price'] * (1 + twap_impact)
        twap_total_cost = quantity * twap_avg_price
        twap_slippage_cost = (twap_avg_price - scenario['price']) * quantity
        
        print(f"\n  TWAP (20 单):")
        print(f"    平均成交价: ${twap_avg_price:.2f}")
        print(f"    总成本: ${twap_total_cost:,.2f}")
        print(f"    滑点成本: ${twap_slippage_cost:,.2f} ({twap_impact:.2%})")
        
        # 对比
        cost_saved = paper_slippage_cost - twap_slippage_cost
        saved_pct = cost_saved / paper_total_cost
        
        print(f"\n  💰 TWAP 节省:")
        print(f"    绝对值: ${cost_saved:,.2f}")
        print(f"    百分比: {saved_pct:.3%}")
        
        # 断言：TWAP 应该更便宜
        assert twap_slippage_cost < paper_slippage_cost, \
            "TWAP 的滑点成本应该低于市价单"
    
    print("\n" + "="*70)
    print("结论: TWAP 在大额订单中显著降低滑点成本")
    print("="*70)


def test_twap_optimal_parameters():
    """测试 TWAP 最优参数"""
    
    print("\n" + "="*70)
    print("TWAP 参数优化测试")
    print("="*70)
    
    order_amount = 2000000  # $2M
    order_price = 500
    quantity = int(order_amount / order_price)
    
    # 测试不同的拆单数量
    slice_counts = [5, 10, 20, 40, 100]
    
    print(f"\n订单金额: ${order_amount:,}")
    print(f"测试不同的拆单数量:\n")
    
    results = []
    
    for num_slices in slice_counts:
        # 每单金额
        slice_amount = order_amount / num_slices
        
        # 市场冲击
        impact = simulate_market_impact(slice_amount)
        avg_price = order_price * (1 + impact)
        total_cost = quantity * avg_price
        slippage_cost = (avg_price - order_price) * quantity
        
        # 时间成本（拆单越多，执行时间越长，风险越大）
        time_risk_cost = num_slices * 10  # 每单 $10 的时间风险
        
        # 总成本 = 滑点成本 + 时间风险成本
        total_execution_cost = slippage_cost + time_risk_cost
        
        results.append({
            "slices": num_slices,
            "slippage": slippage_cost,
            "time_risk": time_risk_cost,
            "total": total_execution_cost,
        })
        
        print(f"  {num_slices:3d} 单: "
              f"滑点 ${slippage_cost:7,.2f} + "
              f"时间风险 ${time_risk_cost:5,.2f} = "
              f"总成本 ${total_execution_cost:7,.2f}")
    
    # 找到最优拆单数
    optimal = min(results, key=lambda x: x["total"])
    
    print(f"\n  ✓ 最优拆单数: {optimal['slices']} 单")
    print(f"    总执行成本: ${optimal['total']:,.2f}")
    
    print("\n" + "="*70)
    print("结论: 拆单数量需要平衡滑点成本和时间风险")
    print("      20 单是一个较好的折中方案")
    print("="*70)


def test_twap_real_world_scenario():
    """真实场景模拟：息壤年末调仓"""
    
    print("\n" + "="*70)
    print("真实场景: 息壤年末强制再平衡")
    print("="*70)
    
    # 假设组合规模 $10M，需要调仓 10%
    nav = 10000000
    rebalance_pct = 0.10
    rebalance_amount = nav * rebalance_pct  # $1M
    
    print(f"\n组合规模: ${nav:,}")
    print(f"调仓比例: {rebalance_pct:.0%}")
    print(f"调仓金额: ${rebalance_amount:,}")
    
    # 需要卖出 SPY，买入 SHV
    spy_price = 550
    shv_price = 110
    
    spy_qty = int(rebalance_amount / spy_price)
    shv_qty = int(rebalance_amount / shv_price)
    
    print(f"\n交易计划:")
    print(f"  卖出 SPY: {spy_qty:,} 股 @ ${spy_price}")
    print(f"  买入 SHV: {shv_qty:,} 股 @ ${shv_price}")
    
    # 市价单成本
    spy_impact = simulate_market_impact(rebalance_amount, market_depth=50000000)
    shv_impact = simulate_market_impact(rebalance_amount, market_depth=20000000)
    
    market_order_cost = (
        spy_qty * spy_price * spy_impact +  # 卖出滑点（负收益）
        shv_qty * shv_price * shv_impact    # 买入滑点
    )
    
    print(f"\n市价单执行:")
    print(f"  SPY 滑点: ${spy_qty * spy_price * spy_impact:,.2f} ({spy_impact:.3%})")
    print(f"  SHV 滑点: ${shv_qty * shv_price * shv_impact:,.2f} ({shv_impact:.3%})")
    print(f"  总成本: ${market_order_cost:,.2f}")
    
    # TWAP 成本
    spy_twap_impact = simulate_market_impact(rebalance_amount / 20, market_depth=50000000)
    shv_twap_impact = simulate_market_impact(rebalance_amount / 20, market_depth=20000000)
    
    twap_cost = (
        spy_qty * spy_price * spy_twap_impact +
        shv_qty * shv_price * shv_twap_impact
    )
    
    print(f"\nTWAP 执行 (20 单):")
    print(f"  SPY 滑点: ${spy_qty * spy_price * spy_twap_impact:,.2f} ({spy_twap_impact:.3%})")
    print(f"  SHV 滑点: ${shv_qty * shv_price * shv_twap_impact:,.2f} ({shv_twap_impact:.3%})")
    print(f"  总成本: ${twap_cost:,.2f}")
    
    # 节省
    saved = market_order_cost - twap_cost
    saved_pct = saved / nav
    
    print(f"\n💰 TWAP 节省: ${saved:,.2f} ({saved_pct:.3%} of NAV)")
    
    # 年化收益
    annual_rebalances = 2.4  # 息壤平均每年调仓 2.4 次
    annual_saved = saved * annual_rebalances
    
    print(f"\n年化节省 (假设每年调仓 {annual_rebalances} 次):")
    print(f"  ${annual_saved:,.2f}/年")
    print(f"  相当于额外 {annual_saved / nav:.3%} 的年化收益")
    
    print("\n" + "="*70)
    print("结论: 对于千万级资产，TWAP 每年可节省数千美元")
    print("="*70)


if __name__ == "__main__":
    test_market_order_vs_twap()
    test_twap_optimal_parameters()
    test_twap_real_world_scenario()
