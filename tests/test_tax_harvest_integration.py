"""
税损收割集成测试

模拟一个完整的年末税损收割场景
"""

import tempfile
from pathlib import Path
from datetime import datetime

from db.database import Database
from engine.insurance import InsuranceState, build_authority_decision
from engine.tax_optimizer import TaxLossHarvester


def test_full_tax_harvest_scenario():
    """完整的税损收割场景测试"""
    
    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    
    db = Database(db_path)
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["test safe"])
    with db.insurance_decision_writer("test"):
        db.save_insurance_decision(
            portfolio_id="us",
            previous_state="SAFE",
            decision=safe,
            risk_score=0.0,
            hard_blocks=[],
            source_signals=[],
        )
    harvester = TaxLossHarvester(db, min_loss_pct=0.05)
    
    portfolio_id = "us"
    
    print("\n" + "="*60)
    print("税损收割集成测试")
    print("="*60)
    
    # 场景：2024年初买入，年末部分资产亏损
    print("\n1. 设置初始持仓（2024-01-01）")
    print("-" * 60)
    
    # SPY: 买入价 500，当前 450（亏损 10%）
    db.save_cost_basis(
        asset="SPY",
        purchase_date="2024-01-01",
        quantity=100,
        cost_per_share=500.0,
        portfolio_id=portfolio_id,
    )
    print(f"  SPY: 100 股 @ $500 = $50,000")
    
    # TLT: 买入价 100，当前 92（亏损 8%）
    db.save_cost_basis(
        asset="TLT",
        purchase_date="2024-01-01",
        quantity=200,
        cost_per_share=100.0,
        portfolio_id=portfolio_id,
    )
    print(f"  TLT: 200 股 @ $100 = $20,000")
    
    # GLD: 买入价 200，当前 210（盈利 5%）
    db.save_cost_basis(
        asset="GLD",
        purchase_date="2024-01-01",
        quantity=150,
        cost_per_share=200.0,
        portfolio_id=portfolio_id,
    )
    print(f"  GLD: 150 股 @ $200 = $30,000")
    
    # SHV: 买入价 110，当前 110（持平）
    db.save_cost_basis(
        asset="SHV",
        purchase_date="2024-01-01",
        quantity=100,
        cost_per_share=110.0,
        portfolio_id=portfolio_id,
    )
    print(f"  SHV: 100 股 @ $110 = $11,000")
    
    print(f"\n  总成本: $111,000")
    
    # 年末价格
    print("\n2. 年末价格（2024-12-15）")
    print("-" * 60)
    current_prices = {
        "SPY": 450.0,  # 亏损 $5,000
        "TLT": 92.0,   # 亏损 $1,600
        "GLD": 210.0,  # 盈利 $1,500
        "SHV": 110.0,  # 持平
    }
    
    for asset, price in current_prices.items():
        cost_basis = db.get_cost_basis(asset, portfolio_id)
        if cost_basis:
            quantity = cost_basis["quantity"]
            cost = cost_basis["total_cost"]
            current_value = quantity * price
            pnl = current_value - cost
            pnl_pct = pnl / cost
            status = "📉" if pnl < 0 else "📈" if pnl > 0 else "➡️"
            print(f"  {status} {asset}: ${price:.2f} | 市值 ${current_value:,.2f} | 盈亏 ${pnl:+,.2f} ({pnl_pct:+.1%})")
    
    # 执行税损收割
    print("\n3. 执行税损收割")
    print("-" * 60)
    
    result = harvester.run_year_end_harvest(
        portfolio_id=portfolio_id,
        current_prices=current_prices,
        current_date="2024-12-15",
    )
    
    print(f"  状态: {'✓ 成功' if result.success else '✗ 失败'}")
    print(f"  收割数量: {len(result.harvested_positions)} 个持仓")
    print(f"  总税损: ${result.total_loss_harvested:,.2f}")
    print(f"  预估节税: ${result.estimated_tax_saved:,.2f} (按 20% 税率)")
    
    if result.harvested_positions:
        print(f"\n  收割明细:")
        for pos in result.harvested_positions:
            print(f"    • {pos.asset} → {pos.substitute}")
            print(f"      亏损: ${abs(pos.unrealized_loss):,.2f} ({pos.loss_pct:.1%})")
    
    # 检查数据库记录
    print("\n4. 验证数据库记录")
    print("-" * 60)
    
    pending = db.get_pending_reversals("2024-12-15", portfolio_id)
    print(f"  待换回事件: {len(pending)} 个")
    
    # 检查年度税务报告
    report = db.get_annual_tax_report(2024, portfolio_id)
    if report:
        print(f"\n  2024 年度税务报告:")
        print(f"    收割税损: ${report['total_harvested_losses']:,.2f}")
        print(f"    预估节税: ${report['estimated_tax_saved']:,.2f}")
        print(f"    收割次数: {report['harvest_count']}")
    
    # 测试 Wash Sale 规则
    print("\n5. 测试 Wash Sale 规则（30天内不能换回）")
    print("-" * 60)
    
    reversed = harvester.check_and_reverse_harvests("2025-01-10", portfolio_id)
    print(f"  2025-01-10 (26天后): 换回 {reversed} 个 ❌")
    
    reversed = harvester.check_and_reverse_harvests("2025-01-16", portfolio_id)
    print(f"  2025-01-16 (32天后): 换回 {reversed} 个 ✓")
    
    # 验证换回后状态
    pending_after = db.get_pending_reversals("2025-01-16", portfolio_id)
    print(f"  剩余待换回: {len(pending_after)} 个")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)
    
    # 清理
    db_path.unlink()
    
    # 断言
    assert result.success
    assert len(result.harvested_positions) == 2  # SPY 和 TLT
    assert result.total_loss_harvested == 6600.0  # 5000 + 1600
    assert result.estimated_tax_saved == 1320.0  # 6600 * 0.20
    assert reversed == 2  # 32天后应该换回2个
    
    print("\n✓ 所有断言通过")


if __name__ == "__main__":
    test_full_tax_harvest_scenario()
