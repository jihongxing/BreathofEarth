"""
税务优化引擎单元测试
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from db.database import Database
from engine.insurance import InsuranceState, build_authority_decision
from engine.tax_optimizer import TaxLossHarvester, HarvestablePosition


@pytest.fixture
def temp_db():
    """创建临时数据库用于测试"""
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
    yield db
    
    # 清理
    db_path.unlink()


@pytest.fixture
def harvester(temp_db):
    """创建税损收割引擎实例"""
    return TaxLossHarvester(temp_db, min_loss_pct=0.05)


class TestTaxLossHarvester:
    """税损收割引擎测试"""

    def test_scan_no_losses(self, harvester, temp_db):
        """测试：没有亏损时，不应该有可收割的持仓"""
        portfolio_id = "us"
        
        # 设置成本基础：SPY 买入价 500
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        # 当前价格 550（盈利 10%）
        current_prices = {"SPY": 550.0}
        
        harvestable = harvester.scan_harvestable_losses(portfolio_id, current_prices)
        
        assert len(harvestable) == 0, "盈利的持仓不应该被收割"

    def test_scan_small_loss(self, harvester, temp_db):
        """测试：小额亏损（< 5%）不应该被收割"""
        portfolio_id = "us"
        
        # 设置成本基础：SPY 买入价 500
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        # 当前价格 480（亏损 4%）
        current_prices = {"SPY": 480.0}
        
        harvestable = harvester.scan_harvestable_losses(portfolio_id, current_prices)
        
        assert len(harvestable) == 0, "小额亏损不应该被收割"

    def test_scan_harvestable_loss(self, harvester, temp_db):
        """测试：亏损 >= 5% 应该被识别为可收割"""
        portfolio_id = "us"
        
        # 设置成本基础：SPY 买入价 500
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        # 当前价格 450（亏损 10%）
        current_prices = {"SPY": 450.0}
        
        harvestable = harvester.scan_harvestable_losses(portfolio_id, current_prices)
        
        assert len(harvestable) == 1, "应该识别出 1 个可收割的持仓"
        
        position = harvestable[0]
        assert position.asset == "SPY"
        assert position.quantity == 100
        assert position.cost_basis == 50000.0  # 100 * 500
        assert position.current_value == 45000.0  # 100 * 450
        assert position.unrealized_loss == -5000.0
        assert position.loss_pct == pytest.approx(-0.10)
        assert position.substitute == "VOO"  # SPY 的第一个替代品

    def test_scan_multiple_losses(self, harvester, temp_db):
        """测试：多个资产同时亏损"""
        portfolio_id = "us"
        
        # SPY 亏损 10%
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        # TLT 亏损 8%
        temp_db.save_cost_basis(
            asset="TLT",
            purchase_date="2024-01-01",
            quantity=200,
            cost_per_share=100.0,
            portfolio_id=portfolio_id,
        )
        
        # GLD 盈利 5%（不应该被收割）
        temp_db.save_cost_basis(
            asset="GLD",
            purchase_date="2024-01-01",
            quantity=150,
            cost_per_share=200.0,
            portfolio_id=portfolio_id,
        )
        
        current_prices = {
            "SPY": 450.0,  # 亏损 10%
            "TLT": 92.0,   # 亏损 8%
            "GLD": 210.0,  # 盈利 5%
        }
        
        harvestable = harvester.scan_harvestable_losses(portfolio_id, current_prices)
        
        assert len(harvestable) == 2, "应该识别出 2 个可收割的持仓"
        
        assets = {p.asset for p in harvestable}
        assert assets == {"SPY", "TLT"}

    def test_execute_harvest(self, harvester, temp_db):
        """测试：执行税损收割"""
        portfolio_id = "us"
        current_date = "2024-12-15"
        
        position = HarvestablePosition(
            asset="SPY",
            quantity=100,
            cost_basis=50000.0,
            current_value=45000.0,
            unrealized_loss=-5000.0,
            loss_pct=-0.10,
            substitute="VOO",
            purchase_date="2024-01-01",
        )
        
        success = harvester.execute_harvest(position, current_date, portfolio_id)
        
        assert success, "税损收割应该执行成功"
        
        # 验证数据库记录
        pending = temp_db.get_pending_reversals("2024-12-15", portfolio_id)
        assert len(pending) == 0, "刚收割的不应该立即可换回"
        
        # 31 天后应该可以换回
        pending = temp_db.get_pending_reversals("2025-01-16", portfolio_id)
        assert len(pending) == 1, "31 天后应该可以换回"
        
        event = pending[0]
        assert event["sold_asset"] == "SPY"
        assert event["substitute_asset"] == "VOO"
        assert event["loss_harvested"] == 5000.0
        assert event["status"] == "PENDING"

    def test_check_and_reverse_harvests(self, harvester, temp_db):
        """测试：检查并换回税损收割"""
        portfolio_id = "us"
        
        # 先执行一次收割
        position = HarvestablePosition(
            asset="SPY",
            quantity=100,
            cost_basis=50000.0,
            current_value=45000.0,
            unrealized_loss=-5000.0,
            loss_pct=-0.10,
            substitute="VOO",
            purchase_date="2024-01-01",
        )
        
        harvester.execute_harvest(position, "2024-12-15", portfolio_id)
        
        # 30 天内不应该换回
        reversed = harvester.check_and_reverse_harvests("2025-01-10", portfolio_id)
        assert reversed == 0, "30 天内不应该换回"
        
        # 31 天后应该换回
        reversed = harvester.check_and_reverse_harvests("2025-01-16", portfolio_id)
        assert reversed == 1, "31 天后应该换回"
        
        # 验证状态已更新
        pending = temp_db.get_pending_reversals("2025-01-16", portfolio_id)
        assert len(pending) == 0, "换回后不应该再有 PENDING 状态的记录"

    def test_run_year_end_harvest_no_losses(self, harvester, temp_db):
        """测试：年末收割 - 没有亏损"""
        portfolio_id = "us"
        
        # 设置盈利的持仓
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        current_prices = {"SPY": 550.0}
        current_date = "2024-12-31"
        
        result = harvester.run_year_end_harvest(portfolio_id, current_prices, current_date)
        
        assert result.success
        assert len(result.harvested_positions) == 0
        assert result.total_loss_harvested == 0.0
        assert result.estimated_tax_saved == 0.0
        assert "未发现" in result.message

    def test_run_year_end_harvest_with_losses(self, harvester, temp_db):
        """测试：年末收割 - 有亏损"""
        portfolio_id = "us"
        
        # SPY 亏损 10%
        temp_db.save_cost_basis(
            asset="SPY",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=500.0,
            portfolio_id=portfolio_id,
        )
        
        # TLT 亏损 8%
        temp_db.save_cost_basis(
            asset="TLT",
            purchase_date="2024-01-01",
            quantity=200,
            cost_per_share=100.0,
            portfolio_id=portfolio_id,
        )
        
        current_prices = {
            "SPY": 450.0,  # 亏损 5000
            "TLT": 92.0,   # 亏损 1600
        }
        current_date = "2024-12-31"
        
        result = harvester.run_year_end_harvest(portfolio_id, current_prices, current_date)
        
        assert result.success
        assert len(result.harvested_positions) == 2
        assert result.total_loss_harvested == pytest.approx(6600.0)
        assert result.estimated_tax_saved == pytest.approx(1320.0)  # 6600 * 0.20
        
        # 验证年度税务报告
        report = temp_db.get_annual_tax_report(2024, portfolio_id)
        assert report is not None
        assert report["total_harvested_losses"] == pytest.approx(6600.0)
        assert report["estimated_tax_saved"] == pytest.approx(1320.0)
        assert report["harvest_count"] == 2

    def test_no_substitute_available(self, harvester, temp_db):
        """测试：没有配置替代品的资产不应该被收割"""
        portfolio_id = "us"
        
        # 设置一个没有替代品的资产（假设 XYZ 不在 SUBSTITUTE_MAP 中）
        temp_db.save_cost_basis(
            asset="XYZ",
            purchase_date="2024-01-01",
            quantity=100,
            cost_per_share=100.0,
            portfolio_id=portfolio_id,
        )
        
        current_prices = {"XYZ": 90.0}  # 亏损 10%
        
        harvestable = harvester.scan_harvestable_losses(portfolio_id, current_prices)
        
        assert len(harvestable) == 0, "没有替代品的资产不应该被收割"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
