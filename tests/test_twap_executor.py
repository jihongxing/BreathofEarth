"""
TWAP 执行器单元测试
"""

import pytest
from unittest.mock import Mock

from engine.execution.twap import TWAPExecutor
from engine.execution.base import OrderSide, OrderStatus


@pytest.fixture
def mock_market():
    """模拟市场数据服务"""
    market = Mock()
    return market


@pytest.fixture
def twap_executor(mock_market):
    """创建 TWAP 执行器实例"""
    return TWAPExecutor(
        market_data_service=mock_market,
        time_window_minutes=120,
        num_slices=20,
        min_order_size=500000.0,
        simulate=True,
    )


class TestTWAPExecutor:
    """TWAP 执行器测试"""

    def test_small_order_no_twap(self, twap_executor):
        """测试：小额订单不触发 TWAP，直接执行"""
        from engine.execution.base import TradeOrder

        # 小额订单（$100k < $500k 阈值）
        order = TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=200,
            estimated_price=500.0,
            estimated_amount=100000.0,
        )

        result = twap_executor.execute([order])

        assert result.success
        assert len(result.orders) == 1, "小额订单应该只有 1 个订单"
        assert result.orders[0].status == OrderStatus.FILLED
        assert result.orders[0].filled_quantity == 200

    def test_large_order_triggers_twap(self, twap_executor):
        """测试：大额订单触发 TWAP 拆单"""
        from engine.execution.base import TradeOrder

        # 大额订单（$1M > $500k 阈值）
        order = TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2000,
            estimated_price=500.0,
            estimated_amount=1000000.0,
        )

        result = twap_executor.execute([order])

        assert result.success
        assert len(result.orders) == 20, "应该拆分成 20 个小单"

        # 验证总数量正确
        total_qty = sum(o.filled_quantity for o in result.orders)
        assert total_qty == 2000, "拆单后总数量应该等于原订单"

        # 验证所有小单都已成交
        for o in result.orders:
            assert o.status == OrderStatus.FILLED
            assert o.filled_price > 0

    def test_twap_average_price(self, twap_executor):
        """测试：TWAP 平均成交价接近预估价"""
        from engine.execution.base import TradeOrder

        order = TradeOrder(
            symbol="TLT",
            side=OrderSide.SELL,
            quantity=10000,
            estimated_price=100.0,
            estimated_amount=1000000.0,
        )

        result = twap_executor.execute([order])

        # 计算平均成交价
        total_qty = sum(o.filled_quantity for o in result.orders)
        avg_price = sum(o.filled_price * o.filled_quantity for o in result.orders) / total_qty

        # 平均价应该接近预估价（±1%）
        assert abs(avg_price - order.estimated_price) / order.estimated_price < 0.01

    def test_twap_slice_distribution(self, twap_executor):
        """测试：TWAP 拆单分布均匀"""
        from engine.execution.base import TradeOrder

        order = TradeOrder(
            symbol="GLD",
            side=OrderSide.BUY,
            quantity=2000,
            estimated_price=200.0,
            estimated_amount=400000.0,  # 不触发 TWAP（< $500k）
        )

        # 手动降低阈值以触发 TWAP
        twap_executor.min_order_size = 300000.0

        result = twap_executor.execute([order])

        # 每个小单应该是 100 股（2000 / 20）
        expected_slice_qty = 2000 // 20

        for i, o in enumerate(result.orders[:-1]):  # 除了最后一单
            assert o.filled_quantity == expected_slice_qty, \
                f"第 {i+1} 单应该是 {expected_slice_qty} 股"

        # 最后一单包含余数
        last_order = result.orders[-1]
        assert last_order.filled_quantity == expected_slice_qty + (2000 % 20)

    def test_multiple_orders_mixed(self, twap_executor):
        """测试：多个订单混合执行（部分触发 TWAP，部分不触发）"""
        from engine.execution.base import TradeOrder

        orders = [
            # 大单：触发 TWAP
            TradeOrder(
                symbol="SPY",
                side=OrderSide.BUY,
                quantity=2000,
                estimated_price=500.0,
                estimated_amount=1000000.0,
            ),
            # 小单：直接执行
            TradeOrder(
                symbol="TLT",
                side=OrderSide.SELL,
                quantity=500,
                estimated_price=100.0,
                estimated_amount=50000.0,
            ),
            # 大单：触发 TWAP
            TradeOrder(
                symbol="GLD",
                side=OrderSide.BUY,
                quantity=3000,
                estimated_price=200.0,
                estimated_amount=600000.0,
            ),
        ]

        result = twap_executor.execute(orders)

        assert result.success
        # SPY 20 单 + TLT 1 单 + GLD 20 单 = 41 单
        assert len(result.orders) == 41

        # 验证 SPY 和 GLD 被拆分
        spy_orders = [o for o in result.orders if o.symbol == "SPY"]
        assert len(spy_orders) == 20

        gld_orders = [o for o in result.orders if o.symbol == "GLD"]
        assert len(gld_orders) == 20

        # 验证 TLT 没有被拆分
        tlt_orders = [o for o in result.orders if o.symbol == "TLT"]
        assert len(tlt_orders) == 1

    def test_twap_commission_calculation(self, twap_executor):
        """测试：TWAP 手续费计算正确"""
        from engine.execution.base import TradeOrder
        from engine.config import FEE_RATE

        order = TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2000,
            estimated_price=500.0,
            estimated_amount=1000000.0,
        )

        result = twap_executor.execute([order])

        # 手续费 = 总成交金额 × 费率
        total_amount = sum(o.filled_price * o.filled_quantity for o in result.orders)
        expected_commission = total_amount * FEE_RATE

        assert abs(result.total_commission - expected_commission) < 1.0, \
            f"手续费计算错误: {result.total_commission} vs {expected_commission}"

    def test_twap_summary_message(self, twap_executor):
        """测试：TWAP 执行摘要生成正确"""
        from engine.execution.base import TradeOrder

        order = TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2000,
            estimated_price=500.0,
            estimated_amount=1000000.0,
        )

        result = twap_executor.execute([order])

        # 摘要应该包含关键信息
        assert "TWAP" in result.message
        assert "SPY" in result.message
        assert "2000" in result.message or "2,000" in result.message
        assert "20" in result.message  # 拆单数量

    def test_translate_orders(self, twap_executor):
        """测试：订单翻译功能"""
        current_positions = {
            "SPY": 250000.0,
            "TLT": 250000.0,
            "GLD": 250000.0,
            "SHV": 250000.0,
        }

        target_weights = [0.30, 0.25, 0.20, 0.25]  # SPY 增加到 30%
        total_nav = 1000000.0

        current_prices = {
            "SPY": 500.0,
            "TLT": 100.0,
            "GLD": 200.0,
            "SHV": 110.0,
        }

        orders = twap_executor.translate_orders(
            current_positions, target_weights, total_nav, current_prices
        )

        # 应该有买入 SPY 的订单（从 25% 增加到 30%）
        spy_orders = [o for o in orders if o.symbol == "SPY"]
        assert len(spy_orders) == 1
        assert spy_orders[0].side == OrderSide.BUY

        # 目标金额 = 1M × 30% = 300k
        # 当前持仓 = 250k
        # 需要买入 = 50k / 500 = 100 股
        assert spy_orders[0].quantity == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
