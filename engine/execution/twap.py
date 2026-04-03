"""
息壤（Xi-Rang）TWAP 执行器

Time-Weighted Average Price（时间加权平均价格）算法：
将大额订单拆分成多个小单，在指定时间窗口内均匀执行，降低市场冲击和滑点。

适用场景：
- 大额调仓（> 50 万美元或 > 100 万人民币）
- 流动性较差的 ETF
- 避免单笔大单对市场造成冲击

核心思想：
不是一次性砸盘/扫货，而是像"滴灌"一样慢慢执行，让市场有时间消化。
"""

import time
import logging
from typing import Optional
from datetime import datetime, timedelta

from engine.execution.base import (
    BaseExecutor, TradeOrder, ExecutionResult,
    OrderSide, OrderStatus,
)
from engine.config import FEE_RATE

logger = logging.getLogger("xirang.execution.twap")

class TWAPExecutor(BaseExecutor):
    """
    TWAP 执行器：将大单拆分为小单，在时间窗口内均匀执行。
    
    参数：
    - time_window_minutes: 执行时间窗口（分钟），默认 120 分钟（2小时）
    - num_slices: 拆分成多少个小单，默认 20
    - min_order_size: 触发 TWAP 的最小订单金额，默认 $500,000
    """

    def __init__(
        self,
        market_data_service=None,
        time_window_minutes: int = 120,
        num_slices: int = 20,
        min_order_size: float = 500000.0,
        simulate: bool = True,
    ):
        """
        Args:
            market_data_service: 市场数据服务
            time_window_minutes: 执行时间窗口（分钟）
            num_slices: 拆分成多少个小单
            min_order_size: 触发 TWAP 的最小订单金额
            simulate: 是否模拟执行（True=仿真，False=真实执行）
        """
        self.market = market_data_service
        self.time_window = time_window_minutes
        self.num_slices = num_slices
        self.min_order_size = min_order_size
        self.simulate = simulate

    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        """
        计算需要买卖的具体股数（与 PaperExecutor 相同）。
        """
        from engine.config import ASSETS
        
        orders = []

        for i, asset in enumerate(ASSETS):
            target_amount = total_nav * target_weights[i]
            current_amount = current_positions.get(asset, 0.0)
            diff = target_amount - current_amount
            price = current_prices.get(asset, 0.0)

            if price <= 0 or abs(diff) < price:
                continue

            quantity = int(abs(diff) / price)
            if quantity == 0:
                continue

            side = OrderSide.BUY if diff > 0 else OrderSide.SELL
            orders.append(TradeOrder(
                symbol=asset,
                side=side,
                quantity=quantity,
                estimated_price=price,
                estimated_amount=quantity * price,
            ))

        # 先卖后买
        orders.sort(key=lambda o: (0 if o.side == OrderSide.SELL else 1))
        return orders

    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        """
        执行订单：大单使用 TWAP 拆分，小单直接执行。
        """
        total_bought = 0.0
        total_sold = 0.0
        total_commission = 0.0
        all_executed_orders = []

        for order in orders:
            # 判断是否需要 TWAP
            if order.estimated_amount >= self.min_order_size:
                logger.info(
                    f"大额订单 {order.symbol} ${order.estimated_amount:,.2f} "
                    f"触发 TWAP 拆单（阈值 ${self.min_order_size:,.2f}）"
                )
                executed = self._execute_twap(order)
            else:
                logger.info(
                    f"小额订单 {order.symbol} ${order.estimated_amount:,.2f} "
                    f"直接执行"
                )
                executed = self._execute_market_order(order)

            all_executed_orders.extend(executed)

            # 统计
            for o in executed:
                amount = o.filled_quantity * o.filled_price
                commission = amount * FEE_RATE

                if o.side == OrderSide.BUY:
                    total_bought += amount
                else:
                    total_sold += amount

                total_commission += commission

        # 生成摘要
        message = self._generate_summary(all_executed_orders)

        return ExecutionResult(
            success=True,
            orders=all_executed_orders,
            total_bought=total_bought,
            total_sold=total_sold,
            total_commission=total_commission,
            message=message,
        )

    def _execute_twap(self, order: TradeOrder) -> list[TradeOrder]:
        """
        执行 TWAP 拆单。
        
        Returns:
            拆分后的小单列表
        """
        slices = []
        slice_quantity = order.quantity // self.num_slices
        remainder = order.quantity % self.num_slices
        interval_seconds = (self.time_window * 60) / self.num_slices

        logger.info(
            f"TWAP 拆单: {order.symbol} {order.quantity} 股 → "
            f"{self.num_slices} 个小单，每单 {slice_quantity} 股，"
            f"间隔 {interval_seconds:.1f} 秒"
        )

        for i in range(self.num_slices):
            # 最后一单加上余数
            qty = slice_quantity + (remainder if i == self.num_slices - 1 else 0)

            if qty == 0:
                continue

            # 模拟价格波动（±0.1%）
            price_variation = 1.0 + (i - self.num_slices / 2) * 0.001
            slice_price = order.estimated_price * price_variation

            slice_order = TradeOrder(
                symbol=order.symbol,
                side=order.side,
                quantity=qty,
                estimated_price=order.estimated_price,
                estimated_amount=qty * order.estimated_price,
                status=OrderStatus.FILLED,
                filled_price=slice_price,
                filled_quantity=qty,
            )

            slices.append(slice_order)

            # 模拟时间间隔（仿真模式下不真的等待）
            if not self.simulate:
                time.sleep(interval_seconds)

            logger.debug(
                f"TWAP [{i+1}/{self.num_slices}] {order.side.value} "
                f"{order.symbol} {qty} 股 @ ${slice_price:.2f}"
            )

        # 计算平均成交价
        total_qty = sum(s.filled_quantity for s in slices)
        avg_price = sum(s.filled_price * s.filled_quantity for s in slices) / total_qty

        logger.info(
            f"TWAP 完成: {order.symbol} 平均成交价 ${avg_price:.2f} "
            f"(预估 ${order.estimated_price:.2f}, "
            f"偏差 {(avg_price - order.estimated_price) / order.estimated_price:.2%})"
        )

        return slices

    def _execute_market_order(self, order: TradeOrder) -> list[TradeOrder]:
        """
        执行市价单（小额订单）。
        """
        # 模拟滑点（±0.05%）
        slippage = 0.0005 if order.side == OrderSide.BUY else -0.0005
        filled_price = order.estimated_price * (1 + slippage)

        order.status = OrderStatus.FILLED
        order.filled_price = filled_price
        order.filled_quantity = order.quantity

        return [order]

    def _generate_summary(self, orders: list[TradeOrder]) -> str:
        """生成执行摘要"""
        if not orders:
            return "无交易"

        # 按资产分组统计
        by_symbol = {}
        for order in orders:
            if order.symbol not in by_symbol:
                by_symbol[order.symbol] = {
                    "side": order.side,
                    "total_qty": 0,
                    "total_amount": 0.0,
                    "avg_price": 0.0,
                    "num_slices": 0,
                }

            by_symbol[order.symbol]["total_qty"] += order.filled_quantity
            by_symbol[order.symbol]["total_amount"] += order.filled_quantity * order.filled_price
            by_symbol[order.symbol]["num_slices"] += 1

        # 计算平均价格
        for symbol, data in by_symbol.items():
            data["avg_price"] = data["total_amount"] / data["total_qty"]

        # 生成摘要
        parts = []
        for symbol, data in by_symbol.items():
            side_text = "买入" if data["side"] == OrderSide.BUY else "卖出"
            if data["num_slices"] > 1:
                parts.append(
                    f"{side_text} {symbol} {data['total_qty']} 股 "
                    f"(TWAP {data['num_slices']} 单, 均价 ${data['avg_price']:.2f})"
                )
            else:
                parts.append(
                    f"{side_text} {symbol} {data['total_qty']} 股 @ ${data['avg_price']:.2f}"
                )

        return "TWAP 执行: " + ", ".join(parts)

    def sync_positions(self) -> dict[str, float]:
        """同步持仓（仿真阶段返回空）"""
        return {}

    def get_current_prices(self) -> dict[str, float]:
        """获取当前价格"""
        if self.market is None:
            return {}

        from engine.config import ASSETS
        prices = self.market.fetch_latest(lookback_days=5)
        return {asset: float(prices[asset].iloc[-1]) for asset in ASSETS}
