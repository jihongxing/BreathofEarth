"""
息壤（Xi-Rang）Phase 1: 仿真执行器

当前阶段使用。所有交易都是虚拟的，在数据库里改数字。
这是系统的默认执行器，也是回测和 Paper Trading 的基础。

特点：
- 不接触任何真实资金
- 不调用任何券商 API
- 用预估价格直接"成交"
- 扣除模拟摩擦成本
"""

from engine.execution.base import (
    BaseExecutor, TradeOrder, ExecutionResult,
    OrderSide, OrderStatus,
)
from engine.config import ASSETS, FEE_RATE


class PaperExecutor(BaseExecutor):
    """
    仿真执行器：所有交易都是虚拟的。

    这是当前 daily_runner.py 里的逻辑抽象出来的版本。
    行为与现有系统完全一致。
    """

    def __init__(self, market_data_service=None):
        """
        Args:
            market_data_service: MarketDataService 实例，用于获取价格
        """
        self.market = market_data_service

    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        """
        计算需要买卖的具体股数。

        逻辑：
        1. 计算每个资产的目标金额 = total_nav * target_weight
        2. 差额 = 目标金额 - 当前持仓金额
        3. 股数 = 差额 / 当前价格（取整）
        4. 先卖后买（卖出的排前面）
        """
        orders = []

        for i, asset in enumerate(ASSETS):
            target_amount = total_nav * target_weights[i]
            current_amount = current_positions.get(asset, 0.0)
            diff = target_amount - current_amount
            price = current_prices.get(asset, 0.0)

            if price <= 0 or abs(diff) < price:
                # 差额不足一股，跳过
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

        # 先卖后买：卖出回笼资金后再买入
        orders.sort(key=lambda o: (0 if o.side == OrderSide.SELL else 1))
        return orders

    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        """
        仿真执行：直接以预估价格"成交"。

        所有订单立即成功，扣除模拟手续费。
        """
        total_bought = 0.0
        total_sold = 0.0
        total_commission = 0.0

        for order in orders:
            # 仿真：直接成交
            order.status = OrderStatus.FILLED
            order.filled_price = order.estimated_price
            order.filled_quantity = order.quantity

            amount = order.quantity * order.estimated_price
            commission = amount * FEE_RATE

            if order.side == OrderSide.BUY:
                total_bought += amount
            else:
                total_sold += amount

            total_commission += commission

        # 生成摘要
        sell_orders = [o for o in orders if o.side == OrderSide.SELL]
        buy_orders = [o for o in orders if o.side == OrderSide.BUY]

        parts = []
        for o in sell_orders:
            parts.append(f"卖出 {o.quantity} 股 {o.symbol}")
        for o in buy_orders:
            parts.append(f"买入 {o.quantity} 股 {o.symbol}")

        message = "仿真执行: " + ", ".join(parts) if parts else "无交易"

        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=total_bought,
            total_sold=total_sold,
            total_commission=total_commission,
            message=message,
        )

    def sync_positions(self) -> dict[str, float]:
        """
        仿真阶段：返回数据库里的虚拟持仓。

        实际由 daily_runner 从数据库读取，这里只是接口占位。
        """
        # TODO: 从数据库读取当前持仓
        return {}

    def get_current_prices(self) -> dict[str, float]:
        """
        从 MarketDataService 获取最新价格。
        """
        if self.market is None:
            return {}

        prices = self.market.fetch_latest(lookback_days=5)
        return {asset: float(prices[asset].iloc[-1]) for asset in ASSETS}
