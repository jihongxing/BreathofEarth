"""
息壤（Xi-Rang）券商执行器

这里只保留“执行器”职责：
- 把调仓意图翻译为订单
- 在未来调用 BrokerAdapter 提交订单
- 从适配器读取账户与报价

具体券商协议放在 broker_adapter.py。
"""

import logging
from typing import Optional

from engine.config import ASSETS
from engine.execution.base import (
    BaseExecutor,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    TradeOrder,
)
from engine.execution.broker_adapter import BrokerAdapter

logger = logging.getLogger("xirang.broker")


class BrokerExecutor(BaseExecutor):
    """
    券商执行器。

    当前阶段仍然是“骨架先行”：
    - 上层已经可以选择主/备/沙箱适配器
    - 下单链路仍需等只读同步、对账、影子运行稳定后再接入
    """

    def __init__(
        self,
        broker: Optional[BrokerAdapter] = None,
        auto_confirm: bool = False,
        assets: Optional[list[str]] = None,
    ):
        self.broker = broker
        self.auto_confirm = auto_confirm
        self.assets = assets or ASSETS

    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        orders = []

        for i, asset in enumerate(self.assets):
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
            orders.append(
                TradeOrder(
                    symbol=asset,
                    side=side,
                    quantity=quantity,
                    estimated_price=price,
                    estimated_amount=quantity * price,
                )
            )

        orders.sort(key=lambda order: (0 if order.side == OrderSide.SELL else 1))
        return orders

    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        raise NotImplementedError(
            "BrokerExecutor.execute() 尚未实现。"
            "请先完成只读对账、Shadow Run 与小额真实资金验证。"
        )

    def sync_positions(self) -> dict[str, float]:
        if self.broker is None:
            return {}

        snapshot = self.broker.get_account_snapshot()
        return {
            symbol: position.market_value
            for symbol, position in snapshot.positions.items()
            if symbol in self.assets
        }

    def get_current_prices(self) -> dict[str, float]:
        if self.broker is None:
            return {}

        prices = {}
        for asset in self.assets:
            try:
                quote = self.broker.get_quote(asset)
                prices[asset] = quote.mid_price
            except Exception as exc:
                logger.error("获取 %s 报价失败: %s", asset, exc)

        return prices
