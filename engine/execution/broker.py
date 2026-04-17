"""
息壤（Xi-Rang）券商执行器

这里只保留“执行器”职责：
- 把调仓意图翻译为订单
- 在未来调用 BrokerAdapter 提交订单
- 从适配器读取账户与报价

具体券商协议放在 broker_adapter.py。
"""

import logging
import os
import time
from datetime import datetime, timezone
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
        if not orders:
            return ExecutionResult(
                success=True,
                orders=[],
                total_bought=0.0,
                total_sold=0.0,
                total_commission=0.0,
                message="无需执行订单",
            )
        if self.broker is None:
            raise RuntimeError("BrokerExecutor 缺少 broker 适配器，无法执行真实订单")

        if not self.auto_confirm:
            for order in orders:
                order.status = OrderStatus.AWAITING_CONFIRM
            total_bought = sum(order.estimated_amount for order in orders if order.side == OrderSide.BUY)
            total_sold = sum(order.estimated_amount for order in orders if order.side == OrderSide.SELL)
            return ExecutionResult(
                success=True,
                orders=orders,
                total_bought=float(total_bought),
                total_sold=float(total_sold),
                total_commission=0.0,
                message="订单已生成，等待人工确认后再提交到券商",
            )

        final_statuses = {OrderStatus.FILLED, OrderStatus.FAILED, OrderStatus.CANCELLED}
        total_bought = 0.0
        total_sold = 0.0
        total_commission = 0.0
        failed_symbols = []
        broker_events = []

        for index, order in enumerate(orders, start=1):
            client_order_id = f"xirang-{order.symbol.lower()}-{index}"
            submit_receipt = self.broker.place_order(
                order=self._to_broker_order(order, client_order_id=client_order_id)
            )
            broker_events.append(
                self._build_broker_event(
                    event_type="SUBMIT_RESULT",
                    order=order,
                    receipt=submit_receipt,
                    client_order_id=client_order_id,
                )
            )
            receipt, poll_events = self._poll_receipt_until_terminal(
                submit_receipt,
                order=order,
                client_order_id=client_order_id,
            )
            broker_events.extend(poll_events)
            self._apply_receipt(order, receipt)

            filled_quantity = float(order.filled_quantity or 0)
            fill_price = float(order.filled_price or order.estimated_price or 0.0)
            if filled_quantity > 0 and fill_price > 0:
                notional = filled_quantity * fill_price
                if order.side == OrderSide.BUY:
                    total_bought += notional
                else:
                    total_sold += notional

            total_commission += float(getattr(receipt, "commission", 0.0) or 0.0)
            if order.status in {OrderStatus.FAILED, OrderStatus.CANCELLED}:
                failed_symbols.append(order.symbol)

            if order.status not in final_statuses:
                logger.warning(
                    "券商订单 %s 尚未获得最终状态: %s",
                    order.symbol,
                    order.status.value,
                )

        success = not failed_symbols
        status_counts = {}
        for order in orders:
            status_counts[order.status.value] = status_counts.get(order.status.value, 0) + 1
        summary = ", ".join(f"{status}:{count}" for status, count in sorted(status_counts.items()))
        message = f"券商执行完成: {summary}"
        if failed_symbols:
            message += f" | 失败标的: {', '.join(failed_symbols)}"

        return ExecutionResult(
            success=success,
            orders=orders,
            total_bought=round(total_bought, 4),
            total_sold=round(total_sold, 4),
            total_commission=round(total_commission, 4),
            message=message,
            broker_events=broker_events,
        )

    def _to_broker_order(self, order: TradeOrder, client_order_id: str):
        from engine.execution.broker_adapter import BrokerOrderRequest

        return BrokerOrderRequest(
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            order_type="MARKET",
            client_order_id=client_order_id,
        )

    def _poll_receipt_until_terminal(self, receipt, *, order: TradeOrder, client_order_id: str):
        terminal_statuses = {OrderStatus.FILLED, OrderStatus.FAILED, OrderStatus.CANCELLED}
        if receipt.status in terminal_statuses or not receipt.order_id:
            return receipt, []

        max_polls = max(int(os.environ.get("XIRANG_BROKER_ORDER_MAX_POLLS", "3")), 0)
        poll_interval_sec = max(float(os.environ.get("XIRANG_BROKER_ORDER_POLL_INTERVAL_SEC", "0")), 0.0)
        events = []
        for _ in range(max_polls):
            if poll_interval_sec > 0:
                time.sleep(poll_interval_sec)
            latest = self.broker.get_order_status(receipt.order_id)
            if latest is not None:
                receipt = latest
                events.append(
                    self._build_broker_event(
                        event_type="STATUS_POLL",
                        order=order,
                        receipt=latest,
                        client_order_id=client_order_id,
                    )
                )
            if receipt.status in terminal_statuses:
                break
        return receipt, events

    def _apply_receipt(self, order: TradeOrder, receipt):
        order.status = receipt.status
        order.broker_order_id = receipt.order_id or order.broker_order_id
        order.broker_reference = receipt.broker_reference or order.broker_reference
        order.filled_quantity = receipt.filled_quantity
        order.filled_price = receipt.avg_fill_price
        if receipt.status in {OrderStatus.FAILED, OrderStatus.CANCELLED} and receipt.message:
            order.error_message = receipt.message

    def _build_broker_event(self, *, event_type: str, order: TradeOrder, receipt, client_order_id: str) -> dict:
        return {
            "event_type": event_type,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "broker_name": getattr(self.broker, "broker_name", "unknown"),
            "broker_role": getattr(self.broker, "broker_role", "primary"),
            "broker_mode": getattr(getattr(self.broker, "mode", None), "value", str(getattr(self.broker, "mode", ""))),
            "order_id": receipt.order_id,
            "client_order_id": client_order_id,
            "broker_reference": receipt.broker_reference,
            "symbol": order.symbol,
            "side": order.side.value,
            "requested_quantity": order.quantity,
            "filled_quantity": receipt.filled_quantity,
            "avg_fill_price": receipt.avg_fill_price,
            "commission": getattr(receipt, "commission", None),
            "status": receipt.status.value,
            "message": receipt.message,
            "raw": receipt.raw,
        }

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
