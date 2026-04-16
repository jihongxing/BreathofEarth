"""
息壤（Xi-Rang）统一模拟券商适配器

PaperExecutor 负责“模拟执行逻辑”；
PaperAdapter 负责“模拟券商接口”。

这样 Shadow / Sandbox / 回归测试都能使用相同的账户、报价、订单协议。
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from engine.execution.base import OrderSide, OrderStatus
from engine.execution.broker_adapter import (
    AccountSnapshot,
    BrokerAdapter,
    BrokerCapabilities,
    BrokerMode,
    BrokerOrderReceipt,
    BrokerOrderRequest,
    PositionSnapshot,
    QuoteSnapshot,
)


class PaperAdapter(BrokerAdapter):
    broker_name = "paper"

    def __init__(
        self,
        market_data_service=None,
        assets: Optional[list[str]] = None,
        cash: float = 0.0,
        positions: Optional[dict[str, float]] = None,
        currency: str = "USD",
    ):
        super().__init__(mode=BrokerMode.PAPER, assets=assets)
        self.market = market_data_service
        self.cash = float(cash)
        self.position_quantities = dict(positions or {})
        self.currency = currency
        self._receipts: dict[str, BrokerOrderReceipt] = {}
        self._order_seq = 0

    @property
    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            supports_live_trading=False,
            supports_paper_trading=True,
            supports_read_only=True,
            supports_order_updates=True,
            supported_markets=("SANDBOX",),
        )

    def connect(self) -> bool:
        return True

    def _latest_frame(self):
        if self.market is None:
            raise RuntimeError("PaperAdapter 需要 market_data_service 才能提供报价")
        return self.market.fetch_latest(lookback_days=5)

    def _last_price(self, symbol: str) -> tuple[float, datetime | None]:
        frame = self._latest_frame()
        if symbol not in frame.columns:
            raise KeyError(f"市场数据缺少标的 {symbol}")
        latest_idx = frame.index[-1]
        latest_time = latest_idx.to_pydatetime() if hasattr(latest_idx, "to_pydatetime") else None
        return float(frame[symbol].iloc[-1]), latest_time

    def get_account_snapshot(self) -> AccountSnapshot:
        positions = {}
        total_value = self.cash
        as_of = None

        for symbol, quantity in self.position_quantities.items():
            price, as_of = self._last_price(symbol)
            market_value = float(quantity) * price
            total_value += market_value
            positions[symbol] = PositionSnapshot(
                symbol=symbol,
                quantity=float(quantity),
                market_value=market_value,
                avg_cost=price,
            )

        return AccountSnapshot(
            broker_name=self.broker_name,
            mode=self.mode,
            account_id="paper-sandbox",
            currency=self.currency,
            cash=self.cash,
            total_value=total_value,
            positions=positions,
            as_of=as_of,
        )

    def get_quote(self, symbol: str) -> QuoteSnapshot:
        price, as_of = self._last_price(symbol)
        return QuoteSnapshot(
            symbol=symbol,
            bid=price,
            ask=price,
            last=price,
            as_of=as_of,
        )

    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        self._order_seq += 1
        price, _ = self._last_price(order.symbol)
        signed_qty = order.quantity if order.side == OrderSide.BUY else -order.quantity
        self.position_quantities[order.symbol] = self.position_quantities.get(order.symbol, 0.0) + signed_qty
        self.cash -= signed_qty * price

        receipt = BrokerOrderReceipt(
            order_id=f"paper-{self._order_seq}",
            status=OrderStatus.FILLED,
            symbol=order.symbol,
            side=order.side,
            requested_quantity=order.quantity,
            filled_quantity=order.quantity,
            avg_fill_price=price,
            message="paper fill",
            broker_reference=f"paper-{self._order_seq}",
        )
        self._receipts[receipt.order_id] = receipt
        return receipt

    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        if order_id not in self._receipts:
            raise KeyError(f"未知 paper 订单 {order_id}")
        return self._receipts[order_id]

    def cancel_order(self, order_id: str) -> bool:
        return False
