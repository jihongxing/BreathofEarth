"""
息壤（Xi-Rang）券商适配层基础设施

把“如何和券商说话”从执行器里拆出来。
执行器负责生成调仓动作，适配器负责读取账户、报价、订单与回执。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from engine.execution.base import OrderSide, OrderStatus

logger = logging.getLogger("xirang.execution.adapter")


class BrokerMode(Enum):
    READ_ONLY = "read_only"
    PAPER = "paper"
    LIVE = "live"


@dataclass(frozen=True)
class BrokerCapabilities:
    supports_live_trading: bool = False
    supports_paper_trading: bool = False
    supports_read_only: bool = True
    supports_order_updates: bool = False
    supported_markets: tuple[str, ...] = ()


@dataclass
class PositionSnapshot:
    symbol: str
    quantity: float
    market_value: float
    avg_cost: Optional[float] = None


@dataclass
class AccountSnapshot:
    broker_name: str
    mode: BrokerMode
    account_id: Optional[str]
    currency: str
    cash: float
    total_value: float
    positions: dict[str, PositionSnapshot] = field(default_factory=dict)
    as_of: Optional[datetime] = None


@dataclass
class QuoteSnapshot:
    symbol: str
    bid: float
    ask: float
    last: float
    as_of: Optional[datetime] = None

    @property
    def mid_price(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last


@dataclass
class BrokerOrderRequest:
    symbol: str
    side: OrderSide
    quantity: int
    order_type: str = "MARKET"
    limit_price: Optional[float] = None
    client_order_id: Optional[str] = None


@dataclass
class BrokerOrderReceipt:
    order_id: str
    status: OrderStatus
    symbol: str
    side: OrderSide
    requested_quantity: int
    filled_quantity: int = 0
    avg_fill_price: Optional[float] = None
    message: str = ""
    broker_reference: Optional[str] = None
    raw: dict = field(default_factory=dict)


class BrokerAdapter(ABC):
    """
    券商 API 适配器基类。

    约束：
    - 适配器不负责投资决策
    - 适配器不负责更新本地组合状态
    - 适配器先做“可读、可验、可对账”，再做“可交易”
    """

    broker_name = "unknown"

    def __init__(self, mode: BrokerMode = BrokerMode.READ_ONLY, assets: Optional[list[str]] = None):
        self.mode = mode
        self.assets = list(assets or [])

    @property
    @abstractmethod
    def capabilities(self) -> BrokerCapabilities:
        """返回适配器能力矩阵。"""

    @abstractmethod
    def connect(self) -> bool:
        """建立连接。"""

    @abstractmethod
    def get_account_snapshot(self) -> AccountSnapshot:
        """拉取账户快照。"""

    @abstractmethod
    def get_quote(self, symbol: str) -> QuoteSnapshot:
        """获取单一标的报价。"""

    def get_quotes(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        return {symbol: self.get_quote(symbol) for symbol in symbols}

    @abstractmethod
    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        """提交订单。"""

    @abstractmethod
    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        """查询订单状态。"""

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """取消订单。"""

    def supports_trading(self) -> bool:
        if self.mode == BrokerMode.LIVE:
            return self.capabilities.supports_live_trading
        if self.mode == BrokerMode.PAPER:
            return self.capabilities.supports_paper_trading
        return False

    def ensure_can_trade(self):
        if not self.supports_trading():
            raise RuntimeError(
                f"{self.broker_name} 当前模式为 {self.mode.value}，"
                "未启用真实交易能力。请先完成只读对账与影子运行。"
            )


class PlaceholderBrokerAdapter(BrokerAdapter):
    """
    尚未接通真实 API 的占位适配器。

    目的不是伪装“已完成”，而是先把边界钉住，让上层代码与测试可以围绕统一协议演进。
    """

    capability_matrix = BrokerCapabilities()

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self.capability_matrix

    def connect(self) -> bool:
        logger.warning("%s 适配器尚未接入真实 API", self.broker_name)
        return False

    def get_account_snapshot(self) -> AccountSnapshot:
        raise NotImplementedError(f"{self.broker_name} 账户同步尚未实现")

    def get_quote(self, symbol: str) -> QuoteSnapshot:
        raise NotImplementedError(f"{self.broker_name} 行情接口尚未实现")

    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        raise NotImplementedError(f"{self.broker_name} 下单接口尚未实现")

    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        raise NotImplementedError(f"{self.broker_name} 订单查询尚未实现")

    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError(f"{self.broker_name} 撤单接口尚未实现")


class IBKRAdapter(PlaceholderBrokerAdapter):
    broker_name = "ibkr"
    capability_matrix = BrokerCapabilities(
        supports_live_trading=True,
        supports_paper_trading=True,
        supports_read_only=True,
        supports_order_updates=True,
        supported_markets=("US", "HK", "SG", "GLOBAL"),
    )


class FutuAdapter(PlaceholderBrokerAdapter):
    broker_name = "futu"
    capability_matrix = BrokerCapabilities(
        supports_live_trading=True,
        supports_paper_trading=True,
        supports_read_only=True,
        supports_order_updates=True,
        supported_markets=("US", "HK", "CN_CONNECT"),
    )
