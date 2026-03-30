"""
息壤（Xi-Rang）Phase 3 & 4: 券商执行器

Phase 3（半自动）：接入券商 API，自动下单，但需要人工确认。
Phase 4（全自动）：去掉人工确认，完全自动执行。

支持的券商（按优先级）：
- IBKR (Interactive Brokers) — 全球覆盖最广，API 最成熟
- 富途 (Futu/Moomoo) — 港美股，API 友好
- 老虎 (Tiger Brokers) — 港美股

⚠ 安全警告：
- 券商 API Key 必须存储在 .env 文件中，绝不能硬编码
- Phase 3 必须有人工确认环节，防止程序 bug 导致错误交易
- 建议先用小资金（如 $1,000）验证执行层可靠性

当前状态：框架已搭建，具体实现待 Paper Trading 验证通过后开发。
"""

import os
import logging
from typing import Optional

from engine.execution.base import (
    BaseExecutor, TradeOrder, ExecutionResult,
    OrderSide, OrderStatus,
)
from engine.config import ASSETS

logger = logging.getLogger("xirang.broker")


# ── 券商适配器基类 ────────────────────────────────────

class BrokerAdapter:
    """
    券商 API 适配器基类。

    每个券商实现一个子类，统一接口。
    """

    def connect(self) -> bool:
        """连接券商 API"""
        raise NotImplementedError

    def get_account_balance(self) -> dict:
        """
        获取账户余额和持仓。

        Returns:
            {
                "cash": 10000.0,
                "positions": {
                    "SPY": {"quantity": 45, "market_value": 24750.0, "avg_cost": 540.0},
                    "TLT": {"quantity": 270, "market_value": 24840.0, "avg_cost": 91.0},
                    ...
                },
                "total_value": 100000.0,
            }
        """
        raise NotImplementedError

    def get_quote(self, symbol: str) -> dict:
        """
        获取实时报价。

        Returns:
            {"bid": 549.8, "ask": 550.2, "last": 550.0}
        """
        raise NotImplementedError

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        order_type: str = "MARKET",
        limit_price: Optional[float] = None,
    ) -> dict:
        """
        下单。

        Args:
            symbol: ETF 代码
            side: "BUY" / "SELL"
            quantity: 股数
            order_type: "MARKET"（市价）/ "LIMIT"（限价）
            limit_price: 限价单价格

        Returns:
            {"order_id": "xxx", "status": "SUBMITTED"}
        """
        raise NotImplementedError

    def get_order_status(self, order_id: str) -> dict:
        """
        查询订单状态。

        Returns:
            {
                "order_id": "xxx",
                "status": "FILLED" / "PARTIAL" / "PENDING" / "FAILED",
                "filled_quantity": 45,
                "filled_price": 550.1,
            }
        """
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> bool:
        """取消订单"""
        raise NotImplementedError


# ── IBKR 适配器（待实现）──────────────────────────────

class IBKRAdapter(BrokerAdapter):
    """
    Interactive Brokers API 适配器。

    使用 ib_insync 库连接 IBKR TWS 或 IB Gateway。

    配置（.env）：
        IBKR_HOST=127.0.0.1
        IBKR_PORT=7497          # 7497=模拟, 7496=实盘
        IBKR_CLIENT_ID=1

    依赖：
        pip install ib_insync

    ⚠ 注意：
        - TWS 或 IB Gateway 必须在服务器上运行
        - 首次连接需要在 TWS 中授权 API 访问
        - 建议先用 7497 端口（模拟账户）测试
    """

    def connect(self) -> bool:
        # TODO: 实现 IBKR 连接
        # from ib_insync import IB
        # self.ib = IB()
        # self.ib.connect(
        #     host=os.environ.get("IBKR_HOST", "127.0.0.1"),
        #     port=int(os.environ.get("IBKR_PORT", "7497")),
        #     clientId=int(os.environ.get("IBKR_CLIENT_ID", "1")),
        # )
        logger.warning("IBKRAdapter.connect() 尚未实现")
        return False

    def get_account_balance(self) -> dict:
        # TODO: 实现账户查询
        # account = self.ib.accountSummary()
        # positions = self.ib.positions()
        raise NotImplementedError("IBKRAdapter 尚未实现")

    def get_quote(self, symbol: str) -> dict:
        raise NotImplementedError("IBKRAdapter 尚未实现")

    def place_order(self, symbol, side, quantity, order_type="MARKET", limit_price=None):
        raise NotImplementedError("IBKRAdapter 尚未实现")

    def get_order_status(self, order_id: str) -> dict:
        raise NotImplementedError("IBKRAdapter 尚未实现")

    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("IBKRAdapter 尚未实现")


# ── 富途适配器（待实现）──────────────────────────────

class FutuAdapter(BrokerAdapter):
    """
    富途 (Futu/Moomoo) API 适配器。

    使用 futu-api 库连接 FutuOpenD。

    配置（.env）：
        FUTU_HOST=127.0.0.1
        FUTU_PORT=11111

    依赖：
        pip install futu-api

    ⚠ 注意：
        - FutuOpenD 必须在服务器上运行
        - 需要在富途牛牛 APP 中开通 API 权限
    """

    def connect(self) -> bool:
        # TODO: 实现富途连接
        logger.warning("FutuAdapter.connect() 尚未实现")
        return False

    def get_account_balance(self) -> dict:
        raise NotImplementedError("FutuAdapter 尚未实现")

    def get_quote(self, symbol: str) -> dict:
        raise NotImplementedError("FutuAdapter 尚未实现")

    def place_order(self, symbol, side, quantity, order_type="MARKET", limit_price=None):
        raise NotImplementedError("FutuAdapter 尚未实现")

    def get_order_status(self, order_id: str) -> dict:
        raise NotImplementedError("FutuAdapter 尚未实现")

    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("FutuAdapter 尚未实现")


# ── 券商执行器（Phase 3 & 4）─────────────────────────

class BrokerExecutor(BaseExecutor):
    """
    券商执行器。

    Phase 3（半自动）：
        - auto_confirm=False
        - 下单后推送通知，等待人工在系统中确认
        - 超时未确认自动取消

    Phase 4（全自动）：
        - auto_confirm=True
        - 下单后自动等待成交，无需人工干预
    """

    def __init__(
        self,
        broker: Optional[BrokerAdapter] = None,
        auto_confirm: bool = False,
    ):
        """
        Args:
            broker: 券商适配器实例
            auto_confirm: True=全自动(Phase 4), False=需人工确认(Phase 3)
        """
        self.broker = broker
        self.auto_confirm = auto_confirm

    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        """
        与 PaperExecutor 相同的计算逻辑，但使用券商实时报价。
        """
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

        orders.sort(key=lambda o: (0 if o.side == OrderSide.SELL else 1))
        return orders

    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        """
        通过券商 API 执行交易。

        TODO: 实现以下流程：
        1. 先执行所有卖出订单（回笼资金）
        2. 等待卖出全部成交
        3. 再执行所有买入订单
        4. 等待买入全部成交
        5. 如果 auto_confirm=False，在步骤 1 之前推送通知等待确认
        6. 处理部分成交、超时、失败等异常情况
        """
        raise NotImplementedError(
            "BrokerExecutor.execute() 尚未实现。"
            "请先完成 Paper Trading 阶段的验证。"
        )

    def sync_positions(self) -> dict[str, float]:
        """
        从券商 API 获取真实持仓。

        这是实盘阶段最关键的方法——系统不再"自己算"持仓，
        而是每天从券商拉取真实数据，确保数据库与真实账户一致。

        TODO: 实现以下逻辑：
        1. 调用 broker.get_account_balance()
        2. 提取各 ETF 的持仓市值
        3. 与数据库中的记录对比
        4. 如果偏差 > 1%，发出警告（可能是手动交易或分红导致）
        5. 更新数据库
        """
        raise NotImplementedError(
            "BrokerExecutor.sync_positions() 尚未实现。"
        )

    def get_current_prices(self) -> dict[str, float]:
        """
        从券商获取实时报价。

        比 Yahoo Finance 更准确，因为是券商的实际可交易价格。
        """
        if self.broker is None:
            return {}

        prices = {}
        for asset in ASSETS:
            try:
                quote = self.broker.get_quote(asset)
                # 用买卖中间价
                prices[asset] = (quote["bid"] + quote["ask"]) / 2
            except Exception as e:
                logger.error(f"获取 {asset} 报价失败: {e}")

        return prices
