"""
息壤（Xi-Rang）Phase 2: 人工执行器

系统生成具体的买卖指令（"卖出 12 股 SPY，买入 8 股 TLT"），
通过推送发给你，你在券商 APP 上手动执行。

这是从仿真到实盘的过渡阶段，验证：
- 指令是否合理
- 执行是否顺畅
- 真实滑点与模拟滑点的差距

特点：
- 不调用任何券商 API
- 通过 notifier 推送具体操作指令
- 人工执行后，手动在系统中确认完成
- 系统记录"指令 vs 实际"的偏差，用于校准摩擦成本模型

预计使用周期：6-12 个月
"""

from engine.execution.base import (
    BaseExecutor, TradeOrder, ExecutionResult,
    OrderSide, OrderStatus,
)
from engine.execution.paper import PaperExecutor


class ManualExecutor(BaseExecutor):
    """
    人工执行器：生成指令 → 推送给人 → 人手动执行 → 人确认完成。

    继承 PaperExecutor 的 translate_orders 逻辑（计算买卖股数），
    但 execute 阶段改为推送指令而非直接"成交"。
    """

    def __init__(self, market_data_service=None, assets=None):
        self._paper = PaperExecutor(market_data_service, assets=assets)
        self.market = market_data_service

    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        """复用 PaperExecutor 的计算逻辑"""
        return self._paper.translate_orders(
            current_positions, target_weights, total_nav, current_prices
        )

    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        """
        生成人类可读的操作指令，通过推送发出。

        订单状态设为 AWAITING_CONFIRM，等待人工确认。
        """
        if not orders:
            return ExecutionResult(
                success=True, orders=[], total_bought=0, total_sold=0,
                total_commission=0, message="无需交易",
            )

        # 生成操作指令文本
        lines = ["📋 息壤 · 调仓指令（请手动执行）", "━" * 30]

        sell_orders = [o for o in orders if o.side == OrderSide.SELL]
        buy_orders = [o for o in orders if o.side == OrderSide.BUY]

        if sell_orders:
            lines.append("【卖出】")
            for o in sell_orders:
                o.status = OrderStatus.AWAITING_CONFIRM
                lines.append(f"  {o.symbol}: 卖出 {o.quantity} 股 (约 ${o.estimated_amount:,.0f})")

        if buy_orders:
            lines.append("【买入】")
            for o in buy_orders:
                o.status = OrderStatus.AWAITING_CONFIRM
                lines.append(f"  {o.symbol}: 买入 {o.quantity} 股 (约 ${o.estimated_amount:,.0f})")

        lines.append("━" * 30)
        lines.append("请在券商 APP 中按以上指令执行，完成后在系统中确认。")

        message = "\n".join(lines)

        # 通过 notifier 推送
        # TODO: 当 notifier 支持自定义消息时，直接发送 message
        # 目前先打印到控制台
        print(message)

        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=sum(o.estimated_amount for o in buy_orders),
            total_sold=sum(o.estimated_amount for o in sell_orders),
            total_commission=0,  # 人工执行，手续费由券商扣
            message=message,
        )

    def confirm_execution(
        self,
        orders: list[TradeOrder],
        actual_prices: dict[str, float],
    ) -> ExecutionResult:
        """
        人工确认执行完成。

        在你手动执行完交易后，调用此方法录入实际成交价格。
        系统会计算"指令 vs 实际"的偏差，用于校准摩擦成本模型。

        Args:
            orders: 之前生成的订单列表
            actual_prices: 实际成交价格 {"SPY": 551.2, "TLT": 91.8, ...}

        Returns:
            更新后的 ExecutionResult
        """
        total_bought = 0.0
        total_sold = 0.0
        slippage_total = 0.0

        for order in orders:
            actual_price = actual_prices.get(order.symbol, order.estimated_price)
            order.filled_price = actual_price
            order.filled_quantity = order.quantity
            order.status = OrderStatus.FILLED

            amount = order.quantity * actual_price
            slippage = abs(actual_price - order.estimated_price) / order.estimated_price

            if order.side == OrderSide.BUY:
                total_bought += amount
            else:
                total_sold += amount

            slippage_total += slippage

        avg_slippage = slippage_total / len(orders) if orders else 0

        return ExecutionResult(
            success=True,
            orders=orders,
            total_bought=total_bought,
            total_sold=total_sold,
            total_commission=0,
            message=f"人工执行确认完成，平均滑点: {avg_slippage:.3%}",
        )

    def sync_positions(self) -> dict[str, float]:
        """
        人工执行阶段：仍然从数据库读取。

        但建议定期与券商账户核对，确保数据库与真实持仓一致。
        """
        # TODO: 提供一个 CLI 命令让用户手动录入真实持仓
        return {}

    def get_current_prices(self) -> dict[str, float]:
        return self._paper.get_current_prices()
