"""
息壤（Xi-Rang）执行层基类

定义了从"调仓指令"到"实际交易"的标准接口。
所有执行适配器（模拟、人工、半自动、全自动）都必须实现这个接口。

四步演进路径：
    Phase 1: PaperExecutor      — 纯仿真，数据库里改数字（当前阶段）
    Phase 2: ManualExecutor     — 生成具体买卖指令，推送给人，人手动执行
    Phase 3: SemiAutoExecutor   — 接入券商 API，自动下单，但需要人工确认
    Phase 4: AutoExecutor       — 全自动执行，只在风控触发时通知
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    PENDING = "PENDING"           # 待执行
    AWAITING_CONFIRM = "AWAITING_CONFIRM"  # 等待人工确认（Phase 3）
    SUBMITTED = "SUBMITTED"       # 已提交到券商
    PARTIAL_FILLED = "PARTIAL_FILLED"  # 部分成交
    FILLED = "FILLED"             # 全部成交
    FAILED = "FAILED"             # 失败
    CANCELLED = "CANCELLED"       # 已取消


@dataclass
class TradeOrder:
    """
    一笔具体的交易指令。

    由 PortfolioEngine 的 RebalanceOrder（目标权重级别）
    翻译成具体的买卖股数。
    """
    symbol: str                   # ETF 代码，如 "SPY"
    side: OrderSide               # 买入 / 卖出
    quantity: int                  # 股数（整数，ETF 不支持碎股）
    estimated_price: float        # 预估价格（用于计算，非限价）
    estimated_amount: float       # 预估金额 = quantity * estimated_price
    status: OrderStatus = OrderStatus.PENDING
    filled_price: Optional[float] = None      # 实际成交价
    filled_quantity: Optional[int] = None      # 实际成交股数
    broker_order_id: Optional[str] = None      # 券商订单号
    error_message: Optional[str] = None        # 错误信息


@dataclass
class ExecutionResult:
    """
    一次调仓的完整执行结果。

    包含多笔 TradeOrder 的汇总。
    """
    success: bool                 # 是否全部成功
    orders: list[TradeOrder]      # 所有订单
    total_bought: float           # 总买入金额
    total_sold: float             # 总卖出金额
    total_commission: float       # 总手续费
    message: str                  # 人类可读的摘要


class BaseExecutor(ABC):
    """
    执行层基类。

    所有执行适配器必须实现以下方法：
    - translate_orders: 把目标权重翻译成具体买卖指令
    - execute: 执行交易指令
    - sync_positions: 从券商同步真实持仓（实盘阶段）
    """

    @abstractmethod
    def translate_orders(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
    ) -> list[TradeOrder]:
        """
        把目标权重翻译成具体的买卖指令。

        Args:
            current_positions: 当前各资产的持仓金额 {"SPY": 25000, ...}
            target_weights: 目标权重 [0.25, 0.25, 0.25, 0.25]
            total_nav: 当前总净值
            current_prices: 各资产当前价格 {"SPY": 550.0, ...}

        Returns:
            TradeOrder 列表，按"先卖后买"排序（卖出回笼资金后再买入）
        """
        pass

    @abstractmethod
    def execute(self, orders: list[TradeOrder]) -> ExecutionResult:
        """
        执行交易指令。

        不同阶段的行为：
        - PaperExecutor: 直接在数据库里改数字
        - ManualExecutor: 推送指令给人，等待人工确认完成
        - SemiAutoExecutor: 提交到券商，等待人工点确认
        - AutoExecutor: 提交到券商，自动确认

        Returns:
            ExecutionResult 执行结果
        """
        pass

    @abstractmethod
    def sync_positions(self) -> dict[str, float]:
        """
        从券商同步真实持仓。

        仿真阶段：返回数据库里的虚拟持仓
        实盘阶段：调用券商 API 获取真实持仓和现金余额

        Returns:
            {"SPY": 25000.0, "TLT": 25000.0, ...} 各资产市值
        """
        pass

    @abstractmethod
    def get_current_prices(self) -> dict[str, float]:
        """
        获取各资产当前价格。

        Returns:
            {"SPY": 550.0, "TLT": 92.0, ...}
        """
        pass
