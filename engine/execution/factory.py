"""
息壤（Xi-Rang）执行器工厂

根据配置自动选择正确的执行器。

配置方式（.env 或环境变量）：
    XIRANG_EXECUTOR=paper       # Phase 1: 仿真（默认）
    XIRANG_EXECUTOR=manual      # Phase 2: 人工执行
    XIRANG_EXECUTOR=semi_auto   # Phase 3: 半自动（券商 API + 人工确认）
    XIRANG_EXECUTOR=auto        # Phase 4: 全自动

    XIRANG_BROKER=ibkr          # 券商选择（Phase 3/4）
    XIRANG_BROKER=futu
"""

import os
import logging

from engine.execution.base import BaseExecutor
from engine.execution.paper import PaperExecutor
from engine.execution.manual import ManualExecutor
from engine.execution.broker import BrokerExecutor, IBKRAdapter, FutuAdapter
from engine.execution.twap import TWAPExecutor

logger = logging.getLogger("xirang.execution")


def create_executor(market_data_service=None, use_twap: bool = False) -> BaseExecutor:
    """
    根据环境变量创建执行器。

    Args:
        market_data_service: 市场数据服务
        use_twap: 是否使用 TWAP 执行器（大额订单自动拆单）

    Returns:
        对应阶段的执行器实例
    """
    mode = os.environ.get("XIRANG_EXECUTOR", "paper").lower()

    # 如果启用 TWAP，优先返回 TWAP 执行器
    if use_twap and mode == "paper":
        logger.info("执行模式: Paper Trading + TWAP（智能拆单）")
        return TWAPExecutor(
            market_data_service=market_data_service,
            time_window_minutes=120,
            num_slices=20,
            min_order_size=500000.0,  # $500k 以上触发 TWAP
            simulate=True,
        )

    if mode == "paper":
        logger.info("执行模式: Paper Trading（仿真）")
        return PaperExecutor(market_data_service)

    elif mode == "manual":
        logger.info("执行模式: Manual（人工执行）")
        return ManualExecutor(market_data_service)

    elif mode in ("semi_auto", "auto"):
        broker_name = os.environ.get("XIRANG_BROKER", "ibkr").lower()

        if broker_name == "ibkr":
            broker = IBKRAdapter()
        elif broker_name == "futu":
            broker = FutuAdapter()
        else:
            raise ValueError(f"不支持的券商: {broker_name}")

        auto_confirm = (mode == "auto")
        phase = "Phase 4 全自动" if auto_confirm else "Phase 3 半自动"
        logger.info(f"执行模式: {phase}，券商: {broker_name}")

        return BrokerExecutor(broker=broker, auto_confirm=auto_confirm)

    else:
        raise ValueError(
            f"不支持的执行模式: {mode}。"
            f"可选: paper / manual / semi_auto / auto"
        )
