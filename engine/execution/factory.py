"""
息壤（Xi-Rang）执行器与券商适配器工厂

执行器负责“怎么调仓”。
适配器负责“跟谁对话”。

从这一层开始，系统正式支持：
- 主券商
- 备券商
- 沙箱适配器
- 影子运行入口
"""

import logging
import os

from engine.execution.base import BaseExecutor
from engine.execution.broker import BrokerExecutor
from engine.execution.broker_adapter import BrokerAdapter, BrokerMode, FutuAdapter, IBKRAdapter
from engine.execution.manual import ManualExecutor
from engine.execution.paper import PaperExecutor
from engine.execution.paper_adapter import PaperAdapter
from engine.execution.reconciliation import ReconciliationService
from engine.execution.shadow_run import ShadowRun
from engine.execution.twap import TWAPExecutor

logger = logging.getLogger("xirang.execution")


def get_broker_topology() -> dict[str, str]:
    """
    券商拓扑配置。

    兼容旧变量 XIRANG_BROKER，但长期以主/备/沙箱三路配置为准。
    """
    primary = os.environ.get("XIRANG_BROKER_PRIMARY") or os.environ.get("XIRANG_BROKER") or "ibkr"
    backup = os.environ.get("XIRANG_BROKER_BACKUP", "futu")
    sandbox = os.environ.get("XIRANG_BROKER_SANDBOX", "paper")
    return {
        "primary": primary.lower(),
        "backup": backup.lower(),
        "sandbox": sandbox.lower(),
    }


def create_broker_adapter(
    role: str = "primary",
    broker_name: str | None = None,
    mode: BrokerMode | str | None = None,
    market_data_service=None,
    assets=None,
) -> BrokerAdapter:
    topology = get_broker_topology()
    selected_name = (broker_name or topology.get(role, topology["primary"])).lower()

    if isinstance(mode, BrokerMode):
        selected_mode = mode
    elif isinstance(mode, str):
        selected_mode = BrokerMode(mode.lower())
    else:
        selected_mode = BrokerMode.PAPER if role == "sandbox" else BrokerMode.LIVE

    if selected_name in {"paper", "mock", "sandbox"}:
        adapter = PaperAdapter(market_data_service=market_data_service, assets=assets)
        adapter.broker_role = role
        return adapter

    adapter_map = {
        "ibkr": IBKRAdapter,
        "futu": FutuAdapter,
    }
    if selected_name not in adapter_map:
        raise ValueError(f"不支持的券商: {selected_name}")

    adapter = adapter_map[selected_name](mode=selected_mode, assets=assets)
    adapter.broker_role = role
    return adapter


def create_shadow_runner(market_data_service=None, assets=None, executor: BaseExecutor | None = None) -> ShadowRun:
    shadow_executor = executor or PaperExecutor(market_data_service, assets=assets)
    sandbox_adapter = create_broker_adapter(
        role="sandbox",
        market_data_service=market_data_service,
        assets=assets,
    )
    return ShadowRun(
        executor=shadow_executor,
        adapter=sandbox_adapter,
        reconciliation_service=ReconciliationService(),
    )


def create_executor(market_data_service=None, use_twap: bool = False, assets=None) -> BaseExecutor:
    """
    根据环境变量创建执行器。

    Args:
        market_data_service: 市场数据服务
        use_twap: 是否使用 TWAP 执行器

    Returns:
        对应阶段的执行器实例
    """
    mode = os.environ.get("XIRANG_EXECUTOR", "paper").lower()

    if use_twap and mode == "paper":
        logger.info("执行模式: Paper Trading + TWAP（智能拆单）")
        return TWAPExecutor(
            market_data_service=market_data_service,
            assets=assets,
            time_window_minutes=120,
            num_slices=20,
            min_order_size=500000.0,
            simulate=True,
        )

    if mode == "paper":
        logger.info("执行模式: Paper Trading（仿真）")
        return PaperExecutor(market_data_service, assets=assets)

    if mode == "manual":
        logger.info("执行模式: Manual（人工执行）")
        return ManualExecutor(market_data_service, assets=assets)

    if mode in ("semi_auto", "auto"):
        auto_confirm = mode == "auto"
        broker = create_broker_adapter(
            role="primary",
            mode=BrokerMode.LIVE,
            market_data_service=market_data_service,
            assets=assets,
        )
        phase = "Phase 4 全自动" if auto_confirm else "Phase 3 半自动"
        logger.info("执行模式: %s，券商: %s", phase, broker.broker_name)
        return BrokerExecutor(broker=broker, auto_confirm=auto_confirm, assets=assets)

    raise ValueError(
        f"不支持的执行模式: {mode}。"
        f"可选: paper / manual / semi_auto / auto"
    )
