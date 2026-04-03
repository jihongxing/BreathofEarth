"""
Alpha 策略基类

所有沙盒策略必须继承此基类，实现 run() 方法。
策略的启用/禁用通过数据库的 alpha_strategies.status 字段控制。
"""

import logging
from abc import ABC, abstractmethod
from db.database import Database

logger = logging.getLogger("xirang.alpha")


class AlphaStrategy(ABC):
    """Alpha 策略基类"""

    STRATEGY_ID: str = ""        # 子类必须定义
    STRATEGY_NAME: str = ""
    DESCRIPTION: str = ""
    DEFAULT_ALLOCATION: float = 0.10  # 默认占 NAV 10%

    def __init__(self, db: Database):
        self.db = db

    def is_enabled(self, portfolio_id: str = "us") -> bool:
        """检查策略是否已启用"""
        strategy = self.db.get_strategy(self.STRATEGY_ID)
        return strategy is not None and strategy["status"] == "ENABLED"

    def ensure_registered(self, portfolio_id: str = "us"):
        """确保策略已注册到数据库（但不自动启用）"""
        existing = self.db.get_strategy(self.STRATEGY_ID)
        if not existing:
            self.db.upsert_strategy(
                self.STRATEGY_ID,
                name=self.STRATEGY_NAME,
                description=self.DESCRIPTION,
                status="DISABLED",
                portfolio_id=portfolio_id,
                allocation_pct=self.DEFAULT_ALLOCATION,
            )
            logger.info(f"策略 {self.STRATEGY_NAME} 已注册（默认禁用）")

    @abstractmethod
    def run(self, portfolio_id: str, current_date: str, spy_price: float, nav: float) -> dict:
        """
        执行策略逻辑。

        Args:
            portfolio_id: 组合 ID
            current_date: 当前日期 YYYY-MM-DD
            spy_price: SPY 当前价格
            nav: 组合净资产

        Returns:
            执行结果 dict
        """
        pass
