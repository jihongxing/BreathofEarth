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
    DEFAULT_ALLOCATION: float = 0.10  # 默认占 Alpha 账本 10%
    FORMAL_REPORTING_ELIGIBLE: bool = False
    REPORTING_NOTE: str = "当前策略仍处于沙盒实验阶段，收益仅供观察，不得进入正式排行榜或正式收益汇报。"

    def __init__(self, db: Database):
        self.db = db

    @classmethod
    def get_reporting_scope(cls) -> str:
        return "formal" if cls.FORMAL_REPORTING_ELIGIBLE else "sandbox"

    def is_enabled(self, portfolio_id: str = "us") -> bool:
        """检查策略是否已启用"""
        strategy = self.db.get_strategy(self.STRATEGY_ID, portfolio_id=portfolio_id)
        return strategy is not None and strategy["status"] == "ENABLED"

    def ensure_registered(self, portfolio_id: str = "us"):
        """确保策略已注册到数据库（但不自动启用）"""
        self.db.ensure_alpha_account(portfolio_id)
        existing = self.db.get_strategy(self.STRATEGY_ID, portfolio_id=portfolio_id)
        if not existing:
            self.db.upsert_strategy(
                self.STRATEGY_ID,
                portfolio_id=portfolio_id,
                name=self.STRATEGY_NAME,
                description=self.DESCRIPTION,
                status="DISABLED",
                allocation_pct=self.DEFAULT_ALLOCATION,
            )
            logger.info(f"策略 {self.STRATEGY_NAME} 已为组合 {portfolio_id} 注册（默认禁用）")

    def get_allocated_capital(self, portfolio_id: str) -> tuple[dict, dict, float]:
        """
        返回 (strategy, alpha_account, capital)。

        capital 只来自 Alpha 独立账本，不依附主仓 NAV。
        """
        self.ensure_registered(portfolio_id)
        strategy = self.db.get_strategy(self.STRATEGY_ID, portfolio_id=portfolio_id) or {}
        alpha_account = self.db.get_alpha_account(portfolio_id)
        alpha_balance = float(alpha_account.get("cash_balance", 0.0))
        allocation_pct = float(strategy.get("allocation_pct", self.DEFAULT_ALLOCATION))
        capital = alpha_balance * allocation_pct
        return strategy, alpha_account, capital

    @abstractmethod
    def run(self, portfolio_id: str, current_date: str, spy_price: float) -> dict:
        """
        执行策略逻辑。

        Args:
            portfolio_id: 组合 ID
            current_date: 当前日期 YYYY-MM-DD
            spy_price: SPY 当前价格

        Returns:
            执行结果 dict
        """
        pass
