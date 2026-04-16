"""
策略注册表 — 所有沙盒策略在此注册。

新策略只需：
1. 继承 AlphaStrategy
2. 在此文件 REGISTRY 中注册
"""

from engine.alpha.covered_call import CoveredCallStrategy
from engine.alpha.grid_trading import GridTradingStrategy
from engine.alpha.momentum import MomentumRotationStrategy

# 所有可用策略
REGISTRY = {
    "covered_call": CoveredCallStrategy,
    "grid_trading": GridTradingStrategy,
    "momentum_rotation": MomentumRotationStrategy,
}


def get_strategy_class(strategy_id: str):
    return REGISTRY.get(strategy_id)


def list_available_strategies() -> list[dict]:
    return [
        {
            "id": cls.STRATEGY_ID,
            "name": cls.STRATEGY_NAME,
            "description": cls.DESCRIPTION,
            "default_allocation": cls.DEFAULT_ALLOCATION,
            "formal_reporting_eligible": cls.FORMAL_REPORTING_ELIGIBLE,
            "reporting_scope": cls.get_reporting_scope(),
            "reporting_note": cls.REPORTING_NOTE,
        }
        for cls in REGISTRY.values()
    ]
