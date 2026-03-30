"""
息壤（Xi-Rang）全局配置

所有策略参数集中管理，禁止散落在代码各处。
这些参数经过 Phase 2 的 6 轮调参验证，不要随意修改。

多组合支持：PORTFOLIOS 字典定义了所有运行的组合。
每个组合有独立的标的、名称和 portfolio_id，共享同一套风控参数。
"""

# ── 组合定义（多市场）────────────────────────────────

PORTFOLIOS = {
    "us": {
        "name": "美股组合",
        "currency": "$",
        "assets": ["SPY", "TLT", "GLD", "SHV"],
        "asset_names": {
            "SPY": "标普500",
            "TLT": "长期国债",
            "GLD": "黄金",
            "SHV": "现金",
        },
        "data_source": None,  # 自动选择（yfinance 优先，akshare 后备）
    },
    "cn": {
        "name": "中国组合",
        "currency": "¥",
        "assets": ["510300.SS", "511010.SS", "518880.SS", "MONEY"],
        "asset_names": {
            "510300.SS": "沪深300",
            "511010.SS": "国债ETF",
            "518880.SS": "黄金ETF",
            "MONEY": "货币基金",
        },
        "data_source": "akshare_cn",  # 中国 A 股专用数据源
    },
}

# 默认组合（向后兼容）
DEFAULT_PORTFOLIO = "us"
ASSETS = PORTFOLIOS[DEFAULT_PORTFOLIO]["assets"]

ASSET_NAMES = {
    "SPY": "美股大盘 S&P500",
    "TLT": "长期国债 20Y+",
    "GLD": "黄金",
    "SHV": "短期国债/现金",
}

# ── 权重配置（所有组合共享）──────────────────────────

# 正常模式：等权永久组合
WEIGHTS_IDLE = [0.25, 0.25, 0.25, 0.25]

# 常规保护模式：现金提升至 50%
WEIGHTS_PROTECT = [0.10, 0.20, 0.20, 0.50]

# 紧急避险模式：现金提升至 75%
WEIGHTS_EMERGENCY = [0.03, 0.07, 0.15, 0.75]

# ── 风控阈值 ──────────────────────────────────────────

RISK_DD_THRESHOLD = -0.12
HARD_STOP_DD = -0.14
RISK_CORR_THRESHOLD = 0.5
CORR_WINDOW = 30

# ── 再平衡参数 ────────────────────────────────────────

DRIFT_THRESHOLD = 0.05
FEE_RATE = 0.001

# ── 冷却期 ────────────────────────────────────────────

COOLDOWN_DAYS = 20

# ── 状态定义 ──────────────────────────────────────────

STATE_IDLE = "IDLE"
STATE_PROTECTION = "PROTECTION"
