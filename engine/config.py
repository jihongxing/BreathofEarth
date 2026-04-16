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
        "max_data_lag_days": 4,  # 跨时区 + 周末容忍，超过视为数据过期
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
        "max_data_lag_days": 3,  # 周末容忍，超过视为数据过期
        "asset_names": {
            "510300.SS": "沪深300",
            "511010.SS": "国债ETF",
            "518880.SS": "黄金ETF",
            "MONEY": "货币基金",
        },
        "data_source": None,  # 自动选择（优先本地 CSV）
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

# ── 三层资产结构占比 ──────────────────────────────────
# Core（抗通胀层）：永久组合，内部等权，不可破坏
# Stability（流动性层）：独立资金池，缓冲入金、出金来源
# Alpha（增长层）：由 alpha_strategies 独立管理，此处仅定义占比上限

LAYER_TARGET_CORE = 0.80          # Core 层目标占比
LAYER_TARGET_STABILITY = 0.15     # Stability 层目标占比
LAYER_TARGET_ALPHA = 0.05         # Alpha 层目标占比（上限）
LAYER_MIN_STABILITY = 0.05        # Stability 层最低安全线（低于触发风控）
LAYER_MAX_STABILITY = 0.30        # Stability 层上限（超过应转入 Core）

# ── 权重配置（Core 层内部，所有组合共享）─────────────

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
MAX_EXECUTION_SLIPPAGE_PCT = 0.005   # 成交偏差超过 0.5% 进入人工复核

# ── 冷却期 ────────────────────────────────────────────

COOLDOWN_DAYS = 20

# ── 状态定义 ──────────────────────────────────────────

STATE_IDLE = "IDLE"
STATE_PROTECTION = "PROTECTION"


# ── 配置校验 ──────────────────────────────────────────


def validate_config():
    """
    校验配置参数的合理性。

    在系统启动时调用，确保所有参数符合预期。

    Raises:
        AssertionError 如果配置不合理
    """
    # 权重配置校验
    assert len(WEIGHTS_IDLE) == 4, f"WEIGHTS_IDLE 必须有 4 个元素，当前: {len(WEIGHTS_IDLE)}"
    assert len(WEIGHTS_PROTECT) == 4, f"WEIGHTS_PROTECT 必须有 4 个元素，当前: {len(WEIGHTS_PROTECT)}"
    assert len(WEIGHTS_EMERGENCY) == 4, f"WEIGHTS_EMERGENCY 必须有 4 个元素，当前: {len(WEIGHTS_EMERGENCY)}"

    assert abs(sum(WEIGHTS_IDLE) - 1.0) < 1e-6, f"WEIGHTS_IDLE 权重和必须为 1.0，当前: {sum(WEIGHTS_IDLE)}"
    assert abs(sum(WEIGHTS_PROTECT) - 1.0) < 1e-6, f"WEIGHTS_PROTECT 权重和必须为 1.0，当前: {sum(WEIGHTS_PROTECT)}"
    assert abs(sum(WEIGHTS_EMERGENCY) - 1.0) < 1e-6, f"WEIGHTS_EMERGENCY 权重和必须为 1.0，当前: {sum(WEIGHTS_EMERGENCY)}"

    assert all(0 <= w <= 1 for w in WEIGHTS_IDLE), "WEIGHTS_IDLE 所有权重必须在 [0, 1] 范围内"
    assert all(0 <= w <= 1 for w in WEIGHTS_PROTECT), "WEIGHTS_PROTECT 所有权重必须在 [0, 1] 范围内"
    assert all(0 <= w <= 1 for w in WEIGHTS_EMERGENCY), "WEIGHTS_EMERGENCY 所有权重必须在 [0, 1] 范围内"

    # 风控阈值校验
    assert HARD_STOP_DD < RISK_DD_THRESHOLD < 0, \
        f"风控阈值必须满足: HARD_STOP_DD ({HARD_STOP_DD}) < RISK_DD_THRESHOLD ({RISK_DD_THRESHOLD}) < 0"

    assert -0.20 <= HARD_STOP_DD <= -0.10, \
        f"HARD_STOP_DD 应在 [-20%, -10%] 范围内，当前: {HARD_STOP_DD:.2%}"

    assert -0.15 <= RISK_DD_THRESHOLD <= -0.05, \
        f"RISK_DD_THRESHOLD 应在 [-15%, -5%] 范围内，当前: {RISK_DD_THRESHOLD:.2%}"

    assert 0 < RISK_CORR_THRESHOLD < 1, \
        f"RISK_CORR_THRESHOLD 必须在 (0, 1) 范围内，当前: {RISK_CORR_THRESHOLD}"

    assert 10 <= CORR_WINDOW <= 60, \
        f"CORR_WINDOW 应在 [10, 60] 天范围内，当前: {CORR_WINDOW}"

    # 再平衡参数校验
    assert 0 < DRIFT_THRESHOLD < 0.2, \
        f"DRIFT_THRESHOLD 应在 (0, 0.2) 范围内，当前: {DRIFT_THRESHOLD}"

    assert 0 <= FEE_RATE < 0.01, \
        f"FEE_RATE 应在 [0, 0.01) 范围内，当前: {FEE_RATE}"

    assert 0 < MAX_EXECUTION_SLIPPAGE_PCT <= 0.02, \
        f"MAX_EXECUTION_SLIPPAGE_PCT 应在 (0, 2%] 范围内，当前: {MAX_EXECUTION_SLIPPAGE_PCT:.2%}"

    # 冷却期校验
    assert 5 <= COOLDOWN_DAYS <= 60, \
        f"COOLDOWN_DAYS 应在 [5, 60] 天范围内，当前: {COOLDOWN_DAYS}"

    # 组合配置校验
    assert len(PORTFOLIOS) > 0, "至少需要定义一个组合"

    for pid, pf in PORTFOLIOS.items():
        assert "name" in pf, f"组合 {pid} 缺少 name 字段"
        assert "currency" in pf, f"组合 {pid} 缺少 currency 字段"
        assert "assets" in pf, f"组合 {pid} 缺少 assets 字段"
        assert "asset_names" in pf, f"组合 {pid} 缺少 asset_names 字段"
        assert "max_data_lag_days" in pf, f"组合 {pid} 缺少 max_data_lag_days 字段"

        assets = pf["assets"]
        assert len(assets) == 4, f"组合 {pid} 必须有 4 个资产，当前: {len(assets)}"
        assert len(assets) == len(set(assets)), f"组合 {pid} 资产列表有重复: {assets}"
        assert 1 <= pf["max_data_lag_days"] <= 7, \
            f"组合 {pid} 的 max_data_lag_days 应在 [1, 7] 范围内，当前: {pf['max_data_lag_days']}"

        asset_names = pf["asset_names"]
        for asset in assets:
            assert asset in asset_names, f"组合 {pid} 的资产 {asset} 在 asset_names 中未定义"

    assert DEFAULT_PORTFOLIO in PORTFOLIOS, \
        f"DEFAULT_PORTFOLIO '{DEFAULT_PORTFOLIO}' 不在 PORTFOLIOS 中"
