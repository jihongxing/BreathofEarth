-- 息壤（Xi-Rang）SQLite 数据库 Schema

-- 组合状态
-- positions = Core 层四资产持仓（永久组合，不可破坏）
-- stability_balance = Stability 层独立资金池（缓冲入金、出金来源）
-- Alpha 层由 alpha_strategies 表独立管理
-- nav = Core positions 总和 + stability_balance（不含 Alpha）
CREATE TABLE IF NOT EXISTS portfolios (
    id TEXT PRIMARY KEY DEFAULT 'default',
    state TEXT NOT NULL DEFAULT 'IDLE',
    nav REAL NOT NULL DEFAULT 100000.0,
    positions TEXT NOT NULL DEFAULT '[25000,25000,25000,25000]',
    stability_balance REAL NOT NULL DEFAULT 0.0,
    high_water_mark REAL NOT NULL DEFAULT 100000.0,
    cooldown_counter INTEGER NOT NULL DEFAULT 0,
    rebalance_count INTEGER NOT NULL DEFAULT 0,
    protection_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 每日快照（核心审计表）
CREATE TABLE IF NOT EXISTS daily_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    state TEXT NOT NULL,
    nav REAL NOT NULL,
    positions TEXT NOT NULL,
    weights TEXT NOT NULL,
    drawdown REAL NOT NULL,
    spy_tlt_corr REAL,
    action TEXT,
    trigger_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, date)
);

-- 交易记录
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    type TEXT NOT NULL,
    target_weights TEXT,
    turnover REAL,
    friction_cost REAL,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 风控事件
CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    drawdown REAL,
    spy_tlt_corr REAL,
    action_taken TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 每日运行记录（幂等性保护）
CREATE TABLE IF NOT EXISTS daily_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'SUCCESS',
    report TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, date)
);

-- 初始化默认组合
INSERT OR IGNORE INTO portfolios (id) VALUES ('default');

-- Insurance Layer 决策审计
CREATE TABLE IF NOT EXISTS insurance_decisions (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    previous_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    risk_score REAL NOT NULL DEFAULT 0,
    hard_blocks TEXT NOT NULL DEFAULT '[]',
    allowed_actions TEXT NOT NULL DEFAULT '{}',
    blocked_actions TEXT NOT NULL DEFAULT '{}',
    forced_actions TEXT NOT NULL DEFAULT '{}',
    reasons TEXT NOT NULL DEFAULT '[]',
    source_signals TEXT NOT NULL DEFAULT '[]',
    recovery_proposal_id TEXT,
    actor TEXT NOT NULL DEFAULT 'insurance',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS insurance_recovery_proposals (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    from_state TEXT NOT NULL,
    proposed_to_state TEXT NOT NULL,
    proposal_created_at TEXT NOT NULL,
    cooldown_until TEXT NOT NULL,
    validation_evidence TEXT NOT NULL DEFAULT '{}',
    unresolved_blocks TEXT NOT NULL DEFAULT '[]',
    required_approvals INTEGER NOT NULL DEFAULT 1,
    approvals TEXT NOT NULL DEFAULT '[]',
    audit_log_ids TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'insurance',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
