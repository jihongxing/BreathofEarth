-- Alpha 沙盒策略层

-- 策略注册表（手动开启/关闭）
CREATE TABLE IF NOT EXISTS alpha_strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'DISABLED',  -- ENABLED / DISABLED / SUSPENDED
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    allocation_pct REAL NOT NULL DEFAULT 0.10,  -- 占组合 NAV 的比例
    capital REAL NOT NULL DEFAULT 0,
    total_premium REAL NOT NULL DEFAULT 0,
    total_pnl REAL NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    enabled_at TEXT,
    disabled_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alpha 策略交易记录
CREATE TABLE IF NOT EXISTS alpha_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    date TEXT NOT NULL,
    action TEXT NOT NULL,        -- SELL_CALL / BUY_TO_CLOSE / EXPIRE / ASSIGN
    underlying TEXT,
    strike REAL,
    expiry TEXT,
    contracts INTEGER,
    premium REAL NOT NULL DEFAULT 0,
    pnl REAL NOT NULL DEFAULT 0,
    spy_price REAL,
    detail TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES alpha_strategies(id)
);

-- Alpha 策略日快照（用于绩效评估）
CREATE TABLE IF NOT EXISTS alpha_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    date TEXT NOT NULL,
    capital REAL NOT NULL,
    nav REAL NOT NULL,
    daily_return REAL NOT NULL DEFAULT 0,
    cumulative_return REAL NOT NULL DEFAULT 0,
    drawdown REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(strategy_id, date)
);
