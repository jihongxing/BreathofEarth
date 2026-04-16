-- Alpha 沙盒策略层

-- Alpha 独立资金账本（与 Core/Stability 分离）
CREATE TABLE IF NOT EXISTS alpha_accounts (
    portfolio_id TEXT PRIMARY KEY,
    cash_balance REAL NOT NULL DEFAULT 0,
    total_inflows REAL NOT NULL DEFAULT 0,
    total_outflows REAL NOT NULL DEFAULT 0,
    last_manual_adjustment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alpha 账本出金申请（仅申请与留痕，不在系统内自动执行）
CREATE TABLE IF NOT EXISTS alpha_withdrawal_requests (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    amount REAL NOT NULL,
    reason TEXT NOT NULL,
    requester TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING_MANUAL',  -- PENDING_MANUAL / HANDLED / REJECTED / CANCELLED
    external_reference TEXT,
    handled_by TEXT,
    handled_note TEXT,
    handled_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alpha 账本人工记账流水（线下资金动作在系统内补记，不触发真实打款）
CREATE TABLE IF NOT EXISTS alpha_ledger_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    direction TEXT NOT NULL,  -- IN / OUT
    amount REAL NOT NULL,
    balance_after REAL NOT NULL,
    note TEXT,
    external_reference TEXT,
    related_request_id TEXT,
    actor TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 策略注册表（手动开启/关闭）
CREATE TABLE IF NOT EXISTS alpha_strategies (
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'DISABLED',  -- ENABLED / DISABLED / SUSPENDED
    allocation_pct REAL NOT NULL DEFAULT 0.10,  -- 占 Alpha 独立账本的比例
    capital REAL NOT NULL DEFAULT 0,
    total_premium REAL NOT NULL DEFAULT 0,
    total_pnl REAL NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    enabled_at TEXT,
    disabled_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, id)
);

-- Alpha 策略交易记录
CREATE TABLE IF NOT EXISTS alpha_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    strategy_id TEXT NOT NULL,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alpha 策略日快照（用于绩效评估）
CREATE TABLE IF NOT EXISTS alpha_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    strategy_id TEXT NOT NULL,
    date TEXT NOT NULL,
    capital REAL NOT NULL,
    nav REAL NOT NULL,
    daily_return REAL NOT NULL DEFAULT 0,
    cumulative_return REAL NOT NULL DEFAULT 0,
    drawdown REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, strategy_id, date)
);
