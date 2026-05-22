-- 家族办公室平台化：成员、资产账户、账户授权

CREATE TABLE IF NOT EXISTS family_offices (
    id TEXT PRIMARY KEY DEFAULT 'default',
    name TEXT NOT NULL,
    base_currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO family_offices (id, name, base_currency)
VALUES ('default', '默认家族办公室', 'USD');

CREATE TABLE IF NOT EXISTS family_members (
    id TEXT PRIMARY KEY,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    display_name TEXT NOT NULL,
    member_type TEXT NOT NULL DEFAULT 'individual',
    risk_profile TEXT NOT NULL DEFAULT 'balanced',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_office_id) REFERENCES family_offices(id)
);

CREATE TABLE IF NOT EXISTS capital_accounts (
    id TEXT PRIMARY KEY,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    member_id TEXT NOT NULL,
    account_name TEXT NOT NULL,
    base_currency TEXT NOT NULL DEFAULT 'USD',
    default_portfolio_id TEXT NOT NULL DEFAULT 'us',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_office_id) REFERENCES family_offices(id),
    FOREIGN KEY (member_id) REFERENCES family_members(id)
);

CREATE TABLE IF NOT EXISTS account_permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    permission TEXT NOT NULL DEFAULT 'view',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES api_users(id),
    FOREIGN KEY (account_id) REFERENCES capital_accounts(id),
    UNIQUE(user_id, account_id, permission)
);

CREATE INDEX IF NOT EXISTS idx_capital_accounts_member
ON capital_accounts(member_id);

CREATE INDEX IF NOT EXISTS idx_capital_accounts_portfolio
ON capital_accounts(default_portfolio_id);

CREATE INDEX IF NOT EXISTS idx_account_permissions_user
ON account_permissions(user_id);

CREATE INDEX IF NOT EXISTS idx_account_permissions_account
ON account_permissions(account_id);

-- Phase 2/3: 入金账本化与投资池份额

-- 入金申请：成员提交申请，管理员确认真实到账后才影响组合和总账。
CREATE TABLE IF NOT EXISTS deposit_requests (
    id TEXT PRIMARY KEY,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    account_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    requested_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'REQUESTED',
    confirmed_by TEXT,
    rejected_by TEXT,
    confirmed_at TEXT,
    rejected_at TEXT,
    external_reference TEXT,
    note TEXT,
    allocation TEXT,
    legacy_deposit_record_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_office_id) REFERENCES family_offices(id),
    FOREIGN KEY (account_id) REFERENCES capital_accounts(id)
);

CREATE TABLE IF NOT EXISTS investment_pools (
    id TEXT PRIMARY KEY,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    portfolio_id TEXT NOT NULL,
    pool_type TEXT NOT NULL DEFAULT 'core',
    currency TEXT NOT NULL DEFAULT 'USD',
    nav REAL NOT NULL DEFAULT 0,
    shares_outstanding REAL NOT NULL DEFAULT 0,
    share_price REAL NOT NULL DEFAULT 100,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    last_valued_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_office_id) REFERENCES family_offices(id)
);

CREATE TABLE IF NOT EXISTS pool_nav_snapshots (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    nav REAL NOT NULL,
    shares_outstanding REAL NOT NULL,
    share_price REAL NOT NULL,
    snapshot_date TEXT NOT NULL,
    is_locked INTEGER NOT NULL DEFAULT 1,
    source TEXT NOT NULL DEFAULT 'SYSTEM',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 账户对投资池的份额持仓。
CREATE TABLE IF NOT EXISTS account_pool_positions (
    account_id TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    shares REAL NOT NULL DEFAULT 0,
    cost_basis REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, pool_id),
    FOREIGN KEY (account_id) REFERENCES capital_accounts(id)
);

-- 平台总账：用户资产相关资金事件的可追溯事实表。
CREATE TABLE IF NOT EXISTS ledger_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    account_id TEXT NOT NULL,
    portfolio_id TEXT,
    pool_id TEXT,
    entry_type TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    shares_delta REAL,
    share_price REAL,
    actor TEXT NOT NULL,
    source_ref_type TEXT,
    source_ref_id TEXT,
    memo TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_office_id) REFERENCES family_offices(id),
    FOREIGN KEY (account_id) REFERENCES capital_accounts(id)
);

CREATE INDEX IF NOT EXISTS idx_deposit_requests_account
ON deposit_requests(account_id);

CREATE INDEX IF NOT EXISTS idx_deposit_requests_status
ON deposit_requests(status);

CREATE INDEX IF NOT EXISTS idx_investment_pools_portfolio
ON investment_pools(portfolio_id);

CREATE INDEX IF NOT EXISTS idx_pool_nav_snapshots_pool
ON pool_nav_snapshots(pool_id, is_locked, created_at);

CREATE INDEX IF NOT EXISTS idx_account_pool_positions_account
ON account_pool_positions(account_id);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account
ON ledger_entries(account_id, created_at);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_pool
ON ledger_entries(pool_id, created_at);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_source
ON ledger_entries(source_ref_type, source_ref_id);
