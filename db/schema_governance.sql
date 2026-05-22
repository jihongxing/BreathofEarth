-- 治理层：出金请求与审批

CREATE TABLE IF NOT EXISTS withdrawal_requests (
    id TEXT PRIMARY KEY,
    family_office_id TEXT NOT NULL DEFAULT 'default',
    account_id TEXT,
    member_id TEXT,
    requested_by_user_id INTEGER,
    approved_by_user_id INTEGER,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    source_pool_id TEXT,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    shares_requested REAL NOT NULL DEFAULT 0,
    share_price REAL,
    shares_redeemed REAL NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    requester TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING / APPROVED / REJECTED / EXPIRED / EXECUTED
    required_approvals INTEGER NOT NULL DEFAULT 2,
    cooling_days INTEGER NOT NULL DEFAULT 7,
    expires_at TEXT NOT NULL,
    executed_by TEXT,
    executed_at TEXT,
    ledger_entry_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS withdrawal_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    withdrawal_id TEXT NOT NULL,
    approver TEXT NOT NULL,
    approver_user_id INTEGER,
    approver_role TEXT,
    decision TEXT NOT NULL DEFAULT 'APPROVED',  -- APPROVED / REJECTED
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (withdrawal_id) REFERENCES withdrawal_requests(id),
    UNIQUE(withdrawal_id, approver)
);

CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_account
ON withdrawal_requests(account_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_pool
ON withdrawal_requests(source_pool_id, status, created_at);

-- 审计日志
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    detail TEXT,
    ip_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 入金记录
CREATE TABLE IF NOT EXISTS deposit_records (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    account_id TEXT,
    deposit_request_id TEXT,
    ledger_entry_id TEXT,
    amount REAL NOT NULL,
    depositor TEXT NOT NULL,
    allocation TEXT,  -- JSON: 各资产分配明细
    shares_issued REAL NOT NULL DEFAULT 0,
    share_price REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API 用户（JWT 认证）
CREATE TABLE IF NOT EXISTS api_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',  -- admin / member / viewer
    member_id TEXT,
    display_name TEXT,
    email TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
