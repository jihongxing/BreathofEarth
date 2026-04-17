CREATE TABLE IF NOT EXISTS broker_account_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL,
    broker_role TEXT NOT NULL,
    broker_name TEXT NOT NULL,
    broker_mode TEXT NOT NULL,
    account_id TEXT,
    currency TEXT,
    cash REAL NOT NULL DEFAULT 0,
    total_value REAL NOT NULL DEFAULT 0,
    positions_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT '{}',
    snapshot_time TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS broker_reconciliation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL,
    broker_role TEXT NOT NULL,
    broker_name TEXT NOT NULL,
    status TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    items_json TEXT NOT NULL DEFAULT '[]',
    report_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
