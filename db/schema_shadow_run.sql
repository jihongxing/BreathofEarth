CREATE TABLE IF NOT EXISTS shadow_run_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL,
    broker_role TEXT NOT NULL DEFAULT 'sandbox',
    broker_name TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    dry_run INTEGER NOT NULL DEFAULT 1,
    order_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_status TEXT,
    requires_attention INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    report_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
