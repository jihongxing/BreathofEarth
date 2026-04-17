CREATE TABLE IF NOT EXISTS broker_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL,
    run_date TEXT NOT NULL,
    broker_role TEXT NOT NULL,
    broker_name TEXT NOT NULL,
    broker_mode TEXT,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    order_id TEXT,
    client_order_id TEXT,
    broker_reference TEXT,
    symbol TEXT,
    side TEXT,
    requested_quantity INTEGER NOT NULL DEFAULT 0,
    filled_quantity INTEGER NOT NULL DEFAULT 0,
    avg_fill_price REAL,
    commission REAL,
    status TEXT,
    message TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_broker_execution_events_portfolio_run
ON broker_execution_events (portfolio_id, run_date, id DESC);

CREATE INDEX IF NOT EXISTS idx_broker_execution_events_order
ON broker_execution_events (order_id, broker_name, broker_role);
