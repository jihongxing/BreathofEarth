-- 税损收割扩展表（Tax-Loss Harvesting Extension）

-- 税损收割事件表
CREATE TABLE IF NOT EXISTS tax_harvest_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    date TEXT NOT NULL,
    sold_asset TEXT NOT NULL,              -- 卖出的亏损资产（如 "SPY"）
    substitute_asset TEXT NOT NULL,        -- 买入的替代品（如 "VOO"）
    quantity REAL NOT NULL,                -- 交易数量
    cost_basis REAL NOT NULL,              -- 原始成本
    sale_price REAL NOT NULL,              -- 卖出价格
    loss_harvested REAL NOT NULL,          -- 收割的税损金额
    washsale_safe_date TEXT NOT NULL,      -- 可以换回的安全日期（30天后）
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING / REVERSED / EXPIRED
    reversed_at TEXT,                      -- 换回日期
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, date, sold_asset)
);

-- 资产成本基础表（用于计算未实现损益）
CREATE TABLE IF NOT EXISTS asset_cost_basis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    asset TEXT NOT NULL,                   -- 资产代码
    purchase_date TEXT NOT NULL,           -- 购买日期
    quantity REAL NOT NULL,                -- 数量
    cost_per_share REAL NOT NULL,          -- 单位成本
    total_cost REAL NOT NULL,              -- 总成本
    status TEXT NOT NULL DEFAULT 'HOLDING', -- HOLDING / SOLD / HARVESTED
    sold_date TEXT,                        -- 卖出日期
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, asset, purchase_date)
);

-- 年度税务报告表
CREATE TABLE IF NOT EXISTS annual_tax_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    year INTEGER NOT NULL,
    total_realized_gains REAL NOT NULL DEFAULT 0.0,    -- 已实现收益
    total_realized_losses REAL NOT NULL DEFAULT 0.0,   -- 已实现损失
    total_harvested_losses REAL NOT NULL DEFAULT 0.0,  -- 通过税损收割获得的损失
    net_taxable_income REAL NOT NULL DEFAULT 0.0,      -- 净应税收入
    estimated_tax_saved REAL NOT NULL DEFAULT 0.0,     -- 预估节省的税款
    harvest_count INTEGER NOT NULL DEFAULT 0,          -- 收割次数
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, year)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_harvest_events_portfolio_date
    ON tax_harvest_events(portfolio_id, date);

CREATE INDEX IF NOT EXISTS idx_harvest_events_status
    ON tax_harvest_events(status);

CREATE INDEX IF NOT EXISTS idx_cost_basis_portfolio_asset
    ON asset_cost_basis(portfolio_id, asset);

CREATE INDEX IF NOT EXISTS idx_cost_basis_status
    ON asset_cost_basis(status);
