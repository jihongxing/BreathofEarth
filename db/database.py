"""
息壤（Xi-Rang）数据库层

SQLite 封装，负责持久化组合状态、快照、交易记录。
"""

import sqlite3
import json
from pathlib import Path
from typing import Optional
from contextlib import contextmanager


DB_PATH = Path(__file__).parent / "xirang.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
SCHEMA_TAX_HARVEST_PATH = Path(__file__).parent / "schema_tax_harvest.sql"
SCHEMA_GOVERNANCE_PATH = Path(__file__).parent / "schema_governance.sql"
SCHEMA_ALPHA_PATH = Path(__file__).parent / "schema_alpha.sql"


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        """初始化数据库，执行 schema"""
        with self._conn() as conn:
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            # 迁移：为已有 portfolios 表添加 stability_balance 字段
            try:
                conn.execute("SELECT stability_balance FROM portfolios LIMIT 1")
            except Exception:
                conn.execute("ALTER TABLE portfolios ADD COLUMN stability_balance REAL NOT NULL DEFAULT 0.0")
            # 加载税损收割扩展表
            if SCHEMA_TAX_HARVEST_PATH.exists():
                conn.executescript(SCHEMA_TAX_HARVEST_PATH.read_text(encoding="utf-8"))
            # 加载治理层扩展表
            if SCHEMA_GOVERNANCE_PATH.exists():
                conn.executescript(SCHEMA_GOVERNANCE_PATH.read_text(encoding="utf-8"))
            # 加载 Alpha 沙盒扩展表
            if SCHEMA_ALPHA_PATH.exists():
                conn.executescript(SCHEMA_ALPHA_PATH.read_text(encoding="utf-8"))
                self._migrate_alpha_schema(conn)

    def _table_exists(self, conn, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, conn, table_name: str) -> list[dict]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [dict(r) for r in rows]

    def _migrate_alpha_schema(self, conn):
        """将旧版 Alpha 表迁移到按 portfolio_id 隔离的新结构。"""
        if self._table_exists(conn, "alpha_accounts"):
            account_cols = {c["name"] for c in self._table_columns(conn, "alpha_accounts")}
            if "cash_balance" not in account_cols:
                conn.execute("ALTER TABLE alpha_accounts ADD COLUMN cash_balance REAL NOT NULL DEFAULT 0")
            if "total_inflows" not in account_cols:
                conn.execute("ALTER TABLE alpha_accounts ADD COLUMN total_inflows REAL NOT NULL DEFAULT 0")
            if "total_outflows" not in account_cols:
                conn.execute("ALTER TABLE alpha_accounts ADD COLUMN total_outflows REAL NOT NULL DEFAULT 0")
            if "last_manual_adjustment" not in account_cols:
                conn.execute("ALTER TABLE alpha_accounts ADD COLUMN last_manual_adjustment TEXT")

        if self._table_exists(conn, "alpha_strategies"):
            strategy_cols = self._table_columns(conn, "alpha_strategies")
            pk_cols = [c["name"] for c in strategy_cols if c["pk"]]
            if pk_cols != ["portfolio_id", "id"]:
                self._rebuild_alpha_strategies(conn)

        if self._table_exists(conn, "alpha_snapshots"):
            snapshot_cols = {c["name"] for c in self._table_columns(conn, "alpha_snapshots")}
            if "portfolio_id" not in snapshot_cols:
                self._rebuild_alpha_snapshots(conn)

    def _rebuild_alpha_strategies(self, conn):
        """重建 Alpha 策略主表和交易表，按 portfolio_id + strategy_id 作用域隔离。"""
        if self._table_exists(conn, "alpha_transactions"):
            conn.execute("ALTER TABLE alpha_transactions RENAME TO alpha_transactions_legacy")
        conn.execute("ALTER TABLE alpha_strategies RENAME TO alpha_strategies_legacy")

        conn.executescript(
            """
            CREATE TABLE alpha_strategies (
                portfolio_id TEXT NOT NULL DEFAULT 'us',
                id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'DISABLED',
                allocation_pct REAL NOT NULL DEFAULT 0.10,
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

            CREATE TABLE alpha_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id TEXT NOT NULL DEFAULT 'us',
                strategy_id TEXT NOT NULL,
                date TEXT NOT NULL,
                action TEXT NOT NULL,
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
            """
        )

        conn.execute(
            """
            INSERT INTO alpha_strategies (
                portfolio_id, id, name, description, status, allocation_pct, capital,
                total_premium, total_pnl, trade_count, enabled_at, disabled_at,
                created_at, updated_at
            )
            SELECT
                COALESCE(portfolio_id, 'us'),
                id, name, description, status, allocation_pct, capital,
                total_premium, total_pnl, trade_count, enabled_at, disabled_at,
                created_at, updated_at
            FROM alpha_strategies_legacy
            """
        )

        if self._table_exists(conn, "alpha_transactions_legacy"):
            conn.execute(
                """
                INSERT INTO alpha_transactions (
                    id, portfolio_id, strategy_id, date, action, underlying, strike,
                    expiry, contracts, premium, pnl, spy_price, detail, created_at
                )
                SELECT
                    id, COALESCE(portfolio_id, 'us'), strategy_id, date, action, underlying, strike,
                    expiry, contracts, premium, pnl, spy_price, detail, created_at
                FROM alpha_transactions_legacy
                """
            )
            conn.execute("DROP TABLE alpha_transactions_legacy")

        conn.execute("DROP TABLE alpha_strategies_legacy")

    def _rebuild_alpha_snapshots(self, conn):
        """为 Alpha 快照增加 portfolio_id 作用域。"""
        conn.execute("ALTER TABLE alpha_snapshots RENAME TO alpha_snapshots_legacy")
        conn.executescript(
            """
            CREATE TABLE alpha_snapshots (
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
            """
        )
        conn.execute(
            """
            INSERT INTO alpha_snapshots (
                id, portfolio_id, strategy_id, date, capital, nav,
                daily_return, cumulative_return, drawdown, created_at
            )
            SELECT
                id, 'us', strategy_id, date, capital, nav,
                daily_return, cumulative_return, drawdown, created_at
            FROM alpha_snapshots_legacy
            """
        )
        conn.execute("DROP TABLE alpha_snapshots_legacy")

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        """
        显式事务上下文管理器。

        用于将多个数据库操作包装在一个事务中，确保原子性。

        Example:
            with db.transaction() as conn:
                db.update_portfolio(...)
                db.save_snapshot(...)
                db.save_transaction(...)
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── 组合状态 ──────────────────────────────────────

    def get_portfolio(self, portfolio_id: str = "default") -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"组合 {portfolio_id} 不存在")
            return dict(row)

    def ensure_portfolio(self, portfolio_id: str, assets: list):
        """确保组合存在，不存在则创建"""
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
            if row is None:
                initial_nav = 100000.0
                n = len(assets)
                positions = [initial_nav / n] * n
                conn.execute(
                    """INSERT INTO portfolios (id, state, nav, positions, high_water_mark,
                       cooldown_counter, rebalance_count, protection_count)
                       VALUES (?, 'IDLE', ?, ?, ?, 0, 0, 0)""",
                    (portfolio_id, initial_nav, json.dumps(positions), initial_nav),
                )

    def update_portfolio(self, portfolio_id: str = "default", conn=None, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [portfolio_id]
        sql = f"UPDATE portfolios SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    # ── 快照 ──────────────────────────────────────────

    def save_snapshot(
        self,
        date: str,
        state: str,
        nav: float,
        positions: list,
        weights: list,
        drawdown: float,
        spy_tlt_corr: float = 0.0,
        action: Optional[str] = None,
        trigger_reason: Optional[str] = None,
        portfolio_id: str = "default",
        conn=None,
    ):
        sql = """INSERT OR REPLACE INTO daily_snapshots
                   (portfolio_id, date, state, nav, positions, weights, drawdown,
                    spy_tlt_corr, action, trigger_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id, date, state, nav,
            json.dumps(positions), json.dumps(weights),
            drawdown, spy_tlt_corr, action, trigger_reason,
        )

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_snapshots(self, portfolio_id: str = "default", limit: int = 30) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM daily_snapshots
                   WHERE portfolio_id = ?
                   ORDER BY date DESC LIMIT ?""",
                (portfolio_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── 交易记录 ──────────────────────────────────────

    def save_transaction(
        self,
        date: str,
        tx_type: str,
        target_weights: Optional[list] = None,
        turnover: float = 0.0,
        friction_cost: float = 0.0,
        reason: str = "",
        portfolio_id: str = "default",
        conn=None,
    ):
        sql = """INSERT INTO transactions
                   (portfolio_id, date, type, target_weights, turnover, friction_cost, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id, date, tx_type,
            json.dumps(target_weights) if target_weights else None,
            turnover, friction_cost, reason,
        )

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    # ── 风控事件 ──────────────────────────────────────

    def save_risk_event(
        self,
        date: str,
        event_type: str,
        severity: str,
        drawdown: float = 0.0,
        spy_tlt_corr: float = 0.0,
        action_taken: str = "",
        portfolio_id: str = "default",
        conn=None,
    ):
        sql = """INSERT INTO risk_events
                   (portfolio_id, date, event_type, severity, drawdown, spy_tlt_corr, action_taken)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"""
        values = (portfolio_id, date, event_type, severity, drawdown, spy_tlt_corr, action_taken)

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    # ── 幂等性保护 ────────────────────────────────────

    def has_run_today(self, date: str, portfolio_id: str = "default") -> bool:
        """检查今天是否已经成功运行过"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM daily_runs WHERE portfolio_id = ? AND date = ? AND status = 'SUCCESS'",
                (portfolio_id, date),
            ).fetchone()
            return row is not None

    def record_run(self, date: str, status: str = "SUCCESS", report: str = "", portfolio_id: str = "default", conn=None):
        """记录本次运行"""
        sql = """INSERT OR REPLACE INTO daily_runs (portfolio_id, date, status, report)
                   VALUES (?, ?, ?, ?)"""
        values = (portfolio_id, date, status, report)

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    # ── 税损收割（Tax-Loss Harvesting）────────────────

    def save_harvest_event(
        self,
        date: str,
        sold_asset: str,
        substitute_asset: str,
        quantity: float,
        cost_basis: float,
        sale_price: float,
        loss_harvested: float,
        washsale_safe_date: str,
        portfolio_id: str = "us",
        conn=None,
    ):
        """记录税损收割事件"""
        sql = """INSERT INTO tax_harvest_events
                   (portfolio_id, date, sold_asset, substitute_asset, quantity,
                    cost_basis, sale_price, loss_harvested, washsale_safe_date, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')"""
        values = (
            portfolio_id, date, sold_asset, substitute_asset, quantity,
            cost_basis, sale_price, loss_harvested, washsale_safe_date,
        )

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_pending_reversals(self, today: str, portfolio_id: str = "us") -> list[dict]:
        """获取可以换回的税损收割事件（已过 30 天 Wash Sale 期）"""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM tax_harvest_events
                   WHERE portfolio_id = ? AND status = 'PENDING'
                   AND washsale_safe_date <= ?
                   ORDER BY date""",
                (portfolio_id, today),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_harvest_reversed(self, harvest_id: int, reversed_date: str, conn=None):
        """标记税损收割已换回"""
        sql = """UPDATE tax_harvest_events
                 SET status = 'REVERSED', reversed_at = ?
                 WHERE id = ?"""
        values = (reversed_date, harvest_id)

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def save_cost_basis(
        self,
        asset: str,
        purchase_date: str,
        quantity: float,
        cost_per_share: float,
        portfolio_id: str = "us",
        conn=None,
    ):
        """记录资产成本基础"""
        total_cost = quantity * cost_per_share
        sql = """INSERT OR REPLACE INTO asset_cost_basis
                   (portfolio_id, asset, purchase_date, quantity, cost_per_share, total_cost, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'HOLDING')"""
        values = (portfolio_id, asset, purchase_date, quantity, cost_per_share, total_cost)

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_cost_basis(self, asset: str, portfolio_id: str = "us") -> Optional[dict]:
        """获取资产的成本基础（FIFO：先进先出）"""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM asset_cost_basis
                   WHERE portfolio_id = ? AND asset = ? AND status = 'HOLDING'
                   ORDER BY purchase_date ASC LIMIT 1""",
                (portfolio_id, asset),
            ).fetchone()
            return dict(row) if row else None

    def update_annual_tax_report(
        self,
        year: int,
        portfolio_id: str = "us",
        conn=None,
        **kwargs,
    ):
        """更新年度税务报告"""
        # 先尝试插入，如果已存在则更新
        with self._conn() as c:
            existing = c.execute(
                "SELECT 1 FROM annual_tax_reports WHERE portfolio_id = ? AND year = ?",
                (portfolio_id, year),
            ).fetchone()

            if existing:
                sets = ", ".join(f"{k} = ?" for k in kwargs)
                values = list(kwargs.values()) + [portfolio_id, year]
                sql = f"""UPDATE annual_tax_reports
                          SET {sets}, updated_at = CURRENT_TIMESTAMP
                          WHERE portfolio_id = ? AND year = ?"""
                c.execute(sql, values)
            else:
                # 插入新记录
                fields = ["portfolio_id", "year"] + list(kwargs.keys())
                placeholders = ", ".join(["?"] * len(fields))
                values = [portfolio_id, year] + list(kwargs.values())
                sql = f"""INSERT INTO annual_tax_reports ({", ".join(fields)})
                          VALUES ({placeholders})"""
                c.execute(sql, values)

    def get_annual_tax_report(self, year: int, portfolio_id: str = "us") -> Optional[dict]:
        """获取年度税务报告"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM annual_tax_reports WHERE portfolio_id = ? AND year = ?",
                (portfolio_id, year),
            ).fetchone()
            return dict(row) if row else None

    # ── 治理层（Governance）─────────────────────────────

    def create_withdrawal_request(
        self,
        withdrawal_id: str,
        amount: float,
        reason: str,
        requester: str,
        expires_at: str,
        portfolio_id: str = "us",
        required_approvals: int = 2,
        cooling_days: int = 7,
        conn=None,
    ):
        """创建出金请求"""
        sql = """INSERT INTO withdrawal_requests
                   (id, portfolio_id, amount, reason, requester,
                    required_approvals, cooling_days, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (withdrawal_id, portfolio_id, amount, reason, requester,
                  required_approvals, cooling_days, expires_at)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_withdrawal_request(self, withdrawal_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM withdrawal_requests WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_withdrawal_requests(self, status: Optional[str] = None, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM withdrawal_requests WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM withdrawal_requests ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def update_withdrawal_status(self, withdrawal_id: str, status: str, conn=None):
        sql = "UPDATE withdrawal_requests SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        values = (status, withdrawal_id)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def save_deposit_record(
        self, deposit_id: str, amount: float, depositor: str,
        portfolio_id: str = "us", allocation: str = "", conn=None,
    ):
        sql = """INSERT INTO deposit_records (id, portfolio_id, amount, depositor, allocation)
                 VALUES (?, ?, ?, ?, ?)"""
        values = (deposit_id, portfolio_id, amount, depositor, allocation)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def list_deposit_records(self, portfolio_id: str = None, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            if portfolio_id:
                rows = conn.execute(
                    "SELECT * FROM deposit_records WHERE portfolio_id = ? ORDER BY created_at DESC LIMIT ?",
                    (portfolio_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM deposit_records ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def add_withdrawal_approval(
        self, withdrawal_id: str, approver: str, decision: str = "APPROVED", comment: str = "", conn=None
    ):
        sql = """INSERT OR REPLACE INTO withdrawal_approvals
                   (withdrawal_id, approver, decision, comment) VALUES (?, ?, ?, ?)"""
        values = (withdrawal_id, approver, decision, comment)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_withdrawal_approvals(self, withdrawal_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM withdrawal_approvals WHERE withdrawal_id = ? ORDER BY created_at",
                (withdrawal_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_audit_log(self, action: str, actor: str, detail: str = "", ip_address: str = "", conn=None):
        sql = "INSERT INTO audit_log (action, actor, detail, ip_address) VALUES (?, ?, ?, ?)"
        values = (action, actor, detail, ip_address)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_audit_log(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ── API 用户 ──────────────────────────────────────

    def get_user_by_username(self, username: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM api_users WHERE username = ? AND is_active = 1", (username,)
            ).fetchone()
            return dict(row) if row else None

    def create_user(self, username: str, password_hash: str, role: str = "viewer",
                    display_name: str = "", email: str = ""):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO api_users (username, password_hash, role, display_name, email)
                   VALUES (?, ?, ?, ?, ?)""",
                (username, password_hash, role, display_name, email),
            )

    # ── Alpha 沙盒策略 ────────────────────────────────

    def ensure_alpha_account(self, portfolio_id: str = "us"):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO alpha_accounts (portfolio_id) VALUES (?)",
                (portfolio_id,),
            )

    def get_alpha_account(self, portfolio_id: str = "us") -> dict:
        self.ensure_alpha_account(portfolio_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM alpha_accounts WHERE portfolio_id = ?",
                (portfolio_id,),
            ).fetchone()
            return dict(row) if row else {
                "portfolio_id": portfolio_id,
                "cash_balance": 0.0,
                "total_inflows": 0.0,
                "total_outflows": 0.0,
                "last_manual_adjustment": None,
            }

    def update_alpha_account(self, portfolio_id: str = "us", conn=None, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [portfolio_id]
        sql = f"UPDATE alpha_accounts SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE portfolio_id = ?"

        if conn:
            conn.execute(
                "INSERT OR IGNORE INTO alpha_accounts (portfolio_id) VALUES (?)",
                (portfolio_id,),
            )
            conn.execute(sql, values)
        else:
            self.ensure_alpha_account(portfolio_id)
            with self._conn() as c:
                c.execute(sql, values)

    def adjust_alpha_account_balance(
        self,
        portfolio_id: str,
        delta: float,
        note: str = "",
        conn=None,
    ) -> dict:
        if conn:
            conn.execute(
                "INSERT OR IGNORE INTO alpha_accounts (portfolio_id) VALUES (?)",
                (portfolio_id,),
            )
            row = conn.execute(
                "SELECT * FROM alpha_accounts WHERE portfolio_id = ?",
                (portfolio_id,),
            ).fetchone()
            account = dict(row) if row else {
                "portfolio_id": portfolio_id,
                "cash_balance": 0.0,
                "total_inflows": 0.0,
                "total_outflows": 0.0,
                "last_manual_adjustment": None,
            }
        else:
            self.ensure_alpha_account(portfolio_id)
            account = self.get_alpha_account(portfolio_id)
        new_balance = float(account.get("cash_balance", 0.0)) + delta
        if new_balance < 0:
            raise ValueError(f"Alpha 账本余额不足，调整后将为 {new_balance:.2f}")

        inflows = float(account.get("total_inflows", 0.0))
        outflows = float(account.get("total_outflows", 0.0))
        if delta >= 0:
            inflows += delta
        else:
            outflows += abs(delta)

        self.update_alpha_account(
            portfolio_id=portfolio_id,
            cash_balance=round(new_balance, 2),
            total_inflows=round(inflows, 2),
            total_outflows=round(outflows, 2),
            last_manual_adjustment=note,
            conn=conn,
        )
        if conn:
            row = conn.execute(
                "SELECT * FROM alpha_accounts WHERE portfolio_id = ?",
                (portfolio_id,),
            ).fetchone()
            return dict(row) if row else {
                "portfolio_id": portfolio_id,
                "cash_balance": round(new_balance, 2),
                "total_inflows": round(inflows, 2),
                "total_outflows": round(outflows, 2),
                "last_manual_adjustment": note,
            }
        return self.get_alpha_account(portfolio_id)

    def record_alpha_ledger_entry(
        self,
        portfolio_id: str,
        direction: str,
        amount: float,
        actor: str,
        note: str = "",
        external_reference: str = "",
        related_request_id: str = "",
    ) -> tuple[dict, dict]:
        direction = direction.upper()
        if direction not in ("IN", "OUT"):
            raise ValueError(f"不支持的 Alpha 账本方向: {direction}")
        if amount <= 0:
            raise ValueError("记账金额必须大于 0")

        delta = amount if direction == "IN" else -amount
        entry_note = note or (f"Alpha 人工入账 +{amount:.2f}" if direction == "IN" else f"Alpha 人工出账 -{amount:.2f}")

        with self.transaction() as conn:
            account = self.adjust_alpha_account_balance(
                portfolio_id=portfolio_id,
                delta=delta,
                note=entry_note,
                conn=conn,
            )
            cursor = conn.execute(
                """INSERT INTO alpha_ledger_entries
                   (portfolio_id, direction, amount, balance_after, note,
                    external_reference, related_request_id, actor)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    portfolio_id,
                    direction,
                    round(amount, 2),
                    round(float(account.get("cash_balance", 0.0)), 2),
                    entry_note,
                    external_reference,
                    related_request_id,
                    actor,
                ),
            )
            entry_id = cursor.lastrowid

        return self.get_alpha_ledger_entry(entry_id), self.get_alpha_account(portfolio_id)

    def get_alpha_ledger_entry(self, entry_id: int) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM alpha_ledger_entries WHERE id = ?",
                (entry_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_alpha_ledger_entries(
        self,
        portfolio_id: str = "us",
        direction: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        with self._conn() as conn:
            if direction:
                rows = conn.execute(
                    """SELECT * FROM alpha_ledger_entries
                       WHERE portfolio_id = ? AND direction = ?
                       ORDER BY created_at DESC, id DESC LIMIT ?""",
                    (portfolio_id, direction.upper(), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM alpha_ledger_entries
                       WHERE portfolio_id = ?
                       ORDER BY created_at DESC, id DESC LIMIT ?""",
                    (portfolio_id, limit),
                ).fetchall()
            return [dict(r) for r in rows]

    def create_alpha_withdrawal_request(
        self,
        request_id: str,
        amount: float,
        reason: str,
        requester: str,
        portfolio_id: str = "us",
        status: str = "PENDING_MANUAL",
        conn=None,
    ):
        sql = """INSERT INTO alpha_withdrawal_requests
                   (id, portfolio_id, amount, reason, requester, status)
                   VALUES (?, ?, ?, ?, ?, ?)"""
        values = (request_id, portfolio_id, amount, reason, requester, status)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def has_year_end_rebalance(self, year: int, portfolio_id: str = "default") -> bool:
        """检查指定年份是否已执行过年末强制再平衡。"""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT 1 FROM transactions
                   WHERE portfolio_id = ?
                     AND type = 'REBALANCE'
                     AND reason LIKE '年末强制再平衡%'
                     AND substr(date, 1, 4) = ?
                   LIMIT 1""",
                (portfolio_id, str(year)),
            ).fetchone()
            return row is not None

    def get_alpha_withdrawal_request(self, request_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM alpha_withdrawal_requests WHERE id = ?",
                (request_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_alpha_withdrawal_requests(
        self,
        portfolio_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        with self._conn() as conn:
            where = []
            values = []
            if portfolio_id:
                where.append("portfolio_id = ?")
                values.append(portfolio_id)
            if status:
                where.append("status = ?")
                values.append(status)

            where_sql = f"WHERE {' AND '.join(where)}" if where else ""
            rows = conn.execute(
                f"""SELECT * FROM alpha_withdrawal_requests
                    {where_sql}
                    ORDER BY created_at DESC LIMIT ?""",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

    def update_alpha_withdrawal_request(self, request_id: str, conn=None, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [request_id]
        sql = f"""UPDATE alpha_withdrawal_requests
                  SET {sets}, updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?"""
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_strategy(self, strategy_id: str, portfolio_id: str = "us") -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM alpha_strategies WHERE portfolio_id = ? AND id = ?",
                (portfolio_id, strategy_id),
            ).fetchone()
            return dict(row) if row else None

    def list_strategies(self, portfolio_id: str = None) -> list[dict]:
        with self._conn() as conn:
            if portfolio_id:
                rows = conn.execute(
                    "SELECT * FROM alpha_strategies WHERE portfolio_id = ? ORDER BY created_at",
                    (portfolio_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM alpha_strategies ORDER BY created_at").fetchall()
            return [dict(r) for r in rows]

    def upsert_strategy(self, strategy_id: str, portfolio_id: str = "us", **kwargs):
        """创建或更新策略"""
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT 1 FROM alpha_strategies WHERE portfolio_id = ? AND id = ?",
                (portfolio_id, strategy_id),
            ).fetchone()
            if existing:
                sets = ", ".join(f"{k} = ?" for k in kwargs)
                values = list(kwargs.values()) + [portfolio_id, strategy_id]
                conn.execute(
                    f"UPDATE alpha_strategies SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE portfolio_id = ? AND id = ?",
                    values,
                )
            else:
                fields = ["portfolio_id", "id"] + list(kwargs.keys())
                placeholders = ", ".join(["?"] * len(fields))
                values = [portfolio_id, strategy_id] + list(kwargs.values())
                conn.execute(
                    f"INSERT INTO alpha_strategies ({', '.join(fields)}) VALUES ({placeholders})",
                    values,
                )

    def update_strategy_status(self, strategy_id: str, status: str, portfolio_id: str = "us"):
        """启用/禁用策略"""
        now = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            extra = ""
            if status == "ENABLED":
                extra = ", enabled_at = ?"
            elif status in ("DISABLED", "SUSPENDED"):
                extra = ", disabled_at = ?"
            conn.execute(
                f"UPDATE alpha_strategies SET status = ?{extra}, updated_at = CURRENT_TIMESTAMP WHERE portfolio_id = ? AND id = ?",
                (status, now, portfolio_id, strategy_id) if extra else (status, portfolio_id, strategy_id),
            )

    def save_alpha_transaction(self, strategy_id: str, portfolio_id: str, date: str,
                                action: str, premium: float = 0, pnl: float = 0, **kwargs):
        """记录 Alpha 策略交易"""
        fields = ["strategy_id", "portfolio_id", "date", "action", "premium", "pnl"]
        values = [strategy_id, portfolio_id, date, action, premium, pnl]
        for k, v in kwargs.items():
            fields.append(k)
            values.append(v)
        placeholders = ", ".join(["?"] * len(fields))
        with self._conn() as conn:
            conn.execute(
                f"INSERT INTO alpha_transactions ({', '.join(fields)}) VALUES ({placeholders})",
                values,
            )
            # 更新策略汇总
            conn.execute(
                """UPDATE alpha_strategies
                   SET total_premium = total_premium + ?,
                       total_pnl = total_pnl + ?,
                       trade_count = trade_count + 1,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE portfolio_id = ? AND id = ?""",
                (premium, pnl, portfolio_id, strategy_id),
            )

    def get_alpha_transactions(self, strategy_id: str, portfolio_id: str = "us", limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM alpha_transactions
                   WHERE portfolio_id = ? AND strategy_id = ?
                   ORDER BY date DESC LIMIT ?""",
                (portfolio_id, strategy_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_alpha_snapshot(self, strategy_id: str, portfolio_id: str, date: str, capital: float,
                             nav: float, daily_return: float = 0, cumulative_return: float = 0,
                             drawdown: float = 0):
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO alpha_snapshots
                   (portfolio_id, strategy_id, date, capital, nav, daily_return, cumulative_return, drawdown)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (portfolio_id, strategy_id, date, capital, nav, daily_return, cumulative_return, drawdown),
            )
