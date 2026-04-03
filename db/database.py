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

    def get_strategy(self, strategy_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM alpha_strategies WHERE id = ?", (strategy_id,)).fetchone()
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

    def upsert_strategy(self, strategy_id: str, **kwargs):
        """创建或更新策略"""
        with self._conn() as conn:
            existing = conn.execute("SELECT 1 FROM alpha_strategies WHERE id = ?", (strategy_id,)).fetchone()
            if existing:
                sets = ", ".join(f"{k} = ?" for k in kwargs)
                values = list(kwargs.values()) + [strategy_id]
                conn.execute(
                    f"UPDATE alpha_strategies SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    values,
                )
            else:
                fields = ["id"] + list(kwargs.keys())
                placeholders = ", ".join(["?"] * len(fields))
                values = [strategy_id] + list(kwargs.values())
                conn.execute(
                    f"INSERT INTO alpha_strategies ({', '.join(fields)}) VALUES ({placeholders})",
                    values,
                )

    def update_strategy_status(self, strategy_id: str, status: str):
        """启用/禁用策略"""
        now = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            extra = ""
            if status == "ENABLED":
                extra = ", enabled_at = ?"
            elif status in ("DISABLED", "SUSPENDED"):
                extra = ", disabled_at = ?"
            conn.execute(
                f"UPDATE alpha_strategies SET status = ?{extra}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, now, strategy_id) if extra else (status, strategy_id),
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
                   WHERE id = ?""",
                (premium, pnl, strategy_id),
            )

    def get_alpha_transactions(self, strategy_id: str, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alpha_transactions WHERE strategy_id = ? ORDER BY date DESC LIMIT ?",
                (strategy_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_alpha_snapshot(self, strategy_id: str, date: str, capital: float,
                             nav: float, daily_return: float = 0, cumulative_return: float = 0,
                             drawdown: float = 0):
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO alpha_snapshots
                   (strategy_id, date, capital, nav, daily_return, cumulative_return, drawdown)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (strategy_id, date, capital, nav, daily_return, cumulative_return, drawdown),
            )
