"""
息壤（Xi-Rang）数据库层

SQLite 封装，负责持久化组合状态、快照、交易记录。
"""

import sqlite3
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from engine.insurance import (
    RecoveryProposal,
    RecoveryStatus,
    build_authority_decision,
    coerce_insurance_state,
)


DB_PATH = Path(__file__).parent / "xirang.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"
SCHEMA_TAX_HARVEST_PATH = Path(__file__).parent / "schema_tax_harvest.sql"
SCHEMA_GOVERNANCE_PATH = Path(__file__).parent / "schema_governance.sql"
SCHEMA_FAMILY_OFFICE_PATH = Path(__file__).parent / "schema_family_office.sql"
SCHEMA_ALPHA_PATH = Path(__file__).parent / "schema_alpha.sql"
SCHEMA_BROKER_SYNC_PATH = Path(__file__).parent / "schema_broker_sync.sql"
SCHEMA_SHADOW_RUN_PATH = Path(__file__).parent / "schema_shadow_run.sql"
SCHEMA_BROKER_EXECUTION_PATH = Path(__file__).parent / "schema_broker_execution.sql"


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self._alpha_authority_stack: list[str] = []
        self._insurance_writer_stack: list[str] = []
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
                self._migrate_governance_schema(conn)
            # 加载家族办公室平台化扩展表
            if SCHEMA_FAMILY_OFFICE_PATH.exists():
                conn.executescript(SCHEMA_FAMILY_OFFICE_PATH.read_text(encoding="utf-8"))
                self._migrate_family_office_schema(conn)
            # 加载 Alpha 沙盒扩展表
            if SCHEMA_ALPHA_PATH.exists():
                conn.executescript(SCHEMA_ALPHA_PATH.read_text(encoding="utf-8"))
                self._migrate_alpha_schema(conn)
            # 加载券商同步与对账扩展表
            if SCHEMA_BROKER_SYNC_PATH.exists():
                conn.executescript(SCHEMA_BROKER_SYNC_PATH.read_text(encoding="utf-8"))
            # 加载影子运行扩展表
            if SCHEMA_SHADOW_RUN_PATH.exists():
                conn.executescript(SCHEMA_SHADOW_RUN_PATH.read_text(encoding="utf-8"))
            # 加载券商执行审计扩展表
            if SCHEMA_BROKER_EXECUTION_PATH.exists():
                conn.executescript(SCHEMA_BROKER_EXECUTION_PATH.read_text(encoding="utf-8"))

    def _table_exists(self, conn, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, conn, table_name: str) -> list[dict]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [dict(r) for r in rows]

    def _migrate_governance_schema(self, conn):
        """迁移治理层用户表，为平台化账户模型补齐身份绑定字段。"""
        if self._table_exists(conn, "api_users"):
            user_cols = {c["name"] for c in self._table_columns(conn, "api_users")}
            if "member_id" not in user_cols:
                conn.execute("ALTER TABLE api_users ADD COLUMN member_id TEXT")
        if self._table_exists(conn, "withdrawal_requests"):
            withdrawal_cols = {c["name"] for c in self._table_columns(conn, "withdrawal_requests")}
            migrations = {
                "family_office_id": "ALTER TABLE withdrawal_requests ADD COLUMN family_office_id TEXT NOT NULL DEFAULT 'default'",
                "account_id": "ALTER TABLE withdrawal_requests ADD COLUMN account_id TEXT",
                "member_id": "ALTER TABLE withdrawal_requests ADD COLUMN member_id TEXT",
                "requested_by_user_id": "ALTER TABLE withdrawal_requests ADD COLUMN requested_by_user_id INTEGER",
                "approved_by_user_id": "ALTER TABLE withdrawal_requests ADD COLUMN approved_by_user_id INTEGER",
                "source_pool_id": "ALTER TABLE withdrawal_requests ADD COLUMN source_pool_id TEXT",
                "currency": "ALTER TABLE withdrawal_requests ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD'",
                "shares_requested": "ALTER TABLE withdrawal_requests ADD COLUMN shares_requested REAL NOT NULL DEFAULT 0",
                "share_price": "ALTER TABLE withdrawal_requests ADD COLUMN share_price REAL",
                "shares_redeemed": "ALTER TABLE withdrawal_requests ADD COLUMN shares_redeemed REAL NOT NULL DEFAULT 0",
                "executed_by": "ALTER TABLE withdrawal_requests ADD COLUMN executed_by TEXT",
                "ledger_entry_id": "ALTER TABLE withdrawal_requests ADD COLUMN ledger_entry_id TEXT",
            }
            for column, sql in migrations.items():
                if column not in withdrawal_cols:
                    conn.execute(sql)
            conn.execute(
                "UPDATE withdrawal_requests SET source_pool_id = portfolio_id WHERE source_pool_id IS NULL"
            )
        if self._table_exists(conn, "withdrawal_approvals"):
            approval_cols = {c["name"] for c in self._table_columns(conn, "withdrawal_approvals")}
            migrations = {
                "approver_user_id": "ALTER TABLE withdrawal_approvals ADD COLUMN approver_user_id INTEGER",
                "approver_role": "ALTER TABLE withdrawal_approvals ADD COLUMN approver_role TEXT",
            }
            for column, sql in migrations.items():
                if column not in approval_cols:
                    conn.execute(sql)

    def _migrate_family_office_schema(self, conn):
        """迁移家族办公室扩展表，保持旧入金记录可兼容读取。"""
        if self._table_exists(conn, "deposit_records"):
            deposit_cols = {c["name"] for c in self._table_columns(conn, "deposit_records")}
            migrations = {
                "account_id": "ALTER TABLE deposit_records ADD COLUMN account_id TEXT",
                "deposit_request_id": "ALTER TABLE deposit_records ADD COLUMN deposit_request_id TEXT",
                "ledger_entry_id": "ALTER TABLE deposit_records ADD COLUMN ledger_entry_id TEXT",
                "shares_issued": "ALTER TABLE deposit_records ADD COLUMN shares_issued REAL NOT NULL DEFAULT 0",
                "share_price": "ALTER TABLE deposit_records ADD COLUMN share_price REAL",
            }
            for column, sql in migrations.items():
                if column not in deposit_cols:
                    conn.execute(sql)
        if self._table_exists(conn, "deposit_requests"):
            request_cols = {c["name"] for c in self._table_columns(conn, "deposit_requests")}
            migrations = {
                "rejected_by": "ALTER TABLE deposit_requests ADD COLUMN rejected_by TEXT",
                "rejected_at": "ALTER TABLE deposit_requests ADD COLUMN rejected_at TEXT",
                "external_reference": "ALTER TABLE deposit_requests ADD COLUMN external_reference TEXT",
                "allocation": "ALTER TABLE deposit_requests ADD COLUMN allocation TEXT",
                "legacy_deposit_record_id": "ALTER TABLE deposit_requests ADD COLUMN legacy_deposit_record_id TEXT",
            }
            for column, sql in migrations.items():
                if column not in request_cols:
                    conn.execute(sql)
        if self._table_exists(conn, "ledger_entries"):
            ledger_cols = {c["name"]: c for c in self._table_columns(conn, "ledger_entries")}
            if ledger_cols.get("id", {}).get("type", "").upper() != "INTEGER":
                self._rebuild_family_ledger_entries(conn)
                ledger_cols = {c["name"]: c for c in self._table_columns(conn, "ledger_entries")}
            migrations = {
                "portfolio_id": "ALTER TABLE ledger_entries ADD COLUMN portfolio_id TEXT",
                "pool_id": "ALTER TABLE ledger_entries ADD COLUMN pool_id TEXT",
                "memo": "ALTER TABLE ledger_entries ADD COLUMN memo TEXT",
            }
            for column, sql in migrations.items():
                if column not in ledger_cols:
                    conn.execute(sql)
        if self._table_exists(conn, "investment_pools"):
            pool_cols = {c["name"] for c in self._table_columns(conn, "investment_pools")}
            migrations = {
                "family_office_id": "ALTER TABLE investment_pools ADD COLUMN family_office_id TEXT NOT NULL DEFAULT 'default'",
                "portfolio_id": "ALTER TABLE investment_pools ADD COLUMN portfolio_id TEXT NOT NULL DEFAULT 'us'",
                "pool_type": "ALTER TABLE investment_pools ADD COLUMN pool_type TEXT NOT NULL DEFAULT 'core'",
                "currency": "ALTER TABLE investment_pools ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD'",
                "nav": "ALTER TABLE investment_pools ADD COLUMN nav REAL NOT NULL DEFAULT 0",
                "shares_outstanding": "ALTER TABLE investment_pools ADD COLUMN shares_outstanding REAL NOT NULL DEFAULT 0",
                "share_price": "ALTER TABLE investment_pools ADD COLUMN share_price REAL NOT NULL DEFAULT 100",
                "status": "ALTER TABLE investment_pools ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE'",
                "last_valued_at": "ALTER TABLE investment_pools ADD COLUMN last_valued_at TEXT",
            }
            for column, sql in migrations.items():
                if column not in pool_cols:
                    conn.execute(sql)

    def _rebuild_family_ledger_entries(self, conn):
        """重建早期文本主键版本的家族总账，改为自增主键。"""
        conn.execute("ALTER TABLE ledger_entries RENAME TO ledger_entries_legacy")
        conn.executescript(
            """
            CREATE TABLE ledger_entries (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        legacy_cols = {c["name"] for c in self._table_columns(conn, "ledger_entries_legacy")}
        select_portfolio = "portfolio_id" if "portfolio_id" in legacy_cols else "NULL"
        select_pool = "pool_id" if "pool_id" in legacy_cols else "NULL"
        select_memo = "memo" if "memo" in legacy_cols else (
            "metadata" if "metadata" in legacy_cols else "NULL"
        )
        conn.execute(
            f"""
            INSERT INTO ledger_entries (
                family_office_id, account_id, portfolio_id, pool_id, entry_type,
                amount, currency, shares_delta, share_price, actor,
                source_ref_type, source_ref_id, memo, created_at
            )
            SELECT
                family_office_id, account_id, {select_portfolio}, {select_pool}, entry_type,
                amount, currency, shares_delta, share_price, actor,
                source_ref_type, source_ref_id, {select_memo}, created_at
            FROM ledger_entries_legacy
            WHERE account_id IS NOT NULL
            """
        )
        conn.execute("DROP TABLE ledger_entries_legacy")

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

        if self._table_exists(conn, "alpha_ledger_entries"):
            ledger_cols = {c["name"] for c in self._table_columns(conn, "alpha_ledger_entries")}
            if "insurance_decision_id" not in ledger_cols:
                conn.execute("ALTER TABLE alpha_ledger_entries ADD COLUMN insurance_decision_id TEXT")

        if self._table_exists(conn, "alpha_transactions"):
            tx_cols = {c["name"] for c in self._table_columns(conn, "alpha_transactions")}
            if "insurance_decision_id" not in tx_cols:
                conn.execute("ALTER TABLE alpha_transactions ADD COLUMN insurance_decision_id TEXT")

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
                insurance_decision_id TEXT,
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

    @contextmanager
    def alpha_authority(self, insurance_decision_id: str):
        if not insurance_decision_id:
            raise ValueError("Alpha 操作缺少 InsuranceDecision 授权")
        self._alpha_authority_stack.append(insurance_decision_id)
        try:
            yield insurance_decision_id
        finally:
            self._alpha_authority_stack.pop()

    def _current_alpha_authority_id(self) -> str | None:
        if not self._alpha_authority_stack:
            return None
        return self._alpha_authority_stack[-1]

    @contextmanager
    def insurance_decision_writer(self, actor: str):
        if actor not in {"daily_runner", "test"}:
            raise ValueError(f"不允许的 InsuranceDecision 写入者: {actor}")
        self._insurance_writer_stack.append(actor)
        try:
            yield actor
        finally:
            self._insurance_writer_stack.pop()

    def _current_insurance_writer(self) -> str | None:
        if not self._insurance_writer_stack:
            return None
        return self._insurance_writer_stack[-1]

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

    def get_latest_daily_run(self, portfolio_id: str = "default") -> Optional[dict]:
        """获取最近一次日常运行记录。"""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM daily_runs
                   WHERE portfolio_id = ?
                   ORDER BY date DESC, id DESC
                   LIMIT 1""",
                (portfolio_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_daily_runs(self, portfolio_id: str = "default", limit: int = 10) -> list[dict]:
        """获取最近几次日常运行记录。"""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM daily_runs
                   WHERE portfolio_id = ?
                   ORDER BY date DESC, id DESC
                   LIMIT ?""",
                (portfolio_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── Insurance Layer 审计 ──────────────────────────

    def save_insurance_decision(
        self,
        portfolio_id: str,
        previous_state: str,
        decision,
        risk_score: float,
        hard_blocks: list,
        source_signals: list,
        actor: str = "insurance",
        recovery_proposal_id: str | None = None,
        conn=None,
    ) -> str:
        writer = self._current_insurance_writer()
        if writer is None:
            raise ValueError("InsuranceDecision 写入必须通过受控写入上下文")
        if actor == "insurance":
            actor = writer
        elif actor != writer:
            raise ValueError("InsuranceDecision actor 与写入上下文不匹配")

        decision_id = str(uuid.uuid4())[:12]
        allowed_actions = {
            "allow_observation": decision.allow_observation,
            "allow_suggestions": decision.allow_suggestions,
            "allow_core_rebalance": decision.allow_core_rebalance,
            "allow_risk_reducing_rebalance": decision.allow_risk_reducing_rebalance,
            "allow_live_execution": decision.allow_live_execution,
            "allow_alpha_execution": decision.allow_alpha_execution,
            "allow_withdrawal_request": decision.allow_withdrawal_request,
            "allow_withdrawal_approval": decision.allow_withdrawal_approval,
            "allow_withdrawal_execution": decision.allow_withdrawal_execution,
            "allow_deposit": decision.allow_deposit,
            "allow_tax_harvest": decision.allow_tax_harvest,
        }
        forced_actions = {
            "force_de_risk": decision.force_de_risk,
            "force_cash_floor": decision.force_cash_floor,
        }
        blocked_actions = {
            "block_trading": decision.block_trading,
            "freeze_execution": decision.freeze_execution,
            "require_manual_review": decision.require_manual_review,
            "require_recovery_proposal": decision.require_recovery_proposal,
        }
        values = (
            decision_id,
            portfolio_id,
            previous_state,
            decision.state.value,
            float(risk_score),
            json.dumps(hard_blocks, ensure_ascii=False),
            json.dumps(allowed_actions, ensure_ascii=False),
            json.dumps(blocked_actions, ensure_ascii=False),
            json.dumps(forced_actions, ensure_ascii=False),
            json.dumps(decision.reasons, ensure_ascii=False),
            json.dumps(source_signals, ensure_ascii=False, default=str),
            recovery_proposal_id,
            actor,
        )
        sql = """
            INSERT INTO insurance_decisions (
                id, portfolio_id, previous_state, new_state, risk_score,
                hard_blocks, allowed_actions, blocked_actions, forced_actions,
                reasons, source_signals, recovery_proposal_id, actor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)
        return decision_id

    def get_insurance_decision(self, decision_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM insurance_decisions WHERE id = ?",
                (decision_id,),
            ).fetchone()

        if not row:
            return None

        data = dict(row)
        for key in (
            "hard_blocks",
            "allowed_actions",
            "blocked_actions",
            "forced_actions",
            "reasons",
            "source_signals",
        ):
            data[key] = json.loads(data[key])
        return data

    def get_latest_insurance_decision(self, portfolio_id: str = "us") -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM insurance_decisions
                   WHERE portfolio_id = ?
                   ORDER BY created_at DESC, rowid DESC
                   LIMIT 1""",
                (portfolio_id,),
            ).fetchone()

        if not row:
            return None

        data = dict(row)
        for key in (
            "hard_blocks",
            "allowed_actions",
            "blocked_actions",
            "forced_actions",
            "reasons",
            "source_signals",
        ):
            data[key] = json.loads(data[key])
        return data

    # ── Insurance Recovery Proposals ───────────────────

    def save_recovery_proposal(self, proposal: RecoveryProposal, actor: str = "insurance", conn=None) -> str:
        values = (
            proposal.id,
            proposal.portfolio_id,
            proposal.from_state.value,
            proposal.proposed_to_state.value,
            proposal.created_at.isoformat(),
            proposal.cooldown_until.isoformat(),
            json.dumps(proposal.validation_evidence, ensure_ascii=False),
            json.dumps(proposal.unresolved_blocks, ensure_ascii=False),
            int(proposal.required_approvals),
            json.dumps(proposal.approvals, ensure_ascii=False),
            json.dumps(proposal.audit_log_ids, ensure_ascii=False),
            proposal.status.value,
            actor,
        )
        sql = """
            INSERT INTO insurance_recovery_proposals (
                id, portfolio_id, from_state, proposed_to_state, proposal_created_at,
                cooldown_until, validation_evidence, unresolved_blocks,
                required_approvals, approvals, audit_log_ids, status, actor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                portfolio_id = excluded.portfolio_id,
                from_state = excluded.from_state,
                proposed_to_state = excluded.proposed_to_state,
                proposal_created_at = excluded.proposal_created_at,
                cooldown_until = excluded.cooldown_until,
                validation_evidence = excluded.validation_evidence,
                unresolved_blocks = excluded.unresolved_blocks,
                required_approvals = excluded.required_approvals,
                approvals = excluded.approvals,
                audit_log_ids = excluded.audit_log_ids,
                status = excluded.status,
                actor = excluded.actor,
                updated_at = CURRENT_TIMESTAMP
        """
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)
        return proposal.id

    def get_recovery_proposal(self, proposal_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM insurance_recovery_proposals WHERE id = ?",
                (proposal_id,),
            ).fetchone()
        if not row:
            return None

        data = dict(row)
        for key in ("validation_evidence", "unresolved_blocks", "approvals", "audit_log_ids"):
            data[key] = json.loads(data[key])
        return data

    def load_recovery_proposal(self, proposal_id: str) -> RecoveryProposal | None:
        data = self.get_recovery_proposal(proposal_id)
        if not data:
            return None
        return RecoveryProposal(
            id=data["id"],
            portfolio_id=data["portfolio_id"],
            from_state=coerce_insurance_state(data["from_state"]),
            proposed_to_state=coerce_insurance_state(data["proposed_to_state"]),
            created_at=datetime.fromisoformat(data["proposal_created_at"]),
            cooldown_until=datetime.fromisoformat(data["cooldown_until"]),
            validation_evidence=data["validation_evidence"],
            unresolved_blocks=data["unresolved_blocks"],
            required_approvals=int(data["required_approvals"]),
            approvals=data["approvals"],
            audit_log_ids=data["audit_log_ids"],
            status=RecoveryStatus(data["status"]),
        )

    # ── 券商同步与对账 ────────────────────────────────

    def save_broker_account_snapshot(
        self,
        portfolio_id: str,
        broker_role: str,
        broker_name: str,
        broker_mode: str,
        account_id: str,
        currency: str,
        cash: float,
        total_value: float,
        positions_json: str,
        raw_json: str,
        snapshot_time: str = "",
        conn=None,
    ):
        sql = """INSERT INTO broker_account_snapshots
                   (portfolio_id, broker_role, broker_name, broker_mode, account_id, currency,
                    cash, total_value, positions_json, raw_json, snapshot_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id,
            broker_role,
            broker_name,
            broker_mode,
            account_id,
            currency,
            cash,
            total_value,
            positions_json,
            raw_json,
            snapshot_time,
        )
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_latest_broker_account_snapshot(self, portfolio_id: str, broker_role: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM broker_account_snapshots
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT 1""",
                (portfolio_id, broker_role),
            ).fetchone()
            return dict(row) if row else None

    def list_broker_account_snapshots(
        self,
        portfolio_id: str,
        broker_role: str,
        limit: int = 5,
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM broker_account_snapshots
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT ?""",
                (portfolio_id, broker_role, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_broker_reconciliation_run(
        self,
        portfolio_id: str,
        broker_role: str,
        broker_name: str,
        status: str,
        checked_at: str,
        items_json: str,
        report_json: str,
        conn=None,
    ):
        sql = """INSERT INTO broker_reconciliation_runs
                   (portfolio_id, broker_role, broker_name, status, checked_at, items_json, report_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id,
            broker_role,
            broker_name,
            status,
            checked_at,
            items_json,
            report_json,
        )
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_latest_broker_reconciliation_run(self, portfolio_id: str, broker_role: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM broker_reconciliation_runs
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT 1""",
                (portfolio_id, broker_role),
            ).fetchone()
            return dict(row) if row else None

    def list_broker_reconciliation_runs(
        self,
        portfolio_id: str,
        broker_role: str,
        limit: int = 5,
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM broker_reconciliation_runs
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT ?""",
                (portfolio_id, broker_role, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── 影子运行 ──────────────────────────────────────

    def save_shadow_run_report(
        self,
        portfolio_id: str,
        broker_role: str,
        broker_name: str,
        checked_at: str,
        dry_run: bool,
        order_count: int,
        reconciliation_status: str = "",
        requires_attention: bool = False,
        warnings_json: str = "[]",
        report_json: str = "{}",
        conn=None,
    ):
        sql = """INSERT INTO shadow_run_reports
                   (portfolio_id, broker_role, broker_name, checked_at, dry_run, order_count,
                    reconciliation_status, requires_attention, warnings_json, report_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id,
            broker_role,
            broker_name,
            checked_at,
            1 if dry_run else 0,
            order_count,
            reconciliation_status,
            1 if requires_attention else 0,
            warnings_json,
            report_json,
        )
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_latest_shadow_run_report(self, portfolio_id: str, broker_role: str = "sandbox") -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM shadow_run_reports
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT 1""",
                (portfolio_id, broker_role),
            ).fetchone()
            return dict(row) if row else None

    def list_shadow_run_reports(
        self,
        portfolio_id: str,
        broker_role: str = "sandbox",
        limit: int = 5,
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM shadow_run_reports
                   WHERE portfolio_id = ? AND broker_role = ?
                   ORDER BY id DESC LIMIT ?""",
                (portfolio_id, broker_role, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ── 券商执行审计 ──────────────────────────────────

    def save_broker_execution_event(
        self,
        *,
        portfolio_id: str,
        run_date: str,
        broker_role: str,
        broker_name: str,
        broker_mode: str = "",
        event_type: str,
        event_time: str,
        order_id: str = "",
        client_order_id: str = "",
        broker_reference: str = "",
        symbol: str = "",
        side: str = "",
        requested_quantity: int = 0,
        filled_quantity: int = 0,
        avg_fill_price: float | None = None,
        commission: float | None = None,
        status: str = "",
        message: str = "",
        raw_json: str = "{}",
        conn=None,
    ):
        sql = """INSERT INTO broker_execution_events
                   (portfolio_id, run_date, broker_role, broker_name, broker_mode,
                    event_type, event_time, order_id, client_order_id, broker_reference,
                    symbol, side, requested_quantity, filled_quantity, avg_fill_price,
                    commission, status, message, raw_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            portfolio_id,
            run_date,
            broker_role,
            broker_name,
            broker_mode,
            event_type,
            event_time,
            order_id,
            client_order_id,
            broker_reference,
            symbol,
            side,
            requested_quantity,
            filled_quantity,
            avg_fill_price,
            commission,
            status,
            message,
            raw_json,
        )
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def save_broker_execution_events(
        self,
        *,
        portfolio_id: str,
        run_date: str,
        events: list[dict],
        conn=None,
    ):
        if not events:
            return
        for event in events:
            self.save_broker_execution_event(
                portfolio_id=portfolio_id,
                run_date=run_date,
                broker_role=str(event.get("broker_role") or "primary"),
                broker_name=str(event.get("broker_name") or "unknown"),
                broker_mode=str(event.get("broker_mode") or ""),
                event_type=str(event.get("event_type") or "UNKNOWN"),
                event_time=str(event.get("event_time") or ""),
                order_id=str(event.get("order_id") or ""),
                client_order_id=str(event.get("client_order_id") or ""),
                broker_reference=str(event.get("broker_reference") or ""),
                symbol=str(event.get("symbol") or ""),
                side=str(event.get("side") or ""),
                requested_quantity=int(float(event.get("requested_quantity") or 0)),
                filled_quantity=int(float(event.get("filled_quantity") or 0)),
                avg_fill_price=float(event["avg_fill_price"]) if event.get("avg_fill_price") not in (None, "") else None,
                commission=float(event["commission"]) if event.get("commission") not in (None, "") else None,
                status=str(event.get("status") or ""),
                message=str(event.get("message") or ""),
                raw_json=json.dumps(event.get("raw") or {}, ensure_ascii=False, default=str),
                conn=conn,
            )

    def list_broker_execution_events(
        self,
        portfolio_id: str,
        run_date: Optional[str] = None,
        broker_role: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        with self._conn() as conn:
            where = ["portfolio_id = ?"]
            values = [portfolio_id]
            if run_date:
                where.append("run_date = ?")
                values.append(run_date)
            if broker_role:
                where.append("broker_role = ?")
                values.append(broker_role)
            rows = conn.execute(
                f"""SELECT * FROM broker_execution_events
                    WHERE {' AND '.join(where)}
                    ORDER BY id DESC LIMIT ?""",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

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
        account_id: str | None = None,
        member_id: str | None = None,
        requested_by_user_id: int | None = None,
        source_pool_id: str | None = None,
        currency: str = "USD",
        shares_requested: float = 0.0,
        share_price: float | None = None,
        family_office_id: str = "default",
        conn=None,
    ) -> dict:
        """创建出金请求"""
        sql = """INSERT INTO withdrawal_requests
                   (id, family_office_id, account_id, member_id, requested_by_user_id,
                    portfolio_id, source_pool_id, amount, currency, shares_requested,
                    share_price, reason, requester, required_approvals, cooling_days, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            withdrawal_id,
            family_office_id,
            account_id,
            member_id,
            requested_by_user_id,
            portfolio_id,
            source_pool_id or portfolio_id,
            round(float(amount), 2),
            currency,
            round(float(shares_requested), 8),
            share_price,
            reason,
            requester,
            required_approvals,
            cooling_days,
            expires_at,
        )
        if conn:
            conn.execute(sql, values)
            row = conn.execute("SELECT * FROM withdrawal_requests WHERE id = ?", (withdrawal_id,)).fetchone()
            return dict(row) if row else {"id": withdrawal_id}
        else:
            with self._conn() as c:
                c.execute(sql, values)
                row = c.execute("SELECT * FROM withdrawal_requests WHERE id = ?", (withdrawal_id,)).fetchone()
                return dict(row) if row else {"id": withdrawal_id}

    def get_withdrawal_request(self, withdrawal_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM withdrawal_requests WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_withdrawal_requests(
        self,
        status: Optional[str] = None,
        portfolio_id: str | None = None,
        account_id: str | None = None,
        account_ids: list[str] | None = None,
        limit: int = 20,
    ) -> list[dict]:
        with self._conn() as conn:
            conditions = []
            values = []
            if status:
                conditions.append("status = ?")
                values.append(status)
            if portfolio_id:
                conditions.append("portfolio_id = ?")
                values.append(portfolio_id)
            if account_id:
                conditions.append("account_id = ?")
                values.append(account_id)
            if account_ids is not None:
                if not account_ids:
                    return []
                placeholders = ", ".join("?" for _ in account_ids)
                conditions.append(f"account_id IN ({placeholders})")
                values.extend(account_ids)
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            rows = conn.execute(
                f"SELECT * FROM withdrawal_requests {where} ORDER BY created_at DESC LIMIT ?",
                values + [limit],
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

    def update_withdrawal_request(self, withdrawal_id: str, conn=None, **kwargs):
        if not kwargs:
            return
        sets = ", ".join(f"{key} = ?" for key in kwargs)
        values = list(kwargs.values()) + [withdrawal_id]
        sql = f"""UPDATE withdrawal_requests
                  SET {sets}, updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?"""
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def save_deposit_record(
        self, deposit_id: str, amount: float, depositor: str,
        portfolio_id: str = "us", allocation: str = "", conn=None,
        account_id: str | None = None,
        deposit_request_id: str | None = None,
        ledger_entry_id: str | None = None,
        shares_issued: float = 0.0,
        share_price: float | None = None,
    ):
        sql = """INSERT INTO deposit_records (
                    id, portfolio_id, account_id, deposit_request_id, ledger_entry_id,
                    amount, depositor, allocation, shares_issued, share_price
                 )
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            deposit_id,
            portfolio_id,
            account_id,
            deposit_request_id,
            ledger_entry_id,
            amount,
            depositor,
            allocation,
            shares_issued,
            share_price,
        )
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def list_deposit_records(
        self,
        portfolio_id: str = None,
        account_id: str | None = None,
        account_ids: list[str] | None = None,
        limit: int = 20,
    ) -> list[dict]:
        with self._conn() as conn:
            conditions = []
            values = []
            if portfolio_id:
                conditions.append("portfolio_id = ?")
                values.append(portfolio_id)
            if account_id:
                conditions.append("account_id = ?")
                values.append(account_id)
            if account_ids is not None:
                if not account_ids:
                    return []
                placeholders = ", ".join("?" for _ in account_ids)
                conditions.append(f"account_id IN ({placeholders})")
                values.extend(account_ids)
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            rows = conn.execute(
                f"SELECT * FROM deposit_records {where} ORDER BY created_at DESC LIMIT ?",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

    def add_withdrawal_approval(
        self,
        withdrawal_id: str,
        approver: str,
        decision: str = "APPROVED",
        comment: str = "",
        approver_user_id: int | None = None,
        approver_role: str | None = None,
        conn=None,
    ):
        sql = """INSERT OR REPLACE INTO withdrawal_approvals
                   (withdrawal_id, approver, approver_user_id, approver_role, decision, comment)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        values = (withdrawal_id, approver, approver_user_id, approver_role, decision, comment)
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

    def get_reserved_withdrawal_shares(self, account_id: str, pool_id: str) -> float:
        """Return shares reserved by pending/approved withdrawals that are not executed."""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(shares_requested), 0) AS reserved
                   FROM withdrawal_requests
                   WHERE account_id = ?
                     AND COALESCE(source_pool_id, portfolio_id) = ?
                     AND status IN ('PENDING', 'APPROVED')""",
                (account_id, pool_id),
            ).fetchone()
            return float(row["reserved"] or 0.0)

    def get_account_monthly_report(self, account_id: str, year: int, month: int) -> dict:
        account = self.get_capital_account(account_id)
        if account is None:
            raise ValueError(f"资产账户不存在: {account_id}")
        month_prefix = f"{int(year):04d}-{int(month):02d}"
        asset_view = self.get_account_asset_view(account_id)
        with self._conn() as conn:
            ledger_rows = conn.execute(
                """SELECT * FROM ledger_entries
                   WHERE account_id = ? AND substr(created_at, 1, 7) = ?
                   ORDER BY created_at, id""",
                (account_id, month_prefix),
            ).fetchall()
            withdrawal_rows = conn.execute(
                """SELECT * FROM withdrawal_requests
                   WHERE account_id = ? AND substr(created_at, 1, 7) = ?
                   ORDER BY created_at, id""",
                (account_id, month_prefix),
            ).fetchall()
        ledger = [dict(row) for row in ledger_rows]
        withdrawals = [dict(row) for row in withdrawal_rows]
        deposits = sum(
            float(entry["amount"] or 0.0)
            for entry in ledger
            if entry["entry_type"] == "DEPOSIT_CONFIRMED"
        )
        executed_withdrawals = sum(
            float(entry["amount"] or 0.0)
            for entry in ledger
            if entry["entry_type"] == "WITHDRAWAL_EXECUTED"
        )
        return {
            "report_type": "member_monthly",
            "period": month_prefix,
            "account": account,
            "asset_view": asset_view,
            "cashflows": {
                "deposits": round(deposits, 2),
                "withdrawals": round(executed_withdrawals, 2),
                "net": round(deposits - executed_withdrawals, 2),
            },
            "ledger_entries": ledger,
            "withdrawals": withdrawals,
        }

    def get_family_global_report(self, year: int, month: int, family_office_id: str = "default") -> dict:
        month_prefix = f"{int(year):04d}-{int(month):02d}"
        aum = self.get_family_aum_summary(family_office_id=family_office_id)
        with self._conn() as conn:
            ledger_rows = conn.execute(
                """SELECT le.*, ca.account_name, fm.display_name AS member_name
                   FROM ledger_entries le
                   LEFT JOIN capital_accounts ca ON ca.id = le.account_id
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE le.family_office_id = ? AND substr(le.created_at, 1, 7) = ?
                   ORDER BY le.created_at, le.id""",
                (family_office_id, month_prefix),
            ).fetchall()
            withdrawal_rows = conn.execute(
                """SELECT wr.*, ca.account_name, fm.display_name AS member_name
                   FROM withdrawal_requests wr
                   LEFT JOIN capital_accounts ca ON ca.id = wr.account_id
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE wr.family_office_id = ? AND substr(wr.created_at, 1, 7) = ?
                   ORDER BY wr.created_at, wr.id""",
                (family_office_id, month_prefix),
            ).fetchall()
        ledger = [dict(row) for row in ledger_rows]
        withdrawals = [dict(row) for row in withdrawal_rows]
        status_counts: dict[str, int] = {}
        for withdrawal in withdrawals:
            status = withdrawal.get("status") or "UNKNOWN"
            status_counts[status] = status_counts.get(status, 0) + 1
        return {
            "report_type": "family_global",
            "family_office_id": family_office_id,
            "period": month_prefix,
            "aum": aum,
            "cashflows": {
                "deposits": round(sum(float(e["amount"] or 0.0) for e in ledger if e["entry_type"] == "DEPOSIT_CONFIRMED"), 2),
                "withdrawals": round(sum(float(e["amount"] or 0.0) for e in ledger if e["entry_type"] == "WITHDRAWAL_EXECUTED"), 2),
            },
            "withdrawal_status_counts": status_counts,
            "ledger_entries": ledger,
            "withdrawals": withdrawals,
        }

    def save_audit_log(self, action: str, actor: str, detail: str = "", ip_address: str = "", conn=None):
        sql = "INSERT INTO audit_log (action, actor, detail, ip_address) VALUES (?, ?, ?, ?)"
        values = (action, actor, detail, ip_address)
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def get_audit_log(
        self,
        limit: int = 50,
        action: str | None = None,
        actor: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[dict]:
        where = []
        values: list = []
        if action:
            where.append("action = ?")
            values.append(action)
        if actor:
            where.append("actor = ?")
            values.append(actor)
        if since:
            where.append("created_at >= ?")
            values.append(since)
        if until:
            where.append("created_at <= ?")
            values.append(until)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM audit_log {where_sql} ORDER BY created_at DESC LIMIT ?",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

    # ── API 用户 ──────────────────────────────────────

    def get_user_by_username(self, username: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM api_users WHERE username = ? AND is_active = 1", (username,)
            ).fetchone()
            return dict(row) if row else None

    def create_user(
        self,
        username: str,
        password_hash: str,
        role: str = "viewer",
        display_name: str = "",
        email: str = "",
        member_id: str | None = None,
    ):
        if member_id and self.get_family_member(member_id) is None:
            raise ValueError(f"家族成员不存在: {member_id}")
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO api_users (username, password_hash, role, member_id, display_name, email)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (username, password_hash, role, member_id, display_name, email),
            )

    # ── 家族办公室账户与授权 ──────────────────────────

    def create_family_member(
        self,
        display_name: str,
        member_type: str = "individual",
        risk_profile: str = "balanced",
        family_office_id: str = "default",
        member_id: str | None = None,
    ) -> dict:
        member_id = member_id or f"mem_{uuid.uuid4().hex[:12]}"
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO family_members
                   (id, family_office_id, display_name, member_type, risk_profile)
                   VALUES (?, ?, ?, ?, ?)""",
                (member_id, family_office_id, display_name, member_type, risk_profile),
            )
        member = self.get_family_member(member_id)
        if member is None:
            raise RuntimeError("创建家族成员失败")
        return member

    def get_family_member(self, member_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM family_members WHERE id = ?",
                (member_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_family_members(self, family_office_id: str = "default") -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM family_members
                   WHERE family_office_id = ?
                   ORDER BY created_at DESC""",
                (family_office_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def bind_user_member(self, username: str, member_id: str | None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_users SET member_id = ? WHERE username = ?",
                (member_id, username),
            )

    def create_capital_account(
        self,
        member_id: str,
        account_name: str,
        base_currency: str = "USD",
        default_portfolio_id: str = "us",
        family_office_id: str = "default",
        account_id: str | None = None,
    ) -> dict:
        account_id = account_id or f"acct_{uuid.uuid4().hex[:12]}"
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO capital_accounts
                   (id, family_office_id, member_id, account_name, base_currency, default_portfolio_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    account_id,
                    family_office_id,
                    member_id,
                    account_name,
                    base_currency,
                    default_portfolio_id,
                ),
            )
        account = self.get_capital_account(account_id)
        if account is None:
            raise RuntimeError("创建资产账户失败")
        return account

    def get_capital_account(self, account_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT ca.*, fm.display_name AS member_name
                   FROM capital_accounts ca
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE ca.id = ?""",
                (account_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_capital_accounts(self, family_office_id: str = "default") -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT ca.*, fm.display_name AS member_name
                   FROM capital_accounts ca
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE ca.family_office_id = ?
                   ORDER BY ca.created_at DESC""",
                (family_office_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def grant_account_permission(
        self,
        username: str,
        account_id: str,
        permission: str = "view",
    ) -> dict:
        user = self.get_user_by_username(username)
        if user is None:
            raise ValueError(f"用户不存在: {username}")
        if self.get_capital_account(account_id) is None:
            raise ValueError(f"资产账户不存在: {account_id}")
        with self._conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO account_permissions
                   (user_id, account_id, permission)
                   VALUES (?, ?, ?)""",
                (user["id"], account_id, permission),
            )
        return {"username": username, "account_id": account_id, "permission": permission}

    def revoke_account_permission(
        self,
        username: str,
        account_id: str,
        permission: str = "view",
    ):
        user = self.get_user_by_username(username)
        if user is None:
            return
        with self._conn() as conn:
            conn.execute(
                """DELETE FROM account_permissions
                   WHERE user_id = ? AND account_id = ? AND permission = ?""",
                (user["id"], account_id, permission),
            )

    def user_has_account_permission(
        self,
        user_id: int,
        account_id: str,
        permission: str = "view",
    ) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT 1 FROM account_permissions
                   WHERE user_id = ? AND account_id = ?
                   AND permission IN (?, 'admin')
                   LIMIT 1""",
                (user_id, account_id, permission),
            ).fetchone()
            return row is not None

    def list_user_accounts(
        self,
        user_id: int,
        member_id: str | None = None,
        permission: str = "view",
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT DISTINCT ca.*, fm.display_name AS member_name
                   FROM capital_accounts ca
                   LEFT JOIN account_permissions ap
                     ON ap.account_id = ca.id AND ap.user_id = ?
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE ca.member_id = ?
                      OR ap.permission IN (?, 'admin')
                   ORDER BY ca.created_at DESC""",
                (user_id, member_id, permission),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_authorized_portfolio_ids(
        self,
        user_id: int,
        member_id: str | None = None,
        permission: str = "view",
    ) -> list[str]:
        accounts = self.list_user_accounts(user_id, member_id=member_id, permission=permission)
        return sorted({a["default_portfolio_id"] for a in accounts if a.get("default_portfolio_id")})

    def ensure_legacy_capital_account(
        self,
        actor: str,
        portfolio_id: str = "us",
        family_office_id: str = "default",
    ) -> dict:
        """
        Compatibility bridge for pre-platform deposit callers.

        New API paths require account_id. Older engine-level callers only pass a
        depositor string, so we anchor those deposits to a deterministic legacy
        account instead of allowing account-less ledger entries.
        """
        safe_actor = actor or "unknown"
        suffix = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"xirang:{family_office_id}:{safe_actor}:{portfolio_id}",
        ).hex[:12]
        member_id = f"mem_legacy_{suffix}"
        account_id = f"acct_legacy_{suffix}"
        with self._conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO family_members
                   (id, family_office_id, display_name, member_type, risk_profile)
                   VALUES (?, ?, ?, 'legacy', 'balanced')""",
                (member_id, family_office_id, f"Legacy Depositor: {safe_actor}"),
            )
            conn.execute(
                """INSERT OR IGNORE INTO capital_accounts
                   (id, family_office_id, member_id, account_name, base_currency, default_portfolio_id)
                   VALUES (?, ?, ?, ?, 'USD', ?)""",
                (
                    account_id,
                    family_office_id,
                    member_id,
                    f"Legacy Deposit Account: {safe_actor}",
                    portfolio_id,
                ),
            )
        account = self.get_capital_account(account_id)
        if account is None:
            raise RuntimeError("创建兼容资产账户失败")
        return account

    # ── 平台化入金申请与总账 ──────────────────────────

    def create_deposit_request(
        self,
        account_id: str,
        amount: float,
        requested_by: str,
        portfolio_id: str = "us",
        currency: str = "USD",
        note: str = "",
        external_reference: str = "",
        family_office_id: str = "default",
        request_id: str | None = None,
        status: str = "REQUESTED",
        conn=None,
    ) -> dict:
        request_id = request_id or f"dep_{uuid.uuid4().hex[:12]}"
        sql = """INSERT INTO deposit_requests
                   (id, family_office_id, account_id, portfolio_id, amount, currency,
                    requested_by, status, external_reference, note)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            request_id,
            family_office_id,
            account_id,
            portfolio_id,
            round(float(amount), 2),
            currency,
            requested_by,
            status,
            external_reference,
            note,
        )
        if conn:
            conn.execute(sql, values)
            row = conn.execute(
                "SELECT * FROM deposit_requests WHERE id = ?",
                (request_id,),
            ).fetchone()
            return dict(row) if row else {"id": request_id}
        with self._conn() as c:
            c.execute(sql, values)
            row = c.execute(
                "SELECT * FROM deposit_requests WHERE id = ?",
                (request_id,),
            ).fetchone()
            return dict(row) if row else {"id": request_id}

    def get_deposit_request(self, request_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT dr.*, ca.account_name, fm.display_name AS member_name
                   FROM deposit_requests dr
                   LEFT JOIN capital_accounts ca ON ca.id = dr.account_id
                   LEFT JOIN family_members fm ON fm.id = ca.member_id
                   WHERE dr.id = ?""",
                (request_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_deposit_requests(
        self,
        portfolio_id: str | None = None,
        status: str | None = None,
        account_ids: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict]:
        where = []
        values: list = []
        if portfolio_id:
            where.append("dr.portfolio_id = ?")
            values.append(portfolio_id)
        if status:
            where.append("dr.status = ?")
            values.append(status)
        if account_ids is not None:
            if not account_ids:
                return []
            placeholders = ", ".join(["?"] * len(account_ids))
            where.append(f"dr.account_id IN ({placeholders})")
            values.extend(account_ids)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT dr.*, ca.account_name, fm.display_name AS member_name
                    FROM deposit_requests dr
                    LEFT JOIN capital_accounts ca ON ca.id = dr.account_id
                    LEFT JOIN family_members fm ON fm.id = ca.member_id
                    {where_sql}
                    ORDER BY dr.created_at DESC LIMIT ?""",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

    def update_deposit_request(
        self,
        request_id: str,
        conn=None,
        **kwargs,
    ):
        if not kwargs:
            return
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [request_id]
        sql = f"""UPDATE deposit_requests
                  SET {sets}, updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?"""
        if conn:
            conn.execute(sql, values)
        else:
            with self._conn() as c:
                c.execute(sql, values)

    def record_ledger_entry(
        self,
        account_id: str,
        entry_type: str,
        amount: float,
        actor: str,
        portfolio_id: str | None = None,
        pool_id: str | None = None,
        currency: str = "USD",
        shares_delta: float | None = None,
        share_price: float | None = None,
        source_ref_type: str = "",
        source_ref_id: str = "",
        memo: str = "",
        family_office_id: str = "default",
        conn=None,
    ) -> dict:
        sql = """INSERT INTO ledger_entries
                   (family_office_id, account_id, portfolio_id, pool_id, entry_type,
                    amount, currency, shares_delta, share_price, actor,
                    source_ref_type, source_ref_id, memo)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            family_office_id,
            account_id,
            portfolio_id,
            pool_id,
            entry_type,
            round(float(amount), 2),
            currency,
            shares_delta,
            share_price,
            actor,
            source_ref_type,
            source_ref_id,
            memo,
        )
        if conn:
            cursor = conn.execute(sql, values)
            row = conn.execute(
                "SELECT * FROM ledger_entries WHERE id = ?",
                (cursor.lastrowid,),
            ).fetchone()
            return dict(row) if row else {"id": cursor.lastrowid}
        with self._conn() as c:
            cursor = c.execute(sql, values)
            row = c.execute(
                "SELECT * FROM ledger_entries WHERE id = ?",
                (cursor.lastrowid,),
            ).fetchone()
            return dict(row) if row else {"id": cursor.lastrowid}

    def list_ledger_entries(
        self,
        account_id: str | None = None,
        portfolio_id: str | None = None,
        entry_type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        where = []
        values: list = []
        if account_id:
            where.append("le.account_id = ?")
            values.append(account_id)
        if portfolio_id:
            where.append("le.portfolio_id = ?")
            values.append(portfolio_id)
        if entry_type:
            where.append("le.entry_type = ?")
            values.append(entry_type)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT le.*, ca.account_name, fm.display_name AS member_name
                    FROM ledger_entries le
                    LEFT JOIN capital_accounts ca ON ca.id = le.account_id
                    LEFT JOIN family_members fm ON fm.id = ca.member_id
                    {where_sql}
                    ORDER BY le.created_at DESC, le.id DESC LIMIT ?""",
                values + [limit],
            ).fetchall()
            return [dict(r) for r in rows]

    def upsert_investment_pool(
        self,
        pool_id: str,
        portfolio_id: str | None = None,
        pool_type: str = "core",
        currency: str = "USD",
        nav: float = 0.0,
        shares_outstanding: float = 0.0,
        share_price: float = 100.0,
        status: str = "ACTIVE",
        family_office_id: str = "default",
        last_valued_at: str | None = None,
        conn=None,
    ) -> dict:
        portfolio_id = portfolio_id or pool_id
        last_valued_at = last_valued_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = (
            pool_id,
            family_office_id,
            portfolio_id,
            pool_type,
            currency,
            round(float(nav), 2),
            round(float(shares_outstanding), 8),
            round(float(share_price), 8),
            status,
            last_valued_at,
        )
        sql = """INSERT INTO investment_pools
                   (id, family_office_id, portfolio_id, pool_type, currency,
                    nav, shares_outstanding, share_price, status, last_valued_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                    family_office_id = excluded.family_office_id,
                    portfolio_id = excluded.portfolio_id,
                    pool_type = excluded.pool_type,
                    currency = excluded.currency,
                    nav = excluded.nav,
                    shares_outstanding = excluded.shares_outstanding,
                    share_price = excluded.share_price,
                    status = excluded.status,
                    last_valued_at = excluded.last_valued_at,
                    updated_at = CURRENT_TIMESTAMP"""
        if conn:
            conn.execute(sql, values)
            row = conn.execute("SELECT * FROM investment_pools WHERE id = ?", (pool_id,)).fetchone()
            return dict(row) if row else {"id": pool_id}
        with self._conn() as c:
            c.execute(sql, values)
            row = c.execute("SELECT * FROM investment_pools WHERE id = ?", (pool_id,)).fetchone()
            return dict(row) if row else {"id": pool_id}

    def get_investment_pool(self, pool_id: str, conn=None) -> Optional[dict]:
        sql = "SELECT * FROM investment_pools WHERE id = ?"
        if conn:
            row = conn.execute(sql, (pool_id,)).fetchone()
            return dict(row) if row else None
        with self._conn() as c:
            row = c.execute(sql, (pool_id,)).fetchone()
            return dict(row) if row else None

    def list_investment_pools(
        self,
        family_office_id: str = "default",
        portfolio_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        where = ["family_office_id = ?"]
        values: list = [family_office_id]
        if portfolio_id:
            where.append("portfolio_id = ?")
            values.append(portfolio_id)
        if status:
            where.append("status = ?")
            values.append(status)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT * FROM investment_pools
                    WHERE {' AND '.join(where)}
                    ORDER BY portfolio_id, id""",
                values,
            ).fetchall()
            return [dict(r) for r in rows]

    def ensure_investment_pool(
        self,
        pool_id: str,
        portfolio_id: str | None = None,
        nav: float | None = None,
        currency: str = "USD",
        pool_type: str = "core",
        conn=None,
    ) -> dict:
        existing = self.get_investment_pool(pool_id, conn=conn)
        if existing:
            return existing

        portfolio_id = portfolio_id or pool_id
        latest = self.get_latest_pool_nav_snapshot(pool_id, locked_only=True, conn=conn)
        if latest:
            return self.upsert_investment_pool(
                pool_id=pool_id,
                portfolio_id=portfolio_id,
                pool_type=pool_type,
                currency=currency,
                nav=float(latest["nav"]),
                shares_outstanding=float(latest["shares_outstanding"]),
                share_price=float(latest["share_price"]),
                conn=conn,
            )

        if nav is None:
            try:
                portfolio = self.get_portfolio(portfolio_id)
                nav = float(portfolio["nav"])
            except ValueError:
                nav = 0.0
        share_price = 100.0
        shares_outstanding = float(nav) / share_price if float(nav) > 0 else 0.0
        return self.upsert_investment_pool(
            pool_id=pool_id,
            portfolio_id=portfolio_id,
            pool_type=pool_type,
            currency=currency,
            nav=float(nav),
            shares_outstanding=shares_outstanding,
            share_price=share_price,
            conn=conn,
        )

    def get_latest_pool_nav_snapshot(
        self,
        pool_id: str,
        locked_only: bool = True,
        conn=None,
    ) -> Optional[dict]:
        sql = "SELECT * FROM pool_nav_snapshots WHERE pool_id = ?"
        values: list = [pool_id]
        if locked_only:
            sql += " AND is_locked = 1"
        sql += " ORDER BY created_at DESC, snapshot_date DESC LIMIT 1"
        if conn:
            row = conn.execute(sql, values).fetchone()
            return dict(row) if row else None
        with self._conn() as c:
            row = c.execute(sql, values).fetchone()
            return dict(row) if row else None

    def save_pool_nav_snapshot(
        self,
        pool_id: str,
        nav: float,
        shares_outstanding: float,
        share_price: float,
        snapshot_date: str | None = None,
        is_locked: bool = True,
        source: str = "SYSTEM",
        snapshot_id: str | None = None,
        conn=None,
    ) -> str:
        snapshot_id = snapshot_id or f"nav_{uuid.uuid4().hex[:12]}"
        snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
        sql = """INSERT INTO pool_nav_snapshots
                   (id, pool_id, nav, shares_outstanding, share_price, snapshot_date, is_locked, source)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
        values = (
            snapshot_id,
            pool_id,
            round(float(nav), 2),
            round(float(shares_outstanding), 8),
            round(float(share_price), 8),
            snapshot_date,
            1 if is_locked else 0,
            source,
        )
        if conn:
            conn.execute(sql, values)
            self.upsert_investment_pool(
                pool_id=pool_id,
                portfolio_id=pool_id,
                nav=nav,
                shares_outstanding=shares_outstanding,
                share_price=share_price,
                last_valued_at=f"{snapshot_date} 00:00:00",
                conn=conn,
            )
        else:
            with self._conn() as c:
                c.execute(sql, values)
                self.upsert_investment_pool(
                    pool_id=pool_id,
                    portfolio_id=pool_id,
                    nav=nav,
                    shares_outstanding=shares_outstanding,
                    share_price=share_price,
                    last_valued_at=f"{snapshot_date} 00:00:00",
                    conn=c,
                )
        return snapshot_id

    def ensure_initial_pool_nav_snapshot(
        self,
        pool_id: str,
        nav: float,
        default_share_price: float = 100.0,
        conn=None,
    ) -> dict:
        latest = self.get_latest_pool_nav_snapshot(pool_id, locked_only=True, conn=conn)
        if latest:
            self.upsert_investment_pool(
                pool_id=pool_id,
                portfolio_id=pool_id,
                nav=float(latest["nav"]),
                shares_outstanding=float(latest["shares_outstanding"]),
                share_price=float(latest["share_price"]),
                conn=conn,
            )
            return latest
        shares_outstanding = nav / default_share_price if nav > 0 else 0.0
        snapshot_id = self.save_pool_nav_snapshot(
            pool_id=pool_id,
            nav=nav,
            shares_outstanding=shares_outstanding,
            share_price=default_share_price,
            source="INITIALIZED_FROM_PORTFOLIO",
            conn=conn,
        )
        if conn:
            row = conn.execute("SELECT * FROM pool_nav_snapshots WHERE id = ?", (snapshot_id,)).fetchone()
            if row:
                return dict(row)
        latest = self.get_latest_pool_nav_snapshot(pool_id, locked_only=True)
        if latest is None:
            raise RuntimeError("初始化投资池净值快照失败")
        return latest

    def revalue_investment_pool(
        self,
        pool_id: str,
        nav: float,
        actor: str = "system",
        source: str = "NAV_REVALUED",
        snapshot_date: str | None = None,
        conn=None,
    ) -> dict:
        """Lock a new pool NAV and derive share_price from current shares."""
        pool = self.ensure_investment_pool(pool_id, nav=nav, conn=conn)
        shares_outstanding = float(pool.get("shares_outstanding") or 0.0)
        if shares_outstanding <= 0 and float(nav) > 0:
            shares_outstanding = round(float(nav) / 100.0, 8)
        share_price = round(float(nav) / shares_outstanding, 8) if shares_outstanding > 0 else 100.0
        snapshot_id = self.save_pool_nav_snapshot(
            pool_id=pool_id,
            nav=nav,
            shares_outstanding=shares_outstanding,
            share_price=share_price,
            snapshot_date=snapshot_date,
            source=source,
            conn=conn,
        )
        result = self.get_investment_pool(pool_id, conn=conn)
        if result is None:
            raise RuntimeError("投资池重估失败")
        result["snapshot_id"] = snapshot_id
        result["actor"] = actor
        return result

    def get_account_pool_position(self, account_id: str, pool_id: str, conn=None) -> dict:
        sql = "SELECT * FROM account_pool_positions WHERE account_id = ? AND pool_id = ?"
        if conn:
            row = conn.execute(sql, (account_id, pool_id)).fetchone()
            if row:
                return dict(row)
        else:
            with self._conn() as c:
                row = c.execute(sql, (account_id, pool_id)).fetchone()
                if row:
                    return dict(row)
        return {
            "account_id": account_id,
            "pool_id": pool_id,
            "shares": 0.0,
            "cost_basis": 0.0,
        }

    def adjust_account_pool_position(
        self,
        account_id: str,
        pool_id: str,
        shares_delta: float,
        cost_basis_delta: float = 0.0,
        conn=None,
    ) -> dict:
        if conn is None:
            raise ValueError("账户份额更新必须在受控 ledger 事务内执行")
        current = self.get_account_pool_position(account_id, pool_id, conn=conn)
        new_shares = round(float(current.get("shares", 0.0)) + float(shares_delta), 8)
        new_cost_basis = round(float(current.get("cost_basis", 0.0)) + float(cost_basis_delta), 2)
        if new_shares < -1e-8:
            raise ValueError("账户份额不足")
        if new_cost_basis < 0:
            new_cost_basis = 0.0
        conn.execute(
            """INSERT INTO account_pool_positions
                   (account_id, pool_id, shares, cost_basis)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(account_id, pool_id) DO UPDATE SET
                   shares = excluded.shares,
                   cost_basis = excluded.cost_basis,
                   updated_at = CURRENT_TIMESTAMP""",
            (account_id, pool_id, new_shares, new_cost_basis),
        )
        return self.get_account_pool_position(account_id, pool_id, conn=conn)

    def get_account_asset_view(self, account_id: str) -> dict:
        account = self.get_capital_account(account_id)
        if account is None:
            raise ValueError(f"资产账户不存在: {account_id}")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM account_pool_positions WHERE account_id = ? ORDER BY pool_id",
                (account_id,),
            ).fetchall()
            positions = []
            total_value = 0.0
            total_cost_basis = 0.0
            for row in rows:
                pos = dict(row)
                pool = self.get_investment_pool(pos["pool_id"], conn=conn)
                snapshot = None
                if pool is None:
                    snapshot = self.get_latest_pool_nav_snapshot(pos["pool_id"], conn=conn)
                share_price = float((pool or snapshot or {}).get("share_price") or 0.0)
                market_value = round(float(pos["shares"]) * share_price, 2)
                cost_basis = round(float(pos.get("cost_basis", 0.0)), 2)
                total_value += market_value
                total_cost_basis += cost_basis
                positions.append({
                    "pool_id": pos["pool_id"],
                    "portfolio_id": (pool or {}).get("portfolio_id") or pos["pool_id"],
                    "pool_type": (pool or {}).get("pool_type", "core"),
                    "pool_status": (pool or {}).get("status", "ACTIVE"),
                    "pool_nav": round(float((pool or snapshot or {}).get("nav") or 0.0), 2),
                    "currency": (pool or {}).get("currency") or account.get("base_currency") or "USD",
                    "shares": round(float(pos["shares"]), 8),
                    "share_price": round(share_price, 8),
                    "market_value": market_value,
                    "cost_basis": cost_basis,
                    "unrealized_pnl": round(market_value - cost_basis, 2),
                })
        return {
            "account": account,
            "positions": positions,
            "total_value": round(total_value, 2),
            "total_cost_basis": round(total_cost_basis, 2),
            "unrealized_pnl": round(total_value - total_cost_basis, 2),
        }

    def get_family_aum_summary(self, family_office_id: str = "default") -> dict:
        pools = self.list_investment_pools(family_office_id=family_office_id, status="ACTIVE")
        total_nav = round(sum(float(pool.get("nav") or 0.0) for pool in pools), 2)
        total_shares = round(sum(float(pool.get("shares_outstanding") or 0.0) for pool in pools), 8)
        accounts = self.list_capital_accounts(family_office_id=family_office_id)
        account_views = []
        total_account_value = 0.0
        for account in accounts:
            view = self.get_account_asset_view(account["id"])
            total_account_value += float(view.get("total_value") or 0.0)
            account_views.append({
                "account_id": account["id"],
                "account_name": account["account_name"],
                "member_id": account["member_id"],
                "member_name": account.get("member_name"),
                "total_value": view["total_value"],
                "total_cost_basis": view["total_cost_basis"],
                "unrealized_pnl": view["unrealized_pnl"],
            })
        return {
            "family_office_id": family_office_id,
            "pool_count": len(pools),
            "account_count": len(accounts),
            "total_pool_nav": total_nav,
            "total_pool_shares": total_shares,
            "total_account_value": round(total_account_value, 2),
            "pools": pools,
            "accounts": account_views,
        }

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

    def _update_alpha_account(self, portfolio_id: str = "us", conn=None, **kwargs):
        if conn is None:
            raise ValueError("Alpha 账本账户更新必须在受控 ledger 事务内执行")
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [portfolio_id]
        sql = f"UPDATE alpha_accounts SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE portfolio_id = ?"

        conn.execute(
            "INSERT OR IGNORE INTO alpha_accounts (portfolio_id) VALUES (?)",
            (portfolio_id,),
        )
        conn.execute(sql, values)

    def _adjust_alpha_account_balance(
        self,
        portfolio_id: str,
        delta: float,
        note: str = "",
        conn=None,
    ) -> dict:
        if conn is None:
            raise ValueError("Alpha 账本余额调整必须在受控 ledger 事务内执行")

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
        new_balance = float(account.get("cash_balance", 0.0)) + delta
        if new_balance < 0:
            raise ValueError(f"Alpha 账本余额不足，调整后将为 {new_balance:.2f}")

        inflows = float(account.get("total_inflows", 0.0))
        outflows = float(account.get("total_outflows", 0.0))
        if delta >= 0:
            inflows += delta
        else:
            outflows += abs(delta)

        self._update_alpha_account(
            portfolio_id=portfolio_id,
            cash_balance=round(new_balance, 2),
            total_inflows=round(inflows, 2),
            total_outflows=round(outflows, 2),
            last_manual_adjustment=note,
            conn=conn,
        )
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

    def record_alpha_ledger_entry(
        self,
        portfolio_id: str,
        direction: str,
        amount: float,
        actor: str,
        insurance_decision_id: str,
        note: str = "",
        external_reference: str = "",
        related_request_id: str = "",
    ) -> tuple[dict, dict]:
        direction = direction.upper()
        if direction not in ("IN", "OUT"):
            raise ValueError(f"不支持的 Alpha 账本方向: {direction}")
        if amount <= 0:
            raise ValueError("记账金额必须大于 0")
        self._require_alpha_ledger_insurance_authority(
            portfolio_id=portfolio_id,
            direction=direction,
            insurance_decision_id=insurance_decision_id,
        )

        delta = amount if direction == "IN" else -amount
        entry_note = note or (f"Alpha 人工入账 +{amount:.2f}" if direction == "IN" else f"Alpha 人工出账 -{amount:.2f}")

        with self.transaction() as conn:
            account = self._adjust_alpha_account_balance(
                portfolio_id=portfolio_id,
                delta=delta,
                note=entry_note,
                conn=conn,
            )
            cursor = conn.execute(
                """INSERT INTO alpha_ledger_entries
                   (portfolio_id, direction, amount, balance_after, note,
                    external_reference, related_request_id, insurance_decision_id, actor)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    portfolio_id,
                    direction,
                    round(amount, 2),
                    round(float(account.get("cash_balance", 0.0)), 2),
                    entry_note,
                    external_reference,
                    related_request_id,
                    insurance_decision_id,
                    actor,
                ),
            )
            entry_id = cursor.lastrowid

        return self.get_alpha_ledger_entry(entry_id), self.get_alpha_account(portfolio_id)

    def _require_alpha_ledger_insurance_authority(
        self,
        portfolio_id: str,
        direction: str,
        insurance_decision_id: str,
    ) -> None:
        if not insurance_decision_id:
            raise ValueError("Alpha 账本记账缺少 InsuranceDecision 授权")

        latest = self.get_latest_insurance_decision(portfolio_id)
        if not latest:
            raise ValueError("Alpha 账本记账缺少持久化 InsuranceDecision")
        if latest.get("id") != insurance_decision_id:
            raise ValueError("Alpha 账本记账 InsuranceDecision 不是当前组合最新授权")

        decision = build_authority_decision(
            coerce_insurance_state(latest.get("new_state")),
            reasons=latest.get("reasons", []),
        )
        allowed = decision.allow_deposit if direction == "IN" else decision.allow_withdrawal_execution
        if not allowed:
            raise ValueError(
                "Insurance Layer blocked Alpha ledger "
                f"{'deposit' if direction == 'IN' else 'withdrawal'}"
            )

    def _require_alpha_execution_authority(
        self,
        portfolio_id: str,
        insurance_decision_id: str,
    ) -> None:
        if not insurance_decision_id:
            raise ValueError("Alpha 执行缺少 InsuranceDecision 授权")

        latest = self.get_latest_insurance_decision(portfolio_id)
        if not latest:
            raise ValueError("Alpha 执行缺少持久化 InsuranceDecision")
        if latest.get("id") != insurance_decision_id:
            raise ValueError("Alpha 执行 InsuranceDecision 不是当前组合最新授权")

        decision = build_authority_decision(
            coerce_insurance_state(latest.get("new_state")),
            reasons=latest.get("reasons", []),
        )
        if not decision.allow_alpha_execution:
            raise ValueError("Insurance Layer blocked Alpha execution")

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

    def upsert_strategy(
        self,
        strategy_id: str,
        portfolio_id: str = "us",
        insurance_decision_id: str | None = None,
        **kwargs,
    ):
        """创建或更新策略"""
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM alpha_strategies WHERE portfolio_id = ? AND id = ?",
                (portfolio_id, strategy_id),
            ).fetchone()
            existing_data = dict(existing) if existing else None
            target_status = kwargs.get("status") or (existing_data or {}).get("status")
            requires_authority = target_status == "ENABLED" and any(
                key in kwargs for key in ("allocation_pct", "capital", "status")
            )
            if requires_authority:
                self._require_alpha_execution_authority(
                    portfolio_id,
                    insurance_decision_id or self._current_alpha_authority_id(),
                )
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

    def update_strategy_status(
        self,
        strategy_id: str,
        status: str,
        portfolio_id: str = "us",
        insurance_decision_id: str | None = None,
    ):
        """启用/禁用策略"""
        if status == "ENABLED":
            self._require_alpha_execution_authority(
                portfolio_id,
                insurance_decision_id or self._current_alpha_authority_id(),
            )
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

    def save_alpha_transaction(
        self,
        strategy_id: str,
        portfolio_id: str,
        date: str,
        action: str,
        premium: float = 0,
        pnl: float = 0,
        insurance_decision_id: str | None = None,
        **kwargs,
    ):
        """记录 Alpha 策略交易"""
        authority_id = insurance_decision_id or self._current_alpha_authority_id()
        self._require_alpha_execution_authority(portfolio_id, authority_id)

        fields = [
            "strategy_id",
            "portfolio_id",
            "date",
            "action",
            "premium",
            "pnl",
            "insurance_decision_id",
        ]
        values = [strategy_id, portfolio_id, date, action, premium, pnl, authority_id]
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
