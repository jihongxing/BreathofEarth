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


class Database:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        """初始化数据库，执行 schema"""
        with self._conn() as conn:
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
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

    def update_portfolio(self, portfolio_id: str = "default", **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [portfolio_id]
        with self._conn() as conn:
            conn.execute(
                f"UPDATE portfolios SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                values,
            )

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
    ):
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO daily_snapshots
                   (portfolio_id, date, state, nav, positions, weights, drawdown,
                    spy_tlt_corr, action, trigger_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    portfolio_id, date, state, nav,
                    json.dumps(positions), json.dumps(weights),
                    drawdown, spy_tlt_corr, action, trigger_reason,
                ),
            )

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
    ):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO transactions
                   (portfolio_id, date, type, target_weights, turnover, friction_cost, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    portfolio_id, date, tx_type,
                    json.dumps(target_weights) if target_weights else None,
                    turnover, friction_cost, reason,
                ),
            )

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
    ):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO risk_events
                   (portfolio_id, date, event_type, severity, drawdown, spy_tlt_corr, action_taken)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (portfolio_id, date, event_type, severity, drawdown, spy_tlt_corr, action_taken),
            )

    # ── 幂等性保护 ────────────────────────────────────

    def has_run_today(self, date: str, portfolio_id: str = "default") -> bool:
        """检查今天是否已经成功运行过"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM daily_runs WHERE portfolio_id = ? AND date = ? AND status = 'SUCCESS'",
                (portfolio_id, date),
            ).fetchone()
            return row is not None

    def record_run(self, date: str, status: str = "SUCCESS", report: str = "", portfolio_id: str = "default"):
        """记录本次运行"""
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO daily_runs (portfolio_id, date, status, report)
                   VALUES (?, ?, ?, ?)""",
                (portfolio_id, date, status, report),
            )
