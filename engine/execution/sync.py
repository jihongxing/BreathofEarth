"""
息壤（Xi-Rang）Phase 1 券商只读同步与对账服务

职责：
- 从券商读取账户快照
- 与本地 Core 账本做对账
- 将快照与对账报告落盘

不负责：
- 下单
- 迁仓
- 自动修正本地账本
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime

from db.database import Database
from engine.config import PORTFOLIOS
from engine.execution.broker_adapter import BrokerMode
from engine.execution.factory import create_broker_adapter
from engine.execution.reconciliation import ReconciliationService


class BrokerSyncService:
    def __init__(self, db: Database | None = None, reconciliation_service: ReconciliationService | None = None):
        self.db = db or Database()
        self.reconciliation = reconciliation_service or ReconciliationService()

    def _sync_against_local_state(
        self,
        *,
        portfolio_id: str,
        local_positions: dict[str, float],
        local_cash: float,
        local_nav: float,
        broker_role: str = "primary",
        broker_name: str | None = None,
        local_state_source: str = "ledger",
    ) -> dict:
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]

        adapter = create_broker_adapter(
            role=broker_role,
            broker_name=broker_name,
            mode=BrokerMode.READ_ONLY,
            assets=assets,
        )

        if not adapter.connect():
            raise RuntimeError(f"{adapter.broker_name} 只读连接失败，未进入同步阶段")

        snapshot = adapter.get_account_snapshot()
        report = self.reconciliation.reconcile(
            local_positions=local_positions,
            local_cash=local_cash,
            local_nav=local_nav,
            broker_snapshot=snapshot,
        )

        positions_json = json.dumps(
            {
                symbol: asdict(position)
                for symbol, position in snapshot.positions.items()
            },
            ensure_ascii=False,
        )
        raw_json = json.dumps(snapshot.raw, ensure_ascii=False, default=str)
        items_json = json.dumps([asdict(item) for item in report.items], ensure_ascii=False)
        report_json = json.dumps(
            {
                "status": report.status.value,
                "broker_name": report.broker_name,
                "checked_at": report.checked_at.isoformat(),
                "requires_manual_intervention": report.requires_manual_intervention,
                "local_state_source": local_state_source,
                "items": [asdict(item) for item in report.items],
                "local_nav": local_nav,
                "local_cash": local_cash,
                "local_positions": local_positions,
            },
            ensure_ascii=False,
            default=str,
        )

        with self.db.transaction() as conn:
            self.db.save_broker_account_snapshot(
                portfolio_id=portfolio_id,
                broker_role=broker_role,
                broker_name=snapshot.broker_name,
                broker_mode=snapshot.mode.value,
                account_id=snapshot.account_id or "",
                currency=snapshot.currency,
                cash=snapshot.cash,
                total_value=snapshot.total_value,
                positions_json=positions_json,
                raw_json=raw_json,
                snapshot_time=snapshot.as_of.isoformat() if snapshot.as_of else "",
                conn=conn,
            )
            self.db.save_broker_reconciliation_run(
                portfolio_id=portfolio_id,
                broker_role=broker_role,
                broker_name=report.broker_name,
                status=report.status.value,
                checked_at=report.checked_at.isoformat(),
                items_json=items_json,
                report_json=report_json,
                conn=conn,
            )

        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "portfolio": portfolio_id,
            "broker_role": broker_role,
            "broker_name": snapshot.broker_name,
            "account_id": snapshot.account_id,
            "currency": snapshot.currency,
            "cash": round(snapshot.cash, 2),
            "total_value": round(snapshot.total_value, 2),
            "status": report.status.value,
            "requires_manual_intervention": report.requires_manual_intervention,
            "difference_count": len(report.items),
            "checked_at": report.checked_at.isoformat(),
            "local_state_source": local_state_source,
            "items": [asdict(item) for item in report.items],
        }

    def sync_portfolio(
        self,
        portfolio_id: str,
        broker_role: str = "primary",
        broker_name: str | None = None,
    ) -> dict:
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]

        self.db.ensure_portfolio(portfolio_id, assets)
        state = self.db.get_portfolio(portfolio_id)

        local_positions_values = json.loads(state["positions"])
        local_positions = {
            asset: float(value)
            for asset, value in zip(assets, local_positions_values)
        }
        local_cash = 0.0
        local_nav = sum(local_positions.values()) + local_cash

        return self._sync_against_local_state(
            portfolio_id=portfolio_id,
            local_positions=local_positions,
            local_cash=local_cash,
            local_nav=local_nav,
            broker_role=broker_role,
            broker_name=broker_name,
            local_state_source="ledger",
        )

    def reconcile_expected_state(
        self,
        *,
        portfolio_id: str,
        local_positions: dict[str, float],
        local_cash: float,
        broker_role: str = "primary",
        broker_name: str | None = None,
    ) -> dict:
        local_nav = sum(float(value) for value in local_positions.values()) + float(local_cash)
        return self._sync_against_local_state(
            portfolio_id=portfolio_id,
            local_positions={symbol: float(value) for symbol, value in local_positions.items()},
            local_cash=float(local_cash),
            local_nav=float(local_nav),
            broker_role=broker_role,
            broker_name=broker_name,
            local_state_source="expected_post_execution",
        )
