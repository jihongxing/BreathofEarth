"""
息壤（Xi-Rang）影子运行器

影子运行只做三件事：
- 读取账户
- 生成拟执行单
- 输出对账与偏差结果

它永远不负责真实下单。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from engine.execution.broker_adapter import AccountSnapshot, BrokerAdapter
from engine.execution.reconciliation import ReconciliationReport, ReconciliationService


@dataclass
class ShadowRunReport:
    broker_name: str
    order_count: int
    dry_run: bool
    account_snapshot: AccountSnapshot | None = None
    reconciliation: ReconciliationReport | None = None
    warnings: list[str] = field(default_factory=list)


class ShadowRun:
    def __init__(self, executor, adapter: BrokerAdapter, reconciliation_service: ReconciliationService | None = None):
        self.executor = executor
        self.adapter = adapter
        self.reconciliation = reconciliation_service or ReconciliationService()

    def run(
        self,
        current_positions: dict[str, float],
        target_weights: list[float],
        total_nav: float,
        current_prices: dict[str, float],
        local_cash: float = 0.0,
    ) -> tuple[list, ShadowRunReport]:
        trade_orders = self.executor.translate_orders(
            current_positions=current_positions,
            target_weights=target_weights,
            total_nav=total_nav,
            current_prices=current_prices,
        )

        warnings = []
        account_snapshot = None
        reconciliation = None

        try:
            account_snapshot = self.adapter.get_account_snapshot()
            reconciliation = self.reconciliation.reconcile(
                local_positions=current_positions,
                local_cash=local_cash,
                local_nav=total_nav,
                broker_snapshot=account_snapshot,
            )
        except Exception as exc:
            warnings.append(f"影子运行未完成账户对账: {type(exc).__name__}: {exc}")

        report = ShadowRunReport(
            broker_name=self.adapter.broker_name,
            order_count=len(trade_orders),
            dry_run=True,
            account_snapshot=account_snapshot,
            reconciliation=reconciliation,
            warnings=warnings,
        )
        return trade_orders, report
