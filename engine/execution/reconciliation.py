"""
息壤（Xi-Rang）本地账本与券商账本对账器

原则：
- 先对账，后执行
- 差异可解释，才能放大自动化
- 发现异常时优先 fail-closed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

from engine.execution.broker_adapter import AccountSnapshot


class ReconciliationStatus(Enum):
    MATCHED = "MATCHED"
    DRIFT = "DRIFT"
    BROKEN = "BROKEN"


@dataclass
class ReconciliationItem:
    category: str
    key: str
    local_value: float
    broker_value: float
    delta: float
    threshold: float
    message: str


@dataclass
class ReconciliationReport:
    status: ReconciliationStatus
    broker_name: str
    checked_at: datetime
    items: list[ReconciliationItem] = field(default_factory=list)

    @property
    def requires_manual_intervention(self) -> bool:
        return self.status != ReconciliationStatus.MATCHED


class ReconciliationService:
    def __init__(
        self,
        nav_tolerance: float = 1.0,
        cash_tolerance: float = 1.0,
        position_value_tolerance: float = 50.0,
        severe_multiplier: float = 5.0,
    ):
        self.nav_tolerance = float(nav_tolerance)
        self.cash_tolerance = float(cash_tolerance)
        self.position_value_tolerance = float(position_value_tolerance)
        self.severe_multiplier = float(severe_multiplier)

    def _build_item(
        self,
        category: str,
        key: str,
        local_value: float,
        broker_value: float,
        threshold: float,
    ) -> ReconciliationItem | None:
        delta = broker_value - local_value
        if abs(delta) <= threshold:
            return None
        return ReconciliationItem(
            category=category,
            key=key,
            local_value=local_value,
            broker_value=broker_value,
            delta=delta,
            threshold=threshold,
            message=f"{category}:{key} 偏差 {delta:,.2f} 超过阈值 {threshold:,.2f}",
        )

    def reconcile(
        self,
        local_positions: dict[str, float],
        local_cash: float,
        local_nav: float,
        broker_snapshot: AccountSnapshot,
    ) -> ReconciliationReport:
        items: list[ReconciliationItem] = []

        cash_item = self._build_item(
            category="cash",
            key="cash",
            local_value=float(local_cash),
            broker_value=float(broker_snapshot.cash),
            threshold=self.cash_tolerance,
        )
        if cash_item:
            items.append(cash_item)

        nav_item = self._build_item(
            category="nav",
            key="total_value",
            local_value=float(local_nav),
            broker_value=float(broker_snapshot.total_value),
            threshold=self.nav_tolerance,
        )
        if nav_item:
            items.append(nav_item)

        all_symbols = sorted(set(local_positions) | set(broker_snapshot.positions))
        for symbol in all_symbols:
            local_value = float(local_positions.get(symbol, 0.0))
            broker_position = broker_snapshot.positions.get(symbol)
            broker_value = float(broker_position.market_value) if broker_position else 0.0
            item = self._build_item(
                category="position",
                key=symbol,
                local_value=local_value,
                broker_value=broker_value,
                threshold=self.position_value_tolerance,
            )
            if item:
                items.append(item)

        status = ReconciliationStatus.MATCHED
        for item in items:
            if item.category in {"cash", "nav"} and abs(item.delta) > item.threshold * self.severe_multiplier:
                status = ReconciliationStatus.BROKEN
                break
            if item.category == "position" and (
                item.local_value == 0.0 or item.broker_value == 0.0 or abs(item.delta) > item.threshold * self.severe_multiplier
            ):
                status = ReconciliationStatus.BROKEN
                break
            status = ReconciliationStatus.DRIFT

        return ReconciliationReport(
            status=status,
            broker_name=broker_snapshot.broker_name,
            checked_at=datetime.now(UTC),
            items=items,
        )
