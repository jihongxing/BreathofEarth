import pandas as pd

from engine.execution.base import OrderSide
from engine.execution.factory import (
    create_broker_adapter,
    create_shadow_runner,
    get_broker_topology,
)
from engine.execution.paper import PaperExecutor
from engine.execution.paper_adapter import PaperAdapter
from engine.execution.reconciliation import ReconciliationService, ReconciliationStatus


class StubMarketDataService:
    def __init__(self, assets=None, data_source=None):
        self.assets = assets or []
        self.data_source = data_source

    def fetch_latest(self, lookback_days=5):
        return pd.DataFrame(
            {
                "SPY": [100.0, 101.0],
                "TLT": [100.0, 99.0],
                "GLD": [100.0, 102.0],
                "SHV": [100.0, 100.0],
            },
            index=pd.to_datetime(["2026-04-16", "2026-04-17"]),
        )


def test_get_broker_topology_defaults(monkeypatch):
    monkeypatch.delenv("XIRANG_BROKER", raising=False)
    monkeypatch.delenv("XIRANG_BROKER_PRIMARY", raising=False)
    monkeypatch.delenv("XIRANG_BROKER_BACKUP", raising=False)
    monkeypatch.delenv("XIRANG_BROKER_SANDBOX", raising=False)

    topology = get_broker_topology()

    assert topology == {
        "primary": "ibkr",
        "backup": "futu",
        "sandbox": "paper",
    }


def test_create_broker_adapter_supports_role_split(monkeypatch):
    monkeypatch.setenv("XIRANG_BROKER_PRIMARY", "futu")
    monkeypatch.setenv("XIRANG_BROKER_BACKUP", "ibkr")
    monkeypatch.setenv("XIRANG_BROKER_SANDBOX", "paper")

    primary = create_broker_adapter(role="primary")
    backup = create_broker_adapter(role="backup")
    sandbox = create_broker_adapter(role="sandbox", market_data_service=StubMarketDataService())

    assert primary.broker_name == "futu"
    assert backup.broker_name == "ibkr"
    assert isinstance(sandbox, PaperAdapter)


def test_reconciliation_breaks_on_missing_position():
    snapshot = PaperAdapter(
        market_data_service=StubMarketDataService(),
        positions={"SPY": 10},
        cash=0.0,
    ).get_account_snapshot()

    report = ReconciliationService(position_value_tolerance=10.0).reconcile(
        local_positions={"TLT": 990.0},
        local_cash=0.0,
        local_nav=990.0,
        broker_snapshot=snapshot,
    )

    assert report.status == ReconciliationStatus.BROKEN
    assert report.requires_manual_intervention is True


def test_shadow_runner_generates_orders_without_execution():
    market = StubMarketDataService()
    executor = PaperExecutor(market_data_service=market, assets=["SPY", "TLT", "GLD", "SHV"])
    shadow = create_shadow_runner(
        market_data_service=market,
        assets=["SPY", "TLT", "GLD", "SHV"],
        executor=executor,
    )

    orders, report = shadow.run(
        current_positions={"SPY": 1000.0, "TLT": 3000.0, "GLD": 3000.0, "SHV": 3000.0},
        target_weights=[0.25, 0.25, 0.25, 0.25],
        total_nav=10000.0,
        current_prices={"SPY": 101.0, "TLT": 99.0, "GLD": 102.0, "SHV": 100.0},
        local_cash=0.0,
    )

    assert report.dry_run is True
    assert report.order_count == len(orders)
    assert any(order.side == OrderSide.BUY for order in orders)
    assert report.reconciliation is not None
