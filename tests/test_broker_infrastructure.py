import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from db.database import Database
from api.routes.dashboard_routes import (
    _get_broker_sync_payload,
    _get_core_observation_payload,
    _get_observation_overview_payload,
    _get_shadow_run_payload,
)
from engine.execution.base import OrderSide, OrderStatus, TradeOrder
from engine.execution.broker import BrokerExecutor
from engine.execution.broker_adapter import (
    AccountSnapshot,
    BrokerOrderRequest,
    BrokerOrderReceipt,
    BrokerMode,
    FutuAdapter,
    IBKRAdapter,
    PositionSnapshot,
)
from engine.execution.factory import (
    create_broker_adapter,
    create_shadow_runner,
    get_broker_topology,
)
from engine.execution.paper import PaperExecutor
from engine.execution.paper_adapter import PaperAdapter
from engine.execution.reconciliation import ReconciliationService, ReconciliationStatus
from engine.execution.sync import BrokerSyncService


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


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


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


def test_ibkr_adapter_parses_account_snapshot(monkeypatch):
    adapter = IBKRAdapter(mode=BrokerMode.READ_ONLY, assets=["SPY", "TLT"])

    def fake_request(path, query=None):
        if path == "/portfolio/accounts":
            return [{"accountId": "U1234567"}]
        if path == "/portfolio/U1234567/ledger":
            return {
                "BASE": {
                    "currency": "USD",
                    "cashbalance": 1250.5,
                    "netliquidationvalue": 100500.25,
                }
            }
        if path == "/portfolio/U1234567/positions/0":
            return [
                {"contractDesc": "SPY", "position": 10, "mktValue": 5000.0, "avgCost": 490.0},
                {"contractDesc": "TLT", "position": 20, "mktValue": 2000.0, "avgCost": 98.0},
            ]
        return []

    monkeypatch.setattr(adapter, "_request_json", fake_request)

    snapshot = adapter.get_account_snapshot()

    assert snapshot.account_id == "U1234567"
    assert snapshot.currency == "USD"
    assert snapshot.cash == pytest.approx(1250.5)
    assert snapshot.total_value == pytest.approx(100500.25)
    assert snapshot.positions["SPY"].quantity == pytest.approx(10)
    assert snapshot.positions["TLT"].market_value == pytest.approx(2000.0)


def test_futu_adapter_parses_account_snapshot(monkeypatch):
    class EnumValue:
        def __init__(self, name):
            self.name = name

    class FakeModule:
        RET_OK = 0

        class TrdMarket:
            US = EnumValue("US")

        class TrdEnv:
            REAL = EnumValue("REAL")
            SIMULATE = EnumValue("SIMULATE")

        class SecurityFirm:
            FUTUSECURITIES = EnumValue("FUTUSECURITIES")

        class OpenSecTradeContext:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def get_acc_list(self):
                return 0, pd.DataFrame([{"acc_id": 1001, "trd_env": "REAL"}])

            def accinfo_query(self, trd_env=None, acc_id=None, refresh_cache=True):
                return 0, pd.DataFrame([{"cash": 888.8, "total_assets": 20000.0, "currency": "USD"}])

            def position_list_query(self, trd_env=None, acc_id=None, refresh_cache=True):
                return 0, pd.DataFrame(
                    [
                        {"code": "US.SPY", "qty": 5, "market_val": 2500.0, "cost_price": 490.0},
                        {"code": "US.TLT", "qty": 7, "market_val": 700.0, "cost_price": 99.0},
                    ]
                )

            def close(self):
                return None

    adapter = FutuAdapter(mode=BrokerMode.READ_ONLY, assets=["SPY", "TLT"])
    monkeypatch.setattr(adapter, "_load_futu", lambda: FakeModule)
    monkeypatch.setenv("FUTU_TRD_MARKET", "US")

    snapshot = adapter.get_account_snapshot()

    assert snapshot.account_id == "1001"
    assert snapshot.currency == "USD"
    assert snapshot.cash == pytest.approx(888.8)
    assert snapshot.total_value == pytest.approx(20000.0)
    assert snapshot.positions["US.SPY"].quantity == pytest.approx(5)


def test_futu_adapter_requires_explicit_order_submission_gate(monkeypatch):
    monkeypatch.delenv("FUTU_ENABLE_ORDER_SUBMISSION", raising=False)
    adapter = FutuAdapter(mode=BrokerMode.LIVE, assets=["SPY"])

    with pytest.raises(RuntimeError, match="FUTU_ENABLE_ORDER_SUBMISSION"):
        adapter.place_order(
            BrokerOrderRequest(
                symbol="SPY",
                side=OrderSide.BUY,
                quantity=1,
            )
        )


def test_futu_adapter_place_order_get_status_and_cancel(monkeypatch):
    monkeypatch.setenv("FUTU_ENABLE_ORDER_SUBMISSION", "1")
    monkeypatch.setenv("FUTU_UNLOCK_TRADE_PWD", "secret")
    monkeypatch.setenv("FUTU_TRD_MARKET", "US")

    class EnumValue:
        def __init__(self, name):
            self.name = name

        def __repr__(self):
            return self.name

    class FakeModule:
        RET_OK = 0

        class TrdMarket:
            US = EnumValue("US")

        class TrdEnv:
            REAL = EnumValue("REAL")
            SIMULATE = EnumValue("SIMULATE")

        class SecurityFirm:
            FUTUSECURITIES = EnumValue("FUTUSECURITIES")

        class TrdSide:
            BUY = EnumValue("BUY")
            SELL = EnumValue("SELL")

        class OrderType:
            MARKET = EnumValue("MARKET")
            NORMAL = EnumValue("NORMAL")

        class ModifyOrderOp:
            CANCEL = EnumValue("CANCEL")

        class OpenSecTradeContext:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def unlock_trade(self, password=None):
                assert password == "secret"
                return 0, "ok"

            def get_acc_list(self):
                return 0, pd.DataFrame([{"acc_id": 1001, "trd_env": "REAL"}])

            def place_order(self, **kwargs):
                assert kwargs["code"] == "US.SPY"
                assert kwargs["qty"] == 2
                assert kwargs["remark"] == "xirang-spy-1"
                return 0, pd.DataFrame(
                    [
                        {
                            "order_id": "futu-1",
                            "code": "US.SPY",
                            "trd_side": "BUY",
                            "qty": 2,
                            "dealt_qty": 0,
                            "price": 0.0,
                            "order_status": "SUBMITTED",
                            "remark": "xirang-spy-1",
                        }
                    ]
                )

            def order_list_query(self, order_id=None, trd_env=None, acc_id=None, refresh_cache=True):
                if str(order_id) == "futu-1":
                    return 0, pd.DataFrame(
                        [
                            {
                                "order_id": "futu-1",
                                "code": "US.SPY",
                                "trd_side": "BUY",
                                "qty": 2,
                                "dealt_qty": 2,
                                "dealt_avg_price": 503.2,
                                "order_status": "FILLED_ALL",
                                "remark": "xirang-spy-1",
                            }
                        ]
                    )
                return 0, pd.DataFrame([])

            def order_fee_query(self, order_id_list=None, trd_env=None, acc_id=None):
                return 0, pd.DataFrame([{"order_id": "futu-1", "fee_amount": 1.23}])

            def modify_order(self, modify_order_op=None, order_id=None, **kwargs):
                assert str(order_id) == "futu-1"
                return 0, pd.DataFrame(
                    [
                        {
                            "order_id": "futu-1",
                            "code": "US.SPY",
                            "trd_side": "BUY",
                            "qty": 2,
                            "dealt_qty": 0,
                            "order_status": "CANCELLED_ALL",
                            "remark": "xirang-spy-1",
                        }
                    ]
                )

            def close(self):
                return None

    adapter = FutuAdapter(mode=BrokerMode.LIVE, assets=["SPY"])
    monkeypatch.setattr(adapter, "_load_futu", lambda: FakeModule)

    submit_receipt = adapter.place_order(
        BrokerOrderRequest(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2,
            order_type="MARKET",
            client_order_id="xirang-spy-1",
        )
    )
    status_receipt = adapter.get_order_status("futu-1")
    cancel_receipt = adapter.cancel_order("futu-1")

    assert submit_receipt.order_id == "futu-1"
    assert submit_receipt.status == OrderStatus.SUBMITTED
    assert submit_receipt.commission == pytest.approx(1.23)
    assert status_receipt.status == OrderStatus.FILLED
    assert status_receipt.avg_fill_price == pytest.approx(503.2)
    assert cancel_receipt.status == OrderStatus.CANCELLED


def test_database_persists_broker_execution_events(temp_db):
    temp_db.save_broker_execution_events(
        portfolio_id="us",
        run_date="2026-04-17",
        events=[
            {
                "event_type": "SUBMIT_RESULT",
                "event_time": "2026-04-17T09:30:00+00:00",
                "broker_name": "ibkr",
                "broker_role": "primary",
                "broker_mode": "live",
                "order_id": "ibkr-1",
                "client_order_id": "xirang-spy-1",
                "symbol": "SPY",
                "side": "BUY",
                "requested_quantity": 1,
                "filled_quantity": 0,
                "status": "SUBMITTED",
                "message": "submitted",
                "raw": {"order_status": "Submitted"},
            }
        ],
    )

    rows = temp_db.list_broker_execution_events("us", run_date="2026-04-17")

    assert len(rows) == 1
    assert rows[0]["order_id"] == "ibkr-1"
    assert json.loads(rows[0]["raw_json"])["order_status"] == "Submitted"


def test_ibkr_adapter_requires_explicit_order_submission_gate(monkeypatch):
    monkeypatch.delenv("IBKR_ENABLE_ORDER_SUBMISSION", raising=False)
    adapter = IBKRAdapter(mode=BrokerMode.LIVE, assets=["SPY"])

    with pytest.raises(RuntimeError, match="IBKR_ENABLE_ORDER_SUBMISSION"):
        adapter.place_order(
            BrokerOrderRequest(
                symbol="SPY",
                side=OrderSide.BUY,
                quantity=1,
            )
        )


def test_ibkr_adapter_place_order_confirms_reply_chain(monkeypatch):
    monkeypatch.setenv("IBKR_ENABLE_ORDER_SUBMISSION", "1")
    adapter = IBKRAdapter(mode=BrokerMode.LIVE, assets=["SPY"])

    def fake_request(path, query=None, method="GET", body=None):
        if path == "/iserver/accounts":
            return {"selectedAccount": "U1234567"}
        if path == "/portfolio/accounts":
            return [{"accountId": "U1234567"}]
        if path == "/iserver/secdef/search":
            assert query == {"symbol": "SPY"}
            return [{"symbol": "SPY", "conid": "756733", "secType": "STK"}]
        if path == "/iserver/account/U1234567/orders":
            assert method == "POST"
            assert body["orders"][0]["conid"] == "756733"
            return [{"id": "reply-1", "message": ["confirm required"]}]
        if path == "/iserver/reply/reply-1":
            assert method == "POST"
            assert body == {"confirmed": True}
            return [{"order_id": "98765", "order_status": "Submitted", "ticker": "SPY", "totalSize": 2}]
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr(adapter, "_request_json", fake_request)

    receipt = adapter.place_order(
        BrokerOrderRequest(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2,
            client_order_id="xirang-spy-1",
        )
    )

    assert receipt.order_id == "98765"
    assert receipt.status == OrderStatus.SUBMITTED
    assert receipt.requested_quantity == 2
    assert receipt.symbol == "SPY"


def test_ibkr_adapter_get_order_status_parses_fill(monkeypatch):
    adapter = IBKRAdapter(mode=BrokerMode.LIVE, assets=["SPY"])

    def fake_request(path, query=None, method="GET", body=None):
        if path == "/iserver/accounts":
            return {"selectedAccount": "U1234567"}
        if path == "/iserver/account/order/status/98765":
            return {
                "orderId": "98765",
                "order_status": "Filled",
                "ticker": "SPY",
                "side": "SELL",
                "filled_qty": 3,
                "avg_price": 501.25,
            }
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr(adapter, "_request_json", fake_request)

    receipt = adapter.get_order_status("98765")

    assert receipt.order_id == "98765"
    assert receipt.status == OrderStatus.FILLED
    assert receipt.side == OrderSide.SELL
    assert receipt.filled_quantity == 3
    assert receipt.avg_fill_price == pytest.approx(501.25)


def test_broker_executor_execute_maps_receipts_to_orders():
    class StubAdapter:
        def __init__(self):
            self.placed = []

        def place_order(self, order):
            self.placed.append(order)
            return BrokerOrderReceipt(
                order_id="ibkr-1",
                status=OrderStatus.SUBMITTED,
                symbol=order.symbol,
                side=order.side,
                requested_quantity=order.quantity,
            )

        def get_order_status(self, order_id):
            return BrokerOrderReceipt(
                order_id=order_id,
                status=OrderStatus.FILLED,
                symbol="SPY",
                side=OrderSide.BUY,
                requested_quantity=1,
                filled_quantity=1,
                avg_fill_price=101.5,
            )

    executor = BrokerExecutor(broker=StubAdapter(), auto_confirm=True, assets=["SPY"])
    orders = [
        TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=1,
            estimated_price=100.0,
            estimated_amount=100.0,
        )
    ]

    result = executor.execute(orders)

    assert result.success is True
    assert result.total_bought == pytest.approx(101.5)
    assert result.total_sold == pytest.approx(0.0)
    assert result.orders[0].status == OrderStatus.FILLED
    assert result.orders[0].filled_price == pytest.approx(101.5)
    assert result.orders[0].broker_order_id == "ibkr-1"


def test_broker_executor_semi_auto_stays_in_awaiting_confirm():
    class StubAdapter:
        def place_order(self, order):
            raise AssertionError("semi_auto should not submit order")

        def get_order_status(self, order_id):
            raise AssertionError("semi_auto should not poll order")

    executor = BrokerExecutor(broker=StubAdapter(), auto_confirm=False, assets=["SPY"])
    orders = [
        TradeOrder(
            symbol="SPY",
            side=OrderSide.SELL,
            quantity=1,
            estimated_price=99.0,
            estimated_amount=99.0,
        )
    ]

    result = executor.execute(orders)

    assert result.success is True
    assert result.orders[0].status == OrderStatus.AWAITING_CONFIRM
    assert "等待人工确认" in result.message


def test_broker_executor_returns_failure_when_broker_rejects():
    class StubAdapter:
        def place_order(self, order):
            return BrokerOrderReceipt(
                order_id="ibkr-2",
                status=OrderStatus.FAILED,
                symbol=order.symbol,
                side=order.side,
                requested_quantity=order.quantity,
                message="rejected",
            )

        def get_order_status(self, order_id):
            raise AssertionError("rejected order should not poll")

    executor = BrokerExecutor(broker=StubAdapter(), auto_confirm=True, assets=["SPY"])
    orders = [
        TradeOrder(
            symbol="SPY",
            side=OrderSide.BUY,
            quantity=2,
            estimated_price=100.0,
            estimated_amount=200.0,
        )
    ]

    result = executor.execute(orders)

    assert result.success is False
    assert result.orders[0].status == OrderStatus.FAILED
    assert result.orders[0].error_message == "rejected"


def test_broker_sync_service_persists_snapshot_and_reconciliation(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    class StubAdapter:
        broker_name = "ibkr"

        def connect(self):
            return True

        def get_account_snapshot(self):
            return AccountSnapshot(
                broker_name="ibkr",
                mode=BrokerMode.READ_ONLY,
                account_id="U1234567",
                currency="USD",
                cash=0.0,
                total_value=100000.0,
                positions={
                    "SPY": PositionSnapshot(symbol="SPY", quantity=1, market_value=25000.0, avg_cost=500.0),
                    "TLT": PositionSnapshot(symbol="TLT", quantity=1, market_value=25000.0, avg_cost=100.0),
                    "GLD": PositionSnapshot(symbol="GLD", quantity=1, market_value=25000.0, avg_cost=200.0),
                    "SHV": PositionSnapshot(symbol="SHV", quantity=1, market_value=25000.0, avg_cost=110.0),
                },
                raw={"source": "test"},
            )

    monkeypatch.setattr("engine.execution.sync.create_broker_adapter", lambda **kwargs: StubAdapter())

    result = BrokerSyncService(temp_db).sync_portfolio("us")

    assert result["status"] == "MATCHED"

    snapshot_row = temp_db.get_latest_broker_account_snapshot("us", "primary")
    reconciliation_row = temp_db.get_latest_broker_reconciliation_run("us", "primary")

    assert snapshot_row is not None
    assert snapshot_row["broker_name"] == "ibkr"
    assert reconciliation_row is not None
    assert reconciliation_row["status"] == "MATCHED"


def test_dashboard_broker_sync_payload_groups_levels(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    monkeypatch.setenv("XIRANG_BROKER_PRIMARY", "ibkr")
    monkeypatch.setenv("XIRANG_BROKER_BACKUP", "futu")

    temp_db.save_broker_account_snapshot(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        broker_mode="read_only",
        account_id="U1234567",
        currency="USD",
        cash=90.0,
        total_value=99800.0,
        positions_json="{}",
        raw_json="{}",
        snapshot_time="2026-04-16T09:30:00+00:00",
    )
    temp_db.save_broker_reconciliation_run(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        status="MATCHED",
        checked_at="2026-04-16T09:31:00+00:00",
        items_json="[]",
        report_json='{"requires_manual_intervention": false}',
    )
    temp_db.save_broker_account_snapshot(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        broker_mode="read_only",
        account_id="U1234567",
        currency="USD",
        cash=100.0,
        total_value=100000.0,
        positions_json="{}",
        raw_json="{}",
        snapshot_time="2026-04-17T09:30:00+00:00",
    )
    temp_db.save_broker_reconciliation_run(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        status="DRIFT",
        checked_at="2026-04-17T09:31:00+00:00",
        items_json='[{"category":"position","key":"SPY","local_value":25000,"broker_value":25200,"delta":200,"threshold":50,"message":"position:SPY 偏差 200 超过阈值 50"}]',
        report_json='{"requires_manual_intervention": true}',
    )

    payload = _get_broker_sync_payload(temp_db, "us")

    assert payload["summary"]["overall_level"] == "warning"
    assert payload["summary"]["warning_count"] == 1
    assert payload["policy"]["required_role"] == "primary"
    assert payload["policy"]["require_snapshot_cover_market_date"] is True
    assert payload["policy"]["max_snapshot_lag_days"] == 0
    assert payload["roles"][0]["account_id_masked"].endswith("4567")
    assert payload["roles"][0]["level"] == "warning"
    assert payload["roles"][0]["anomaly_streak_runs"] == 1
    assert payload["roles"][0]["drift_streak_days"] == 1
    assert payload["roles"][1]["status"] == "MISSING"
    assert len(payload["history_by_role"]["primary"]) == 2
    assert payload["history_by_role"]["primary"][0]["level"] == "warning"
    assert payload["history_by_role"]["primary"][0]["medium_count"] == 1
    assert payload["history_by_role"]["primary"][0]["drift_streak_days"] == 1
    assert payload["history_by_role"]["primary"][1]["level"] == "healthy"


def test_broker_sync_history_links_core_execution_closure(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    monkeypatch.setenv("XIRANG_BROKER_PRIMARY", "ibkr")
    monkeypatch.setenv("XIRANG_BROKER_BACKUP", "futu")

    temp_db.save_broker_account_snapshot(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        broker_mode="read_only",
        account_id="U1234567",
        currency="USD",
        cash=100.0,
        total_value=100000.0,
        positions_json="{}",
        raw_json="{}",
        snapshot_time="2026-04-17T09:30:00+00:00",
    )
    temp_db.save_broker_reconciliation_run(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        status="MATCHED",
        checked_at="2026-04-17T09:31:00+00:00",
        items_json="[]",
        report_json='{"requires_manual_intervention": false}',
    )
    temp_db.record_run(
        "2026-04-17",
        "MANUAL_INTERVENTION_REQUIRED",
        json.dumps(
            {
                "run_status": "MANUAL_INTERVENTION_REQUIRED",
                "action": "人工介入: 执行后对账未闭环",
                "broker_sync_policy": {
                    "required_role": "primary",
                    "require_snapshot_cover_market_date": True,
                    "require_reconciliation_cover_market_date": True,
                    "max_snapshot_lag_days": 0,
                    "max_reconciliation_lag_days": 0,
                },
                "execution": {
                    "status": "FILLED",
                    "message": "已成交",
                    "orders": [{"symbol": "SPY"}, {"symbol": "TLT"}],
                },
                "post_execution_reconciliation": {
                    "status": "DRIFT",
                    "broker_name": "IBKR",
                    "broker_role": "primary",
                    "checked_at": "2026-04-17T09:45:00+00:00",
                    "difference_count": 2,
                    "requires_manual_intervention": True,
                },
            }
        ),
        portfolio_id="us",
    )

    payload = _get_broker_sync_payload(temp_db, "us")
    closure = payload["history_by_role"]["primary"][0]["core_closure"]

    assert closure["run_date"] == "2026-04-17"
    assert closure["execution_status"] == "FILLED"
    assert closure["execution_order_count"] == 2
    assert closure["post_execution_reconciliation"]["status"] == "DRIFT"
    assert payload["history_by_role"]["primary"][0]["market_date"] == "2026-04-17"


def test_shadow_run_payload_reads_latest_report(temp_db):
    temp_db.save_shadow_run_report(
        portfolio_id="us",
        broker_role="sandbox",
        broker_name="paper",
        checked_at="2026-04-16T10:00:00+00:00",
        dry_run=True,
        order_count=1,
        reconciliation_status="MATCHED",
        requires_attention=False,
        warnings_json="[]",
        report_json='{"order_count":1,"reconciliation_status":"MATCHED"}',
    )
    temp_db.save_shadow_run_report(
        portfolio_id="us",
        broker_role="sandbox",
        broker_name="paper",
        checked_at="2026-04-17T10:00:00+00:00",
        dry_run=True,
        order_count=2,
        reconciliation_status="DRIFT",
        requires_attention=True,
        warnings_json='["影子运行未完成账户对账"]',
        report_json='{"order_count":2,"reconciliation_status":"DRIFT"}',
    )

    payload = _get_shadow_run_payload(temp_db, "us")

    assert payload["broker_name"] == "paper"
    assert payload["order_count"] == 2
    assert payload["requires_attention"] is True
    assert payload["reconciliation_level"] == "warning"
    assert payload["attention_streak_runs"] == 1
    assert payload["attention_streak_days"] == 1
    assert len(payload["history"]) == 2
    assert payload["history"][0]["requires_attention"] is True
    assert payload["history"][0]["warning_count"] == 1
    assert payload["history"][1]["requires_attention"] is False


def test_core_observation_payload_reads_latest_run(temp_db):
    temp_db.record_run(
        "2026-04-17",
        "MANUAL_INTERVENTION_REQUIRED",
        json.dumps(
            {
                "run_status": "MANUAL_INTERVENTION_REQUIRED",
                "action": "人工介入: 数据过期",
                "manual_intervention_required": True,
                "manual_intervention_reasons": [
                    {"code": "STALE_DATA", "message": "数据过期: 最新交易日落后 2 天"}
                ],
                "execution": {
                    "status": "MANUAL_REVIEW",
                    "message": "等待人工复核",
                    "orders": [{"symbol": "SPY"}],
                },
                "nav": 101234.56,
            }
        ),
        portfolio_id="us",
    )

    payload = _get_core_observation_payload(temp_db, "us")

    assert payload["status"] == "MANUAL_INTERVENTION_REQUIRED"
    assert payload["level"] == "warning"
    assert payload["manual_intervention_required"] is True
    assert payload["manual_intervention_reasons"][0]["code"] == "STALE_DATA"
    assert payload["execution_status"] == "MANUAL_REVIEW"
    assert payload["execution_order_count"] == 1
    assert payload["nav"] == pytest.approx(101234.56)


def test_core_observation_payload_reads_broker_sync_gate(temp_db):
    temp_db.record_run(
        "2026-04-17",
        "FAILED_EXECUTION",
        json.dumps(
            {
                "run_status": "FAILED_EXECUTION",
                "action": "系统拦截: 主券商 IBKR 对账仍存在漂移，系统停止本次 Core 调仓",
                "manual_intervention_required": False,
                "broker_sync_gate": {
                    "code": "BROKER_RECONCILIATION_DRIFT",
                    "message": "主券商 IBKR 对账仍存在漂移，系统停止本次 Core 调仓",
                    "broker_role": "primary",
                    "broker_name": "IBKR",
                    "status": "DRIFT",
                },
                "nav": 100000.0,
            }
        ),
        portfolio_id="us",
    )

    payload = _get_core_observation_payload(temp_db, "us")

    assert payload["status"] == "FAILED_EXECUTION"
    assert payload["level"] == "critical"
    assert payload["broker_sync_gate"]["code"] == "BROKER_RECONCILIATION_DRIFT"
    assert payload["broker_sync_gate"]["status"] == "DRIFT"


def test_core_observation_payload_reads_post_execution_reconciliation(temp_db):
    temp_db.record_run(
        "2026-04-17",
        "MANUAL_INTERVENTION_REQUIRED",
        json.dumps(
            {
                "run_status": "MANUAL_INTERVENTION_REQUIRED",
                "action": "人工介入: 执行后对账未闭环",
                "manual_intervention_required": True,
                "post_execution_reconciliation": {
                    "status": "DRIFT",
                    "broker_name": "IBKR",
                    "broker_role": "primary",
                    "checked_at": "2026-04-17T09:45:00+00:00",
                    "difference_count": 2,
                    "requires_manual_intervention": True,
                    "local_state_source": "expected_post_trade_state",
                    "expected_local_nav": 99880.5,
                },
            }
        ),
        portfolio_id="us",
    )

    payload = _get_core_observation_payload(temp_db, "us")

    assert payload["post_execution_reconciliation"]["status"] == "DRIFT"
    assert payload["post_execution_reconciliation"]["difference_count"] == 2
    assert payload["post_execution_reconciliation"]["broker_name"] == "IBKR"
    assert payload["history"][0]["post_execution_reconciliation"]["status"] == "DRIFT"


def test_core_observation_payload_reads_execution_policy_gate(temp_db):
    temp_db.record_run(
        "2026-04-17",
        "FAILED_EXECUTION",
        json.dumps(
            {
                "run_status": "FAILED_EXECUTION",
                "action": "系统拦截: 真实执行单笔金额过大",
                "execution_policy_gate": {
                    "code": "LIVE_EXECUTION_SINGLE_ORDER_TOO_LARGE",
                    "message": "真实执行单笔金额过大: SPY 预估 20,000.00，超过上限 10,000.00",
                    "symbol": "SPY",
                    "estimated_amount": 20000.0,
                    "max_single_order_notional": 10000.0,
                },
                "live_execution_policy": {
                    "enabled": True,
                    "allowed_assets": ["SPY", "TLT", "GLD", "SHV"],
                    "allowed_order_sides": ["BUY", "SELL"],
                    "max_single_order_notional": 10000.0,
                    "max_daily_order_count": 4,
                    "max_daily_turnover_ratio": 0.2,
                },
            }
        ),
        portfolio_id="us",
    )

    payload = _get_core_observation_payload(temp_db, "us")

    assert payload["execution_policy_gate"]["code"] == "LIVE_EXECUTION_SINGLE_ORDER_TOO_LARGE"
    assert payload["execution_policy_gate"]["estimated_amount"] == pytest.approx(20000.0)
    assert payload["live_execution_policy"]["max_single_order_notional"] == pytest.approx(10000.0)
    assert payload["history"][0]["execution_policy_gate"]["code"] == "LIVE_EXECUTION_SINGLE_ORDER_TOO_LARGE"
    assert payload["history"][0]["live_execution_policy"]["max_daily_order_count"] == 4


def test_core_observation_history_contains_policy_snapshot(temp_db):
    temp_db.record_run(
        "2026-04-17",
        "FAILED_EXECUTION",
        json.dumps(
            {
                "run_status": "FAILED_EXECUTION",
                "action": "系统拦截: 主券商 IBKR 最新对账未覆盖交易日，停止本次 Core 调仓",
                "broker_sync_gate": {
                    "code": "BROKER_SYNC_STALE",
                    "message": "主券商 IBKR 最新对账未覆盖交易日，停止本次 Core 调仓",
                    "status": "MATCHED",
                },
                "broker_sync_policy": {
                    "required_role": "primary",
                    "require_snapshot_cover_market_date": True,
                    "require_reconciliation_cover_market_date": True,
                    "max_snapshot_lag_days": 0,
                    "max_reconciliation_lag_days": 0,
                },
            }
        ),
        portfolio_id="us",
    )

    payload = _get_core_observation_payload(temp_db, "us")

    assert payload["history"][0]["broker_sync_gate"]["code"] == "BROKER_SYNC_STALE"
    assert payload["history"][0]["broker_sync_policy"]["required_role"] == "primary"
    assert payload["history"][0]["broker_sync_policy"]["max_reconciliation_lag_days"] == 0


def test_observation_overview_prioritizes_core_then_broker_then_shadow(temp_db, monkeypatch):
    monkeypatch.setenv("XIRANG_BROKER_PRIMARY", "ibkr")
    monkeypatch.setenv("XIRANG_BROKER_BACKUP", "futu")

    temp_db.record_run(
        "2026-04-17",
        "MANUAL_INTERVENTION_REQUIRED",
        json.dumps(
            {
                "run_status": "MANUAL_INTERVENTION_REQUIRED",
                "action": "人工介入: 券商回执缺失",
                "manual_intervention_required": True,
            }
        ),
        portfolio_id="us",
    )
    temp_db.save_broker_account_snapshot(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        broker_mode="read_only",
        account_id="U1234567",
        currency="USD",
        cash=100.0,
        total_value=100000.0,
        positions_json="{}",
        raw_json="{}",
        snapshot_time="2026-04-17T09:30:00+00:00",
    )
    temp_db.save_broker_reconciliation_run(
        portfolio_id="us",
        broker_role="primary",
        broker_name="ibkr",
        status="DRIFT",
        checked_at="2026-04-17T09:31:00+00:00",
        items_json='[{"category":"position","key":"SPY","delta":200,"threshold":50}]',
        report_json='{"requires_manual_intervention": true}',
    )
    temp_db.save_shadow_run_report(
        portfolio_id="us",
        broker_role="sandbox",
        broker_name="paper",
        checked_at="2026-04-17T10:00:00+00:00",
        dry_run=True,
        order_count=2,
        reconciliation_status="DRIFT",
        requires_attention=True,
        warnings_json='["影子运行未完成账户对账"]',
        report_json='{"order_count":2,"reconciliation_status":"DRIFT"}',
    )

    overview = _get_observation_overview_payload(
        _get_core_observation_payload(temp_db, "us"),
        _get_broker_sync_payload(temp_db, "us"),
        _get_shadow_run_payload(temp_db, "us"),
    )

    assert overview["overall_level"] == "warning"
    assert overview["focus_chain"] == "core"
    assert [item["id"] for item in overview["items"]] == ["core", "broker_sync", "shadow_run"]
