from datetime import datetime

from engine.execution.broker_adapter import (
    AccountSnapshot,
    BrokerMode,
    BrokerOrderRequest,
    PositionSnapshot,
    QuoteSnapshot,
)
from live import margin_monitor


def test_extract_margin_fields_from_nested_ibkr_like_payload():
    raw = {
        "ledger": {"BASE": {"netliquidationvalue": "2000000"}},
        "accountSummary": [
            {"key": "FullMaintainMarginReq", "value": "300000"},
            {"key": "ExcessLiquidity", "value": "1700000"},
            {"tag": "BuyingPower", "value": "8000000"},
        ],
    }

    fields = margin_monitor.extract_margin_fields(raw)

    assert fields["FullMaintainMarginReq"]["value"] == 300_000.0
    assert fields["ExcessLiquidity"]["value"] == 1_700_000.0
    assert fields["BuyingPower"]["value"] == 8_000_000.0
    assert fields["NetLiquidation"]["value"] == 2_000_000.0


def test_missing_margin_fields_are_unavailable():
    status, warnings = margin_monitor.margin_status({})

    assert status == "UNAVAILABLE"
    assert warnings


class FakeMarginAdapter:
    broker_name = "fake"

    def __init__(self):
        self.place_order_called = False

    def connect(self):
        return True

    def get_account_snapshot(self):
        return AccountSnapshot(
            broker_name="fake",
            mode=BrokerMode.READ_ONLY,
            account_id="DU123",
            currency="USD",
            cash=50_000.0,
            total_value=2_000_000.0,
            positions={
                "SPY": PositionSnapshot("SPY", quantity=100.0, market_value=50_000.0),
            },
            as_of=datetime(2026, 6, 23),
            raw={
                "accountSummary": [
                    {"key": "NetLiquidation", "value": "2000000"},
                    {"key": "ExcessLiquidity", "value": "1500000"},
                    {"key": "FullMaintainMarginReq", "value": "350000"},
                ]
            },
        )

    def get_quote(self, symbol):
        return QuoteSnapshot(symbol=symbol, bid=100.0, ask=100.1, last=100.0)

    def place_order(self, order: BrokerOrderRequest):
        self.place_order_called = True
        raise AssertionError("margin monitor must not submit orders")


def test_margin_monitor_persists_observation_without_trading(monkeypatch, tmp_path):
    adapter = FakeMarginAdapter()
    monkeypatch.setattr(margin_monitor, "create_broker_adapter", lambda **_: adapter)

    report = margin_monitor.run_margin_monitor(
        output_dir=tmp_path,
        persist_db=False,
    )

    assert report["status"] == "OBSERVED"
    assert report["requires_attention"] is False
    assert report["margin_fields"]["ExcessLiquidity"]["value"] == 1_500_000.0
    assert adapter.place_order_called is False
    assert (tmp_path / "latest_margin_snapshot.json").exists()


def test_margin_monitor_unavailable_when_broker_cannot_connect(monkeypatch, tmp_path):
    class DisconnectedAdapter(FakeMarginAdapter):
        def connect(self):
            return False

    monkeypatch.setattr(margin_monitor, "create_broker_adapter", lambda **_: DisconnectedAdapter())

    report = margin_monitor.run_margin_monitor(
        output_dir=tmp_path,
        persist_db=False,
    )

    assert report["status"] == "UNAVAILABLE"
    assert report["requires_attention"] is True
    assert report["warnings"]


def test_margin_monitor_no_broker_mode_never_creates_adapter(monkeypatch, tmp_path):
    def fail_if_called(**kwargs):
        raise AssertionError("no_broker margin monitor must not create a broker adapter")

    monkeypatch.setattr(margin_monitor, "create_broker_adapter", fail_if_called)

    report = margin_monitor.run_margin_monitor(
        output_dir=tmp_path,
        persist_db=False,
        no_broker=True,
    )

    assert report["status"] == "UNAVAILABLE"
    assert report["requires_attention"] is True
    assert report["broker"]["name"] == "offline"
    assert report["broker"]["connected"] is False
    assert report["trading_disabled"] is True
    assert "no_broker" in " ".join(report["warnings"])
