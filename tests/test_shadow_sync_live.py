from datetime import datetime

import pandas as pd
import pytest

from engine.execution.broker_adapter import (
    AccountSnapshot,
    BrokerMode,
    PositionSnapshot,
    QuoteSnapshot,
)
from live import shadow_sync


def test_candidate_weights_for_qqq_spy_gld_sum_to_one():
    weights = shadow_sync.build_candidate_weights("qqq_spy_gld")

    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights == pytest.approx(
        {
            "GLD": 0.255,
            "QQQ": 0.04,
            "SHV": 0.225,
            "SPY": 0.255,
            "TLT": 0.225,
        }
    )


def test_observe_quote_calculates_spread_and_half_spread_slippage():
    quote = QuoteSnapshot(symbol="SPY", bid=99.5, ask=100.5, last=100.0)

    observed = shadow_sync.observe_quote("SPY", quote, source="fake")

    assert observed.mid == pytest.approx(100.0)
    assert observed.spread_bps == pytest.approx(100.0)
    assert observed.estimated_one_way_slippage_bps == pytest.approx(50.0)


class FakeReadOnlyAdapter:
    broker_name = "fake"

    def __init__(self):
        self.mode = BrokerMode.READ_ONLY
        self.place_order_called = False

    def connect(self):
        return True

    def get_account_snapshot(self):
        return AccountSnapshot(
            broker_name="fake",
            mode=BrokerMode.READ_ONLY,
            account_id="DU123",
            currency="USD",
            cash=100_000.0,
            total_value=2_000_000.0,
            positions={
                "SPY": PositionSnapshot("SPY", quantity=100.0, market_value=50_000.0),
            },
            as_of=datetime(2026, 6, 23),
            raw={"NetLiquidation": 2_000_000.0},
        )

    def get_quote(self, symbol):
        return QuoteSnapshot(symbol=symbol, bid=100.0, ask=100.2, last=100.1)

    def place_order(self, order):
        self.place_order_called = True
        raise AssertionError("shadow sync must not submit orders")


def test_shadow_sync_with_broker_generates_orders_without_trading(monkeypatch, tmp_path):
    adapter = FakeReadOnlyAdapter()
    monkeypatch.setattr(shadow_sync, "create_broker_adapter", lambda **_: adapter)

    report = shadow_sync.run_shadow_sync(
        aum=2_000_000.0,
        output_dir=tmp_path,
        no_broker=False,
        persist_db=False,
    )

    assert report["dry_run"] is True
    assert report["trading_disabled"] is True
    assert report["broker"]["connected"] is True
    assert report["shadow_orders"]
    assert adapter.place_order_called is False
    assert (tmp_path / "latest_shadow_sync.json").exists()


def _write_local_prices(data_dir, symbols):
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    dates = pd.date_range("2026-06-19", periods=2)
    for index, symbol in enumerate(symbols):
        frame = pd.DataFrame(
            {"adj_close": [100.0 + index, 101.0 + index]},
            index=dates,
        )
        frame.index.name = "date"
        frame.to_csv(raw_dir / f"{symbol}.csv")


def test_shadow_sync_offline_fallback_writes_report_and_warns(tmp_path):
    data_dir = tmp_path / "data"
    _write_local_prices(data_dir, ["SPY", "TLT", "GLD", "SHV", "QQQ"])

    report = shadow_sync.run_shadow_sync(
        aum=2_000_000.0,
        output_dir=tmp_path / "shadow",
        data_dir=data_dir,
        no_broker=True,
        persist_db=False,
    )

    assert report["status"] == "WARNING"
    assert report["slippage_audit"]["status"] == "LOCAL_PRICE_ONLY"
    assert report["shadow_orders"] == []
    assert "current positions unavailable" in " ".join(report["warnings"])
    assert (tmp_path / "shadow" / "latest_shadow_sync.json").exists()
