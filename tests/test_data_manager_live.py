from datetime import datetime, timedelta

import pandas as pd
import pytest

from data import data_manager as dm_mod


@pytest.fixture
def isolated_live_data(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)

    monkeypatch.setattr(dm_mod, "DATA_DIR", data_dir)
    monkeypatch.setattr(dm_mod, "RAW_DIR", raw_dir)
    monkeypatch.setattr(dm_mod, "FETCH_STATE_FILE", data_dir / "data_fetch_state.json")
    monkeypatch.setattr(
        dm_mod,
        "LIVE_CONFIGS",
        {
            "live_us": {
                "file": "live_us.csv",
                "tickers": ["SPY"],
                "lookback_days": 40,
            }
        },
    )
    return data_dir


def _write_raw(data_dir, ticker, dates):
    values = range(100, 100 + len(dates))
    series = pd.Series(values, index=pd.to_datetime(dates), name=ticker, dtype=float)
    df = series.to_frame("adj_close")
    df.index.name = "date"
    df.to_csv(data_dir / "raw" / f"{ticker}.csv")
    return series


def _write_live(data_dir, series):
    df = series.to_frame("SPY")
    df.index.name = "date"
    df.to_csv(data_dir / "live_us.csv")


def test_update_live_skips_api_and_timestamp_when_expected_date_is_cached(
    isolated_live_data,
):
    data_dir = isolated_live_data
    series = _write_raw(
        data_dir,
        "SPY",
        ["2026-05-18", "2026-05-19", "2026-05-20", "2026-05-21"],
    )
    _write_live(data_dir, series)
    dm = dm_mod.DataManager(min_interval=0)

    class FailingSource:
        def fetch_ticker(self, *args, **kwargs):
            raise AssertionError("API should not be called when expected date is cached")

    dm.source = FailingSource()
    now = datetime(2026, 5, 22, 11, 5)
    first = dm.update_live(now=now)
    assert first["api_attempted"] == 0

    ts_path = data_dir / "last_update.txt"
    ts_path.write_text("old timestamp", encoding="utf-8")
    summary = dm.update_live(now=now)

    assert summary["api_attempted"] == 0
    assert summary["updated_files"] == []
    assert ts_path.read_text(encoding="utf-8") == "old timestamp"


def test_update_live_records_rate_limit_cooldown_and_skips_next_api_call(
    isolated_live_data,
):
    data_dir = isolated_live_data
    _write_raw(
        data_dir,
        "SPY",
        ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24"],
    )

    dm = dm_mod.DataManager(min_interval=0, cooldown_hours=12)

    class RateLimitedSource:
        calls = 0

        def fetch_ticker(self, *args, **kwargs):
            self.calls += 1
            raise RuntimeError("429 too many requests")

    source = RateLimitedSource()
    dm.source = source
    now = datetime(2026, 5, 22, 11, 5)
    summary = dm.update_live(now=now)

    assert source.calls == 1
    assert summary["api_attempted"] == 1
    assert (data_dir / "data_fetch_state.json").exists()

    dm2 = dm_mod.DataManager(min_interval=0, cooldown_hours=12)

    class NoApiSource:
        def fetch_ticker(self, *args, **kwargs):
            raise AssertionError("API should not be called during cooldown")

    dm2.source = NoApiSource()
    summary = dm2.update_live(now=now + timedelta(hours=1))

    assert summary["cooldown_active"] is True
    assert summary["api_attempted"] == 0


def test_update_live_preserves_configured_column_order(isolated_live_data, monkeypatch):
    data_dir = isolated_live_data
    monkeypatch.setattr(
        dm_mod,
        "LIVE_CONFIGS",
        {
            "live_us": {
                "file": "live_us.csv",
                "tickers": ["SPY", "GLD"],
                "lookback_days": 40,
            }
        },
    )
    _write_raw(data_dir, "SPY", ["2026-05-20", "2026-05-21"])
    _write_raw(data_dir, "GLD", ["2026-05-20"])

    dm = dm_mod.DataManager(min_interval=0)

    class FailingSource:
        def fetch_ticker(self, *args, **kwargs):
            raise RuntimeError("source down")

    dm.source = FailingSource()
    dm.update_live(now=datetime(2026, 5, 22, 11, 5))

    df = pd.read_csv(data_dir / "live_us.csv")
    assert list(df.columns) == ["date", "SPY", "GLD"]


def test_data_source_can_disable_yfinance_fallback(monkeypatch):
    source = dm_mod.DataSource(dm_mod.RateLimiter(min_interval=0), allow_yfinance=False)

    def fail_akshare(*args, **kwargs):
        raise RuntimeError("akshare down")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("yfinance fallback should be disabled")

    monkeypatch.setattr(source, "_fetch_akshare_us", fail_akshare)
    monkeypatch.setattr(source, "_fetch_yfinance", fail_if_called)

    with pytest.raises(RuntimeError, match="yfinance_fallback: disabled"):
        source.fetch_ticker("SPY", "2026-05-01", "2026-05-22")


def test_us_data_source_prefers_yfinance_when_allowed(monkeypatch):
    source = dm_mod.DataSource(dm_mod.RateLimiter(min_interval=0), allow_yfinance=True)
    expected = pd.Series(
        [100.0, 101.0],
        index=pd.to_datetime(["2026-05-20", "2026-05-21"]),
        name="SPY",
    )

    def fetch_yfinance(*args, **kwargs):
        return expected

    def fail_if_called(*args, **kwargs):
        raise AssertionError("akshare should not be called before yfinance")

    monkeypatch.setattr(source, "_fetch_yfinance", fetch_yfinance)
    monkeypatch.setattr(source, "_fetch_akshare_us", fail_if_called)

    result = source.fetch_ticker("SPY", "2026-05-01", "2026-05-22")

    pd.testing.assert_series_equal(result, expected)


def test_positive_series_guard_rejects_qfq_like_negative_prices():
    series = pd.Series(
        [51.49, -1.14],
        index=pd.to_datetime(["2009-03-02", "2009-03-03"]),
        name="SPY",
    )

    with pytest.raises(RuntimeError, match="non-positive prices"):
        dm_mod._validate_positive_series(series, "SPY", "akshare qfq")


def test_save_raw_rejects_non_positive_prices(tmp_path):
    manager = dm_mod.DataManager(min_interval=0)
    series = pd.Series(
        [100.0, 0.0],
        index=pd.to_datetime(["2026-05-20", "2026-05-21"]),
        name="SPY",
    )

    with pytest.raises(RuntimeError, match="non-positive prices"):
        manager._save_raw(series, tmp_path / "SPY.csv")


def test_generate_etf_daily_rejects_non_positive_prices(isolated_live_data):
    manager = dm_mod.DataManager(min_interval=0)
    dates = pd.to_datetime(["2026-05-20", "2026-05-21"])
    ticker_data = {
        "SPY": pd.Series([100.0, -1.0], index=dates, name="SPY"),
        "TLT": pd.Series([90.0, 91.0], index=dates, name="TLT"),
        "GLD": pd.Series([180.0, 181.0], index=dates, name="GLD"),
        "SHV": pd.Series([110.0, 110.1], index=dates, name="SHV"),
    }

    with pytest.raises(RuntimeError, match="non-positive prices"):
        manager._generate_etf_daily(ticker_data)
