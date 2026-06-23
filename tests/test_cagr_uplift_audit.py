import json
from pathlib import Path

import pandas as pd
import pytest

import backtest.cagr_uplift_audit as audit


def test_validate_positive_rejects_non_positive_cash_proxy():
    series = pd.Series(
        [100.0, 0.0],
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
        name="BAD",
    )

    with pytest.raises(ValueError, match="non-positive prices"):
        audit._validate_positive(series, "BAD", "test")


def test_build_cash_proxy_prices_replaces_only_shv(monkeypatch):
    dates = pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06"])
    prices = pd.DataFrame(
        {
            "SPY": [100.0, 101.0, 102.0],
            "TLT": [90.0, 91.0, 92.0],
            "GLD": [80.0, 81.0, 82.0],
            "SHV": [50.0, 50.1, 50.2],
        },
        index=dates,
    )
    proxy = pd.Series(
        [200.0, 201.0],
        index=dates[1:],
        name="BIL",
    )
    monkeypatch.setattr(audit, "load_prices", lambda: prices)
    monkeypatch.setattr(audit, "DEFAULT_END", "2020-01-06")

    result = audit.build_cash_proxy_prices(proxy)

    assert list(result.columns) == ["SPY", "TLT", "GLD", "SHV"]
    assert result.index.tolist() == dates[1:].tolist()
    assert result["SPY"].tolist() == [101.0, 102.0]
    assert result["SHV"].tolist() == [200.0, 201.0]


def test_load_or_fetch_cash_proxy_uses_snapshot_without_network(tmp_path):
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    frame = pd.DataFrame(
        {"adj_close": [100.0, 100.1]},
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
    )
    frame.index.name = "date"
    frame.to_csv(snapshot_dir / "BIL.csv")

    series, source = audit.load_or_fetch_cash_proxy(
        "BIL",
        allow_download=False,
        snapshot_dir=snapshot_dir,
    )

    assert series.name == "BIL"
    assert series.tolist() == [100.0, 100.1]
    assert source.endswith("BIL.csv")


def test_write_cash_proxy_manifest_records_sha(tmp_path):
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    frame = pd.DataFrame(
        {"adj_close": [100.0, 101.0]},
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
    )
    frame.index.name = "date"
    frame.to_csv(snapshot_dir / "USFR.csv")

    manifest_path = audit.write_cash_proxy_manifest(snapshot_dir)
    payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

    assert payload["policy"]["production_candidate_change"] is False
    assert payload["tickers"]["USFR"]["rows"] == 2
    assert payload["tickers"]["USFR"]["sha256"]


def test_run_cash_proxy_uplift_audit_baseline_is_self_comparison():
    rows = audit.run_cash_proxy_uplift_audit(tickers=["SHV"], allow_download=False)

    assert len(rows) == 1
    row = rows[0]
    assert row.ticker == "SHV"
    assert abs(row.real_cagr_delta) < 1e-12
    assert abs(row.real_mdd_delta) < 1e-12
    assert row.pass_mdd_guardrail is True
