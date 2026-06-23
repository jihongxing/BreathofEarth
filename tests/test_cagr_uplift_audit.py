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


def test_momentum_target_asset_defaults_to_cash_until_lookback():
    dates = pd.bdate_range("2020-01-02", periods=260)
    prices = pd.DataFrame(
        {
            "QQQ": [100.0 + i for i in range(len(dates))],
            "GLD": [100.0] * len(dates),
            "SHV": [100.0] * len(dates),
        },
        index=dates,
    )

    assert audit.momentum_target_asset(prices, dates[100]) == "SHV"
    assert audit.momentum_target_asset(prices, dates[253]) == "QQQ"


def test_momentum_target_asset_stays_cash_when_12m_return_is_negative():
    dates = pd.bdate_range("2020-01-02", periods=260)
    prices = pd.DataFrame(
        {
            "QQQ": [300.0 - i for i in range(len(dates))],
            "GLD": [100.0] * len(dates),
            "SHV": [100.0] * len(dates),
        },
        index=dates,
    )

    assert audit.momentum_target_asset(prices, dates[253]) == "SHV"


def test_momentum_sleeve_runs_with_quarterly_rebalances():
    dates = pd.bdate_range("2020-01-02", periods=520)
    prices = pd.DataFrame(
        {
            "QQQ": [100.0 + i * 0.1 for i in range(len(dates))],
            "GLD": [100.0 + i * 0.02 for i in range(len(dates))],
            "SHV": [100.0 + i * 0.005 for i in range(len(dates))],
        },
        index=dates,
    )

    sleeve = audit.run_qqq_cash_momentum_sleeve_from_prices(prices)

    assert sleeve.name == "gld_qqq_cash_12m_momentum"
    assert sleeve.final > 100000.0
    assert sleeve.rebalances >= 4
    assert sleeve.total_cost >= 0.0


def test_static_satellite_sleeve_rejects_bad_weights():
    dates = pd.bdate_range("2020-01-02", periods=5)
    prices = pd.DataFrame(
        {
            "QQQ": [100.0, 101.0, 102.0, 103.0, 104.0],
            "GLD": [100.0, 100.5, 101.0, 101.5, 102.0],
        },
        index=dates,
    )

    with pytest.raises(ValueError, match="weights must sum"):
        audit.run_static_satellite_sleeve_from_prices(
            "bad",
            prices,
            {"QQQ": 0.4, "GLD": 0.4},
        )


def test_apply_calibrated_friction_uses_exposure_weighted_costs():
    dates = pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06"])
    nav = pd.Series([100.0, 101.0, 102.0], index=dates, name="research")
    exposures = pd.DataFrame(
        {
            "SPY": [0.50, 0.50, 0.50],
            "QQQ": [0.10, 0.10, 0.10],
            "SHV": [0.25, 0.25, 0.25],
            "action": ["", "进入宏观慢熊防御", ""],
            "event_turnover": [0.0, 0.50, 0.0],
        },
        index=dates,
    )
    assumptions = {
        2020: audit.AnnualFrictionAssumption(
            year=2020,
            fed_funds_rate=0.01,
            spy_dividend_yield=0.02,
            qqq_dividend_yield=0.01,
            withholding_tax_rate=0.30,
            broker_spread_bps=20,
            operational_failure_bps=10,
            rebalance_event_bps=5,
            macro_event_bps=25,
            acute_event_bps=50,
        )
    }

    result = audit.apply_calibrated_friction(nav, exposures, assumptions)

    assert result.cagr < audit.calculate_cagr(nav)
    assert result.tax_cost > 0
    assert result.broker_cost > 0
    assert result.operational_cost > 0
    assert result.event_cost > 0
    assert result.ledger.loc[dates[1], "event"] == "macro"
    assert result.ledger.loc[dates[0], "event_cost"] == 0


def test_load_annual_friction_assumptions_reads_matrix(tmp_path):
    matrix = tmp_path / "annual_friction_matrix.json"
    matrix.write_text(
        json.dumps(
            {
                "annual_assumptions": [
                    {
                        "year": 2020,
                        "fed_funds_rate": 0.01,
                        "spy_dividend_yield": 0.02,
                        "qqq_dividend_yield": 0.01,
                        "withholding_tax_rate": 0.30,
                        "broker_spread_bps": 20,
                        "operational_failure_bps": 10,
                        "rebalance_event_bps": 5,
                        "macro_event_bps": 25,
                        "acute_event_bps": 50,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    assumptions = audit.load_annual_friction_assumptions(matrix)

    assert assumptions[2020].withholding_tax_rate == 0.30
    assert assumptions[2020].macro_event_bps == 25


def test_classify_execution_event_maps_actions():
    assert audit.classify_execution_event("") == "none"
    assert audit.classify_execution_event("进入非对称防御") == "acute"
    assert audit.classify_execution_event("进入宏观慢熊防御") == "macro"
    assert audit.classify_execution_event("年度再平衡") == "rebalance"


def test_research_audit_fails_closed_when_live_gate_enabled(monkeypatch):
    monkeypatch.setenv("XIRANG_ENABLE_LIVE_CORE_EXECUTION", "1")

    with pytest.raises(RuntimeError, match="research-only"):
        audit.assert_research_only_runtime()
