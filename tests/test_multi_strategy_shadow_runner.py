import json

import pytest

from live import multi_strategy_shadow_runner


def test_multi_strategy_shadow_report_contains_all_registered_strategies(tmp_path):
    report = multi_strategy_shadow_runner.run_multi_strategy_shadow(
        aum=2_000_000.0,
        output_dir=tmp_path,
        portfolio_id="us",
    )

    assert report["status"] == "ATTENTION"
    assert report["readonly"] is True
    assert report["trading_disabled"] is True
    assert report["live_leverage_approved"] is False
    assert set(report["strategies"]) == {
        "classic_permanent_portfolio",
        "fixed_defensive_core",
        "production_90_10",
        "benchmark_balanced_proxy",
    }
    assert report["strategies"]["production_90_10"]["admission_status"] == "NOT_APPROVED"
    assert report["strategies"]["production_90_10"]["target_weights"]["QQQ"] == pytest.approx(0.04)
    assert report["strategies"]["production_90_10"]["target_notionals"]["QQQ"] == pytest.approx(80_000.0)
    assert report["strategies"]["production_90_10"]["slippage_audit"]["status"] == "UNAVAILABLE"
    assert report["strategies"]["production_90_10"]["margin_snapshot"]["status"] == "UNAVAILABLE"

    latest = tmp_path / "latest_multi_strategy_shadow.json"
    assert latest.exists()
    payload = json.loads(latest.read_text(encoding="utf-8"))
    assert payload["output_path"].endswith(".json")
    assert payload["live_leverage_approved"] is False


def test_multi_strategy_shadow_can_filter_strategies(tmp_path):
    report = multi_strategy_shadow_runner.run_multi_strategy_shadow(
        aum=100_000.0,
        output_dir=tmp_path,
        strategy_ids=("production_90_10",),
    )

    assert list(report["strategies"]) == ["production_90_10"]
    assert report["config"]["strategy_count"] == 1
    assert report["strategies"]["production_90_10"]["target_notionals"]["SPY"] == pytest.approx(25_500.0)


def test_shadow_turnover_is_observed_when_current_positions_are_supplied(tmp_path):
    current_path = tmp_path / "current.json"
    current_path.write_text(
        json.dumps(
            {
                "positions": {
                    "SPY": {"market_value": 20_000.0},
                    "TLT": {"market_value": 20_000.0},
                    "GLD": {"market_value": 20_000.0},
                    "SHV": {"market_value": 20_000.0},
                    "QQQ": {"market_value": 20_000.0},
                }
            }
        ),
        encoding="utf-8",
    )

    report = multi_strategy_shadow_runner.run_multi_strategy_shadow(
        aum=100_000.0,
        output_dir=tmp_path,
        strategy_ids=("production_90_10",),
        current_json=current_path,
    )

    turnover = report["strategies"]["production_90_10"]["shadow_turnover"]
    assert turnover["status"] == "OBSERVED"
    assert turnover["requires_attention"] is False
    assert turnover["deltas"]["QQQ"] == pytest.approx(-16_000.0)
    assert turnover["turnover_ratio"] > 0


def test_shadow_turnover_fails_closed_without_positions(tmp_path):
    report = multi_strategy_shadow_runner.run_multi_strategy_shadow(
        aum=100_000.0,
        output_dir=tmp_path,
        strategy_ids=("production_90_10",),
    )

    turnover = report["strategies"]["production_90_10"]["shadow_turnover"]
    assert turnover["status"] == "UNAVAILABLE"
    assert turnover["requires_attention"] is True
    assert report["strategies"]["production_90_10"]["audit_result"]["admission_status"] == "NOT_APPROVED"


def test_unknown_strategy_fails_closed(tmp_path):
    with pytest.raises(KeyError, match="unknown strategy_id"):
        multi_strategy_shadow_runner.run_multi_strategy_shadow(
            output_dir=tmp_path,
            strategy_ids=("smh_experiment",),
        )


def test_invalid_aum_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="aum must be positive"):
        multi_strategy_shadow_runner.run_multi_strategy_shadow(aum=0, output_dir=tmp_path)


def test_runner_source_contains_no_order_submission_calls():
    source = multi_strategy_shadow_runner.__loader__.get_source(multi_strategy_shadow_runner.__name__)

    assert "place_order" not in source
    assert "cancel_order" not in source
    assert "create_broker_adapter" not in source
