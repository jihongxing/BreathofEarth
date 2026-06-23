import pytest

from engine.strategy_registry import (
    AUDIT_LAYER_SEQUENCE,
    DEFAULT_STAGE95_STRATEGY_ID,
    RiskPolicy,
    StrategyDefinition,
    get_default_stage95_strategy,
    get_strategy,
    list_strategies,
    validate_registry,
    validate_strategy_definition,
)


def _definition(**overrides):
    values = {
        "strategy_id": "candidate",
        "display_name": "Candidate",
        "status": "research_candidate",
        "confidence": "TEST_ONLY",
        "assets": ("SPY", "SHV"),
        "base_currency": "USD",
        "data_policy": "yahoo_adj_close_clean",
        "risk_policy": RiskPolicy(max_research_mdd=-0.15, max_failure_adjusted_mdd=-0.16),
        "reference_weights": {"SPY": 0.50, "SHV": 0.50},
        "live_execution_allowed": False,
    }
    values.update(overrides)
    return StrategyDefinition(**values)


def test_registry_contains_initial_shadow_observation_set():
    ids = {strategy.strategy_id for strategy in list_strategies()}

    assert {
        "classic_permanent_portfolio",
        "fixed_defensive_core",
        "production_90_10",
        "benchmark_balanced_proxy",
    } <= ids


def test_registry_has_one_default_and_one_production_candidate():
    validate_registry()

    default_strategy = get_default_stage95_strategy()
    production_candidates = list_strategies("production_candidate")

    assert default_strategy.strategy_id == DEFAULT_STAGE95_STRATEGY_ID
    assert len(production_candidates) == 1
    assert production_candidates[0].strategy_id == "production_90_10"


def test_production_90_10_keeps_weights_and_live_execution_locked():
    strategy = get_strategy("production_90_10")

    assert strategy.reference_weights == {
        "SPY": pytest.approx(0.255),
        "TLT": pytest.approx(0.225),
        "GLD": pytest.approx(0.255),
        "SHV": pytest.approx(0.225),
        "QQQ": pytest.approx(0.04),
    }
    assert sum(strategy.reference_weights.values()) == pytest.approx(1.0)
    assert strategy.risk_policy.satellite_cap == pytest.approx(0.10)
    assert strategy.risk_policy.max_failure_adjusted_mdd == pytest.approx(-0.1532)
    assert strategy.live_execution_allowed is False
    assert strategy.risk_policy.live_leverage_approved is False


def test_registry_entries_are_readonly_snapshots():
    strategy = get_strategy("fixed_defensive_core")

    with pytest.raises(TypeError):
        strategy.reference_weights["SPY"] = 0.99
    with pytest.raises(Exception):
        strategy.strategy_id = "mutated"


def test_audit_layers_preserve_fail_closed_order():
    assert AUDIT_LAYER_SEQUENCE == (
        "data_validated",
        "research_gross",
        "execution_adjusted",
        "broker_adjusted",
        "tax_adjusted",
        "failure_adjusted",
        "shadow_observed",
        "admission_gated",
    )


def test_strategy_dict_payload_is_plain_json_shape():
    payload = get_strategy("production_90_10").to_dict()

    assert payload["strategy_id"] == "production_90_10"
    assert payload["assets"] == ["SPY", "TLT", "GLD", "SHV", "QQQ"]
    assert payload["reference_weights"]["QQQ"] == pytest.approx(0.04)
    assert payload["risk_policy"]["live_leverage_approved"] is False
    assert payload["live_execution_allowed"] is False


def test_unknown_strategy_fails_closed():
    with pytest.raises(KeyError, match="unknown strategy_id"):
        get_strategy("smh_breakout_experiment")


def test_registry_rejects_live_execution_or_live_leverage_approval():
    with pytest.raises(ValueError, match="cannot authorize live execution"):
        validate_strategy_definition(_definition(live_execution_allowed=True))

    with pytest.raises(ValueError, match="cannot approve live leverage"):
        validate_strategy_definition(
            _definition(
                risk_policy=RiskPolicy(
                    max_research_mdd=-0.15,
                    max_failure_adjusted_mdd=-0.16,
                    live_leverage_approved=True,
                )
            )
        )


def test_registry_rejects_malformed_assets_and_weights():
    with pytest.raises(ValueError, match="weights must sum"):
        validate_strategy_definition(_definition(reference_weights={"SPY": 0.60, "SHV": 0.50}))

    with pytest.raises(ValueError, match="weights must match assets"):
        validate_strategy_definition(_definition(reference_weights={"SPY": 1.0}))

    with pytest.raises(ValueError, match="assets must be unique"):
        validate_strategy_definition(_definition(assets=("SPY", "SPY"), reference_weights={"SPY": 1.0}))


def test_registry_rejects_satellite_cap_above_ten_percent():
    with pytest.raises(ValueError, match="satellite cap cannot exceed 10%"):
        validate_strategy_definition(
            _definition(
                risk_policy=RiskPolicy(
                    max_research_mdd=-0.15,
                    max_failure_adjusted_mdd=-0.16,
                    satellite_cap=0.11,
                )
            )
        )
