import pytest

from engine.strategy_audit_pipeline import (
    AuditLayerEvidence,
    build_strategy_audit_result,
)


def _layer(layer, *, status="PASS", cagr=None, mdd=None, final_nav=None, warnings=()):
    metric_values = {}
    if layer not in {"data_validated", "shadow_observed"}:
        metric_values = {
            "cagr": 0.0700 if cagr is None else cagr,
            "mdd": -0.1200 if mdd is None else mdd,
            "final_nav": 420000.0 if final_nav is None else final_nav,
        }
    return AuditLayerEvidence(
        layer=layer,
        status=status,
        warnings=tuple(warnings),
        **metric_values,
    )


def _complete_layers(**overrides):
    layers = {
        "data_validated": _layer("data_validated"),
        "research_gross": _layer("research_gross", cagr=0.0760, mdd=-0.1395, final_nav=476436.35),
        "execution_adjusted": _layer("execution_adjusted", cagr=0.0740, mdd=-0.1420, final_nav=455000.0),
        "broker_adjusted": _layer("broker_adjusted", cagr=0.0715, mdd=-0.1440, final_nav=438000.0),
        "tax_adjusted": _layer("tax_adjusted", cagr=0.0703, mdd=-0.1456, final_nav=426022.12),
        "failure_adjusted": _layer("failure_adjusted", cagr=0.0660, mdd=-0.1510, final_nav=390489.22),
        "shadow_observed": _layer("shadow_observed"),
    }
    layers.update(overrides)
    return layers


def test_complete_evidence_can_enter_human_review_but_never_live_approval():
    result = build_strategy_audit_result(
        "production_90_10",
        _complete_layers(),
        as_of="2026-06-23",
        warnings=("tax model is not tax advice",),
    )

    assert result.status == "PASS"
    assert result.admission_status == "READY_FOR_HUMAN_REVIEW"
    assert result.blockers == ()
    assert result.live_leverage_approved is False
    assert result.human_review_required is True
    assert result.readonly is True

    payload = result.to_dict()
    assert payload["live_leverage_approved"] is False
    assert payload["human_review_required"] is True
    assert payload["layers"]["failure_adjusted"]["cagr"] == pytest.approx(0.0660)
    assert payload["warnings"] == ["tax model is not tax advice"]


def test_missing_required_layer_is_partial_and_not_approved():
    layers = _complete_layers()
    layers.pop("tax_adjusted")

    result = build_strategy_audit_result("production_90_10", layers, as_of="2026-06-23")

    assert result.status == "PARTIAL"
    assert result.admission_status == "NOT_APPROVED"
    assert result.live_leverage_approved is False
    blocker_codes = {blocker.code for blocker in result.blockers}
    assert "missing_tax_adjusted_layer" in blocker_codes


def test_non_passing_required_layer_is_not_approved():
    result = build_strategy_audit_result(
        "production_90_10",
        _complete_layers(broker_adjusted=_layer("broker_adjusted", status="UNAVAILABLE", warnings=("broker rate missing",))),
        as_of="2026-06-23",
    )

    assert result.status == "NOT_APPROVED"
    assert result.admission_status == "NOT_APPROVED"
    assert result.warnings == ("broker rate missing",)
    blocker_codes = {blocker.code for blocker in result.blockers}
    assert "broker_adjusted_not_passed" in blocker_codes


def test_failure_adjusted_mdd_breach_is_not_approved():
    result = build_strategy_audit_result(
        "production_90_10",
        _complete_layers(failure_adjusted=_layer("failure_adjusted", cagr=0.0660, mdd=-0.1600, final_nav=380000.0)),
        as_of="2026-06-23",
    )

    assert result.status == "NOT_APPROVED"
    blocker = next(item for item in result.blockers if item.code == "failure_adjusted_mdd_breach")
    assert blocker.details["observed_mdd"] == pytest.approx(-0.1600)
    assert blocker.details["max_failure_adjusted_mdd"] == pytest.approx(-0.1532)


def test_research_mdd_breach_is_not_approved():
    result = build_strategy_audit_result(
        "production_90_10",
        _complete_layers(research_gross=_layer("research_gross", cagr=0.0800, mdd=-0.1510, final_nav=480000.0)),
        as_of="2026-06-23",
    )

    assert result.status == "NOT_APPROVED"
    blocker_codes = {blocker.code for blocker in result.blockers}
    assert "research_mdd_breach" in blocker_codes


def test_layer_schema_rejects_metric_layer_without_metrics():
    with pytest.raises(ValueError, match="metric layers require"):
        AuditLayerEvidence(layer="tax_adjusted", status="PASS")


def test_layer_schema_rejects_unknown_layer_and_status():
    with pytest.raises(ValueError, match="unsupported audit layer"):
        AuditLayerEvidence(layer="secret_live_layer", status="PASS")

    with pytest.raises(ValueError, match="unsupported layer status"):
        AuditLayerEvidence(layer="data_validated", status="APPROVED_FOR_TRADING")


def test_layer_key_mismatch_fails_closed():
    with pytest.raises(ValueError, match="layer key mismatch"):
        build_strategy_audit_result(
            "production_90_10",
            {"tax_adjusted": _layer("research_gross")},
            as_of="2026-06-23",
        )


def test_unknown_strategy_fails_closed():
    with pytest.raises(KeyError, match="unknown strategy_id"):
        build_strategy_audit_result("smh_experiment", _complete_layers(), as_of="2026-06-23")
