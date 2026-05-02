from datetime import datetime, timedelta

import pytest

from engine.insurance import (
    assess_insurance_state,
    build_authority_decision,
    InsuranceLayer,
    InsuranceSignal,
    InsuranceState,
    RecoveryProposal,
    RecoveryStatus,
    SignalSeverity,
    TransitionDecision,
    validate_recovery_proposal,
    validate_state_transition,
)


def test_locked_cannot_auto_exit():
    decision = validate_state_transition(
        current=InsuranceState.LOCKED,
        proposed=InsuranceState.EMERGENCY,
        approved_recovery=False,
    )

    assert decision.allowed is False
    assert decision.reason == "LOCKED exit requires approved recovery"


def test_locked_can_exit_to_emergency_with_approved_recovery():
    decision = validate_state_transition(
        current=InsuranceState.LOCKED,
        proposed=InsuranceState.EMERGENCY,
        approved_recovery=True,
    )

    assert decision.allowed is True
    assert decision.reason == "transition allowed"


def test_locked_to_safe_direct_transition_is_forbidden_even_with_approval():
    decision = validate_state_transition(
        current=InsuranceState.LOCKED,
        proposed=InsuranceState.SAFE,
        approved_recovery=True,
    )

    assert decision.allowed is False
    assert decision.reason == "forbidden transition"


def test_emergency_to_safe_direct_transition_is_forbidden():
    decision = validate_state_transition(
        current=InsuranceState.EMERGENCY,
        proposed=InsuranceState.SAFE,
        approved_recovery=False,
    )

    assert decision.allowed is False
    assert decision.reason == "forbidden transition"


def test_locked_blocks_all_capital_movement_by_default():
    decision = build_authority_decision(
        state=InsuranceState.LOCKED,
        reasons=["manual recovery required"],
    )

    assert decision.allow_observation is True
    assert decision.allow_suggestions is True
    assert decision.allow_core_rebalance is False
    assert decision.allow_live_execution is False
    assert decision.allow_alpha_execution is False
    assert decision.allow_withdrawal_execution is False
    assert decision.allow_deposit is False
    assert decision.block_trading is True
    assert decision.freeze_execution is True
    assert decision.require_recovery_proposal is True


def test_protected_freezes_alpha_and_blocks_normal_rebalance():
    decision = build_authority_decision(
        state=InsuranceState.PROTECTED,
        reasons=["drawdown protection"],
    )

    assert decision.allow_core_rebalance is False
    assert decision.allow_risk_reducing_rebalance is True
    assert decision.allow_alpha_execution is False
    assert decision.force_de_risk is True
    assert decision.force_cash_floor is True


def test_safe_allows_normal_operations_when_no_blocks_exist():
    decision = build_authority_decision(
        state=InsuranceState.SAFE,
        reasons=["all clear"],
    )

    assert decision.allow_core_rebalance is True
    assert decision.allow_live_execution is True
    assert decision.allow_alpha_execution is True
    assert decision.allow_withdrawal_execution is True
    assert decision.block_trading is False


def test_weighted_risk_score_is_reproducible():
    signals = [
        InsuranceSignal(
            source="market",
            severity=SignalSeverity.WARNING,
            score=0.4,
            weight=0.5,
            hard_veto=False,
            reason="drawdown warning",
            evidence={"drawdown": -0.08},
        ),
        InsuranceSignal(
            source="stability",
            severity=SignalSeverity.WARNING,
            score=0.6,
            weight=0.25,
            hard_veto=False,
            reason="cash buffer low",
            evidence={"stability_ratio": 0.04},
        ),
    ]

    assessment = assess_insurance_state(
        current_state=InsuranceState.SAFE,
        signals=signals,
    )

    assert assessment.risk_score == pytest.approx(0.35)
    assert assessment.state == InsuranceState.DEGRADED
    assert assessment.hard_blocks == []


def test_hard_veto_forces_locked_even_when_score_is_low():
    signals = [
        InsuranceSignal(
            source="broker",
            severity=SignalSeverity.CRITICAL,
            score=0.1,
            weight=0.1,
            hard_veto=True,
            reason="BROKER_RECONCILIATION_BROKEN",
            evidence={"status": "BROKEN"},
        )
    ]

    assessment = assess_insurance_state(
        current_state=InsuranceState.SAFE,
        signals=signals,
    )

    assert assessment.state == InsuranceState.LOCKED
    assert assessment.risk_score == pytest.approx(0.01)
    assert assessment.hard_blocks == ["BROKER_RECONCILIATION_BROKEN"]


def _approved_recovery_proposal(**overrides):
    now = datetime(2026, 5, 2, 10, 0, 0)
    data = {
        "id": "rec-001",
        "portfolio_id": "us",
        "from_state": InsuranceState.LOCKED,
        "proposed_to_state": InsuranceState.EMERGENCY,
        "created_at": now - timedelta(days=2),
        "cooldown_until": now - timedelta(hours=1),
        "validation_evidence": {
            "data_integrity": "restored",
            "broker_reconciliation": "MATCHED",
        },
        "unresolved_blocks": [],
        "required_approvals": 2,
        "approvals": ["alice", "bob"],
        "audit_log_ids": ["audit-1"],
        "status": RecoveryStatus.APPROVED,
    }
    data.update(overrides)
    return RecoveryProposal(**data), now


def test_locked_recovery_requires_approval_count():
    proposal, now = _approved_recovery_proposal(approvals=["alice"])

    result = validate_recovery_proposal(proposal, now=now)

    assert result.allowed is False
    assert result.reason == "insufficient recovery approvals"


def test_locked_recovery_requires_audit_evidence():
    proposal, now = _approved_recovery_proposal(audit_log_ids=[])

    result = validate_recovery_proposal(proposal, now=now)

    assert result.allowed is False
    assert result.reason == "missing recovery audit evidence"


def test_locked_recovery_requires_cooldown():
    now = datetime(2026, 5, 2, 10, 0, 0)
    proposal, _ = _approved_recovery_proposal(cooldown_until=now + timedelta(hours=1))

    result = validate_recovery_proposal(proposal, now=now)

    assert result.allowed is False
    assert result.reason == "recovery cooldown still active"


def test_locked_recovery_to_emergency_with_evidence_is_allowed():
    proposal, now = _approved_recovery_proposal()

    result = validate_recovery_proposal(proposal, now=now)

    assert result.allowed is True
    assert result.reason == "recovery proposal valid"


def test_insurance_layer_returns_assessment_and_authority_decision():
    layer = InsuranceLayer(current_state=InsuranceState.SAFE)
    signals = [
        InsuranceSignal(
            source="market",
            severity=SignalSeverity.WARNING,
            score=0.7,
            weight=0.4,
            hard_veto=False,
            reason="market stress",
            evidence={},
        )
    ]

    assessment, decision = layer.evaluate(signals)

    assert assessment.state == InsuranceState.DEGRADED
    assert decision.state == InsuranceState.DEGRADED
    assert decision.force_cash_floor is True
    assert layer.current_state == InsuranceState.DEGRADED
