import pytest

from engine.insurance import (
    build_authority_decision,
    InsuranceState,
    TransitionDecision,
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
