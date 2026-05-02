import pytest

from engine.insurance import (
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
