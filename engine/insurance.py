"""Insurance Layer control-plane primitives.

This module implements the executable specification in:
docs/specs/insurance-layer-executable-spec.md
"""

from dataclasses import dataclass
from enum import Enum


class InsuranceState(str, Enum):
    SAFE = "SAFE"
    DEGRADED = "DEGRADED"
    PROTECTED = "PROTECTED"
    EMERGENCY = "EMERGENCY"
    LOCKED = "LOCKED"


@dataclass(frozen=True)
class TransitionDecision:
    allowed: bool
    reason: str


ALLOWED_TRANSITIONS: set[tuple[InsuranceState, InsuranceState]] = {
    (InsuranceState.SAFE, InsuranceState.DEGRADED),
    (InsuranceState.SAFE, InsuranceState.PROTECTED),
    (InsuranceState.SAFE, InsuranceState.EMERGENCY),
    (InsuranceState.SAFE, InsuranceState.LOCKED),
    (InsuranceState.DEGRADED, InsuranceState.SAFE),
    (InsuranceState.DEGRADED, InsuranceState.PROTECTED),
    (InsuranceState.DEGRADED, InsuranceState.EMERGENCY),
    (InsuranceState.DEGRADED, InsuranceState.LOCKED),
    (InsuranceState.PROTECTED, InsuranceState.DEGRADED),
    (InsuranceState.PROTECTED, InsuranceState.SAFE),
    (InsuranceState.PROTECTED, InsuranceState.EMERGENCY),
    (InsuranceState.PROTECTED, InsuranceState.LOCKED),
    (InsuranceState.EMERGENCY, InsuranceState.PROTECTED),
    (InsuranceState.EMERGENCY, InsuranceState.LOCKED),
    (InsuranceState.LOCKED, InsuranceState.EMERGENCY),
}


def validate_state_transition(
    current: InsuranceState,
    proposed: InsuranceState,
    approved_recovery: bool = False,
) -> TransitionDecision:
    if current == proposed:
        return TransitionDecision(True, "same state")

    if (current, proposed) not in ALLOWED_TRANSITIONS:
        return TransitionDecision(False, "forbidden transition")

    if current == InsuranceState.LOCKED and not approved_recovery:
        return TransitionDecision(False, "LOCKED exit requires approved recovery")

    return TransitionDecision(True, "transition allowed")


@dataclass(frozen=True)
class InsuranceDecision:
    state: InsuranceState
    allow_observation: bool
    allow_suggestions: bool
    allow_core_rebalance: bool
    allow_risk_reducing_rebalance: bool
    allow_live_execution: bool
    allow_alpha_execution: bool
    allow_withdrawal_request: bool
    allow_withdrawal_approval: bool
    allow_withdrawal_execution: bool
    allow_deposit: bool
    allow_tax_harvest: bool
    force_de_risk: bool
    force_cash_floor: bool
    block_trading: bool
    freeze_execution: bool
    require_manual_review: bool
    require_recovery_proposal: bool
    reasons: list[str]


def build_authority_decision(
    state: InsuranceState,
    reasons: list[str] | None = None,
) -> InsuranceDecision:
    reasons = list(reasons or [])

    if state == InsuranceState.SAFE:
        return InsuranceDecision(
            state=state,
            allow_observation=True,
            allow_suggestions=True,
            allow_core_rebalance=True,
            allow_risk_reducing_rebalance=True,
            allow_live_execution=True,
            allow_alpha_execution=True,
            allow_withdrawal_request=True,
            allow_withdrawal_approval=True,
            allow_withdrawal_execution=True,
            allow_deposit=True,
            allow_tax_harvest=True,
            force_de_risk=False,
            force_cash_floor=False,
            block_trading=False,
            freeze_execution=False,
            require_manual_review=False,
            require_recovery_proposal=False,
            reasons=reasons,
        )

    if state == InsuranceState.DEGRADED:
        return InsuranceDecision(
            state=state,
            allow_observation=True,
            allow_suggestions=True,
            allow_core_rebalance=True,
            allow_risk_reducing_rebalance=True,
            allow_live_execution=True,
            allow_alpha_execution=True,
            allow_withdrawal_request=True,
            allow_withdrawal_approval=True,
            allow_withdrawal_execution=True,
            allow_deposit=True,
            allow_tax_harvest=True,
            force_de_risk=False,
            force_cash_floor=True,
            block_trading=False,
            freeze_execution=False,
            require_manual_review=False,
            require_recovery_proposal=False,
            reasons=reasons,
        )

    if state == InsuranceState.PROTECTED:
        return InsuranceDecision(
            state=state,
            allow_observation=True,
            allow_suggestions=True,
            allow_core_rebalance=False,
            allow_risk_reducing_rebalance=True,
            allow_live_execution=True,
            allow_alpha_execution=False,
            allow_withdrawal_request=True,
            allow_withdrawal_approval=True,
            allow_withdrawal_execution=True,
            allow_deposit=True,
            allow_tax_harvest=False,
            force_de_risk=True,
            force_cash_floor=True,
            block_trading=False,
            freeze_execution=False,
            require_manual_review=False,
            require_recovery_proposal=False,
            reasons=reasons,
        )

    if state == InsuranceState.EMERGENCY:
        return InsuranceDecision(
            state=state,
            allow_observation=True,
            allow_suggestions=True,
            allow_core_rebalance=False,
            allow_risk_reducing_rebalance=True,
            allow_live_execution=True,
            allow_alpha_execution=False,
            allow_withdrawal_request=True,
            allow_withdrawal_approval=True,
            allow_withdrawal_execution=False,
            allow_deposit=True,
            allow_tax_harvest=False,
            force_de_risk=True,
            force_cash_floor=True,
            block_trading=False,
            freeze_execution=False,
            require_manual_review=True,
            require_recovery_proposal=False,
            reasons=reasons,
        )

    return InsuranceDecision(
        state=state,
        allow_observation=True,
        allow_suggestions=True,
        allow_core_rebalance=False,
        allow_risk_reducing_rebalance=False,
        allow_live_execution=False,
        allow_alpha_execution=False,
        allow_withdrawal_request=False,
        allow_withdrawal_approval=False,
        allow_withdrawal_execution=False,
        allow_deposit=False,
        allow_tax_harvest=False,
        force_de_risk=False,
        force_cash_floor=False,
        block_trading=True,
        freeze_execution=True,
        require_manual_review=True,
        require_recovery_proposal=True,
        reasons=reasons,
    )
