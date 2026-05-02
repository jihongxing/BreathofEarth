"""Insurance Layer control-plane primitives.

This module implements the executable specification in:
docs/specs/insurance-layer-executable-spec.md
"""

from dataclasses import dataclass
from datetime import datetime
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


class SignalSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class InsuranceSignal:
    source: str
    severity: SignalSeverity
    score: float
    weight: float
    hard_veto: bool
    reason: str
    evidence: dict


@dataclass(frozen=True)
class InsuranceAssessment:
    state: InsuranceState
    risk_score: float
    weighted_signals: list[InsuranceSignal]
    hard_blocks: list[str]
    reasons: list[str]


def _state_from_weighted_score(current_state: InsuranceState, score: float) -> InsuranceState:
    if score >= 0.75:
        return InsuranceState.EMERGENCY
    if score >= 0.50:
        return InsuranceState.PROTECTED
    if score >= 0.25:
        return InsuranceState.DEGRADED
    return current_state


def assess_insurance_state(
    current_state: InsuranceState,
    signals: list[InsuranceSignal],
) -> InsuranceAssessment:
    weighted = [signal for signal in signals if not signal.hard_veto]
    hard_blocks = [signal.reason for signal in signals if signal.hard_veto]
    risk_score = sum(signal.score * signal.weight for signal in signals)

    if hard_blocks:
        state = InsuranceState.LOCKED
    else:
        state = _state_from_weighted_score(current_state, risk_score)

    return InsuranceAssessment(
        state=state,
        risk_score=risk_score,
        weighted_signals=weighted,
        hard_blocks=hard_blocks,
        reasons=[signal.reason for signal in signals],
    )


class RecoveryStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXECUTED = "EXECUTED"


@dataclass(frozen=True)
class RecoveryProposal:
    id: str
    portfolio_id: str
    from_state: InsuranceState
    proposed_to_state: InsuranceState
    created_at: datetime
    cooldown_until: datetime
    validation_evidence: dict
    unresolved_blocks: list[str]
    required_approvals: int
    approvals: list[str]
    audit_log_ids: list[str]
    status: RecoveryStatus


def validate_recovery_proposal(
    proposal: RecoveryProposal,
    now: datetime,
) -> TransitionDecision:
    transition = validate_state_transition(
        current=proposal.from_state,
        proposed=proposal.proposed_to_state,
        approved_recovery=True,
    )
    if not transition.allowed:
        return transition

    if proposal.status != RecoveryStatus.APPROVED:
        return TransitionDecision(False, "recovery proposal is not approved")

    if len(set(proposal.approvals)) < proposal.required_approvals:
        return TransitionDecision(False, "insufficient recovery approvals")

    if now < proposal.cooldown_until:
        return TransitionDecision(False, "recovery cooldown still active")

    if proposal.unresolved_blocks:
        return TransitionDecision(False, "recovery has unresolved hard blocks")

    if not proposal.validation_evidence:
        return TransitionDecision(False, "missing recovery validation evidence")

    if not proposal.audit_log_ids:
        return TransitionDecision(False, "missing recovery audit evidence")

    return TransitionDecision(True, "recovery proposal valid")


class InsuranceLayer:
    def __init__(self, current_state: InsuranceState = InsuranceState.SAFE):
        self.current_state = current_state

    def evaluate(
        self,
        signals: list[InsuranceSignal],
        approved_recovery: bool = False,
    ) -> tuple[InsuranceAssessment, InsuranceDecision]:
        assessment = assess_insurance_state(self.current_state, signals)
        transition = validate_state_transition(
            current=self.current_state,
            proposed=assessment.state,
            approved_recovery=approved_recovery,
        )

        if transition.allowed:
            self.current_state = assessment.state
            decision_state = assessment.state
        else:
            decision_state = self.current_state

        reasons = list(assessment.reasons)
        if not transition.allowed:
            reasons.append(transition.reason)

        return assessment, build_authority_decision(decision_state, reasons=reasons)


def portfolio_state_from_insurance_state(state: InsuranceState) -> str:
    if state in {InsuranceState.SAFE, InsuranceState.DEGRADED}:
        return "IDLE"
    return "PROTECTION"
