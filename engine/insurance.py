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
