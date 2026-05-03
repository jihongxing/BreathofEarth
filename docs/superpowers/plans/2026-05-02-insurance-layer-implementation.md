# Insurance Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Status:** Completed. The Insurance Layer migration and follow-up hardening are implemented and validated in the codebase as of 2026-05-03.

**Goal:** Turn `docs/specs/insurance-layer-executable-spec.md` into executable Python types, validators, tests, and then migrate the system to Insurance Layer as the single safety authority.

**Architecture:** Build the Insurance Layer as a small control-plane module first, with dataclasses/enums and pure validators that are easy to test. Then convert existing risk sources into signal providers, add authority gates to high-risk actions, and only at the end demote legacy local state to compatibility fields.

**Tech Stack:** Python 3, dataclasses, enums, pytest, existing SQLite `Database`, existing `engine/`, `runner/`, `api/`, and `tests/` layout.

---

## File Structure

Create:

- `engine/insurance.py`  
  Owns Insurance enums, signal dataclasses, state transition validation, authority matrix, risk aggregation, and recovery proposal validation.

- `tests/test_insurance_layer.py`  
  Unit tests for the executable spec harness: state transitions, authority matrix, risk aggregation, recovery proposal checks.

- `tests/test_insurance_integration.py`  
  Integration tests for high-risk action gates after migration begins.

Modify in later tasks:

- `engine/risk.py`  
  Add a method to emit `MarketRiskSignal` without removing existing `RiskSignal` behavior immediately.

- `engine/cashflow.py`  
  Add Stability signal generation and InsuranceDecision enforcement before capital-moving methods.

- `engine/alpha/arena.py`  
  Add InsuranceDecision enforcement before running Alpha strategies.

- `runner/daily_runner.py`  
  Run Insurance assessment before high-risk actions and consume `InsuranceDecision`.

- `runner/shadow_run.py`  
  Report `InsuranceState` in shadow output once the Insurance assessment exists.

- `db/schema.sql` and `db/database.py`  
  Add persistence for Insurance decisions and recovery proposals after pure validators pass.

- `runner/report.py`, `engine/notifier.py`, and dashboard serialization paths if needed  
  Display `InsuranceState` as the authoritative system state.

---

## Task 1: Insurance State Machine Spec Harness

**Files:**
- Create: `engine/insurance.py`
- Test: `tests/test_insurance_layer.py`

- [ ] **Step 1: Write failing tests for InsuranceState and transition rules**

Add this to `tests/test_insurance_layer.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'engine.insurance'`.

- [ ] **Step 3: Implement the minimal InsuranceState and transition validator**

Create `engine/insurance.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: PASS for 4 tests.

- [ ] **Step 5: Commit**

```bash
git add engine/insurance.py tests/test_insurance_layer.py
git commit -m "feat: add insurance state transition harness"
```

---

## Task 2: Authority Matrix and Decision Schema

**Files:**
- Modify: `engine/insurance.py`
- Modify: `tests/test_insurance_layer.py`

- [ ] **Step 1: Write failing tests for the authority matrix**

Append to `tests/test_insurance_layer.py`:

```python
from engine.insurance import build_authority_decision


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: FAIL with `ImportError` or `AttributeError` for `build_authority_decision`.

- [ ] **Step 3: Add InsuranceDecision and authority matrix**

Append to `engine/insurance.py`:

```python
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
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: PASS for all tests in `tests/test_insurance_layer.py`.

- [ ] **Step 5: Commit**

```bash
git add engine/insurance.py tests/test_insurance_layer.py
git commit -m "feat: add insurance authority matrix"
```

---

## Task 3: Risk Signals and Insurance Assessment

**Files:**
- Modify: `engine/insurance.py`
- Modify: `tests/test_insurance_layer.py`

- [ ] **Step 1: Write failing tests for weighted risk and hard veto behavior**

Append to `tests/test_insurance_layer.py`:

```python
from engine.insurance import (
    InsuranceSignal,
    SignalSeverity,
    assess_insurance_state,
)


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: FAIL because `InsuranceSignal`, `SignalSeverity`, and `assess_insurance_state` do not exist.

- [ ] **Step 3: Implement signal and assessment types**

Append to `engine/insurance.py`:

```python
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
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/insurance.py tests/test_insurance_layer.py
git commit -m "feat: add insurance risk aggregation"
```

---

## Task 4: Recovery Proposal Validation

**Files:**
- Modify: `engine/insurance.py`
- Modify: `tests/test_insurance_layer.py`

- [ ] **Step 1: Write failing tests for LOCKED recovery proposal rules**

Append to `tests/test_insurance_layer.py`:

```python
from datetime import datetime, timedelta

from engine.insurance import RecoveryProposal, RecoveryStatus, validate_recovery_proposal


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: FAIL because recovery types do not exist.

- [ ] **Step 3: Implement RecoveryProposal and validator**

Append to `engine/insurance.py`:

```python
from datetime import datetime


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
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/insurance.py tests/test_insurance_layer.py
git commit -m "feat: add insurance recovery proposal validation"
```

---

## Task 5: Signal Provider Adapters Without Behavior Migration

**Files:**
- Modify: `engine/risk.py`
- Modify: `engine/cashflow.py`
- Modify: `engine/data_validator.py`
- Test: `tests/test_insurance_integration.py`

- [ ] **Step 1: Write tests proving existing modules can emit signals**

Create `tests/test_insurance_integration.py`:

```python
import pytest

from engine.cashflow import build_stability_signal
from engine.data_validator import build_data_integrity_signal
from engine.insurance import SignalSeverity
from engine.risk import RiskEngine


def test_risk_engine_emits_market_signal_for_drawdown():
    engine = RiskEngine()
    engine.high_water_mark = 100000.0
    risk_signal = engine.evaluate(
        nav=87000.0,
        spy_tlt_corr=0.1,
        spy_30d_ret=-0.05,
        tlt_30d_ret=0.02,
    )

    signal = engine.to_insurance_signal(risk_signal)

    assert signal.source == "market"
    assert signal.severity == SignalSeverity.ERROR
    assert signal.score > 0
    assert signal.hard_veto is False
    assert "回撤" in signal.reason


def test_stability_signal_warns_below_floor():
    signal = build_stability_signal(
        stability_balance=4000.0,
        nav=100000.0,
    )

    assert signal.source == "stability"
    assert signal.severity == SignalSeverity.WARNING
    assert signal.hard_veto is False
    assert signal.evidence["stability_ratio"] == pytest.approx(0.04)


def test_data_integrity_signal_can_represent_failure():
    signal = build_data_integrity_signal(
        ok=False,
        reason="DATA_INTEGRITY_FAILED",
        evidence={"asset": "SPY"},
    )

    assert signal.source == "data"
    assert signal.severity == SignalSeverity.CRITICAL
    assert signal.hard_veto is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_integration.py -v
```

Expected: FAIL because adapter functions do not exist.

- [ ] **Step 3: Add `RiskEngine.to_insurance_signal`**

Modify `engine/risk.py`:

```python
from engine.insurance import InsuranceSignal, SignalSeverity
```

Add this method inside `RiskEngine`:

```python
    def to_insurance_signal(self, signal: RiskSignal) -> InsuranceSignal:
        if signal.is_hard_stop:
            severity = SignalSeverity.ERROR
            score = 0.80
        elif signal.is_protection:
            severity = SignalSeverity.ERROR
            score = 0.55
        elif signal.is_corr_breakdown:
            severity = SignalSeverity.WARNING
            score = 0.45
        else:
            severity = SignalSeverity.INFO
            score = max(0.0, min(abs(signal.current_dd), 0.20))

        return InsuranceSignal(
            source="market",
            severity=severity,
            score=score,
            weight=0.40,
            hard_veto=False,
            reason=signal.trigger_reason or "market risk normal",
            evidence={
                "drawdown": signal.current_dd,
                "spy_tlt_corr": signal.spy_tlt_corr,
                "spy_30d_ret": signal.spy_30d_ret,
                "tlt_30d_ret": signal.tlt_30d_ret,
                "is_hard_stop": signal.is_hard_stop,
                "is_protection": signal.is_protection,
                "is_corr_breakdown": signal.is_corr_breakdown,
            },
        )
```

- [ ] **Step 4: Add Stability signal builder**

Modify imports in `engine/cashflow.py`:

```python
from engine.insurance import InsuranceSignal, SignalSeverity
```

Add a top-level function:

```python
def build_stability_signal(stability_balance: float, nav: float) -> InsuranceSignal:
    ratio = stability_balance / nav if nav > 0 else 0.0
    below_floor = ratio < LAYER_MIN_STABILITY

    return InsuranceSignal(
        source="stability",
        severity=SignalSeverity.WARNING if below_floor else SignalSeverity.INFO,
        score=0.60 if below_floor else 0.0,
        weight=0.25,
        hard_veto=False,
        reason="Stability below minimum floor" if below_floor else "Stability normal",
        evidence={
            "stability_balance": stability_balance,
            "nav": nav,
            "stability_ratio": ratio,
            "min_stability": LAYER_MIN_STABILITY,
        },
    )
```

- [ ] **Step 5: Add DataIntegrity signal builder**

Modify imports in `engine/data_validator.py`:

```python
from engine.insurance import InsuranceSignal, SignalSeverity
```

Add a top-level function:

```python
def build_data_integrity_signal(ok: bool, reason: str, evidence: dict) -> InsuranceSignal:
    return InsuranceSignal(
        source="data",
        severity=SignalSeverity.INFO if ok else SignalSeverity.CRITICAL,
        score=0.0 if ok else 1.0,
        weight=1.0,
        hard_veto=not ok,
        reason=reason,
        evidence=evidence,
    )
```

- [ ] **Step 6: Run tests**

Run:

```bash
pytest tests/test_insurance_integration.py tests/test_insurance_layer.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add engine/risk.py engine/cashflow.py engine/data_validator.py tests/test_insurance_integration.py
git commit -m "feat: expose insurance signal providers"
```

---

## Task 6: Insurance Layer Core Service

**Files:**
- Modify: `engine/insurance.py`
- Modify: `tests/test_insurance_layer.py`

- [ ] **Step 1: Write tests for InsuranceLayer assessment and decision output**

Append to `tests/test_insurance_layer.py`:

```python
from engine.insurance import InsuranceLayer


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_layer.py -v
```

Expected: FAIL because `InsuranceLayer` does not exist.

- [ ] **Step 3: Add InsuranceLayer service**

Append to `engine/insurance.py`:

```python
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
```

- [ ] **Step 4: Run tests**

Run:

```bash
pytest tests/test_insurance_layer.py tests/test_insurance_integration.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/insurance.py tests/test_insurance_layer.py
git commit -m "feat: add insurance layer evaluator"
```

---

## Task 7: Authority Gates in DailyRunner

**Files:**
- Modify: `runner/daily_runner.py`
- Test: `tests/test_daily_runner.py`

- [ ] **Step 1: Write failing test for LOCKED blocking rebalance execution**

Add to `tests/test_daily_runner.py`:

```python
def test_insurance_locked_blocks_core_rebalance(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    class LockedInsuranceLayer:
        def __init__(self, current_state=None):
            pass

        def evaluate(self, signals, approved_recovery=False):
            from engine.insurance import InsuranceAssessment, InsuranceDecision, InsuranceState
            assessment = InsuranceAssessment(
                state=InsuranceState.LOCKED,
                risk_score=1.0,
                weighted_signals=[],
                hard_blocks=["AUTHORITY_BYPASS_ATTEMPT"],
                reasons=["test locked"],
            )
            decision = InsuranceDecision(
                state=InsuranceState.LOCKED,
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
                reasons=["test locked"],
            )
            return assessment, decision

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "InsuranceLayer", LockedInsuranceLayer)
    monkeypatch.setattr(runner_module, "notify", lambda report: None)
    monkeypatch.setenv("XIRANG_EXECUTOR", "auto")

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert result["run_status"] == "FAILED_EXECUTION"
    assert result["insurance"]["state"] == "LOCKED"
    assert "Insurance Layer blocked Core rebalance" in result["action"]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_daily_runner.py::test_insurance_locked_blocks_core_rebalance -v
```

Expected: FAIL because `DailyRunner` does not import or consume `InsuranceLayer`.

- [ ] **Step 3: Import InsuranceLayer and build insurance decision before execution**

Modify imports in `runner/daily_runner.py`:

```python
from engine.insurance import InsuranceLayer, InsuranceState
from engine.cashflow import build_stability_signal
```

After `risk_signal = risk.evaluate(...)`, add:

```python
        insurance_signals = [
            risk.to_insurance_signal(risk_signal),
            build_stability_signal(
                stability_balance=float(engine.stability_balance),
                nav=sim_nav,
            ),
        ]
        insurance = InsuranceLayer(current_state=InsuranceState.SAFE)
        insurance_assessment, insurance_decision = insurance.evaluate(insurance_signals)
```

Add `insurance_assessment` and `insurance_decision` to the report dictionary:

```python
            "insurance": {
                "state": insurance_decision.state.value,
                "risk_score": round(float(insurance_assessment.risk_score), 6),
                "hard_blocks": insurance_assessment.hard_blocks,
                "reasons": insurance_decision.reasons,
            },
```

Inside `if order:`, before creating executor, add:

```python
            if not insurance_decision.allow_core_rebalance:
                execution_status = "FAILED"
                action = "Insurance Layer blocked Core rebalance"
                tx_type = "REBALANCE_BLOCKED"
                execution_policy_gate = {
                    "code": "INSURANCE_CORE_REBALANCE_BLOCKED",
                    "message": action,
                    "insurance_state": insurance_decision.state.value,
                    "reasons": insurance_decision.reasons,
                }
```

Wrap the existing executor branch in `elif executor_mode == "manual" ...` so it only runs if Insurance permits the rebalance.

- [ ] **Step 4: Run targeted test**

Run:

```bash
pytest tests/test_daily_runner.py::test_insurance_locked_blocks_core_rebalance -v
```

Expected: PASS.

- [ ] **Step 5: Run daily runner regression subset**

Run:

```bash
pytest tests/test_daily_runner.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add runner/daily_runner.py tests/test_daily_runner.py
git commit -m "feat: gate daily runner with insurance authority"
```

---

## Task 8: Cashflow and Alpha Authority Gates

**Files:**
- Modify: `engine/cashflow.py`
- Modify: `engine/alpha/arena.py`
- Test: `tests/test_insurance_integration.py`

- [ ] **Step 1: Write tests for cashflow and Alpha gates**

Append to `tests/test_insurance_integration.py`:

```python
from engine.insurance import build_authority_decision, InsuranceState


def test_locked_decision_blocks_withdrawal_execution_before_capital_moves(temp_db):
    from engine.cashflow import CashflowEngine

    engine = CashflowEngine(temp_db)
    decision = build_authority_decision(InsuranceState.LOCKED, reasons=["locked"])

    result = engine.enforce_withdrawal_authority(decision)

    assert result.status == "ERROR"
    assert "Insurance Layer blocked withdrawal execution" in result.message


def test_protected_decision_blocks_alpha_arena_execution(temp_db):
    from engine.alpha.arena import StrategyArena

    arena = StrategyArena(temp_db)
    decision = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])

    result = arena.enforce_alpha_authority(decision)

    assert result["action"] == "BLOCKED"
    assert result["reason"] == "Insurance Layer blocked Alpha execution"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_insurance_integration.py -v
```

Expected: FAIL because gate helpers do not exist.

- [ ] **Step 3: Add cashflow authority helper**

In `engine/cashflow.py`, import:

```python
from engine.insurance import InsuranceDecision
```

Add method to `CashflowEngine`:

```python
    def enforce_withdrawal_authority(self, decision: InsuranceDecision) -> CashflowResult:
        if not decision.allow_withdrawal_execution:
            return CashflowResult(
                "ERROR",
                "Insurance Layer blocked withdrawal execution",
                insurance_state=decision.state.value,
                reasons=decision.reasons,
            )
        return CashflowResult("SUCCESS", "Insurance Layer allowed withdrawal execution")
```

At the start of `execute_withdrawal`, accept an optional decision:

```python
    def execute_withdrawal(
        self,
        withdrawal_id: str,
        executor: str,
        insurance_decision: InsuranceDecision | None = None,
    ) -> CashflowResult:
        if insurance_decision is not None:
            authority = self.enforce_withdrawal_authority(insurance_decision)
            if authority.status == "ERROR":
                return authority
```

- [ ] **Step 4: Add Alpha authority helper**

In `engine/alpha/arena.py`, import:

```python
from engine.insurance import InsuranceDecision
```

Add method to `StrategyArena`:

```python
    def enforce_alpha_authority(self, decision: InsuranceDecision) -> dict:
        if not decision.allow_alpha_execution:
            return {
                "action": "BLOCKED",
                "reason": "Insurance Layer blocked Alpha execution",
                "insurance_state": decision.state.value,
                "reasons": decision.reasons,
            }
        return {
            "action": "ALLOWED",
            "reason": "Insurance Layer allowed Alpha execution",
            "insurance_state": decision.state.value,
        }
```

Change `run_all` signature:

```python
    def run_all(
        self,
        portfolio_id: str,
        current_date: str,
        spy_price: float,
        insurance_decision: InsuranceDecision | None = None,
    ) -> list[dict]:
```

At the start of `run_all`:

```python
        if insurance_decision is not None:
            authority = self.enforce_alpha_authority(insurance_decision)
            if authority["action"] == "BLOCKED":
                return [authority]
```

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_insurance_integration.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/cashflow.py engine/alpha/arena.py tests/test_insurance_integration.py
git commit -m "feat: add insurance gates for cashflow and alpha"
```

---

## Task 9: Persistence and Audit Records

**Files:**
- Modify: `db/schema.sql`
- Modify: `db/database.py`
- Modify: `engine/insurance.py`
- Test: `tests/test_insurance_integration.py`

- [ ] **Step 1: Write database persistence tests**

Append to `tests/test_insurance_integration.py`:

```python
def test_save_and_load_insurance_decision(temp_db):
    from engine.insurance import build_authority_decision, InsuranceState

    decision = build_authority_decision(
        InsuranceState.PROTECTED,
        reasons=["drawdown protection"],
    )

    decision_id = temp_db.save_insurance_decision(
        portfolio_id="us",
        previous_state="SAFE",
        decision=decision,
        risk_score=0.55,
        hard_blocks=[],
        source_signals=[{"source": "market", "reason": "drawdown protection"}],
        actor="insurance",
    )

    stored = temp_db.get_insurance_decision(decision_id)

    assert stored["portfolio_id"] == "us"
    assert stored["new_state"] == "PROTECTED"
    assert stored["risk_score"] == pytest.approx(0.55)
    assert "drawdown protection" in stored["reasons"]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_insurance_integration.py::test_save_and_load_insurance_decision -v
```

Expected: FAIL because database methods and schema do not exist.

- [ ] **Step 3: Add schema**

Append to `db/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS insurance_decisions (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL DEFAULT 'us',
    previous_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    risk_score REAL NOT NULL DEFAULT 0,
    hard_blocks TEXT NOT NULL DEFAULT '[]',
    allowed_actions TEXT NOT NULL DEFAULT '{}',
    blocked_actions TEXT NOT NULL DEFAULT '{}',
    forced_actions TEXT NOT NULL DEFAULT '{}',
    reasons TEXT NOT NULL DEFAULT '[]',
    source_signals TEXT NOT NULL DEFAULT '[]',
    recovery_proposal_id TEXT,
    actor TEXT NOT NULL DEFAULT 'insurance',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

- [ ] **Step 4: Add database methods**

In `db/database.py`, add imports if missing:

```python
import uuid
```

Add methods to `Database`:

```python
    def save_insurance_decision(
        self,
        portfolio_id: str,
        previous_state: str,
        decision,
        risk_score: float,
        hard_blocks: list,
        source_signals: list,
        actor: str = "insurance",
        recovery_proposal_id: str | None = None,
        conn=None,
    ) -> str:
        decision_id = str(uuid.uuid4())[:12]
        owns_conn = conn is None
        if owns_conn:
            conn = self._conn()
        try:
            import json
            allowed_actions = {
                "allow_observation": decision.allow_observation,
                "allow_suggestions": decision.allow_suggestions,
                "allow_core_rebalance": decision.allow_core_rebalance,
                "allow_risk_reducing_rebalance": decision.allow_risk_reducing_rebalance,
                "allow_live_execution": decision.allow_live_execution,
                "allow_alpha_execution": decision.allow_alpha_execution,
                "allow_withdrawal_request": decision.allow_withdrawal_request,
                "allow_withdrawal_approval": decision.allow_withdrawal_approval,
                "allow_withdrawal_execution": decision.allow_withdrawal_execution,
                "allow_deposit": decision.allow_deposit,
                "allow_tax_harvest": decision.allow_tax_harvest,
            }
            forced_actions = {
                "force_de_risk": decision.force_de_risk,
                "force_cash_floor": decision.force_cash_floor,
            }
            blocked_actions = {
                "block_trading": decision.block_trading,
                "freeze_execution": decision.freeze_execution,
                "require_manual_review": decision.require_manual_review,
                "require_recovery_proposal": decision.require_recovery_proposal,
            }
            conn.execute(
                """
                INSERT INTO insurance_decisions (
                    id, portfolio_id, previous_state, new_state, risk_score,
                    hard_blocks, allowed_actions, blocked_actions, forced_actions,
                    reasons, source_signals, recovery_proposal_id, actor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    portfolio_id,
                    previous_state,
                    decision.state.value,
                    float(risk_score),
                    json.dumps(hard_blocks, ensure_ascii=False),
                    json.dumps(allowed_actions, ensure_ascii=False),
                    json.dumps(blocked_actions, ensure_ascii=False),
                    json.dumps(forced_actions, ensure_ascii=False),
                    json.dumps(decision.reasons, ensure_ascii=False),
                    json.dumps(source_signals, ensure_ascii=False, default=str),
                    recovery_proposal_id,
                    actor,
                ),
            )
            if owns_conn:
                conn.commit()
            return decision_id
        finally:
            if owns_conn:
                conn.close()

    def get_insurance_decision(self, decision_id: str) -> dict | None:
        import json
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM insurance_decisions WHERE id = ?",
                (decision_id,),
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        for key in ("hard_blocks", "allowed_actions", "blocked_actions", "forced_actions", "reasons", "source_signals"):
            data[key] = json.loads(data[key])
        return data
```

- [ ] **Step 5: Run persistence test**

Run:

```bash
pytest tests/test_insurance_integration.py::test_save_and_load_insurance_decision -v
```

Expected: PASS.

- [ ] **Step 6: Run database-related tests**

Run:

```bash
pytest tests/test_daily_runner.py tests/test_governance_manual_withdrawal.py tests/test_insurance_integration.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add db/schema.sql db/database.py tests/test_insurance_integration.py
git commit -m "feat: persist insurance decisions"
```

---

## Task 10: Report InsuranceState as Authoritative State

**Files:**
- Modify: `runner/daily_runner.py`
- Modify: `runner/report.py`
- Modify: `engine/notifier.py`
- Test: `tests/test_daily_runner.py`

- [ ] **Step 1: Write failing test that daily report includes authoritative InsuranceState**

Add to `tests/test_daily_runner.py`:

```python
def test_daily_runner_report_includes_authoritative_insurance_state(temp_db, monkeypatch):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_broker_sync(temp_db, checked_day="2026-12-30")

    monkeypatch.setattr(runner_module, "MarketDataService", _make_market_service("2026-12-30"))
    monkeypatch.setattr(runner_module, "create_executor", lambda **kwargs: FilledExecutor())
    monkeypatch.setattr(runner_module, "notify", lambda report: None)

    result = runner_module.DailyRunner(temp_db).run_portfolio("us")

    assert "insurance" in result
    assert result["insurance"]["state"] in {"SAFE", "DEGRADED", "PROTECTED", "EMERGENCY", "LOCKED"}
```

- [ ] **Step 2: Run targeted test**

Run:

```bash
pytest tests/test_daily_runner.py::test_daily_runner_report_includes_authoritative_insurance_state -v
```

Expected: PASS if Task 7 already added report field; FAIL if not wired consistently.

- [ ] **Step 3: Update report/notifier wording**

In `engine/notifier.py`, where report state is formatted, prefer:

```python
    insurance = report.get("insurance", {})
    state = insurance.get("state") or report.get("state", "UNKNOWN")
```

In `runner/report.py`, when displaying current status, add:

```python
    insurance_state = last.get("insurance_state") or last.get("state")
```

If `daily_snapshots` does not yet persist `insurance_state`, keep report changes limited to JSON `daily_runs` payload parsing in this task.

- [ ] **Step 4: Run reporting and daily tests**

Run:

```bash
pytest tests/test_daily_runner.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add runner/daily_runner.py runner/report.py engine/notifier.py tests/test_daily_runner.py
git commit -m "feat: report insurance state as authoritative"
```

---

## Task 11: Legacy State Migration Contract

**Files:**
- Modify: `engine/portfolio.py`
- Modify: `tests/test_portfolio_engine.py`
- Modify: `tests/test_insurance_integration.py`

- [ ] **Step 1: Write test documenting PortfolioEngine state as compatibility**

Add to `tests/test_insurance_integration.py`:

```python
def test_portfolio_state_maps_from_insurance_state_for_compatibility():
    from engine.insurance import portfolio_state_from_insurance_state

    assert portfolio_state_from_insurance_state(InsuranceState.SAFE) == "IDLE"
    assert portfolio_state_from_insurance_state(InsuranceState.DEGRADED) == "IDLE"
    assert portfolio_state_from_insurance_state(InsuranceState.PROTECTED) == "PROTECTION"
    assert portfolio_state_from_insurance_state(InsuranceState.EMERGENCY) == "PROTECTION"
    assert portfolio_state_from_insurance_state(InsuranceState.LOCKED) == "PROTECTION"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_insurance_integration.py::test_portfolio_state_maps_from_insurance_state_for_compatibility -v
```

Expected: FAIL because mapper does not exist.

- [ ] **Step 3: Add compatibility mapper**

Append to `engine/insurance.py`:

```python
def portfolio_state_from_insurance_state(state: InsuranceState) -> str:
    if state in {InsuranceState.SAFE, InsuranceState.DEGRADED}:
        return "IDLE"
    return "PROTECTION"
```

- [ ] **Step 4: Start replacing direct global interpretations**

Modify code that displays or persists portfolio state so it treats `PortfolioEngine.state` as compatibility data. Do not remove existing fields in this task.

In `runner/daily_runner.py`, after Insurance decision:

```python
        compatibility_portfolio_state = portfolio_state_from_insurance_state(insurance_decision.state)
```

Persist `state=compatibility_portfolio_state` only after ensuring existing tests expect legacy `IDLE/PROTECTION` values.

- [ ] **Step 5: Run portfolio and insurance tests**

Run:

```bash
pytest tests/test_portfolio_engine.py tests/test_insurance_integration.py tests/test_daily_runner.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/insurance.py runner/daily_runner.py tests/test_insurance_integration.py
git commit -m "feat: map portfolio state from insurance state"
```

---

## Task 12: Full Regression and Migration Check

**Files:**
- No planned source changes unless tests reveal a direct regression.

- [ ] **Step 1: Run full test suite**

Run:

```bash
pytest -q
```

Expected: PASS.

- [ ] **Step 2: Run targeted Insurance checks**

Run:

```bash
pytest tests/test_insurance_layer.py tests/test_insurance_integration.py -v
```

Expected: PASS.

- [ ] **Step 3: Run daily runner and governance checks**

Run:

```bash
pytest tests/test_daily_runner.py tests/test_governance_manual_withdrawal.py -v
```

Expected: PASS.

- [ ] **Step 4: Verify no unauthorized business logic files changed outside the plan**

Run:

```bash
git status --short
```

Expected: Only planned files or user-existing unrelated files are modified.

- [ ] **Step 5: Commit final migration checkpoint if needed**

If Task 12 required fixes:

```bash
git add <changed-files>
git commit -m "test: stabilize insurance layer migration"
```

If no fixes were needed, do not create an empty commit.

---

## Self-Review Against Spec

Spec coverage:

- INS-AUTH: Covered by Tasks 2, 7, 8, 9, 10.
- INS-SSOT: Covered by Tasks 1, 6, 10, 11.
- INS-STATE: Covered by Tasks 1 and 4.
- INS-RISK: Covered by Tasks 3, 5, 6.
- INS-PERM: Covered by Tasks 2, 7, 8.
- INS-REC: Covered by Task 4 and later persistence extension in Task 9.
- INS-AUDIT: Covered by Task 9.
- Migration C -> B: Covered by Tasks 1 through 12.

Known sequencing choice:

- Recovery proposal persistence is introduced after pure validation, because the constitutional validator must be stable before schema work.
- `PortfolioEngine.state` is not removed in this plan. It is first made compatible with `InsuranceState`, then can be removed in a later cleanup plan after reports and snapshots are fully migrated.

No intentionally deferred implementation is required for the first executable migration. Later cleanup may remove legacy state fields after production data migration is designed.
