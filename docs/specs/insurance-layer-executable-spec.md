# Insurance Layer 可执行规范

> 状态：Accepted for implementation planning  
> 备注：该规范已在代码中实现并通过风险层回归测试验证。  
> 范围：定义息壤系统的唯一安全主权层，不改业务代码。  
> 核心原则：这是 Spec-as-Code，不是概念文档。后续实现、测试、迁移和审计必须从本规范反推。

## 1. Authority Hierarchy Definition

Insurance Layer 是系统最高控制平面，拥有唯一的全局执行授权权力。

### 1.1 权威层级

```text
InsuranceLayer
  > PortfolioEngine
  > Risk signal providers
  > CashflowEngine
  > AlphaArena
  > DailyRunner
  > Execution adapters
  > Data providers
```

### 1.2 强制条款

| ID | Rule | Testable |
| --- | --- | --- |
| INS-AUTH-001 | No module can override `InsuranceDecision`. | Yes |
| INS-AUTH-002 | No module except `InsuranceLayer` can define global system state. | Yes |
| INS-AUTH-003 | No execution path may bypass `InsuranceAuthority`. | Yes |
| INS-AUTH-004 | All engines may propose actions; only Insurance Layer may authorize actions. | Yes |
| INS-AUTH-005 | Any authority conflict must resolve to the more restrictive decision. | Yes |

### 1.3 现有模块地位

现有模块应逐步降级为信号提供者或执行从属者：

| Module | Target Role |
| --- | --- |
| `risk.py` | Market risk signal provider |
| `portfolio.py` | Portfolio proposal engine |
| `cashflow.py` | Cashflow signal provider and authorized executor |
| `alpha/*` | Alpha signal provider and authorized executor |
| `runner/daily_runner.py` | Execution orchestrator, not safety state owner |
| `execution/*` | Broker/action executor, never policy owner |

## 2. Single Source of Truth for System State

系统状态只能由 Insurance Layer 维护。

### 2.1 唯一状态

```text
InsuranceState
```

允许值：

```text
SAFE
DEGRADED
PROTECTED
EMERGENCY
LOCKED
```

### 2.2 禁止的状态来源

以下状态不得作为全局系统状态：

```text
risk.py local state
portfolio.state
runner status
cashflow warning
broker reconciliation status
alpha strategy status
```

这些字段可以保留为局部事实或兼容字段，但不得拥有全局安全语义。

### 2.3 强制条款

| ID | Rule | Testable |
| --- | --- | --- |
| INS-SSOT-001 | `InsuranceState` is the only global safety state. | Yes |
| INS-SSOT-002 | If a local module state conflicts with `InsuranceState`, the system must fail closed or enter manual review. | Yes |
| INS-SSOT-003 | Legacy `PortfolioEngine.state` must eventually be derived from `InsuranceState`, not independently decided. | Yes |
| INS-SSOT-004 | Reports may display local statuses, but must identify `InsuranceState` as the authoritative state. | Yes |

## 3. Safety State Machine

### 3.1 States

| State | Meaning |
| --- | --- |
| `SAFE` | Normal operating state. Full automation may be allowed if all gates pass. |
| `DEGRADED` | System remains operational but has warning-grade deterioration. Permissions are reduced. |
| `PROTECTED` | Defensive mode. Alpha is frozen, cash protection increases, execution is restricted. |
| `EMERGENCY` | Severe risk mode. Only risk-reducing actions may be allowed. |
| `LOCKED` | Execution authority is revoked. Monitoring and logging only, except explicitly approved survival operations. |

### 3.2 Transition Principles

```text
Degradation can be automatic and fast.
Recovery must be slower than degradation.
LOCKED can be entered automatically.
LOCKED cannot be exited automatically.
```

### 3.3 Allowed Transitions

```text
SAFE -> DEGRADED
SAFE -> PROTECTED
SAFE -> EMERGENCY
SAFE -> LOCKED

DEGRADED -> SAFE
DEGRADED -> PROTECTED
DEGRADED -> EMERGENCY
DEGRADED -> LOCKED

PROTECTED -> DEGRADED
PROTECTED -> SAFE
PROTECTED -> EMERGENCY
PROTECTED -> LOCKED

EMERGENCY -> PROTECTED
EMERGENCY -> LOCKED

LOCKED -> EMERGENCY
```

### 3.4 Forbidden Transitions

```text
LOCKED -> SAFE
LOCKED -> PROTECTED
LOCKED -> DEGRADED
EMERGENCY -> SAFE
EMERGENCY -> DEGRADED
```

These transitions may only be represented as multi-step recovery:

```text
LOCKED -> EMERGENCY -> PROTECTED -> DEGRADED -> SAFE
```

or, if `DEGRADED` is not needed:

```text
LOCKED -> EMERGENCY -> PROTECTED -> SAFE
```

### 3.5 Automatic vs Approved Transitions

| Transition Type | Automatic? | Approval Required? |
| --- | --- | --- |
| Any transition to a more restrictive state | Yes | No |
| `DEGRADED -> SAFE` | Yes, if recovery criteria pass | No |
| `PROTECTED -> DEGRADED/SAFE` | Yes, if cooldown and recovery criteria pass | No |
| `EMERGENCY -> PROTECTED` | Conditional | Optional policy gate |
| `LOCKED -> EMERGENCY` | No | Yes, human or multisig |
| Any `LOCKED` exit | No | Yes |

### 3.6 Strong Clauses

| ID | Rule | Testable |
| --- | --- | --- |
| INS-STATE-001 | Any hard veto signal may force `LOCKED`. | Yes |
| INS-STATE-002 | `LOCKED` must be auto-enterable. | Yes |
| INS-STATE-003 | `LOCKED` must not auto-exit. | Yes |
| INS-STATE-004 | Recovery must be slower than degradation. | Yes |
| INS-STATE-005 | Forbidden transitions must fail validation. | Yes |

## 4. Risk Aggregation

Insurance Layer consumes signals. It does not let signal providers decide global state.

### 4.1 Signal Provider Contract

Every signal provider must emit facts, not final system decisions.

```text
Signal:
  source
  severity
  score
  hard_veto
  reason
  observed_at
  evidence
```

### 4.2 Signal Categories

| Signal | Provider | Examples |
| --- | --- | --- |
| `MarketRiskSignal` | `risk.py` | drawdown, correlation breakdown, volatility |
| `StabilitySignal` | `cashflow.py` / portfolio ledger | Stability floor, liquidity buffer |
| `BrokerSyncSignal` | broker sync service | missing sync, stale snapshot, reconciliation drift |
| `ExecutionSignal` | execution layer | slippage, missing broker receipt, failed order |
| `DataIntegritySignal` | data validator | stale data, NaN, abnormal returns |
| `GovernanceSignal` | governance engine | withdrawal status, approval gaps, audit issues |
| `AlphaSignal` | alpha arena | alpha drawdown, strategy suspension, sandbox boundary |

### 4.3 Weighted Risk Score

For non-veto signals:

```text
risk_score = sum(weight_i * normalized_signal_i)
```

The score is advisory for state transition. It never overrides hard veto signals.

### 4.4 Hard Veto Signals

The following must be treated as hard veto candidates:

```text
DATA_INTEGRITY_FAILED
BROKER_RECONCILIATION_BROKEN
BROKER_SYNC_MISSING_FOR_LIVE_EXECUTION
MISSING_BROKER_RECEIPT
POST_EXECUTION_RECONCILIATION_BROKEN
UNAUTHORIZED_LOCKED_RECOVERY
AUTHORITY_BYPASS_ATTEMPT
AUDIT_LOG_WRITE_FAILURE_FOR_HIGH_RISK_ACTION
```

### 4.5 Assessment Output

```text
InsuranceAssessment:
  state
  risk_score
  weighted_signals
  hard_blocks
  reasons
  recovery_requirements
  observed_at
```

### 4.6 Strong Clauses

| ID | Rule | Testable |
| --- | --- | --- |
| INS-RISK-001 | `risk.py` must report signals, not own global state. | Yes |
| INS-RISK-002 | `cashflow.py` must report liquidity/stability signals, not own global state. | Yes |
| INS-RISK-003 | Hard veto signals must dominate weighted risk score. | Yes |
| INS-RISK-004 | Risk score must be reproducible from recorded signals and weights. | Yes |
| INS-RISK-005 | Every state transition must cite signal evidence. | Yes |

## 5. System Authority Matrix

Insurance Layer outputs authority. Other modules consume it.

### 5.1 Permission Levels

| Level | Name | Meaning |
| --- | --- | --- |
| Level 1 | Observation | Read, monitor, log, report |
| Level 2 | Suggestion | Generate proposals, shadow runs, previews |
| Level 3 | Execution Authority | Change portfolio, cashflow, alpha, broker state, or ledger |

Only Insurance Layer grants Level 3 permissions.

### 5.2 Authority Decision Shape

```text
InsuranceDecision:
  state
  allow_observation
  allow_suggestions
  allow_core_rebalance
  allow_live_execution
  allow_alpha_execution
  allow_withdrawal_request
  allow_withdrawal_approval
  allow_withdrawal_execution
  allow_deposit
  allow_tax_harvest
  force_de_risk
  force_cash_floor
  block_trading
  freeze_execution
  require_manual_review
  require_recovery_proposal
  reasons
```

### 5.3 State Permission Matrix

| Action | SAFE | DEGRADED | PROTECTED | EMERGENCY | LOCKED |
| --- | --- | --- | --- | --- | --- |
| Monitoring / logging | Allow | Allow | Allow | Allow | Allow |
| Shadow suggestions | Allow | Allow | Allow | Allow | Allow |
| Core normal rebalance | Allow | Limited | Block | Block | Block |
| Risk-reducing rebalance | Allow | Allow | Allow | Allow | Block |
| Live execution | Allow if gates pass | Limited | Defensive only | Risk-reducing only | Block |
| Alpha execution | Allow | Reduced | Block | Block | Block |
| Deposit | Allow | Allow | Allow to Stability | Allow to Stability | Block unless survival op |
| Withdrawal request | Allow | Allow | Allow | Allow | Observation only |
| Withdrawal approval | Allow | Allow | Allow | Limited | Observation only |
| Withdrawal execution | Allow if approved | Limited | Limited | Block | Block |
| Tax harvest | Allow | Limited | Block | Block | Block |
| Recovery proposal | Not needed | Not needed | Optional | Optional | Required |

### 5.4 Survival Operations

In `LOCKED`, survival operations are forbidden by default and require explicit approval.

Examples:

```text
read-only broker sync
audit log repair
data integrity backfill
manual governance recovery record
```

Survival operations must not move capital unless explicitly approved by recovery governance.

### 5.5 Strong Clauses

| ID | Rule | Testable |
| --- | --- | --- |
| INS-PERM-001 | High-risk execution without `InsuranceDecision` must fail. | Yes |
| INS-PERM-002 | `LOCKED` blocks trading, rebalance, alpha, and cashflow movement by default. | Yes |
| INS-PERM-003 | `EMERGENCY` allows only risk-reducing trades. | Yes |
| INS-PERM-004 | `PROTECTED` freezes Alpha. | Yes |
| INS-PERM-005 | `DEGRADED` must reduce at least one execution or Alpha capability. | Yes |
| INS-PERM-006 | Any permission denial must include a reason suitable for audit. | Yes |

## 6. Recovery Proposal System

Recovery is not the absence of danger. Recovery is an authorized transition backed by evidence.

### 6.1 Recovery Proposal Shape

```text
RecoveryProposal:
  id
  portfolio_id
  from_state
  proposed_to_state
  created_at
  cooldown_until
  validation_evidence
  unresolved_blocks
  required_approvals
  approvals
  audit_log_ids
  status
```

Allowed statuses:

```text
PENDING
APPROVED
REJECTED
EXPIRED
EXECUTED
```

### 6.2 LOCKED Recovery

`LOCKED` recovery must satisfy all conditions:

```text
data integrity restored
broker sync available if broker execution will resume
broker reconciliation not BROKEN
no unresolved authority bypass attempt
no unresolved audit write failure
cooldown satisfied
human or multisig approval recorded
audit log written
```

The only direct exit from `LOCKED` is:

```text
LOCKED -> EMERGENCY
```

### 6.3 Recovery Path

Default recovery path:

```text
LOCKED -> EMERGENCY -> PROTECTED -> SAFE
```

Optional slower recovery:

```text
LOCKED -> EMERGENCY -> PROTECTED -> DEGRADED -> SAFE
```

### 6.4 Recovery Speed Principle

Recovery must be slower than degradation.

Minimum examples:

```text
SAFE -> LOCKED may happen immediately.
LOCKED -> EMERGENCY requires proposal + approval + audit.
EMERGENCY -> PROTECTED requires stable signals over cooldown.
PROTECTED -> SAFE requires neutralized risks and execution consistency.
```

### 6.5 Strong Clauses

| ID | Rule | Testable |
| --- | --- | --- |
| INS-REC-001 | `LOCKED` exit requires a recovery proposal. | Yes |
| INS-REC-002 | `LOCKED` exit requires human or multisig approval. | Yes |
| INS-REC-003 | Recovery proposal must include validation evidence. | Yes |
| INS-REC-004 | Recovery proposal must include audit evidence. | Yes |
| INS-REC-005 | Automatic `LOCKED -> SAFE` must be impossible. | Yes |
| INS-REC-006 | Recovery transitions must satisfy cooldown policy. | Yes |

## 7. Migration Contract

The migration target is C + B:

```text
C: Executable Spec
-> B: Controlled Control-Plane Migration
```

### 7.1 Phase 1: Spec Harness

Goal:

```text
Create test fixtures and validation helpers that encode this specification.
No behavior migration yet.
```

Required outputs:

```text
InsuranceState enum
InsuranceDecision schema
InsuranceAssessment schema
state transition validator
authority matrix validator
recovery proposal validator
```

### 7.2 Phase 2: Signal Provider Refactor

Goal:

```text
Convert existing risk sources into signal providers.
```

Targets:

```text
risk.py -> MarketRiskSignal
cashflow.py -> StabilitySignal
daily_runner broker gates -> BrokerSyncSignal / ExecutionSignal
data_validator.py -> DataIntegritySignal
governance.py -> GovernanceSignal
alpha arena -> AlphaSignal
```

### 7.3 Phase 3: Insurance State Ownership

Goal:

```text
InsuranceLayer owns global state.
```

Rules:

```text
PortfolioEngine.state becomes derived or compatibility-only.
DailyRunner consumes InsuranceDecision before every high-risk action.
CashflowEngine checks InsuranceDecision before capital movement.
AlphaArena checks InsuranceDecision before strategy execution.
```

### 7.4 Phase 4: Legacy State Removal

Goal:

```text
Remove independent global-state decisions from non-insurance modules.
```

This phase is complete only when:

```text
No module except InsuranceLayer can transition global system state.
No high-risk action can execute without InsuranceDecision.
All recovery paths are proposal-driven and audited.
```

## 8. Executable Test Requirements

### 8.1 State Consistency Tests

| ID | Test |
| --- | --- |
| INS-TEST-STATE-001 | Given local module state conflicts with `InsuranceState`, system fails closed or requires manual review. |
| INS-TEST-STATE-002 | Given forbidden transition, transition validator rejects it. |
| INS-TEST-STATE-003 | Given `LOCKED`, automatic recovery runner cannot change state. |
| INS-TEST-STATE-004 | Given `LOCKED -> EMERGENCY` with approved proposal, transition is allowed. |

### 8.2 Authority Violation Tests

| ID | Test |
| --- | --- |
| INS-TEST-AUTH-001 | Core rebalance without InsuranceDecision is blocked. |
| INS-TEST-AUTH-002 | Live execution without InsuranceDecision is blocked. |
| INS-TEST-AUTH-003 | Alpha execution in `PROTECTED`, `EMERGENCY`, or `LOCKED` is blocked. |
| INS-TEST-AUTH-004 | Withdrawal execution in `EMERGENCY` or `LOCKED` is blocked. |
| INS-TEST-AUTH-005 | Execution adapter cannot bypass Insurance authority. |

### 8.3 Risk Aggregation Tests

| ID | Test |
| --- | --- |
| INS-TEST-RISK-001 | Weighted risk score is reproducible from input signals. |
| INS-TEST-RISK-002 | Hard veto overrides low weighted risk score. |
| INS-TEST-RISK-003 | Data integrity failure creates hard block. |
| INS-TEST-RISK-004 | Broker reconciliation BROKEN creates hard block. |
| INS-TEST-RISK-005 | Stability below floor contributes StabilitySignal and can reduce permissions. |

### 8.4 Recovery Correctness Tests

| ID | Test |
| --- | --- |
| INS-TEST-REC-001 | `LOCKED -> SAFE` direct transition is rejected. |
| INS-TEST-REC-002 | `LOCKED -> EMERGENCY` without proposal is rejected. |
| INS-TEST-REC-003 | `LOCKED -> EMERGENCY` without approval is rejected. |
| INS-TEST-REC-004 | `LOCKED -> EMERGENCY` without audit evidence is rejected. |
| INS-TEST-REC-005 | Recovery before cooldown is rejected. |
| INS-TEST-REC-006 | Recovery with unresolved hard block is rejected. |

## 9. Audit Requirements

Every Insurance decision that changes permission, state, or recovery status must be auditable.

### 9.1 Required Audit Fields

```text
decision_id
portfolio_id
previous_state
new_state
risk_score
hard_blocks
allowed_actions
blocked_actions
forced_actions
source_signals
recovery_proposal_id
created_at
actor
```

### 9.2 Strong Clauses

| ID | Rule | Testable |
| --- | --- | --- |
| INS-AUDIT-001 | State transition without audit record must fail closed. | Yes |
| INS-AUDIT-002 | Recovery approval without audit record must not unlock. | Yes |
| INS-AUDIT-003 | High-risk action must reference the Insurance decision that authorized it. | Yes |

## 10. Non-Goals

This specification does not introduce:

```text
Kelly sizing
new alpha strategy
new return target
new broker behavior
automatic withdrawal execution
automatic LOCKED recovery
```

It only defines the constitutional safety layer that governs existing and future behavior.

## 11. Acceptance Criteria

The Insurance Layer migration is not considered complete until:

```text
1. InsuranceState is the only global safety state.
2. Every high-risk action checks InsuranceDecision.
3. LOCKED can be entered automatically.
4. LOCKED cannot be exited automatically.
5. Recovery proposal path is implemented and audited.
6. Existing risk, cashflow, broker, alpha, and data checks are converted into signals.
7. Authority matrix has executable tests.
8. Forbidden transitions have executable tests.
9. Manual/multisig recovery has executable tests.
10. Reports display InsuranceState as the authoritative system state.
```

## 12. One-Line Constitution

> Insurance Layer is the only authority allowed to stop, restrict, recover, or authorize the system.
