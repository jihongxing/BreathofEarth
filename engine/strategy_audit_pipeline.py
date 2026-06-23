"""Strategy-level layered audit result model.

This module defines the evidence schema for the multi-strategy shadow audit
platform. It does not run backtests, read broker state, or authorize execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from engine.strategy_registry import AUDIT_LAYER_SEQUENCE, StrategyDefinition, get_strategy


PASS_LAYER_STATUSES = frozenset({"PASS", "OK", "OBSERVED", "HEALTHY"})
ATTENTION_LAYER_STATUSES = frozenset({"PARTIAL", "UNAVAILABLE", "FAIL_CLOSED", "FAILED", "MISSING"})
ALLOWED_LAYER_STATUSES = PASS_LAYER_STATUSES | ATTENTION_LAYER_STATUSES

REQUIRED_EVIDENCE_LAYERS = tuple(layer for layer in AUDIT_LAYER_SEQUENCE if layer != "admission_gated")
METRIC_LAYERS = frozenset(
    {
        "research_gross",
        "execution_adjusted",
        "broker_adjusted",
        "tax_adjusted",
        "failure_adjusted",
    }
)

PRODUCTION_CONCLUSION = "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED"


@dataclass(frozen=True)
class AuditBlocker:
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class AuditLayerEvidence:
    layer: str
    status: str
    cagr: float | None = None
    mdd: float | None = None
    final_nav: float | None = None
    warnings: tuple[str, ...] = ()
    source: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_status = str(self.status).upper()
        object.__setattr__(self, "status", normalized_status)
        if self.layer not in AUDIT_LAYER_SEQUENCE:
            raise ValueError(f"unsupported audit layer: {self.layer}")
        if normalized_status not in ALLOWED_LAYER_STATUSES:
            raise ValueError(f"{self.layer}: unsupported layer status: {self.status}")
        if self.layer in METRIC_LAYERS:
            if self.cagr is None or self.mdd is None or self.final_nav is None:
                raise ValueError(f"{self.layer}: metric layers require cagr, mdd, and final_nav")
            if self.final_nav <= 0:
                raise ValueError(f"{self.layer}: final_nav must be positive")

    @property
    def passed(self) -> bool:
        return self.status in PASS_LAYER_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "status": self.status,
            "cagr": self.cagr,
            "mdd": self.mdd,
            "final_nav": self.final_nav,
            "warnings": list(self.warnings),
            "source": self.source,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class StrategyAuditResult:
    strategy_id: str
    as_of: str
    status: str
    admission_status: str
    level: str
    layers: dict[str, AuditLayerEvidence]
    blockers: tuple[AuditBlocker, ...]
    warnings: tuple[str, ...]
    live_leverage_approved: bool = False
    human_review_required: bool = True
    readonly: bool = True
    production_conclusion: str = PRODUCTION_CONCLUSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "as_of": self.as_of,
            "status": self.status,
            "admission_status": self.admission_status,
            "level": self.level,
            "layers": {layer: evidence.to_dict() for layer, evidence in self.layers.items()},
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "warnings": list(self.warnings),
            "live_leverage_approved": False,
            "human_review_required": self.human_review_required,
            "readonly": self.readonly,
            "production_conclusion": self.production_conclusion,
        }


def build_strategy_audit_result(
    strategy_id: str,
    layers: dict[str, AuditLayerEvidence],
    *,
    as_of: str,
    warnings: tuple[str, ...] = (),
) -> StrategyAuditResult:
    """Build a fail-closed strategy audit result from externally computed evidence."""
    strategy = get_strategy(strategy_id)
    normalized_layers = _normalize_layers(layers)
    blockers = _collect_blockers(strategy, normalized_layers)
    all_required_present = all(layer in normalized_layers for layer in REQUIRED_EVIDENCE_LAYERS)

    if blockers:
        status = "PARTIAL" if not all_required_present else "NOT_APPROVED"
        admission_status = "NOT_APPROVED"
        level = "warning"
    else:
        status = "PASS"
        admission_status = "READY_FOR_HUMAN_REVIEW"
        level = "healthy"

    all_warnings = tuple(warnings) + tuple(
        warning
        for layer in normalized_layers.values()
        for warning in layer.warnings
    )
    return StrategyAuditResult(
        strategy_id=strategy.strategy_id,
        as_of=as_of,
        status=status,
        admission_status=admission_status,
        level=level,
        layers=normalized_layers,
        blockers=tuple(blockers),
        warnings=all_warnings,
        live_leverage_approved=False,
        human_review_required=True,
        readonly=True,
    )


def _normalize_layers(layers: dict[str, AuditLayerEvidence]) -> dict[str, AuditLayerEvidence]:
    normalized: dict[str, AuditLayerEvidence] = {}
    for layer_name, evidence in layers.items():
        if layer_name != evidence.layer:
            raise ValueError(f"layer key mismatch: {layer_name} != {evidence.layer}")
        normalized[layer_name] = evidence
    return normalized


def _collect_blockers(
    strategy: StrategyDefinition,
    layers: dict[str, AuditLayerEvidence],
) -> list[AuditBlocker]:
    blockers: list[AuditBlocker] = []

    for layer in REQUIRED_EVIDENCE_LAYERS:
        evidence = layers.get(layer)
        if evidence is None:
            blockers.append(
                AuditBlocker(
                    code=f"missing_{layer}_layer",
                    message=f"{layer} layer is unavailable",
                )
            )
            continue
        if not evidence.passed:
            blockers.append(
                AuditBlocker(
                    code=f"{layer}_not_passed",
                    message=f"{layer} layer status is {evidence.status}",
                    details={"status": evidence.status},
                )
            )

    blockers.extend(_risk_blockers(strategy, layers))
    return blockers


def _risk_blockers(
    strategy: StrategyDefinition,
    layers: dict[str, AuditLayerEvidence],
) -> list[AuditBlocker]:
    blockers: list[AuditBlocker] = []
    research = layers.get("research_gross")
    if research and research.mdd is not None and research.mdd < strategy.risk_policy.max_research_mdd:
        blockers.append(
            AuditBlocker(
                code="research_mdd_breach",
                message="research_gross MDD breaches strategy risk policy",
                details={
                    "observed_mdd": research.mdd,
                    "max_research_mdd": strategy.risk_policy.max_research_mdd,
                },
            )
        )
    failure = layers.get("failure_adjusted")
    if failure and failure.mdd is not None and failure.mdd < strategy.risk_policy.max_failure_adjusted_mdd:
        blockers.append(
            AuditBlocker(
                code="failure_adjusted_mdd_breach",
                message="failure_adjusted MDD breaches strategy risk policy",
                details={
                    "observed_mdd": failure.mdd,
                    "max_failure_adjusted_mdd": strategy.risk_policy.max_failure_adjusted_mdd,
                },
            )
        )
    return blockers
