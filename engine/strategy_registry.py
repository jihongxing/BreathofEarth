"""Read-only strategy registry for the multi-strategy shadow audit platform."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


ALLOWED_STRATEGY_STATUSES = frozenset(
    {
        "research_baseline",
        "defensive_anchor",
        "production_candidate",
        "benchmark_proxy",
        "research_candidate",
    }
)

AUDIT_LAYER_SEQUENCE = (
    "data_validated",
    "research_gross",
    "execution_adjusted",
    "broker_adjusted",
    "tax_adjusted",
    "failure_adjusted",
    "shadow_observed",
    "admission_gated",
)

DEFAULT_STAGE95_STRATEGY_ID = "production_90_10"


@dataclass(frozen=True)
class RiskPolicy:
    """Risk guardrails used by admission gates and shadow audit reports."""

    max_research_mdd: float
    max_failure_adjusted_mdd: float
    satellite_cap: float | None = None
    live_leverage_approved: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_research_mdd": self.max_research_mdd,
            "max_failure_adjusted_mdd": self.max_failure_adjusted_mdd,
            "satellite_cap": self.satellite_cap,
            "live_leverage_approved": self.live_leverage_approved,
        }


@dataclass(frozen=True)
class StrategyDefinition:
    """A strategy that can be observed by Stage 9.5, never executed from here."""

    strategy_id: str
    display_name: str
    status: str
    confidence: str
    assets: tuple[str, ...]
    base_currency: str
    data_policy: str
    risk_policy: RiskPolicy
    reference_weights: MappingProxyType[str, float]
    live_execution_allowed: bool = False
    stage95_default: bool = False
    notes: tuple[str, ...] = ()
    metadata: MappingProxyType[str, Any] = MappingProxyType({})

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "display_name": self.display_name,
            "status": self.status,
            "confidence": self.confidence,
            "assets": list(self.assets),
            "base_currency": self.base_currency,
            "data_policy": self.data_policy,
            "risk_policy": self.risk_policy.to_dict(),
            "reference_weights": dict(self.reference_weights),
            "live_execution_allowed": self.live_execution_allowed,
            "stage95_default": self.stage95_default,
            "notes": list(self.notes),
            "metadata": _plain_mapping(self.metadata),
        }


def _frozen_mapping(values: dict[str, Any]) -> MappingProxyType[str, Any]:
    return MappingProxyType(dict(values))


def _plain_mapping(values: MappingProxyType[str, Any] | dict[str, Any]) -> dict[str, Any]:
    plain: dict[str, Any] = {}
    for key, value in values.items():
        if isinstance(value, MappingProxyType):
            plain[key] = _plain_mapping(value)
        elif isinstance(value, dict):
            plain[key] = _plain_mapping(value)
        else:
            plain[key] = value
    return plain


def _strategy(
    *,
    strategy_id: str,
    display_name: str,
    status: str,
    confidence: str,
    assets: tuple[str, ...],
    risk_policy: RiskPolicy,
    reference_weights: dict[str, float],
    stage95_default: bool = False,
    notes: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> StrategyDefinition:
    strategy = StrategyDefinition(
        strategy_id=strategy_id,
        display_name=display_name,
        status=status,
        confidence=confidence,
        assets=assets,
        base_currency="USD",
        data_policy="yahoo_adj_close_clean",
        risk_policy=risk_policy,
        reference_weights=_frozen_mapping(reference_weights),
        live_execution_allowed=False,
        stage95_default=stage95_default,
        notes=notes,
        metadata=_frozen_mapping(metadata or {}),
    )
    validate_strategy_definition(strategy)
    return strategy


def validate_strategy_definition(strategy: StrategyDefinition) -> None:
    """Validate a registry entry and fail closed on live or malformed definitions."""
    if not strategy.strategy_id:
        raise ValueError("strategy_id is required")
    if strategy.status not in ALLOWED_STRATEGY_STATUSES:
        raise ValueError(f"unsupported strategy status: {strategy.status}")
    if not strategy.assets:
        raise ValueError(f"{strategy.strategy_id}: at least one asset is required")
    if len(strategy.assets) != len(set(strategy.assets)):
        raise ValueError(f"{strategy.strategy_id}: assets must be unique")
    if set(strategy.reference_weights) != set(strategy.assets):
        raise ValueError(f"{strategy.strategy_id}: reference weights must match assets")
    weight_sum = sum(float(weight) for weight in strategy.reference_weights.values())
    if abs(weight_sum - 1.0) > 1e-9:
        raise ValueError(f"{strategy.strategy_id}: reference weights must sum to 1.0, got {weight_sum:.12f}")
    if any(float(weight) < 0 for weight in strategy.reference_weights.values()):
        raise ValueError(f"{strategy.strategy_id}: reference weights cannot be negative")
    if strategy.base_currency != "USD":
        raise ValueError(f"{strategy.strategy_id}: only USD strategies are supported in this registry")
    if strategy.data_policy != "yahoo_adj_close_clean":
        raise ValueError(f"{strategy.strategy_id}: US ETF research must use clean Yahoo Adj Close")
    if strategy.live_execution_allowed:
        raise ValueError(f"{strategy.strategy_id}: registry entries cannot authorize live execution")
    if strategy.risk_policy.live_leverage_approved:
        raise ValueError(f"{strategy.strategy_id}: registry entries cannot approve live leverage")
    if not (strategy.risk_policy.max_research_mdd < 0 and strategy.risk_policy.max_failure_adjusted_mdd < 0):
        raise ValueError(f"{strategy.strategy_id}: MDD guardrails must be negative")
    if strategy.risk_policy.satellite_cap is not None:
        if not (0 <= strategy.risk_policy.satellite_cap <= 0.10):
            raise ValueError(f"{strategy.strategy_id}: satellite cap cannot exceed 10%")


STRATEGY_REGISTRY: MappingProxyType[str, StrategyDefinition] = MappingProxyType(
    {
        "classic_permanent_portfolio": _strategy(
            strategy_id="classic_permanent_portfolio",
            display_name="Classic Permanent Portfolio",
            status="research_baseline",
            confidence="FULL_CYCLE_AUDITED",
            assets=("SPY", "TLT", "GLD", "SHV"),
            risk_policy=RiskPolicy(max_research_mdd=-0.18, max_failure_adjusted_mdd=-0.20),
            reference_weights={"SPY": 0.25, "TLT": 0.25, "GLD": 0.25, "SHV": 0.25},
            notes=("Research baseline only; not a production candidate.",),
        ),
        "fixed_defensive_core": _strategy(
            strategy_id="fixed_defensive_core",
            display_name="Fixed Defensive Core",
            status="defensive_anchor",
            confidence="FULL_CYCLE_AUDITED",
            assets=("SPY", "TLT", "GLD", "SHV"),
            risk_policy=RiskPolicy(max_research_mdd=-0.13, max_failure_adjusted_mdd=-0.16),
            reference_weights={"SPY": 0.25, "TLT": 0.25, "GLD": 0.25, "SHV": 0.25},
            notes=("MA150 + 8w fixed defensive core; used as the 90% hub.",),
            metadata={
                "macro_defense_weights": _frozen_mapping(
                    {"SPY": 0.10, "TLT": 0.05, "GLD": 0.35, "SHV": 0.50}
                ),
                "recovery_rule": "8w_both_recovery",
            },
        ),
        "production_90_10": _strategy(
            strategy_id="production_90_10",
            display_name="90% Defensive Core + 10% Satellite",
            status="production_candidate",
            confidence="FULL_CYCLE_AUDITED_CALIBRATED_ESTIMATION",
            assets=("SPY", "TLT", "GLD", "SHV", "QQQ"),
            risk_policy=RiskPolicy(
                max_research_mdd=-0.15,
                max_failure_adjusted_mdd=-0.1532,
                satellite_cap=0.10,
                live_leverage_approved=False,
            ),
            reference_weights={"SPY": 0.255, "TLT": 0.225, "GLD": 0.255, "SHV": 0.225, "QQQ": 0.04},
            stage95_default=True,
            notes=(
                "Current production design candidate.",
                "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED.",
            ),
            metadata={
                "core_weight": 0.90,
                "satellite_weight": 0.10,
                "satellite_assets": ("QQQ", "SPY", "GLD"),
            },
        ),
        "benchmark_balanced_proxy": _strategy(
            strategy_id="benchmark_balanced_proxy",
            display_name="Balanced Benchmark Proxy",
            status="benchmark_proxy",
            confidence="REFERENCE_WINDOW_DEPENDENT",
            assets=("VTI", "AGG"),
            risk_policy=RiskPolicy(max_research_mdd=-0.30, max_failure_adjusted_mdd=-0.35),
            reference_weights={"VTI": 0.60, "AGG": 0.40},
            notes=("External reference proxy only; cannot enter production without a separate audit.",),
        ),
    }
)


def validate_registry(registry: MappingProxyType[str, StrategyDefinition] = STRATEGY_REGISTRY) -> None:
    if not registry:
        raise ValueError("strategy registry cannot be empty")
    default_count = 0
    production_count = 0
    for strategy_id, strategy in registry.items():
        if strategy_id != strategy.strategy_id:
            raise ValueError(f"registry key mismatch: {strategy_id} != {strategy.strategy_id}")
        validate_strategy_definition(strategy)
        default_count += int(strategy.stage95_default)
        production_count += int(strategy.status == "production_candidate")
    if default_count != 1:
        raise ValueError(f"exactly one Stage 9.5 default strategy is required, got {default_count}")
    if production_count != 1:
        raise ValueError(f"exactly one production candidate is required, got {production_count}")


def get_strategy(strategy_id: str) -> StrategyDefinition:
    try:
        return STRATEGY_REGISTRY[strategy_id]
    except KeyError as exc:
        raise KeyError(f"unknown strategy_id: {strategy_id}") from exc


def get_default_stage95_strategy() -> StrategyDefinition:
    return get_strategy(DEFAULT_STAGE95_STRATEGY_ID)


def list_strategies(status: str | None = None) -> tuple[StrategyDefinition, ...]:
    if status is not None and status not in ALLOWED_STRATEGY_STATUSES:
        raise ValueError(f"unsupported strategy status: {status}")
    strategies = tuple(STRATEGY_REGISTRY.values())
    if status is None:
        return strategies
    return tuple(strategy for strategy in strategies if strategy.status == status)


validate_registry()
