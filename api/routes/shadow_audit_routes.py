"""Stage 9.5 shadow audit read-only API."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends

from api.deps import get_current_user


router = APIRouter(prefix="/api", tags=["shadow-audit"])

DEFAULT_SHADOW_DIR = Path(os.environ.get("XIRANG_SHADOW_AUDIT_DIR", "data/shadow"))
DEFAULT_STALE_AFTER_HOURS = int(os.environ.get("XIRANG_SHADOW_STALE_AFTER_HOURS", "24"))

LEVEL_RANK = {
    "healthy": 1,
    "missing": 2,
    "warning": 3,
    "critical": 4,
}


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_report_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _load_json_file(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(payload, dict):
        return None, "invalid_shape"
    return payload, None


def _shadow_level(status_value: str, requires_attention: bool) -> str:
    status = str(status_value or "").upper()
    if status in {"FAIL_CLOSED", "FAILED", "BROKEN", "CRITICAL"}:
        return "critical"
    if requires_attention or status in {"WARNING", "PARTIAL", "UNAVAILABLE", "DRIFT", "STALE", "ATTENTION"}:
        return "warning"
    if status in {"OK", "OBSERVED", "MATCHED", "HEALTHY", "COLLECTING"}:
        return "healthy"
    return "missing"


def _missing_component(name: str, path: Path, reason: str) -> dict[str, Any]:
    warning = (
        f"{name} report is missing"
        if reason == "missing"
        else f"{name} report cannot be parsed: {reason}"
    )
    return {
        "enabled": True,
        "status": "missing",
        "level": "missing" if reason == "missing" else "warning",
        "last_run_at": None,
        "requires_attention": reason != "missing",
        "warnings": [warning],
        "source_path": str(path),
        "stale_report": False,
        "age_hours": None,
        "report": {},
    }


def _apply_stale_guard(
    component: dict[str, Any],
    name: str,
    *,
    now: datetime,
    stale_after_hours: int,
) -> dict[str, Any]:
    if component.get("status") == "missing":
        return component

    warnings = list(component.get("warnings") or [])
    report_time = _parse_report_time(component.get("last_run_at"))
    if report_time is None:
        warnings.append(f"{name} report timestamp is missing or invalid")
        component["stale_report"] = True
        component["age_hours"] = None
    else:
        age_hours = max((now - report_time).total_seconds() / 3600.0, 0.0)
        component["age_hours"] = round(age_hours, 2)
        component["stale_report"] = age_hours > stale_after_hours
        if component["stale_report"]:
            warnings.append(
                f"{name} report is stale: age={age_hours:.1f}h, threshold={stale_after_hours}h"
            )

    if component.get("stale_report"):
        component["warnings"] = warnings
        component["requires_attention"] = True
        if component.get("level") == "healthy":
            component["level"] = "warning"
    return component


def normalize_shadow_sync(payload: dict[str, Any] | None, path: Path, reason: str | None) -> dict[str, Any]:
    if payload is None:
        return _missing_component("shadow_sync", path, reason or "missing")

    warnings = _safe_list(payload.get("warnings"))
    requires_attention = bool(payload.get("requires_attention") or warnings)
    status = str(payload.get("status") or "UNKNOWN")
    level = _shadow_level(status, requires_attention)
    orders = _safe_list(payload.get("shadow_orders"))

    return {
        "enabled": True,
        "status": status,
        "level": level,
        "last_run_at": payload.get("timestamp"),
        "requires_attention": requires_attention,
        "warnings": warnings,
        "source_path": str(path),
        "stale_report": False,
        "age_hours": None,
        "broker": payload.get("broker") if isinstance(payload.get("broker"), dict) else {},
        "candidate_policy": payload.get("candidate_policy") if isinstance(payload.get("candidate_policy"), dict) else {},
        "target_weights": payload.get("target_weights") if isinstance(payload.get("target_weights"), dict) else {},
        "target_notionals": payload.get("target_notionals") if isinstance(payload.get("target_notionals"), dict) else {},
        "slippage_audit": payload.get("slippage_audit") if isinstance(payload.get("slippage_audit"), dict) else {},
        "market_dates": _safe_list(payload.get("market_dates")),
        "order_count": len(orders),
        "dry_run": bool(payload.get("dry_run", True)),
        "trading_disabled": bool(payload.get("trading_disabled", True)),
        "report": payload,
    }


def normalize_margin_snapshot(payload: dict[str, Any] | None, path: Path, reason: str | None) -> dict[str, Any]:
    if payload is None:
        return _missing_component("margin_snapshot", path, reason or "missing")

    warnings = _safe_list(payload.get("warnings"))
    requires_attention = bool(payload.get("requires_attention") or warnings)
    status = str(payload.get("status") or "UNKNOWN")
    level = _shadow_level(status, requires_attention)

    return {
        "enabled": True,
        "status": status,
        "level": level,
        "last_run_at": payload.get("timestamp"),
        "requires_attention": requires_attention,
        "warnings": warnings,
        "source_path": str(path),
        "stale_report": False,
        "age_hours": None,
        "broker": payload.get("broker") if isinstance(payload.get("broker"), dict) else {},
        "account": payload.get("account") if isinstance(payload.get("account"), dict) else {},
        "margin_fields": payload.get("margin_fields") if isinstance(payload.get("margin_fields"), dict) else {},
        "production_conclusion": payload.get("production_conclusion", "OBSERVATION_ONLY_NO_LEVERAGE_APPROVAL"),
        "dry_run": bool(payload.get("dry_run", True)),
        "trading_disabled": bool(payload.get("trading_disabled", True)),
        "report": payload,
    }


def build_shadow_audit_payload(
    portfolio_id: str,
    shadow_dir: Path | None = None,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    shadow_dir = shadow_dir or DEFAULT_SHADOW_DIR
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    shadow_path = shadow_dir / "latest_shadow_sync.json"
    margin_path = shadow_dir / "latest_margin_snapshot.json"

    shadow_payload, shadow_reason = _load_json_file(shadow_path)
    margin_payload, margin_reason = _load_json_file(margin_path)

    shadow_sync = _apply_stale_guard(
        normalize_shadow_sync(shadow_payload, shadow_path, shadow_reason),
        "shadow_sync",
        now=now,
        stale_after_hours=stale_after_hours,
    )
    margin_snapshot = _apply_stale_guard(
        normalize_margin_snapshot(margin_payload, margin_path, margin_reason),
        "margin_snapshot",
        now=now,
        stale_after_hours=stale_after_hours,
    )

    components = [shadow_sync, margin_snapshot]
    overall_level = max(
        (component["level"] for component in components),
        key=lambda level: LEVEL_RANK.get(level, 0),
    )
    requires_attention = any(component["requires_attention"] for component in components)
    warning_count = sum(len(component["warnings"]) for component in components)
    stale_report_count = sum(1 for component in components if component.get("stale_report"))
    if all(component["level"] == "missing" for component in components):
        overall_status = "MISSING"
    elif overall_level == "critical":
        overall_status = "CRITICAL"
    elif requires_attention or overall_level == "warning":
        overall_status = "ATTENTION"
    else:
        overall_status = "HEALTHY"

    return {
        "portfolio_id": portfolio_id,
        "generated_at": _utc_now_text(),
        "stage": "Stage 9.5 Shadow Sync & Structural Audit",
        "status": overall_status,
        "level": overall_level,
        "requires_attention": requires_attention,
        "warning_count": warning_count,
        "stale_after_hours": stale_after_hours,
        "stale_report_count": stale_report_count,
        "live_leverage_approved": False,
        "production_conclusion": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        "components": {
            "shadow_sync": shadow_sync,
            "margin_snapshot": margin_snapshot,
        },
    }


def normalize_observation_summary(
    payload: dict[str, Any] | None,
    path: Path,
    reason: str | None,
) -> dict[str, Any]:
    if payload is None:
        warning = (
            "stage95_observation_summary report is missing"
            if reason == "missing"
            else f"stage95_observation_summary report cannot be parsed: {reason}"
        )
        return {
            "enabled": True,
            "status": "missing",
            "level": "missing" if reason == "missing" else "warning",
            "requires_attention": reason != "missing",
            "warnings": [warning],
            "source_path": str(path),
            "live_leverage_approved": False,
            "report": {},
        }

    warnings = _safe_list(payload.get("warnings"))
    requires_attention = bool(payload.get("requires_attention") or warnings)
    status = str(payload.get("status") or "UNKNOWN")
    level = _shadow_level(status, requires_attention)

    return {
        "enabled": True,
        "status": status,
        "level": level,
        "requires_attention": requires_attention,
        "warnings": warnings,
        "source_path": str(path),
        "generated_at": payload.get("generated_at"),
        "stage": payload.get("stage", "Stage 9.5 Observation Summary"),
        "expected_cycles": int(payload.get("expected_cycles") or 0),
        "observed_cycles": int(payload.get("observed_cycles") or 0),
        "coverage_ratio": float(payload.get("coverage_ratio") or 0.0),
        "first_cycle_at": payload.get("first_cycle_at"),
        "last_cycle_at": payload.get("last_cycle_at"),
        "latest_age_hours": payload.get("latest_age_hours"),
        "latest_is_stale": bool(payload.get("latest_is_stale", False)),
        "stale_after_hours": payload.get("stale_after_hours"),
        "stale_gap_count": int(payload.get("stale_gap_count") or 0),
        "status_counts": payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {},
        "attention_cycles": int(payload.get("attention_cycles") or 0),
        "critical_cycles": int(payload.get("critical_cycles") or 0),
        "broker_unavailable_cycles": int(payload.get("broker_unavailable_cycles") or 0),
        "warning_count_total": int(payload.get("warning_count_total") or 0),
        "component_warning_count_total": int(payload.get("component_warning_count_total") or 0),
        "abnormal_streak": payload.get("abnormal_streak") if isinstance(payload.get("abnormal_streak"), dict) else {},
        "broker_unavailable_streak": (
            payload.get("broker_unavailable_streak")
            if isinstance(payload.get("broker_unavailable_streak"), dict)
            else {}
        ),
        "slippage_bps": payload.get("slippage_bps") if isinstance(payload.get("slippage_bps"), dict) else {},
        "margin_field_coverage": (
            payload.get("margin_field_coverage")
            if isinstance(payload.get("margin_field_coverage"), dict)
            else {}
        ),
        "recent_cycles": _safe_list(payload.get("recent_cycles")),
        "live_leverage_approved": False,
        "production_conclusion": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        "report": payload,
    }


def build_observation_summary_payload(
    portfolio_id: str,
    shadow_dir: Path | None = None,
) -> dict[str, Any]:
    shadow_dir = shadow_dir or DEFAULT_SHADOW_DIR
    summary_path = shadow_dir / "latest_stage95_observation_summary.json"
    summary_payload, summary_reason = _load_json_file(summary_path)
    summary = normalize_observation_summary(summary_payload, summary_path, summary_reason)

    return {
        "portfolio_id": portfolio_id,
        "generated_at": _utc_now_text(),
        "stage": "Stage 9.5 Observation Summary",
        "status": summary["status"],
        "level": summary["level"],
        "requires_attention": summary["requires_attention"],
        "warning_count": len(summary["warnings"]),
        "live_leverage_approved": False,
        "production_conclusion": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        "summary": summary,
    }


@router.get("/shadow-audit/{portfolio_id}")
async def get_shadow_audit(
    portfolio_id: str,
    user: dict = Depends(get_current_user),
):
    """Return Stage 9.5 read-only shadow audit state for the dashboard."""
    _ = user
    return build_shadow_audit_payload(portfolio_id)


@router.get("/stage95-observation-summary/{portfolio_id}")
async def get_stage95_observation_summary(
    portfolio_id: str,
    user: dict = Depends(get_current_user),
):
    """Return read-only Stage 9.5 observation summary state for the dashboard."""
    _ = user
    return build_observation_summary_payload(portfolio_id)
