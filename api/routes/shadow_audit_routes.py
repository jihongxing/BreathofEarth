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

PRODUCTION_CONCLUSION = "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED"


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
        "requires_attention": True,
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


def normalize_ibkr_readonly_preflight(
    payload: dict[str, Any] | None,
    path: Path,
    reason: str | None,
) -> dict[str, Any]:
    if payload is None:
        return _missing_component("ibkr_readonly_preflight", path, reason or "missing")

    warnings = _safe_list(payload.get("warnings"))
    blockers = _safe_list(payload.get("blockers"))
    status = str(payload.get("status") or "UNKNOWN")
    requires_attention = bool(payload.get("requires_attention") or warnings or blockers)
    level = _shadow_level(status, requires_attention)

    if status == "FAIL_CLOSED":
        level = "critical"
        requires_attention = True
    elif status in {"NOT_READY", "ATTENTION", "READY_FOR_READONLY_CONNECT"}:
        level = "warning"
        requires_attention = True
    elif status == "READY" and not blockers and not warnings:
        level = "healthy"

    return {
        "enabled": True,
        "status": status,
        "level": level,
        "last_run_at": payload.get("timestamp"),
        "requires_attention": requires_attention,
        "warnings": warnings,
        "blockers": blockers,
        "source_path": str(path),
        "stale_report": False,
        "age_hours": None,
        "readonly": bool(payload.get("readonly", True)),
        "dry_run": bool(payload.get("dry_run", True)),
        "trading_disabled": bool(payload.get("trading_disabled", True)),
        "live_leverage_approved": False,
        "human_review_required": bool(payload.get("human_review_required", True)),
        "connection": payload.get("connection") if isinstance(payload.get("connection"), dict) else {},
        "env_audit": payload.get("env_audit") if isinstance(payload.get("env_audit"), dict) else {},
        "assets": _safe_list(payload.get("assets")),
        "production_conclusion": payload.get("production_conclusion", "OBSERVATION_ONLY_NO_LEVERAGE_APPROVAL"),
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
        "production_conclusion": PRODUCTION_CONCLUSION,
        "components": {
            "shadow_sync": shadow_sync,
            "margin_snapshot": margin_snapshot,
        },
    }


def build_ibkr_readonly_preflight_payload(
    portfolio_id: str,
    shadow_dir: Path | None = None,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    shadow_dir = shadow_dir or DEFAULT_SHADOW_DIR
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    path = shadow_dir / "latest_ibkr_readonly_preflight.json"
    payload, reason = _load_json_file(path)
    preflight = _apply_stale_guard(
        normalize_ibkr_readonly_preflight(payload, path, reason),
        "ibkr_readonly_preflight",
        now=now,
        stale_after_hours=stale_after_hours,
    )
    level = preflight["level"]
    if preflight["status"] == "missing":
        status = "MISSING"
    elif level == "critical":
        status = "FAIL_CLOSED"
    elif preflight["requires_attention"] or level == "warning":
        status = "ATTENTION"
    else:
        status = "HEALTHY"

    return {
        "portfolio_id": portfolio_id,
        "generated_at": _utc_now_text(),
        "stage": "Stage 9.5 IBKR Read-Only Preflight",
        "status": status,
        "level": level,
        "requires_attention": preflight["requires_attention"],
        "warning_count": len(preflight.get("warnings") or []),
        "blocker_count": len(preflight.get("blockers") or []),
        "stale_after_hours": stale_after_hours,
        "stale_report": preflight["stale_report"],
        "live_leverage_approved": False,
        "human_review_required": True,
        "readonly": True,
        "trading_disabled": True,
        "production_conclusion": PRODUCTION_CONCLUSION,
        "preflight": preflight,
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
            "requires_attention": True,
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
        "production_conclusion": PRODUCTION_CONCLUSION,
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
        "production_conclusion": PRODUCTION_CONCLUSION,
        "summary": summary,
    }


def _check_result(name: str, passed: bool, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "message": message,
        "details": details or {},
    }


def _blocker(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def build_stage95_admission_payload(
    portfolio_id: str,
    shadow_dir: Path | None = None,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a read-only production-admission gate summary.

    The gate can say evidence is ready for human review. It never approves live
    leverage or broker execution.
    """
    shadow_audit = build_shadow_audit_payload(
        portfolio_id,
        shadow_dir=shadow_dir,
        stale_after_hours=stale_after_hours,
        now=now,
    )
    observation = build_observation_summary_payload(portfolio_id, shadow_dir=shadow_dir)
    summary = observation["summary"]

    expected_cycles = int(summary.get("expected_cycles") or 0)
    observed_cycles = int(summary.get("observed_cycles") or 0)
    coverage_ratio = float(summary.get("coverage_ratio") or 0.0)
    broker_unavailable_cycles = int(summary.get("broker_unavailable_cycles") or 0)
    critical_cycles = int(summary.get("critical_cycles") or 0)
    stale_gap_count = int(summary.get("stale_gap_count") or 0)
    margin_coverage = (
        summary.get("margin_field_coverage")
        if isinstance(summary.get("margin_field_coverage"), dict)
        else {}
    )
    all_required_margin_ratio = float(margin_coverage.get("all_required_ratio") or 0.0)

    checks = [
        _check_result(
            "latest_shadow_audit_healthy",
            shadow_audit["status"] == "HEALTHY" and not shadow_audit["requires_attention"],
            "latest shadow sync and margin snapshot are healthy",
            {
                "status": shadow_audit["status"],
                "level": shadow_audit["level"],
                "warning_count": shadow_audit["warning_count"],
            },
        ),
        _check_result(
            "no_stale_latest_reports",
            int(shadow_audit.get("stale_report_count") or 0) == 0 and not bool(summary.get("latest_is_stale")),
            "latest Stage 9.5 reports are fresh",
            {
                "shadow_stale_report_count": shadow_audit.get("stale_report_count"),
                "summary_latest_is_stale": summary.get("latest_is_stale"),
                "stale_after_hours": stale_after_hours,
            },
        ),
        _check_result(
            "observation_window_complete",
            expected_cycles > 0 and observed_cycles >= expected_cycles and coverage_ratio >= 1.0,
            "required Stage 9.5 observation window is complete",
            {
                "expected_cycles": expected_cycles,
                "observed_cycles": observed_cycles,
                "coverage_ratio": coverage_ratio,
            },
        ),
        _check_result(
            "observation_summary_clean",
            observation["status"] == "OBSERVED" and not observation["requires_attention"],
            "observation summary has no attention or critical state",
            {
                "status": observation["status"],
                "level": observation["level"],
                "critical_cycles": critical_cycles,
                "stale_gap_count": stale_gap_count,
            },
        ),
        _check_result(
            "broker_readonly_available",
            broker_unavailable_cycles == 0,
            "broker read-only snapshots were available throughout the observation window",
            {"broker_unavailable_cycles": broker_unavailable_cycles},
        ),
        _check_result(
            "margin_fields_complete",
            observed_cycles > 0 and all_required_margin_ratio >= 1.0,
            "required margin fields were present in every observed cycle",
            {
                "all_required_ratio": all_required_margin_ratio,
                "required_fields": margin_coverage.get("required_fields") or [],
            },
        ),
        _check_result(
            "live_leverage_remains_disabled",
            not bool(shadow_audit.get("live_leverage_approved")) and not bool(observation.get("live_leverage_approved")),
            "live leverage approval remains disabled",
            {
                "shadow_live_leverage_approved": shadow_audit.get("live_leverage_approved"),
                "summary_live_leverage_approved": observation.get("live_leverage_approved"),
            },
        ),
    ]

    blockers = [_blocker(check["name"], check["message"]) for check in checks if not check["passed"]]
    if blockers:
        admission_status = "NOT_APPROVED"
        level = max(
            [shadow_audit["level"], observation["level"], "warning"],
            key=lambda item: LEVEL_RANK.get(item, 0),
        )
        requires_attention = True
    else:
        admission_status = "READY_FOR_HUMAN_REVIEW"
        level = "healthy"
        requires_attention = False

    return {
        "portfolio_id": portfolio_id,
        "generated_at": _utc_now_text(),
        "stage": "Stage 9.5 Production Admission Gate",
        "status": admission_status,
        "level": level,
        "requires_attention": requires_attention,
        "blockers": blockers,
        "checks": checks,
        "live_leverage_approved": False,
        "human_review_required": True,
        "production_conclusion": PRODUCTION_CONCLUSION,
        "readonly": True,
        "shadow_audit": shadow_audit,
        "observation_summary": observation,
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


@router.get("/stage95-admission/{portfolio_id}")
async def get_stage95_admission(
    portfolio_id: str,
    user: dict = Depends(get_current_user),
):
    """Return the read-only Stage 9.5 production admission gate state."""
    _ = user
    return build_stage95_admission_payload(portfolio_id)


@router.get("/ibkr-readonly-preflight/{portfolio_id}")
async def get_ibkr_readonly_preflight(
    portfolio_id: str,
    user: dict = Depends(get_current_user),
):
    """Return the read-only IBKR Stage 9.5 preflight report."""
    _ = user
    return build_ibkr_readonly_preflight_payload(portfolio_id)
