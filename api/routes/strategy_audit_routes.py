"""Read-only multi-strategy shadow audit API."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends

from api.deps import get_current_user
from api.routes.shadow_audit_routes import (
    DEFAULT_STALE_AFTER_HOURS,
    LEVEL_RANK,
    PRODUCTION_CONCLUSION,
    _parse_report_time,
    _safe_list,
    _shadow_level,
    _utc_now_text,
)


router = APIRouter(prefix="/api", tags=["strategy-audit"])

DEFAULT_SHADOW_DIR = Path(os.environ.get("XIRANG_SHADOW_AUDIT_DIR", "data/shadow"))


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


def _missing_multi_strategy_payload(path: Path, reason: str) -> dict[str, Any]:
    warning = (
        "multi_strategy_shadow report is missing"
        if reason == "missing"
        else f"multi_strategy_shadow report cannot be parsed: {reason}"
    )
    return {
        "status": "MISSING" if reason == "missing" else "UNAVAILABLE",
        "level": "missing" if reason == "missing" else "warning",
        "requires_attention": True,
        "warning_count": 1,
        "warnings": [warning],
        "source_path": str(path),
        "last_run_at": None,
        "stale_report": False,
        "age_hours": None,
        "live_leverage_approved": False,
        "trading_disabled": True,
        "readonly": True,
        "human_review_required": True,
        "strategies": {},
        "strategy_count": 0,
        "report": {},
    }


def _apply_stale_guard(
    payload: dict[str, Any],
    *,
    now: datetime,
    stale_after_hours: int,
) -> dict[str, Any]:
    if payload.get("status") in {"MISSING", "UNAVAILABLE"} and not payload.get("report"):
        return payload

    warnings = list(payload.get("warnings") or [])
    report_time = _parse_report_time(payload.get("last_run_at"))
    if report_time is None:
        warnings.append("multi_strategy_shadow report timestamp is missing or invalid")
        payload["stale_report"] = True
        payload["age_hours"] = None
    else:
        age_hours = max((now - report_time).total_seconds() / 3600.0, 0.0)
        payload["age_hours"] = round(age_hours, 2)
        payload["stale_report"] = age_hours > stale_after_hours
        if payload["stale_report"]:
            warnings.append(
                f"multi_strategy_shadow report is stale: age={age_hours:.1f}h, threshold={stale_after_hours}h"
            )

    if payload.get("stale_report"):
        payload["warnings"] = warnings
        payload["requires_attention"] = True
        if payload.get("level") == "healthy":
            payload["level"] = "warning"
        if payload.get("status") == "HEALTHY":
            payload["status"] = "ATTENTION"
    return payload


def normalize_multi_strategy_shadow(
    payload: dict[str, Any] | None,
    path: Path,
    reason: str | None,
) -> dict[str, Any]:
    if payload is None:
        return _missing_multi_strategy_payload(path, reason or "missing")

    warnings = _safe_list(payload.get("warnings"))
    raw_strategies = payload.get("strategies") if isinstance(payload.get("strategies"), dict) else {}
    strategy_payloads = {
        str(strategy_id): strategy
        for strategy_id, strategy in raw_strategies.items()
        if isinstance(strategy, dict)
    }
    strategy_attention = any(bool(strategy.get("requires_attention", True)) for strategy in strategy_payloads.values())
    requires_attention = bool(payload.get("requires_attention") or warnings or strategy_attention)
    status = str(payload.get("status") or "UNKNOWN")
    level = _shadow_level(status, requires_attention)

    return {
        "status": status,
        "level": level,
        "requires_attention": requires_attention,
        "warning_count": len(warnings),
        "warnings": warnings,
        "source_path": str(path),
        "last_run_at": payload.get("timestamp"),
        "stale_report": False,
        "age_hours": None,
        "live_leverage_approved": False,
        "trading_disabled": True,
        "readonly": True,
        "human_review_required": True,
        "strategies": strategy_payloads,
        "strategy_count": len(strategy_payloads),
        "config": payload.get("config") if isinstance(payload.get("config"), dict) else {},
        "report": payload,
    }


def build_multi_strategy_shadow_payload(
    portfolio_id: str,
    shadow_dir: Path | None = None,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    shadow_dir = shadow_dir or DEFAULT_SHADOW_DIR
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    path = shadow_dir / "latest_multi_strategy_shadow.json"
    report, reason = _load_json_file(path)
    normalized = normalize_multi_strategy_shadow(report, path, reason)
    normalized = _apply_stale_guard(normalized, now=now, stale_after_hours=stale_after_hours)
    normalized["warning_count"] = len(normalized["warnings"]) + sum(
        len(_safe_list(strategy.get("warnings"))) for strategy in normalized["strategies"].values()
    )

    level = normalized["level"]
    if normalized["status"] in {"MISSING", "UNAVAILABLE"}:
        status = normalized["status"]
    elif LEVEL_RANK.get(level, 0) >= LEVEL_RANK["warning"] or normalized["requires_attention"]:
        status = "ATTENTION"
    else:
        status = "HEALTHY"

    return {
        "portfolio_id": portfolio_id,
        "generated_at": _utc_now_text(),
        "stage": "Stage 9.5 Multi-Strategy Shadow Audit",
        "status": status,
        "level": level,
        "requires_attention": normalized["requires_attention"],
        "warning_count": normalized["warning_count"],
        "stale_after_hours": stale_after_hours,
        "stale_report": normalized["stale_report"],
        "live_leverage_approved": False,
        "human_review_required": True,
        "readonly": True,
        "trading_disabled": True,
        "production_conclusion": PRODUCTION_CONCLUSION,
        "multi_strategy_shadow": normalized,
    }


@router.get("/multi-strategy-shadow/{portfolio_id}")
async def get_multi_strategy_shadow(
    portfolio_id: str,
    user: dict = Depends(get_current_user),
):
    """Return the read-only multi-strategy shadow audit report."""
    _ = user
    return build_multi_strategy_shadow_payload(portfolio_id)
