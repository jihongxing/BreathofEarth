"""
仪表盘路由 — 聚合数据供前端渲染
"""

import json
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi import status

from db.database import Database
from engine.config import PORTFOLIOS
from engine.execution.factory import get_broker_topology
from api.deps import get_db, get_current_user

router = APIRouter(prefix="/api", tags=["仪表盘"])


BROKER_SYNC_LEVELS = {
    "MATCHED": "healthy",
    "DRIFT": "warning",
    "BROKEN": "critical",
    "MISSING": "missing",
}

CORE_RUN_LEVELS = {
    "SUCCESS": "healthy",
    "SKIPPED": "healthy",
    "MANUAL_INTERVENTION_REQUIRED": "warning",
    "FAILED_EXECUTION": "critical",
    "FAILED": "critical",
}

OBSERVATION_LEVEL_RANKS = {
    "healthy": 1,
    "missing": 2,
    "warning": 3,
    "critical": 4,
}

OBSERVATION_CHAIN_PRIORITY = {
    "shadow_run": 1,
    "broker_sync": 2,
    "core": 3,
}


def _safe_load_json(value, default):
    if value in (None, ""):
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _mask_account_id(account_id: Optional[str]) -> str:
    if not account_id:
        return ""
    account_id = str(account_id)
    if len(account_id) <= 4:
        return account_id
    return "*" * (len(account_id) - 4) + account_id[-4:]


def _classify_reconciliation_item(item: dict, report_status: str) -> str:
    if report_status == "BROKEN":
        return "high"

    threshold = abs(float(item.get("threshold") or 0.0))
    delta = abs(float(item.get("delta") or 0.0))
    if threshold <= 0:
        return "high"

    ratio = delta / threshold
    if ratio >= 5:
        return "high"
    if ratio >= 2:
        return "medium"
    return "low"


def _summarize_reconciliation_items(items: list[dict], report_status: str) -> dict:
    counts = {"high": 0, "medium": 0, "low": 0}
    top_severity = None
    rank_map = {"low": 1, "medium": 2, "high": 3}

    for item in items:
        severity = _classify_reconciliation_item(item, report_status)
        counts[severity] += 1
        if top_severity is None or rank_map[severity] > rank_map[top_severity]:
            top_severity = severity

    return {
        "counts": counts,
        "top_severity": top_severity,
    }


def _parse_history_date(timestamp: Optional[str]) -> Optional[date]:
    if not timestamp:
        return None
    text = str(timestamp).replace("Z", "+00:00")
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        try:
            return __import__("datetime").datetime.fromisoformat(text).date()
        except ValueError:
            return None


def _calculate_history_streaks(history: list[dict]) -> dict:
    anomaly_streak_runs = 0
    streak_dates: list[date] = []

    for item in history:
        if item.get("level") not in {"warning", "critical"}:
            break
        anomaly_streak_runs += 1
        item_date = _parse_history_date(item.get("timestamp"))
        if item_date and item_date not in streak_dates:
            streak_dates.append(item_date)

    return {
        "anomaly_streak_runs": anomaly_streak_runs,
        "drift_streak_days": len(streak_dates),
        "streak_dates": [item.isoformat() for item in streak_dates],
    }


def _calculate_attention_streaks(history: list[dict]) -> dict:
    attention_streak_runs = 0
    streak_dates: list[date] = []

    for item in history:
        if not item.get("requires_attention"):
            break
        attention_streak_runs += 1
        item_date = _parse_history_date(item.get("timestamp"))
        if item_date and item_date not in streak_dates:
            streak_dates.append(item_date)

    return {
        "attention_streak_runs": attention_streak_runs,
        "attention_streak_days": len(streak_dates),
    }


def _latest_timestamp(values: list[Optional[str]]) -> Optional[str]:
    timestamps = [value for value in values if value]
    if not timestamps:
        return None
    return max(timestamps)


def _normalize_broker_role(value: Optional[str], default: str = "primary") -> str:
    role = str(value or "").strip().lower()
    if role in {"primary", "backup"}:
        return role
    return default


def _get_broker_sync_policy_payload(portfolio_id: str) -> dict:
    policy = PORTFOLIOS[portfolio_id].get("broker_sync_policy", {})
    required_role = str(policy.get("required_role", "primary")).lower()
    return {
        "required_role": required_role,
        "require_snapshot_cover_market_date": bool(policy.get("require_snapshot_cover_market_date", True)),
        "require_reconciliation_cover_market_date": bool(policy.get("require_reconciliation_cover_market_date", True)),
        "max_snapshot_lag_days": int(policy.get("max_snapshot_lag_days", 0)),
        "max_reconciliation_lag_days": int(policy.get("max_reconciliation_lag_days", 0)),
    }


def _get_live_execution_policy_payload(portfolio_id: str) -> dict:
    policy = PORTFOLIOS[portfolio_id].get("live_execution_policy", {})
    return {
        "enabled": bool(policy.get("enabled", False)),
        "allowed_assets": list(policy.get("allowed_assets", [])),
        "allowed_order_sides": [str(side).upper() for side in policy.get("allowed_order_sides", ["BUY", "SELL"])],
        "max_single_order_notional": float(policy.get("max_single_order_notional", 0.0)),
        "max_daily_order_count": int(policy.get("max_daily_order_count", 0)),
        "max_daily_turnover_ratio": float(policy.get("max_daily_turnover_ratio", 0.0)),
    }


def _get_core_observation_payload(db: Database, portfolio_id: str) -> dict:
    latest = db.get_latest_daily_run(portfolio_id)
    current_policy = _get_broker_sync_policy_payload(portfolio_id)
    current_live_execution_policy = _get_live_execution_policy_payload(portfolio_id)
    if not latest:
        return {
            "status": "MISSING",
            "level": "missing",
            "requires_attention": False,
            "last_run_at": None,
            "run_date": None,
            "action": None,
            "state": None,
            "nav": None,
            "core_nav": None,
            "stability_balance": None,
            "manual_intervention_required": False,
            "manual_intervention_reasons": [],
            "broker_sync_gate": None,
            "execution_policy_gate": None,
            "post_execution_reconciliation": None,
            "broker_sync_policy": current_policy,
            "live_execution_policy": current_live_execution_policy,
            "history": [],
            "execution_status": None,
            "execution_order_count": 0,
            "execution_message": None,
            "raw_report": None,
        }

    raw_report = latest.get("report")
    report = _safe_load_json(raw_report, {})
    if not isinstance(report, dict):
        report = {}

    manual_reasons = report.get("manual_intervention_reasons", [])
    if not isinstance(manual_reasons, list):
        manual_reasons = []

    execution = report.get("execution", {})
    if not isinstance(execution, dict):
        execution = {}

    broker_sync_gate = report.get("broker_sync_gate")
    if not isinstance(broker_sync_gate, dict):
        broker_sync_gate = None

    execution_policy_gate = report.get("execution_policy_gate")
    if not isinstance(execution_policy_gate, dict):
        execution_policy_gate = None

    post_execution_reconciliation = report.get("post_execution_reconciliation")
    if not isinstance(post_execution_reconciliation, dict):
        post_execution_reconciliation = None

    broker_sync_policy = report.get("broker_sync_policy")
    if not isinstance(broker_sync_policy, dict):
        broker_sync_policy = current_policy

    live_execution_policy = report.get("live_execution_policy")
    if not isinstance(live_execution_policy, dict):
        live_execution_policy = current_live_execution_policy

    execution_orders = execution.get("orders", [])
    if not isinstance(execution_orders, list):
        execution_orders = []

    run_status = report.get("run_status") or latest.get("status") or "SUCCESS"
    level = CORE_RUN_LEVELS.get(run_status, "missing")
    action = report.get("action")
    if not action and isinstance(raw_report, str) and raw_report and not report:
        action = raw_report

    return {
        "status": run_status,
        "level": level,
        "requires_attention": bool(
            report.get("manual_intervention_required")
            or run_status in {"MANUAL_INTERVENTION_REQUIRED", "FAILED_EXECUTION", "FAILED"}
        ),
        "last_run_at": latest.get("created_at"),
        "run_date": latest.get("date"),
        "action": action,
        "state": report.get("state"),
        "nav": report.get("nav"),
        "core_nav": report.get("core_nav"),
        "stability_balance": report.get("stability_balance"),
        "manual_intervention_required": bool(report.get("manual_intervention_required")),
        "manual_intervention_reasons": manual_reasons[:3],
        "broker_sync_gate": broker_sync_gate,
        "execution_policy_gate": execution_policy_gate,
        "post_execution_reconciliation": post_execution_reconciliation,
        "broker_sync_policy": broker_sync_policy,
        "live_execution_policy": live_execution_policy,
        "history": _serialize_core_observation_history(db, portfolio_id, limit=8),
        "execution_status": execution.get("status"),
        "execution_order_count": len(execution_orders),
        "execution_message": execution.get("message"),
        "raw_report": raw_report if isinstance(raw_report, str) else None,
    }


def _serialize_core_observation_history(db: Database, portfolio_id: str, limit: int = 6) -> list[dict]:
    rows = db.list_daily_runs(portfolio_id, limit=limit)
    current_policy = _get_broker_sync_policy_payload(portfolio_id)
    current_live_execution_policy = _get_live_execution_policy_payload(portfolio_id)
    history = []

    for row in rows:
        report = _safe_load_json(row.get("report"), {})
        if not isinstance(report, dict):
            report = {}

        run_status = report.get("run_status") or row.get("status") or "SUCCESS"
        broker_sync_gate = report.get("broker_sync_gate")
        if not isinstance(broker_sync_gate, dict):
            broker_sync_gate = None

        execution_policy_gate = report.get("execution_policy_gate")
        if not isinstance(execution_policy_gate, dict):
            execution_policy_gate = None

        post_execution_reconciliation = report.get("post_execution_reconciliation")
        if not isinstance(post_execution_reconciliation, dict):
            post_execution_reconciliation = None

        broker_sync_policy = report.get("broker_sync_policy")
        if not isinstance(broker_sync_policy, dict):
            broker_sync_policy = current_policy

        live_execution_policy = report.get("live_execution_policy")
        if not isinstance(live_execution_policy, dict):
            live_execution_policy = current_live_execution_policy

        history.append(
            {
                "date": row.get("date"),
                "timestamp": row.get("created_at"),
                "status": run_status,
                "level": CORE_RUN_LEVELS.get(run_status, "missing"),
                "action": report.get("action"),
                "state": report.get("state"),
                "nav": report.get("nav"),
                "broker_sync_gate": broker_sync_gate,
                "execution_policy_gate": execution_policy_gate,
                "post_execution_reconciliation": post_execution_reconciliation,
                "broker_sync_policy": broker_sync_policy,
                "live_execution_policy": live_execution_policy,
            }
        )

    return history


def _build_observation_item(chain_id: str, payload: dict, timestamp: Optional[str], status: str) -> dict:
    level = payload.get("level", "missing")
    level_rank = OBSERVATION_LEVEL_RANKS.get(level, 0)
    chain_rank = OBSERVATION_CHAIN_PRIORITY.get(chain_id, 0)
    return {
        "id": chain_id,
        "level": level,
        "status": status,
        "requires_attention": bool(payload.get("requires_attention")),
        "timestamp": timestamp,
        "level_rank": level_rank,
        "priority_rank": level_rank * 10 + chain_rank,
        "payload": payload,
    }


def _get_observation_overview_payload(core_payload: dict, broker_payload: dict, shadow_payload: dict) -> dict:
    broker_summary = broker_payload.get("summary", {})
    broker_observation_payload = dict(broker_payload)
    broker_observation_payload["level"] = broker_summary.get("overall_level", "missing")
    broker_observation_payload["requires_attention"] = broker_summary.get("requires_attention", False)

    items = [
        _build_observation_item(
            "core",
            core_payload,
            core_payload.get("last_run_at") or core_payload.get("run_date"),
            core_payload.get("status", "MISSING"),
        ),
        _build_observation_item(
            "broker_sync",
            broker_observation_payload,
            broker_summary.get("last_sync_at"),
            broker_summary.get("overall_level", "missing").upper(),
        ),
        _build_observation_item(
            "shadow_run",
            shadow_payload,
            shadow_payload.get("last_run_at"),
            shadow_payload.get("status", "missing"),
        ),
    ]
    items.sort(key=lambda item: (item["priority_rank"], item["timestamp"] or ""), reverse=True)

    overall_rank = max((item["level_rank"] for item in items), default=0)
    overall_level = next(
        (level_name for level_name, rank in OBSERVATION_LEVEL_RANKS.items() if rank == overall_rank),
        "missing",
    )
    focus_item = items[0] if items else None

    return {
        "overall_level": overall_level,
        "focus_chain": focus_item["id"] if focus_item else None,
        "focus_item": focus_item,
        "items": items,
    }


def _serialize_broker_sync_role(db: Database, portfolio_id: str, role: str, configured_broker: str) -> dict:
    snapshot = db.get_latest_broker_account_snapshot(portfolio_id, role)
    reconciliation = db.get_latest_broker_reconciliation_run(portfolio_id, role)

    if not snapshot and not reconciliation:
        return {
            "role": role,
            "configured_broker": configured_broker,
            "broker_name": configured_broker,
            "status": "MISSING",
            "level": BROKER_SYNC_LEVELS["MISSING"],
            "difference_count": 0,
            "requires_attention": False,
            "last_sync_at": None,
            "account_id_masked": "",
            "currency": None,
            "cash": None,
            "total_value": None,
            "anomalies": [],
        }

    items = _safe_load_json(reconciliation["items_json"], []) if reconciliation else []
    report = _safe_load_json(reconciliation["report_json"], {}) if reconciliation else {}
    status_value = reconciliation["status"] if reconciliation else "MISSING"
    level = BROKER_SYNC_LEVELS.get(status_value, "missing")
    broker_name = (
        (snapshot or {}).get("broker_name")
        or (reconciliation or {}).get("broker_name")
        or configured_broker
    )
    last_sync_at = None
    if snapshot:
        last_sync_at = snapshot.get("snapshot_time") or snapshot.get("created_at")
    elif reconciliation:
        last_sync_at = reconciliation.get("checked_at") or reconciliation.get("created_at")

    anomalies = []
    for item in items:
        anomalies.append(
            {
                "role": role,
                "broker_name": broker_name,
                "category": item.get("category"),
                "key": item.get("key"),
                "message": item.get("message") or "",
                "delta": item.get("delta"),
                "threshold": item.get("threshold"),
                "severity": _classify_reconciliation_item(item, status_value),
            }
        )

    history = _serialize_broker_sync_history(db, portfolio_id, role, configured_broker, limit=30)
    streaks = _calculate_history_streaks(history)

    return {
        "role": role,
        "configured_broker": configured_broker,
        "broker_name": broker_name,
        "status": status_value,
        "level": level,
        "difference_count": len(items),
        "requires_attention": bool(report.get("requires_manual_intervention", status_value != "MATCHED")),
        "last_sync_at": last_sync_at,
        "account_id_masked": _mask_account_id((snapshot or {}).get("account_id")),
        "currency": (snapshot or {}).get("currency"),
        "cash": (snapshot or {}).get("cash"),
        "total_value": (snapshot or {}).get("total_value"),
        "anomaly_streak_runs": streaks["anomaly_streak_runs"],
        "drift_streak_days": streaks["drift_streak_days"],
        "anomalies": anomalies,
    }


def _build_core_closure_lookup(db: Database, portfolio_id: str, limit: int = 12) -> dict[tuple[str, str], dict]:
    rows = db.list_daily_runs(portfolio_id, limit=limit)
    current_policy = _get_broker_sync_policy_payload(portfolio_id)
    lookup: dict[tuple[str, str], dict] = {}

    for row in rows:
        report = _safe_load_json(row.get("report"), {})
        if not isinstance(report, dict):
            report = {}

        execution = report.get("execution")
        if not isinstance(execution, dict):
            execution = {}

        execution_orders = execution.get("orders", [])
        if not isinstance(execution_orders, list):
            execution_orders = []

        broker_sync_gate = report.get("broker_sync_gate")
        if not isinstance(broker_sync_gate, dict):
            broker_sync_gate = None

        post_execution_reconciliation = report.get("post_execution_reconciliation")
        if not isinstance(post_execution_reconciliation, dict):
            post_execution_reconciliation = None

        broker_sync_policy = report.get("broker_sync_policy")
        if not isinstance(broker_sync_policy, dict):
            broker_sync_policy = current_policy

        run_date = row.get("date")
        if not run_date:
            parsed_date = _parse_history_date(row.get("created_at"))
            run_date = parsed_date.isoformat() if parsed_date else None
        if not run_date:
            continue

        role = _normalize_broker_role(
            (post_execution_reconciliation or {}).get("broker_role")
            or (broker_sync_gate or {}).get("broker_role")
            or broker_sync_policy.get("required_role"),
            default="primary",
        )
        lookup_key = (role, run_date)
        if lookup_key in lookup:
            continue

        lookup[lookup_key] = {
            "run_date": run_date,
            "timestamp": row.get("created_at"),
            "run_status": report.get("run_status") or row.get("status") or "SUCCESS",
            "action": report.get("action"),
            "execution_status": execution.get("status"),
            "execution_message": execution.get("message"),
            "execution_order_count": len(execution_orders),
            "broker_sync_gate": broker_sync_gate,
            "post_execution_reconciliation": post_execution_reconciliation,
        }

    return lookup


def _serialize_broker_sync_history(
    db: Database,
    portfolio_id: str,
    role: str,
    configured_broker: str,
    limit: int = 5,
    core_closure_lookup: Optional[dict[tuple[str, str], dict]] = None,
) -> list[dict]:
    snapshots = db.list_broker_account_snapshots(portfolio_id, role, limit=limit)
    reconciliations = db.list_broker_reconciliation_runs(portfolio_id, role, limit=limit)
    history = []

    pair_count = max(len(snapshots), len(reconciliations))
    for index in range(pair_count):
        snapshot = snapshots[index] if index < len(snapshots) else None
        reconciliation = reconciliations[index] if index < len(reconciliations) else None
        if not snapshot and not reconciliation:
            continue

        items = _safe_load_json(reconciliation["items_json"], []) if reconciliation else []
        status_value = reconciliation["status"] if reconciliation else "MISSING"
        severity_summary = _summarize_reconciliation_items(items, status_value)
        broker_name = (
            (snapshot or {}).get("broker_name")
            or (reconciliation or {}).get("broker_name")
            or configured_broker
        )
        timestamp = None
        if reconciliation:
            timestamp = reconciliation.get("checked_at") or reconciliation.get("created_at")
        elif snapshot:
            timestamp = snapshot.get("snapshot_time") or snapshot.get("created_at")
        market_date = _parse_history_date(timestamp)
        market_date_text = market_date.isoformat() if market_date else None
        core_closure = None
        if core_closure_lookup and market_date_text:
            core_closure = core_closure_lookup.get((role, market_date_text))

        history.append(
            {
                "role": role,
                "broker_name": broker_name,
                "configured_broker": configured_broker,
                "timestamp": timestamp,
                "market_date": market_date_text,
                "status": status_value,
                "level": BROKER_SYNC_LEVELS.get(status_value, "missing"),
                "difference_count": len(items),
                "high_count": severity_summary["counts"]["high"],
                "medium_count": severity_summary["counts"]["medium"],
                "low_count": severity_summary["counts"]["low"],
                "top_severity": severity_summary["top_severity"],
                "account_id_masked": _mask_account_id((snapshot or {}).get("account_id")),
                "total_value": (snapshot or {}).get("total_value"),
                "currency": (snapshot or {}).get("currency"),
                "requires_attention": bool(
                    (reconciliation and _safe_load_json(reconciliation["report_json"], {}).get("requires_manual_intervention"))
                    or status_value in ("DRIFT", "BROKEN")
                ),
                "core_closure": core_closure,
            }
        )

    streaks = _calculate_history_streaks(history)
    for index, item in enumerate(history):
        item["streak_index"] = index + 1 if item.get("level") in {"warning", "critical"} and index < streaks["anomaly_streak_runs"] else 0
        item["drift_streak_days"] = streaks["drift_streak_days"] if item["streak_index"] else 0
    return history[:limit]


def _get_shadow_run_payload(db: Database, portfolio_id: str) -> dict:
    latest = db.get_latest_shadow_run_report(portfolio_id)
    history_rows = db.list_shadow_run_reports(portfolio_id, limit=10)
    topology = get_broker_topology()

    history = []
    for row in history_rows:
        warnings = _safe_load_json(row.get("warnings_json"), [])
        report = _safe_load_json(row.get("report_json"), {})
        reconciliation_status = row.get("reconciliation_status") or report.get("reconciliation_status")
        reconciliation_level = BROKER_SYNC_LEVELS.get(reconciliation_status or "MISSING", "missing")
        level = "warning" if row.get("requires_attention") else "healthy"
        history.append(
            {
                "timestamp": row.get("checked_at") or row.get("created_at"),
                "broker_name": row.get("broker_name"),
                "dry_run": bool(row.get("dry_run")),
                "order_count": int(row.get("order_count") or 0),
                "requires_attention": bool(row.get("requires_attention")),
                "warning_count": len(warnings),
                "warnings": warnings[:2],
                "reconciliation_status": reconciliation_status,
                "reconciliation_level": reconciliation_level,
                "level": level,
            }
        )

    shadow_streaks = _calculate_attention_streaks(history)

    if not latest:
        return {
            "enabled": True,
            "configured_broker": topology.get("sandbox", "paper"),
            "status": "missing",
            "level": "missing",
            "last_run_at": None,
            "order_count": 0,
            "warning_count": 0,
            "requires_attention": False,
            "warnings": [],
            "reconciliation_status": None,
            "attention_streak_runs": 0,
            "attention_streak_days": 0,
            "history": [],
        }

    warnings = _safe_load_json(latest.get("warnings_json"), [])
    report = _safe_load_json(latest.get("report_json"), {})
    reconciliation_status = latest.get("reconciliation_status") or report.get("reconciliation_status")
    reconciliation_level = BROKER_SYNC_LEVELS.get(reconciliation_status or "MISSING", "missing")

    return {
        "enabled": True,
        "configured_broker": latest.get("broker_name") or topology.get("sandbox", "paper"),
        "status": "attention" if latest.get("requires_attention") else "healthy",
        "level": "warning" if latest.get("requires_attention") else ("healthy" if (latest.get("checked_at") or latest.get("created_at")) else "missing"),
        "last_run_at": latest.get("checked_at") or latest.get("created_at"),
        "order_count": int(latest.get("order_count") or 0),
        "warning_count": len(warnings),
        "requires_attention": bool(latest.get("requires_attention")),
        "warnings": warnings[:3],
        "reconciliation_status": reconciliation_status,
        "reconciliation_level": reconciliation_level,
        "broker_name": latest.get("broker_name"),
        "dry_run": bool(latest.get("dry_run")),
        "attention_streak_runs": shadow_streaks["attention_streak_runs"],
        "attention_streak_days": shadow_streaks["attention_streak_days"],
        "history": history[:6],
    }


def _get_broker_sync_payload(db: Database, portfolio_id: str) -> dict:
    topology = get_broker_topology()
    policy = _get_broker_sync_policy_payload(portfolio_id)
    core_closure_lookup = _build_core_closure_lookup(db, portfolio_id, limit=12)
    roles = [
        _serialize_broker_sync_role(db, portfolio_id, "primary", topology.get("primary", "ibkr")),
        _serialize_broker_sync_role(db, portfolio_id, "backup", topology.get("backup", "futu")),
    ]
    history_by_role = {
        "primary": _serialize_broker_sync_history(
            db,
            portfolio_id,
            "primary",
            topology.get("primary", "ibkr"),
            core_closure_lookup=core_closure_lookup,
        ),
        "backup": _serialize_broker_sync_history(
            db,
            portfolio_id,
            "backup",
            topology.get("backup", "futu"),
            core_closure_lookup=core_closure_lookup,
        ),
    }

    all_anomalies = []
    critical_count = 0
    warning_count = 0
    overall_rank = 0
    rank_map = {"missing": 0, "healthy": 1, "warning": 2, "critical": 3}

    for role in roles:
        overall_rank = max(overall_rank, rank_map.get(role["level"], 0))
        for item in role["anomalies"]:
            all_anomalies.append(item)
            if item["severity"] == "high":
                critical_count += 1
            elif item["severity"] == "medium":
                warning_count += 1

    overall_level = next((name for name, rank in rank_map.items() if rank == overall_rank), "missing")
    last_sync_at = _latest_timestamp([role.get("last_sync_at") for role in roles])

    return {
        "roles": roles,
        "summary": {
            "overall_level": overall_level,
            "critical_count": critical_count,
            "warning_count": warning_count,
            "has_data": any(role["status"] != "MISSING" for role in roles),
            "requires_attention": any(role["requires_attention"] for role in roles),
            "attention_role_count": sum(1 for role in roles if role["requires_attention"]),
            "last_sync_at": last_sync_at,
        },
        "policy": policy,
        "anomalies": all_anomalies[:8],
        "history_by_role": history_by_role,
    }


@router.get("/dashboard/{portfolio_id}")
async def get_dashboard(
    portfolio_id: str,
    days: int = Query(default=90, ge=7, le=365),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取仪表盘数据（NAV 曲线、权重、回撤、调仓）"""
    pf_config = PORTFOLIOS.get(portfolio_id)
    if not pf_config:
        return {"error": f"组合 {portfolio_id} 不存在"}

    # 快照（NAV 曲线 + 回撤 + 权重）
    snapshots = db.get_snapshots(portfolio_id, limit=days)
    snapshots.reverse()  # 按时间正序

    nav_series = []
    drawdown_series = []
    for s in snapshots:
        nav_series.append({"date": s["date"], "nav": s["nav"]})
        drawdown_series.append({"date": s["date"], "drawdown": s["drawdown"]})

    # 当前权重
    current_weights = {}
    if snapshots:
        latest = snapshots[-1]
        weights = json.loads(latest["weights"]) if isinstance(latest["weights"], str) else latest["weights"]
        assets = pf_config["assets"]
        current_weights = {
            assets[i]: {"weight": weights[i], "name": pf_config["asset_names"][assets[i]]}
            for i in range(min(len(assets), len(weights)))
        }

    # 交易记录
    with db._conn() as conn:
        tx_rows = conn.execute(
            """SELECT date, type, turnover, friction_cost, reason
               FROM transactions WHERE portfolio_id = ?
               ORDER BY date DESC LIMIT ?""",
            (portfolio_id, 20),
        ).fetchall()
    transactions = [dict(r) for r in tx_rows]

    # 风控事件
    with db._conn() as conn:
        risk_rows = conn.execute(
            """SELECT date, event_type, severity, drawdown, action_taken
               FROM risk_events WHERE portfolio_id = ?
               ORDER BY date DESC LIMIT 10""",
            (portfolio_id,),
        ).fetchall()
    risk_events = [dict(r) for r in risk_rows]

    # 汇总
    try:
        portfolio = db.get_portfolio(portfolio_id)
        current_nav = portfolio["nav"]
        state = portfolio["state"]
        stability_balance = float(portfolio.get("stability_balance", 0.0))
    except ValueError:
        current_nav = 0
        state = "UNINITIALIZED"
        stability_balance = 0.0

    core_observation = _get_core_observation_payload(db, portfolio_id)
    broker_sync = _get_broker_sync_payload(db, portfolio_id)
    shadow_run = _get_shadow_run_payload(db, portfolio_id)

    return {
        "portfolio_id": portfolio_id,
        "name": pf_config["name"],
        "currency": pf_config["currency"],
        "state": state,
        "current_nav": current_nav,
        "stability_balance": stability_balance,
        "core_balance": round(current_nav - stability_balance, 2),
        "nav_series": nav_series,
        "drawdown_series": drawdown_series,
        "current_weights": current_weights,
        "transactions": transactions,
        "risk_events": risk_events,
        "core_observation": core_observation,
        "broker_sync": broker_sync,
        "shadow_run": shadow_run,
        "observation_overview": _get_observation_overview_payload(
            core_observation,
            broker_sync,
            shadow_run,
        ),
    }


@router.get("/broker-sync/{portfolio_id}")
async def get_broker_sync(
    portfolio_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取最新券商同步与对账状态（只读）"""
    if portfolio_id not in PORTFOLIOS:
        raise HTTPException(status_code=404, detail=f"组合 {portfolio_id} 不存在")
    return _get_broker_sync_payload(db, portfolio_id)


@router.get("/snapshots/{portfolio_id}")
async def get_snapshots(
    portfolio_id: str,
    limit: int = Query(default=30, ge=1, le=365),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取历史快照"""
    snapshots = db.get_snapshots(portfolio_id, limit=limit)
    for s in snapshots:
        s["positions"] = json.loads(s["positions"]) if isinstance(s["positions"], str) else s["positions"]
        s["weights"] = json.loads(s["weights"]) if isinstance(s["weights"], str) else s["weights"]
    return snapshots


@router.get("/transactions/{portfolio_id}")
async def get_transactions(
    portfolio_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """获取交易记录"""
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE portfolio_id = ? ORDER BY date DESC LIMIT ?",
            (portfolio_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/report")
async def get_monthly_report(
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """获取家族月报（HTML 格式，向后兼容）"""
    from fastapi.responses import HTMLResponse
    from runner.dashboard import get_latest_report_html
    html = get_latest_report_html(lang=lang)
    return HTMLResponse(content=html)


@router.get("/reports")
async def list_reports(
    user: dict = Depends(get_current_user),
):
    """列出所有月报（按年月聚合，含组合信息）"""
    from runner.dashboard import list_reports as _list_reports
    return _list_reports()


@router.get("/reports/{year}/{month}/{portfolio_id}")
async def get_portfolio_report(
    year: int,
    month: int,
    portfolio_id: str,
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """获取指定月份、指定组合的月报 HTML"""
    from fastapi.responses import HTMLResponse
    from runner.dashboard import get_report_html
    html = get_report_html(year, month, portfolio_id, lang=lang)
    return HTMLResponse(content=html)


@router.post("/report/generate")
async def generate_report(
    days: int = Query(default=90, ge=7, le=365),
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """手动生成月报（所有组合）"""
    from runner.dashboard import generate_and_save
    push = False
    path = generate_and_save(days=days, push=push, lang=lang)
    msg = "报告已生成" if lang == "zh" else "Report generated"
    return {"message": msg, "path": str(path) if path else None}


@router.post("/reports/generate/{portfolio_id}")
async def generate_portfolio_report_api(
    portfolio_id: str,
    days: int = Query(default=90, ge=7, le=365),
    lang: str = Query(default="zh", pattern="^(zh|en)$"),
    user: dict = Depends(get_current_user),
):
    """为指定组合生成月报"""
    from runner.dashboard import generate_and_save
    from engine.config import PORTFOLIOS
    if portfolio_id not in PORTFOLIOS:
        raise HTTPException(status_code=404, detail=f"组合不存在: {portfolio_id}")
    path = generate_and_save(days=days, portfolio_id=portfolio_id, push=False, lang=lang)
    msg = f"{portfolio_id} 报告已生成" if lang == "zh" else f"{portfolio_id} report generated"
    return {"message": msg, "path": str(path) if path else None}
