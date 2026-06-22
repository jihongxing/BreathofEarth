"""Stage 9.5 read-only shadow audit cycle runner.

This runner stitches together the two observation-only Stage 9.5 probes:
shadow target sync and broker margin monitoring. It persists a cycle-level
report so operators can tell whether the full daily observation completed.

It never submits, cancels, or amends broker orders.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from live import margin_monitor, shadow_sync


DEFAULT_OUTPUT_DIR = Path("data/shadow")

LEVEL_RANK = {
    "healthy": 1,
    "missing": 2,
    "warning": 3,
    "critical": 4,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _level_from_status(status_value: Any, requires_attention: bool) -> str:
    status = str(status_value or "").upper()
    if status in {"FAIL_CLOSED", "FAILED", "BROKEN", "CRITICAL"}:
        return "critical"
    if requires_attention or status in {"WARNING", "PARTIAL", "UNAVAILABLE", "DRIFT"}:
        return "warning"
    if status in {"OK", "OBSERVED", "MATCHED", "HEALTHY"}:
        return "healthy"
    return "missing"


def _component_from_report(name: str, report: dict[str, Any]) -> dict[str, Any]:
    warnings = report.get("warnings") if isinstance(report.get("warnings"), list) else []
    requires_attention = bool(report.get("requires_attention") or warnings)
    level = _level_from_status(report.get("status"), requires_attention)
    return {
        "name": name,
        "status": str(report.get("status") or "UNKNOWN"),
        "level": level,
        "requires_attention": requires_attention,
        "warning_count": len(warnings),
        "warnings": warnings,
        "last_run_at": report.get("timestamp"),
        "output_path": report.get("output_path"),
        "dry_run": bool(report.get("dry_run", True)),
        "trading_disabled": bool(report.get("trading_disabled", True)),
        "report": report,
    }


def _component_from_exception(name: str, exc: Exception, timestamp: str) -> dict[str, Any]:
    warning = f"{name} failed during Stage 9.5 read-only cycle: {exc}"
    return {
        "name": name,
        "status": "FAILED",
        "level": "critical",
        "requires_attention": True,
        "warning_count": 1,
        "warnings": [warning],
        "last_run_at": timestamp,
        "output_path": None,
        "dry_run": True,
        "trading_disabled": True,
        "report": {
            "timestamp": timestamp,
            "status": "FAILED",
            "requires_attention": True,
            "warnings": [warning],
            "dry_run": True,
            "trading_disabled": True,
        },
    }


def _persist_json_report(report: dict[str, Any], output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{stamp}.json"
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / f"latest_{prefix}.json").write_text(text + "\n", encoding="utf-8")
    return path


def build_cycle_report(
    *,
    timestamp: str,
    portfolio_id: str,
    components: dict[str, dict[str, Any]],
    aum: float,
    satellite: str,
    broker_name: str,
    broker_role: str,
    no_broker: bool,
) -> dict[str, Any]:
    component_list = list(components.values())
    overall_level = max(
        (component["level"] for component in component_list),
        key=lambda level: LEVEL_RANK.get(level, 0),
    )
    requires_attention = any(component["requires_attention"] for component in component_list)
    warning_count = sum(int(component["warning_count"]) for component in component_list)
    if overall_level == "critical":
        status = "CRITICAL"
    elif requires_attention or overall_level == "warning":
        status = "ATTENTION"
    elif all(component["level"] == "missing" for component in component_list):
        status = "MISSING"
    else:
        status = "HEALTHY"

    return {
        "timestamp": timestamp,
        "portfolio_id": portfolio_id,
        "stage": "Stage 9.5 Shadow Sync & Structural Audit",
        "status": status,
        "level": overall_level,
        "requires_attention": requires_attention,
        "warning_count": warning_count,
        "live_leverage_approved": False,
        "trading_disabled": True,
        "dry_run": True,
        "production_conclusion": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        "config": {
            "aum": float(aum),
            "satellite": satellite,
            "broker": broker_name,
            "broker_role": broker_role,
            "shadow_sync_no_broker": bool(no_broker),
        },
        "components": components,
    }


def run_stage95_shadow_cycle(
    *,
    aum: float = 2_000_000.0,
    satellite: str = "qqq_spy_gld",
    broker_name: str = "ibkr",
    broker_role: str = "primary",
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    data_dir: Path = shadow_sync.DEFAULT_DATA_DIR,
    no_broker: bool = False,
    current_json: Path | None = None,
    portfolio_id: str = "shadow-90-10",
    persist_db: bool = True,
) -> dict[str, Any]:
    timestamp = utc_now().isoformat().replace("+00:00", "Z")
    components: dict[str, dict[str, Any]] = {}

    try:
        shadow_report = shadow_sync.run_shadow_sync(
            aum=aum,
            satellite=satellite,
            broker_name=broker_name,
            broker_role=broker_role,
            output_dir=output_dir,
            data_dir=data_dir,
            no_broker=no_broker,
            current_json=current_json,
            portfolio_id=portfolio_id,
            persist_db=persist_db,
        )
        components["shadow_sync"] = _component_from_report("shadow_sync", shadow_report)
    except Exception as exc:
        components["shadow_sync"] = _component_from_exception("shadow_sync", exc, timestamp)

    try:
        margin_report = margin_monitor.run_margin_monitor(
            broker_name=broker_name,
            broker_role=broker_role,
            output_dir=output_dir,
            portfolio_id=portfolio_id,
            no_broker=no_broker,
            persist_db=persist_db,
        )
        components["margin_snapshot"] = _component_from_report("margin_snapshot", margin_report)
    except Exception as exc:
        components["margin_snapshot"] = _component_from_exception("margin_snapshot", exc, timestamp)

    report = build_cycle_report(
        timestamp=timestamp,
        portfolio_id=portfolio_id,
        components=components,
        aum=aum,
        satellite=satellite,
        broker_name=broker_name,
        broker_role=broker_role,
        no_broker=no_broker,
    )
    path = _persist_json_report(report, output_dir, "stage95_cycle")
    report["output_path"] = str(path)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / "latest_stage95_cycle.json").write_text(text + "\n", encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full Stage 9.5 read-only shadow audit cycle.")
    parser.add_argument("--aum", type=float, default=2_000_000.0)
    parser.add_argument("--satellite", choices=sorted(shadow_sync.SATELLITE_SCENARIOS), default="qqq_spy_gld")
    parser.add_argument("--broker", default="ibkr")
    parser.add_argument("--broker-role", default="primary")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--data-dir", type=Path, default=shadow_sync.DEFAULT_DATA_DIR)
    parser.add_argument("--current-json", type=Path)
    parser.add_argument("--portfolio-id", default="shadow-90-10")
    parser.add_argument("--no-broker", action="store_true")
    parser.add_argument("--skip-db", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_stage95_shadow_cycle(
        aum=args.aum,
        satellite=args.satellite,
        broker_name=args.broker,
        broker_role=args.broker_role,
        output_dir=args.output_dir,
        data_dir=args.data_dir,
        no_broker=args.no_broker,
        current_json=args.current_json,
        portfolio_id=args.portfolio_id,
        persist_db=not args.skip_db,
    )
    print(
        f"Stage 9.5 cycle {report['status']} | warnings={report['warning_count']} | "
        f"output={report['output_path']}"
    )
    for name, component in report["components"].items():
        print(f"- {name}: {component['status']} ({component['level']})")
        for warning in component["warnings"]:
            print(f"  - {warning}")


if __name__ == "__main__":
    main()
