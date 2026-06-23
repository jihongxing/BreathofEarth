"""Summarize Stage 9.5 read-only shadow audit cycles.

The summary is an observation report over existing JSON artifacts. It does not
connect to brokers, submit orders, or mutate portfolio state.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any


DEFAULT_SHADOW_DIR = Path("data/shadow")
DEFAULT_LOOKBACK_CYCLES = 60
DEFAULT_STALE_AFTER_HOURS = 24
REQUIRED_MARGIN_FIELDS = ("NetLiquidation", "ExcessLiquidity", "FullMaintainMarginReq")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_report_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_cycle_reports(shadow_dir: Path, limit: int = DEFAULT_LOOKBACK_CYCLES) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for path in sorted(shadow_dir.glob("stage95_cycle_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        timestamp = parse_report_time(payload.get("timestamp"))
        if timestamp is None:
            continue
        payload["_source_path"] = str(path)
        payload["_parsed_timestamp"] = timestamp
        reports.append(payload)
    reports.sort(key=lambda item: item["_parsed_timestamp"])
    if limit > 0:
        reports = reports[-limit:]
    return reports


def _component(report: dict[str, Any], name: str) -> dict[str, Any]:
    components = report.get("components")
    if isinstance(components, dict) and isinstance(components.get(name), dict):
        return components[name]
    return {}


def _nested_report(component: dict[str, Any]) -> dict[str, Any]:
    nested = component.get("report")
    return nested if isinstance(nested, dict) else {}


def _slippage_bps(report: dict[str, Any]) -> float | None:
    shadow_component = _component(report, "shadow_sync")
    shadow_report = _nested_report(shadow_component)
    slippage = shadow_report.get("slippage_audit")
    if not isinstance(slippage, dict):
        return None
    value = slippage.get("max_observed_half_spread_bps")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _margin_fields(report: dict[str, Any]) -> dict[str, Any]:
    margin_component = _component(report, "margin_snapshot")
    margin_report = _nested_report(margin_component)
    fields = margin_report.get("margin_fields")
    return fields if isinstance(fields, dict) else {}


def _broker_unavailable(report: dict[str, Any]) -> bool:
    margin_component = _component(report, "margin_snapshot")
    status = str(margin_component.get("status") or "").upper()
    if status in {"UNAVAILABLE", "FAILED"}:
        return True
    warnings = " ".join(str(item) for item in margin_component.get("warnings") or [])
    return "connection unavailable" in warnings.lower() or "broker margin snapshot unavailable" in warnings.lower()


def _cycle_is_abnormal(report: dict[str, Any]) -> bool:
    status = str(report.get("status") or "").upper()
    return status not in {"HEALTHY", "OK", "OBSERVED"}


def _max_streak(values: list[bool]) -> int:
    max_seen = 0
    current = 0
    for value in values:
        if value:
            current += 1
            max_seen = max(max_seen, current)
        else:
            current = 0
    return max_seen


def _current_streak(values: list[bool]) -> int:
    current = 0
    for value in reversed(values):
        if not value:
            break
        current += 1
    return current


def summarize_cycle_reports(
    reports: list[dict[str, Any]],
    *,
    expected_cycles: int = DEFAULT_LOOKBACK_CYCLES,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or utc_now()).astimezone(timezone.utc)
    timestamps = [report["_parsed_timestamp"] for report in reports]
    statuses = [str(report.get("status") or "UNKNOWN").upper() for report in reports]
    abnormal_flags = [_cycle_is_abnormal(report) for report in reports]
    broker_unavailable_flags = [_broker_unavailable(report) for report in reports]
    slippage_values = [value for value in (_slippage_bps(report) for report in reports) if value is not None]

    field_hits = Counter()
    all_required_margin_fields = 0
    for report in reports:
        fields = _margin_fields(report)
        if all(field in fields for field in REQUIRED_MARGIN_FIELDS):
            all_required_margin_fields += 1
        for field in REQUIRED_MARGIN_FIELDS:
            if field in fields:
                field_hits[field] += 1

    stale_gap_count = 0
    max_gap_hours = 0.0
    for previous, current in zip(timestamps, timestamps[1:]):
        gap_hours = max((current - previous).total_seconds() / 3600.0, 0.0)
        max_gap_hours = max(max_gap_hours, gap_hours)
        if gap_hours > stale_after_hours:
            stale_gap_count += 1

    latest_age_hours = None
    latest_is_stale = False
    if timestamps:
        latest_age_hours = max((now - timestamps[-1]).total_seconds() / 3600.0, 0.0)
        latest_is_stale = latest_age_hours > stale_after_hours

    status_counts = Counter(statuses)
    total_cycles = len(reports)
    warning_count_total = sum(int(report.get("warning_count") or 0) for report in reports)
    component_warning_count_total = 0
    for report in reports:
        components = report.get("components") if isinstance(report.get("components"), dict) else {}
        for component in components.values():
            if isinstance(component, dict):
                component_warning_count_total += int(component.get("warning_count") or 0)

    if total_cycles == 0:
        summary_status = "MISSING"
    elif latest_is_stale:
        summary_status = "STALE"
    elif any(status == "CRITICAL" for status in statuses):
        summary_status = "CRITICAL"
    elif any(abnormal_flags) or any(broker_unavailable_flags):
        summary_status = "ATTENTION"
    elif total_cycles < expected_cycles:
        summary_status = "COLLECTING"
    else:
        summary_status = "OBSERVED"

    return {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "stage": "Stage 9.5 Observation Summary",
        "status": summary_status,
        "requires_attention": summary_status in {"STALE", "CRITICAL", "ATTENTION", "MISSING"},
        "live_leverage_approved": False,
        "production_conclusion": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        "expected_cycles": expected_cycles,
        "observed_cycles": total_cycles,
        "coverage_ratio": round(total_cycles / expected_cycles, 4) if expected_cycles > 0 else 0.0,
        "first_cycle_at": timestamps[0].isoformat().replace("+00:00", "Z") if timestamps else None,
        "last_cycle_at": timestamps[-1].isoformat().replace("+00:00", "Z") if timestamps else None,
        "latest_age_hours": round(latest_age_hours, 2) if latest_age_hours is not None else None,
        "latest_is_stale": latest_is_stale,
        "stale_after_hours": stale_after_hours,
        "stale_gap_count": stale_gap_count,
        "max_gap_hours": round(max_gap_hours, 2),
        "status_counts": dict(sorted(status_counts.items())),
        "attention_cycles": sum(abnormal_flags),
        "critical_cycles": status_counts.get("CRITICAL", 0),
        "broker_unavailable_cycles": sum(broker_unavailable_flags),
        "warning_count_total": warning_count_total,
        "component_warning_count_total": component_warning_count_total,
        "abnormal_streak": {
            "current": _current_streak(abnormal_flags),
            "max": _max_streak(abnormal_flags),
        },
        "broker_unavailable_streak": {
            "current": _current_streak(broker_unavailable_flags),
            "max": _max_streak(broker_unavailable_flags),
        },
        "slippage_bps": {
            "observations": len(slippage_values),
            "max": round(max(slippage_values), 4) if slippage_values else None,
            "avg": round(mean(slippage_values), 4) if slippage_values else None,
        },
        "margin_field_coverage": {
            "required_fields": list(REQUIRED_MARGIN_FIELDS),
            "all_required_cycles": all_required_margin_fields,
            "all_required_ratio": round(all_required_margin_fields / total_cycles, 4) if total_cycles else 0.0,
            "fields": {
                field: {
                    "observed_cycles": field_hits[field],
                    "coverage_ratio": round(field_hits[field] / total_cycles, 4) if total_cycles else 0.0,
                }
                for field in REQUIRED_MARGIN_FIELDS
            },
        },
        "recent_cycles": [
            {
                "timestamp": report.get("timestamp"),
                "status": str(report.get("status") or "UNKNOWN").upper(),
                "warning_count": int(report.get("warning_count") or 0),
                "broker_unavailable": _broker_unavailable(report),
                "max_observed_half_spread_bps": _slippage_bps(report),
                "source_path": report.get("_source_path"),
            }
            for report in reports[-10:]
        ],
    }


def _persist_json_report(report: dict[str, Any], output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["generated_at"].replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{stamp}.json"
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / f"latest_{prefix}.json").write_text(text + "\n", encoding="utf-8")
    return path


def run_stage95_observation_summary(
    *,
    shadow_dir: Path = DEFAULT_SHADOW_DIR,
    output_dir: Path | None = None,
    expected_cycles: int = DEFAULT_LOOKBACK_CYCLES,
    limit: int = DEFAULT_LOOKBACK_CYCLES,
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or shadow_dir
    reports = load_cycle_reports(shadow_dir, limit=limit)
    summary = summarize_cycle_reports(
        reports,
        expected_cycles=expected_cycles,
        stale_after_hours=stale_after_hours,
        now=now,
    )
    path = _persist_json_report(summary, output_dir, "stage95_observation_summary")
    summary["output_path"] = str(path)
    text = json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / "latest_stage95_observation_summary.json").write_text(text + "\n", encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize Stage 9.5 read-only observation cycles.")
    parser.add_argument("--shadow-dir", type=Path, default=DEFAULT_SHADOW_DIR)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--expected-cycles", type=int, default=DEFAULT_LOOKBACK_CYCLES)
    parser.add_argument("--limit", type=int, default=DEFAULT_LOOKBACK_CYCLES)
    parser.add_argument("--stale-after-hours", type=int, default=DEFAULT_STALE_AFTER_HOURS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = run_stage95_observation_summary(
        shadow_dir=args.shadow_dir,
        output_dir=args.output_dir,
        expected_cycles=args.expected_cycles,
        limit=args.limit,
        stale_after_hours=args.stale_after_hours,
    )
    print(
        f"Stage 9.5 summary {summary['status']} | "
        f"cycles={summary['observed_cycles']}/{summary['expected_cycles']} | "
        f"broker_unavailable={summary['broker_unavailable_cycles']} | "
        f"output={summary['output_path']}"
    )


if __name__ == "__main__":
    main()
