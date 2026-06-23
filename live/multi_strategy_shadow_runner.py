"""Read-only multi-strategy shadow report generator.

This runner creates the Stage 9.5 multi-strategy observation skeleton from the
strategy registry. It does not read broker adapters, submit orders, or infer
live readiness from missing evidence.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engine.strategy_audit_pipeline import build_strategy_audit_result
from engine.strategy_registry import StrategyDefinition, get_strategy, list_strategies


DEFAULT_OUTPUT_DIR = Path("data/shadow")
PRODUCTION_CONCLUSION = "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def target_notionals(weights: dict[str, float], aum: float) -> dict[str, float]:
    if aum <= 0:
        raise ValueError("aum must be positive")
    return {symbol: round(float(weight) * float(aum), 2) for symbol, weight in sorted(weights.items())}


def load_current_positions(path: Path | None) -> dict[str, float] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    positions = payload.get("positions", payload) if isinstance(payload, dict) else {}
    if not isinstance(positions, dict):
        raise ValueError("current positions payload must be a mapping")
    result: dict[str, float] = {}
    for symbol, value in positions.items():
        if isinstance(value, dict):
            amount = value.get("market_value", value.get("marketValue", value.get("value", 0.0)))
        else:
            amount = value
        result[str(symbol).upper()] = float(amount or 0.0)
    return result


def build_shadow_turnover(
    target: dict[str, float],
    current_positions: dict[str, float] | None,
    aum: float,
) -> dict[str, Any]:
    if current_positions is None:
        return {
            "status": "UNAVAILABLE",
            "requires_attention": True,
            "turnover_ratio": None,
            "deltas": {},
            "message": "current positions unavailable; shadow turnover cannot be computed",
        }

    symbols = sorted(set(target) | set(current_positions))
    deltas = {
        symbol: round(float(target.get(symbol, 0.0)) - float(current_positions.get(symbol, 0.0)), 2)
        for symbol in symbols
    }
    turnover = sum(abs(delta) for delta in deltas.values()) / max(float(aum), 1.0)
    return {
        "status": "OBSERVED",
        "requires_attention": False,
        "turnover_ratio": round(turnover, 8),
        "deltas": deltas,
        "message": "shadow turnover is estimated from supplied current position notionals",
    }


def unavailable_slippage_audit() -> dict[str, Any]:
    return {
        "status": "UNAVAILABLE",
        "requires_attention": True,
        "backtest_panic_assumption_bps": 50.0,
        "observed_half_spread_bps": None,
        "message": "live L1/L2 order book depth is not connected in this multi-strategy skeleton",
    }


def unavailable_margin_snapshot() -> dict[str, Any]:
    return {
        "status": "UNAVAILABLE",
        "requires_attention": True,
        "margin_fields": {},
        "message": "broker margin snapshot is not connected in this multi-strategy skeleton",
    }


def build_strategy_shadow_entry(
    strategy: StrategyDefinition,
    *,
    aum: float,
    as_of: str,
    current_positions: dict[str, float] | None = None,
) -> dict[str, Any]:
    weights = dict(strategy.reference_weights)
    notionals = target_notionals(weights, aum)
    audit_result = build_strategy_audit_result(strategy.strategy_id, {}, as_of=as_of)
    return {
        "strategy_id": strategy.strategy_id,
        "display_name": strategy.display_name,
        "status": "ATTENTION",
        "level": "warning",
        "requires_attention": True,
        "readonly": True,
        "trading_disabled": True,
        "live_leverage_approved": False,
        "strategy_definition": strategy.to_dict(),
        "target_weights": weights,
        "target_notionals": notionals,
        "shadow_turnover": build_shadow_turnover(notionals, current_positions, aum),
        "slippage_audit": unavailable_slippage_audit(),
        "margin_snapshot": unavailable_margin_snapshot(),
        "admission_status": audit_result.admission_status,
        "audit_result": audit_result.to_dict(),
        "warnings": [
            "multi-strategy shadow runner is report-only",
            "layered audit evidence is unavailable in this skeleton run",
        ],
    }


def _persist_json_report(report: dict[str, Any], output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{stamp}.json"
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / f"latest_{prefix}.json").write_text(text + "\n", encoding="utf-8")
    return path


def run_multi_strategy_shadow(
    *,
    aum: float = 2_000_000.0,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    portfolio_id: str = "multi-strategy-shadow",
    strategy_ids: tuple[str, ...] | None = None,
    current_json: Path | None = None,
) -> dict[str, Any]:
    timestamp = utc_now().isoformat().replace("+00:00", "Z")
    as_of = timestamp[:10]
    current_positions = load_current_positions(current_json)
    strategies = (
        tuple(get_strategy(strategy_id) for strategy_id in strategy_ids)
        if strategy_ids
        else list_strategies()
    )
    if not strategies:
        raise ValueError("at least one strategy is required")

    strategy_entries = {
        strategy.strategy_id: build_strategy_shadow_entry(
            strategy,
            aum=aum,
            as_of=as_of,
            current_positions=current_positions,
        )
        for strategy in strategies
    }
    warning_count = sum(len(entry["warnings"]) for entry in strategy_entries.values())
    report: dict[str, Any] = {
        "timestamp": timestamp,
        "portfolio_id": portfolio_id,
        "stage": "Stage 9.5 Multi-Strategy Shadow Audit",
        "status": "ATTENTION",
        "level": "warning",
        "requires_attention": True,
        "warning_count": warning_count,
        "dry_run": True,
        "readonly": True,
        "trading_disabled": True,
        "live_leverage_approved": False,
        "human_review_required": True,
        "production_conclusion": PRODUCTION_CONCLUSION,
        "config": {
            "aum": float(aum),
            "strategy_count": len(strategy_entries),
            "strategy_ids": list(strategy_entries),
            "current_positions_source": str(current_json) if current_json else None,
        },
        "strategies": strategy_entries,
    }
    path = _persist_json_report(report, output_dir, "multi_strategy_shadow")
    report["output_path"] = str(path)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / "latest_multi_strategy_shadow.json").write_text(text + "\n", encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a read-only multi-strategy shadow report.")
    parser.add_argument("--aum", type=float, default=2_000_000.0)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--portfolio-id", default="multi-strategy-shadow")
    parser.add_argument("--current-json", type=Path)
    parser.add_argument("--strategy", action="append", dest="strategies", help="Strategy id to include; repeatable")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_multi_strategy_shadow(
        aum=args.aum,
        output_dir=args.output_dir,
        portfolio_id=args.portfolio_id,
        strategy_ids=tuple(args.strategies) if args.strategies else None,
        current_json=args.current_json,
    )
    print(
        f"Multi-strategy shadow {report['status']} | strategies={report['config']['strategy_count']} | "
        f"output={report['output_path']}"
    )


if __name__ == "__main__":
    main()
