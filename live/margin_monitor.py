"""Read-only broker margin snapshot monitor for Stage 9.5.

The monitor treats broker margin output as an observation stream, not a safety
approval. Missing margin fields are reported as UNAVAILABLE and require manual
attention.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db.database import Database
from engine.execution.broker_adapter import AccountSnapshot, BrokerMode
from engine.execution.factory import create_broker_adapter


DEFAULT_OUTPUT_DIR = Path("data/shadow")

MARGIN_FIELD_ALIASES = {
    "FullMaintainMarginReq": [
        "FullMaintainMarginReq",
        "FullMaintMarginReq",
        "FullMaintenanceMarginReq",
        "fullmaintmarginreq",
    ],
    "MaintMarginReq": [
        "MaintMarginReq",
        "MaintenanceMarginReq",
        "maintmarginreq",
    ],
    "FullInitMarginReq": [
        "FullInitMarginReq",
        "FullInitialMarginReq",
        "fullinitmarginreq",
    ],
    "InitMarginReq": [
        "InitMarginReq",
        "InitialMarginReq",
        "initmarginreq",
    ],
    "ExcessLiquidity": [
        "ExcessLiquidity",
        "excessliquidity",
    ],
    "NetLiquidation": [
        "NetLiquidation",
        "NetLiquidationValue",
        "netliquidation",
        "netliquidationvalue",
    ],
    "AvailableFunds": [
        "AvailableFunds",
        "availablefunds",
    ],
    "BuyingPower": [
        "BuyingPower",
        "buyingpower",
    ],
    "Leverage": [
        "Leverage",
        "GrossPositionValue/NetLiquidation",
        "leverage",
    ],
    "SMA": [
        "SMA",
        "SpecialMemorandumAccount",
        "sma",
    ],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


NORMALIZED_ALIAS_TO_CANONICAL = {
    _normalized_key(alias): canonical
    for canonical, aliases in MARGIN_FIELD_ALIASES.items()
    for alias in aliases
}


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("$", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def extract_margin_fields(raw: Any) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}

    def visit(node: Any, path: str) -> None:
        if isinstance(node, dict):
            key_like_value = node.get("key") or node.get("tag") or node.get("name")
            if key_like_value is not None:
                canonical = NORMALIZED_ALIAS_TO_CANONICAL.get(_normalized_key(str(key_like_value)))
                if canonical and canonical not in found:
                    value = (
                        node.get("value")
                        if "value" in node
                        else node.get("amount", node.get("val"))
                    )
                    numeric = coerce_float(value)
                    found[canonical] = {
                        "value": numeric if numeric is not None else value,
                        "raw_value": value,
                        "path": path,
                    }

            for key, value in node.items():
                canonical = NORMALIZED_ALIAS_TO_CANONICAL.get(_normalized_key(str(key)))
                if canonical and canonical not in found:
                    numeric = coerce_float(value)
                    found[canonical] = {
                        "value": numeric if numeric is not None else value,
                        "raw_value": value,
                        "path": f"{path}.{key}" if path else str(key),
                    }
                visit(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, list):
            for index, item in enumerate(node):
                visit(item, f"{path}[{index}]")

    visit(raw, "")
    return found


def margin_status(fields: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    warnings: list[str] = []
    if not fields:
        return "UNAVAILABLE", ["broker snapshot did not expose recognized margin fields"]
    missing_core = [field for field in ("ExcessLiquidity", "NetLiquidation") if field not in fields]
    if missing_core:
        warnings.append(f"missing core margin fields: {', '.join(missing_core)}")
        return "PARTIAL", warnings
    return "OBSERVED", warnings


def _serialize_positions(snapshot: AccountSnapshot) -> dict[str, dict[str, float | str | None]]:
    return {
        symbol: {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "market_value": position.market_value,
            "avg_cost": position.avg_cost,
        }
        for symbol, position in snapshot.positions.items()
    }


def _persist_json_report(report: dict[str, Any], output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{stamp}.json"
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / f"latest_{prefix}.json").write_text(text + "\n", encoding="utf-8")
    return path


def _save_broker_snapshot_to_db(
    report: dict[str, Any],
    snapshot: AccountSnapshot,
    db: Database | None = None,
) -> None:
    db = db or Database()
    db.save_broker_account_snapshot(
        portfolio_id=str(report["portfolio_id"]),
        broker_role=str(report["broker"]["role"]),
        broker_name=snapshot.broker_name,
        broker_mode=snapshot.mode.value,
        account_id=snapshot.account_id or "",
        currency=snapshot.currency,
        cash=float(snapshot.cash),
        total_value=float(snapshot.total_value),
        positions_json=json.dumps(_serialize_positions(snapshot), ensure_ascii=False),
        raw_json=json.dumps(snapshot.raw, ensure_ascii=False),
        snapshot_time=snapshot.as_of.isoformat() if snapshot.as_of else str(report["timestamp"]),
    )


def run_margin_monitor(
    *,
    broker_name: str = "ibkr",
    broker_role: str = "primary",
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    portfolio_id: str = "shadow-90-10",
    persist_db: bool = True,
    no_broker: bool = False,
) -> dict[str, Any]:
    timestamp = utc_now().isoformat().replace("+00:00", "Z")
    warnings: list[str] = []
    snapshot = None
    connected = False
    adapter_name = "offline" if no_broker else broker_name

    if no_broker:
        warnings.append("broker margin snapshot skipped: no_broker observation mode")
    else:
        try:
            adapter = create_broker_adapter(
                role=broker_role,
                broker_name=broker_name,
                mode=BrokerMode.READ_ONLY,
                assets=["SPY", "TLT", "GLD", "SHV", "QQQ"],
            )
            adapter_name = adapter.broker_name
            connected = bool(adapter.connect())
            if not connected:
                warnings.append("broker read-only connection unavailable")
            else:
                snapshot = adapter.get_account_snapshot()
        except Exception as exc:
            warnings.append(f"broker margin snapshot unavailable: {exc}")

    fields = extract_margin_fields(snapshot.raw) if snapshot is not None else {}
    status, field_warnings = margin_status(fields)
    warnings.extend(field_warnings)

    report: dict[str, Any] = {
        "timestamp": timestamp,
        "portfolio_id": portfolio_id,
        "dry_run": True,
        "trading_disabled": True,
        "status": status,
        "requires_attention": status != "OBSERVED" or bool(warnings),
        "broker": {
            "name": adapter_name,
            "role": broker_role,
            "mode": BrokerMode.READ_ONLY.value,
            "connected": connected,
            "account_id": snapshot.account_id if snapshot else None,
        },
        "account": {
            "currency": snapshot.currency if snapshot else None,
            "cash": snapshot.cash if snapshot else None,
            "total_value": snapshot.total_value if snapshot else None,
            "position_count": len(snapshot.positions) if snapshot else 0,
        },
        "margin_fields": fields,
        "warnings": warnings,
        "production_conclusion": "OBSERVATION_ONLY_NO_LEVERAGE_APPROVAL",
    }

    path = _persist_json_report(report, output_dir, "margin_snapshot")
    report["output_path"] = str(path)
    if persist_db and snapshot is not None:
        try:
            _save_broker_snapshot_to_db(report, snapshot)
        except Exception as exc:
            report["warnings"].append(f"database persistence skipped: {exc}")
            report["requires_attention"] = True
            report["status"] = "PARTIAL" if report["status"] == "OBSERVED" else report["status"]
            _persist_json_report(report, output_dir, "margin_snapshot")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Stage 9.5 read-only margin monitor.")
    parser.add_argument("--broker", default="ibkr")
    parser.add_argument("--broker-role", default="primary")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--portfolio-id", default="shadow-90-10")
    parser.add_argument("--no-broker", action="store_true")
    parser.add_argument("--skip-db", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_margin_monitor(
        broker_name=args.broker,
        broker_role=args.broker_role,
        output_dir=args.output_dir,
        portfolio_id=args.portfolio_id,
        no_broker=args.no_broker,
        persist_db=not args.skip_db,
    )
    print(f"Margin monitor {report['status']} | output={report['output_path']}")
    if report["warnings"]:
        print("Warnings:")
        for warning in report["warnings"]:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
