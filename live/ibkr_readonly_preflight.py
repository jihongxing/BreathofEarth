"""IBKR read-only preflight for Stage 9.5.

This module checks whether the local environment is ready for an IBKR
read-only observation attempt. It does not connect to IBKR unless ``--connect``
is passed, and it never submits or cancels orders.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engine.execution.broker_adapter import BrokerMode
from engine.execution.factory import create_broker_adapter


DEFAULT_OUTPUT_DIR = Path("data/shadow")
PRODUCTION_ASSETS = ("SPY", "TLT", "GLD", "SHV", "QQQ")

REQUIRED_ENVS = (
    "IBKR_API_BASE_URL",
    "IBKR_ACCOUNT_ID",
)
RECOMMENDED_ENVS = (
    "IBKR_VERIFY_TLS",
    "IBKR_TIMEOUT_SEC",
    "IBKR_ORDER_TIF",
    "IBKR_LISTING_EXCHANGE",
    "IBKR_REPLY_CONFIRM_LIMIT",
)
DANGEROUS_GATES = (
    "XIRANG_ENABLE_LIVE_CORE_EXECUTION",
    "IBKR_ENABLE_ORDER_SUBMISSION",
)
READONLY_APPROVAL_ENVS = (
    "XIRANG_LIVE_CORE_APPROVAL_ID",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_present(name: str, environ: dict[str, str]) -> bool:
    return bool(str(environ.get(name, "")).strip())


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _mask_env_value(name: str, value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if name in {"IBKR_ACCOUNT_ID", "XIRANG_LIVE_CORE_APPROVAL_ID"}:
        text = str(value)
        if len(text) <= 4:
            return "***"
        return f"{text[:2]}***{text[-2:]}"
    return str(value)


def _conid_env(symbol: str) -> str:
    return f"IBKR_CONID_{symbol.upper()}"


def _write_json_report(report: dict[str, Any], path: Path) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")


def _persist_json_report(report: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).strftime(
        "%Y%m%d_%H%M%S"
    )
    path = output_dir / f"ibkr_readonly_preflight_{stamp}.json"
    _write_json_report(report, path)
    _write_json_report(report, output_dir / "latest_ibkr_readonly_preflight.json")
    return path


def _build_env_audit(environ: dict[str, str]) -> dict[str, Any]:
    required = {
        name: {
            "present": _env_present(name, environ),
            "value": _mask_env_value(name, environ.get(name)),
        }
        for name in REQUIRED_ENVS
    }
    recommended = {
        name: {
            "present": _env_present(name, environ),
            "value": _mask_env_value(name, environ.get(name)),
        }
        for name in RECOMMENDED_ENVS
    }
    conids = {
        symbol: {
            "env": _conid_env(symbol),
            "present": _env_present(_conid_env(symbol), environ),
            "value": _mask_env_value(_conid_env(symbol), environ.get(_conid_env(symbol))),
        }
        for symbol in PRODUCTION_ASSETS
    }
    gates = {
        name: {
            "present": _env_present(name, environ),
            "enabled": _truthy(environ.get(name)),
            "value": _mask_env_value(name, environ.get(name)),
        }
        for name in DANGEROUS_GATES
    }
    readonly_approval_context = {
        name: {
            "present": _env_present(name, environ),
            "value": _mask_env_value(name, environ.get(name)),
        }
        for name in READONLY_APPROVAL_ENVS
    }
    return {
        "required_envs": required,
        "recommended_envs": recommended,
        "conid_mappings": conids,
        "dangerous_gates": gates,
        "readonly_approval_context": readonly_approval_context,
    }


def _evaluate_static_audit(audit: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []

    for name, gate in audit["dangerous_gates"].items():
        if gate["enabled"]:
            blockers.append(f"{name} is enabled; read-only preflight must fail closed")

    for name, item in audit["readonly_approval_context"].items():
        if item["present"]:
            blockers.append(f"{name} is present; read-only preflight must not run in approval context")

    missing_required = [
        name for name, item in audit["required_envs"].items() if not item["present"]
    ]
    if missing_required:
        blockers.append(f"missing required IBKR envs: {', '.join(missing_required)}")

    missing_conids = [
        symbol
        for symbol, item in audit["conid_mappings"].items()
        if not item["present"]
    ]
    if missing_conids:
        blockers.append(f"missing explicit IBKR conid mappings: {', '.join(missing_conids)}")

    missing_recommended = [
        name for name, item in audit["recommended_envs"].items() if not item["present"]
    ]
    if missing_recommended:
        warnings.append(f"missing recommended IBKR envs: {', '.join(missing_recommended)}")

    if any("must fail closed" in blocker or "approval context" in blocker for blocker in blockers):
        return "FAIL_CLOSED", blockers, warnings
    if blockers:
        return "NOT_READY", blockers, warnings
    return "READY_FOR_READONLY_CONNECT", blockers, warnings


def _attempt_readonly_connect() -> tuple[bool, str | None, str | None]:
    adapter = create_broker_adapter(
        role="primary",
        broker_name="ibkr",
        mode=BrokerMode.READ_ONLY,
        assets=list(PRODUCTION_ASSETS),
    )
    connected = bool(adapter.connect())
    broker_name = getattr(adapter, "broker_name", "ibkr")
    if not connected:
        return False, broker_name, "IBKR read-only adapter connect() returned false"
    return True, broker_name, None


def run_ibkr_readonly_preflight(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    connect: bool = False,
    environ: dict[str, str] | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    env = dict(os.environ if environ is None else environ)
    timestamp = utc_now().isoformat().replace("+00:00", "Z")
    audit = _build_env_audit(env)
    status, blockers, warnings = _evaluate_static_audit(audit)

    connection: dict[str, Any] = {
        "requested": connect,
        "attempted": False,
        "broker": "ibkr",
        "mode": BrokerMode.READ_ONLY.value,
        "connected": False,
        "error": None,
    }

    if connect:
        if status == "READY_FOR_READONLY_CONNECT":
            connection["attempted"] = True
            try:
                connected, broker_name, error = _attempt_readonly_connect()
                connection["broker"] = broker_name or "ibkr"
                connection["connected"] = connected
                connection["error"] = error
                status = "READY" if connected else "ATTENTION"
                if error:
                    warnings.append(error)
            except Exception as exc:
                connection["error"] = str(exc)
                warnings.append(f"IBKR read-only connection failed: {exc}")
                status = "ATTENTION"
        else:
            warnings.append("read-only connection skipped because static preflight is not ready")

    report: dict[str, Any] = {
        "timestamp": timestamp,
        "portfolio_id": "shadow-90-10",
        "status": status,
        "readonly": True,
        "dry_run": True,
        "trading_disabled": True,
        "live_leverage_approved": False,
        "human_review_required": True,
        "production_conclusion": "OBSERVATION_ONLY_NO_LEVERAGE_APPROVAL",
        "assets": list(PRODUCTION_ASSETS),
        "env_audit": audit,
        "connection": connection,
        "blockers": blockers,
        "warnings": warnings,
    }
    report["requires_attention"] = status != "READY" or bool(blockers) or bool(warnings)
    if persist:
        path = _persist_json_report(report, output_dir)
        report["output_path"] = str(path)
        _write_json_report(report, path)
        _write_json_report(report, output_dir / "latest_ibkr_readonly_preflight.json")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run IBKR Stage 9.5 read-only preflight.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--connect",
        action="store_true",
        help="Attempt IBKR READ_ONLY adapter connect() after static checks pass.",
    )
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_ibkr_readonly_preflight(
        output_dir=args.output_dir,
        connect=args.connect,
        persist=not args.no_persist,
    )
    print(
        "IBKR read-only preflight "
        f"{report['status']} | trading_disabled={report['trading_disabled']} "
        f"| live_leverage_approved={report['live_leverage_approved']}"
    )
    if report.get("output_path"):
        print(f"Output: {report['output_path']}")
    if report["blockers"]:
        print("Blockers:")
        for blocker in report["blockers"]:
            print(f"- {blocker}")
    if report["warnings"]:
        print("Warnings:")
        for warning in report["warnings"]:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
