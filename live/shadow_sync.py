"""Stage 9.5 shadow sync for the 90/10 production candidate.

The script is deliberately read-only:
- builds target notionals for the audited 90% defensive + 10% beta topology
- observes quotes when a read-only broker is available
- falls back to local clean adjusted-close data when broker quotes are missing
- writes JSON reports for shadow reconciliation

It never calls broker trading methods.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from db.database import Database
from engine.execution.base import OrderSide, TradeOrder
from engine.execution.broker_adapter import AccountSnapshot, BrokerMode, QuoteSnapshot
from engine.execution.factory import create_broker_adapter
from engine.execution.paper import PaperExecutor


DEFENSIVE_WEIGHT = 0.90
SATELLITE_WEIGHT = 0.10
DEFENSIVE_WEIGHTS = {"SPY": 0.25, "TLT": 0.25, "GLD": 0.25, "SHV": 0.25}
SATELLITE_SCENARIOS = {
    "qqq_spy_gld": {"QQQ": 0.40, "SPY": 0.30, "GLD": 0.30},
    "qqq_vti_gld": {"QQQ": 0.40, "VTI": 0.30, "GLD": 0.30},
}
DEFAULT_OUTPUT_DIR = Path("data/shadow")
DEFAULT_DATA_DIR = Path("data")


@dataclass(frozen=True)
class QuoteObservation:
    symbol: str
    source: str
    bid: float | None
    ask: float | None
    last: float
    mid: float
    spread_bps: float | None
    estimated_one_way_slippage_bps: float | None
    as_of: str | None = None
    warning: str = ""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_candidate_weights(satellite: str = "qqq_spy_gld") -> dict[str, float]:
    if satellite not in SATELLITE_SCENARIOS:
        raise ValueError(f"unsupported satellite sleeve: {satellite}")

    weights: dict[str, float] = {}
    for symbol, weight in DEFENSIVE_WEIGHTS.items():
        weights[symbol] = weights.get(symbol, 0.0) + DEFENSIVE_WEIGHT * weight
    for symbol, weight in SATELLITE_SCENARIOS[satellite].items():
        weights[symbol] = weights.get(symbol, 0.0) + SATELLITE_WEIGHT * weight

    total = sum(weights.values())
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"candidate weights must sum to 1.0, got {total:.12f}")
    return dict(sorted(weights.items()))


def target_notionals(weights: dict[str, float], aum: float) -> dict[str, float]:
    if aum <= 0:
        raise ValueError("aum must be positive")
    return {symbol: round(aum * weight, 2) for symbol, weight in weights.items()}


def _validate_positive_price(symbol: str, price: float, source: Path | str) -> None:
    if price <= 0:
        raise ValueError(f"non-positive price for {symbol} from {source}: {price}")


def load_local_quote(symbol: str, data_dir: Path = DEFAULT_DATA_DIR) -> QuoteObservation:
    baseline = data_dir / "etf_daily.csv"
    if baseline.exists():
        df = pd.read_csv(baseline, index_col="date", parse_dates=True).sort_index()
        if symbol in df.columns:
            series = pd.to_numeric(df[symbol], errors="coerce").dropna()
            if series.empty:
                raise ValueError(f"empty local price series for {symbol} in {baseline}")
            price = float(series.iloc[-1])
            _validate_positive_price(symbol, price, baseline)
            as_of = series.index[-1].date().isoformat()
            return QuoteObservation(
                symbol=symbol,
                source="local_adjusted_close",
                bid=price,
                ask=price,
                last=price,
                mid=price,
                spread_bps=0.0,
                estimated_one_way_slippage_bps=0.0,
                as_of=as_of,
            )

    raw_path = data_dir / "raw" / f"{symbol}.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"missing local price file for {symbol}: {raw_path}")
    df = pd.read_csv(raw_path, index_col="date", parse_dates=True).sort_index()
    column = "adj_close" if "adj_close" in df.columns else df.columns[0]
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    if series.empty:
        raise ValueError(f"empty local price series for {symbol} in {raw_path}")
    price = float(series.iloc[-1])
    _validate_positive_price(symbol, price, raw_path)
    as_of = series.index[-1].date().isoformat()
    return QuoteObservation(
        symbol=symbol,
        source="local_adjusted_close",
        bid=price,
        ask=price,
        last=price,
        mid=price,
        spread_bps=0.0,
        estimated_one_way_slippage_bps=0.0,
        as_of=as_of,
    )


def observe_quote(symbol: str, quote: QuoteSnapshot, source: str = "broker") -> QuoteObservation:
    bid = float(quote.bid or 0.0)
    ask = float(quote.ask or 0.0)
    last = float(quote.last or 0.0)
    if last <= 0 and (bid <= 0 or ask <= 0):
        raise ValueError(f"invalid quote for {symbol}: bid={bid}, ask={ask}, last={last}")

    if bid > 0 and ask > 0 and ask >= bid:
        mid = (bid + ask) / 2.0
        spread_bps = (ask - bid) / mid * 10_000 if mid > 0 else None
        estimated = spread_bps / 2.0 if spread_bps is not None else None
    else:
        mid = last
        spread_bps = None
        estimated = None

    _validate_positive_price(symbol, mid, source)
    as_of = quote.as_of.isoformat() if quote.as_of is not None else None
    return QuoteObservation(
        symbol=symbol,
        source=source,
        bid=bid if bid > 0 else None,
        ask=ask if ask > 0 else None,
        last=last if last > 0 else mid,
        mid=mid,
        spread_bps=spread_bps,
        estimated_one_way_slippage_bps=estimated,
        as_of=as_of,
    )


def load_current_positions(path: Path | None) -> dict[str, float] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    positions = payload.get("positions", payload) if isinstance(payload, dict) else {}
    result: dict[str, float] = {}
    for symbol, value in positions.items():
        if isinstance(value, dict):
            amount = value.get("market_value", value.get("marketValue", value.get("value", 0.0)))
        else:
            amount = value
        result[str(symbol).upper()] = float(amount or 0.0)
    return result


def positions_from_snapshot(snapshot: AccountSnapshot | None) -> dict[str, float] | None:
    if snapshot is None:
        return None
    return {
        symbol.upper(): float(position.market_value)
        for symbol, position in snapshot.positions.items()
        if float(position.market_value) != 0.0
    }


def build_shadow_orders(
    current_positions: dict[str, float] | None,
    target_weights: dict[str, float],
    total_nav: float,
    quotes: dict[str, QuoteObservation],
) -> list[dict[str, Any]]:
    if current_positions is None:
        return []
    assets = list(target_weights)
    prices = {symbol: quotes[symbol].mid for symbol in assets}
    weights = [target_weights[symbol] for symbol in assets]
    executor = PaperExecutor(assets=assets)
    orders = executor.translate_orders(current_positions, weights, total_nav, prices)
    return [serialize_trade_order(order) for order in orders]


def serialize_trade_order(order: TradeOrder) -> dict[str, Any]:
    return {
        "symbol": order.symbol,
        "side": order.side.value if isinstance(order.side, OrderSide) else str(order.side),
        "quantity": int(order.quantity),
        "estimated_price": round(float(order.estimated_price), 6),
        "estimated_amount": round(float(order.estimated_amount), 2),
        "status": order.status.value,
    }


def _persist_json_report(report: dict[str, Any], output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"{prefix}_{stamp}.json"
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    (output_dir / f"latest_{prefix}.json").write_text(text + "\n", encoding="utf-8")
    return path


def _save_shadow_report_to_db(report: dict[str, Any], db: Database | None = None) -> None:
    db = db or Database()
    db.save_shadow_run_report(
        portfolio_id=str(report["portfolio_id"]),
        broker_role=str(report["broker"]["role"]),
        broker_name=str(report["broker"]["name"]),
        checked_at=str(report["timestamp"]),
        dry_run=True,
        order_count=len(report["shadow_orders"]),
        reconciliation_status=str(report["status"]),
        requires_attention=bool(report["requires_attention"]),
        warnings_json=json.dumps(report["warnings"], ensure_ascii=False),
        report_json=json.dumps(report, ensure_ascii=False),
    )


def run_shadow_sync(
    *,
    aum: float = 2_000_000.0,
    satellite: str = "qqq_spy_gld",
    broker_name: str = "ibkr",
    broker_role: str = "primary",
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    data_dir: Path = DEFAULT_DATA_DIR,
    no_broker: bool = False,
    current_json: Path | None = None,
    portfolio_id: str = "shadow-90-10",
    persist_db: bool = True,
) -> dict[str, Any]:
    timestamp = utc_now().isoformat().replace("+00:00", "Z")
    warnings: list[str] = []
    weights = build_candidate_weights(satellite)
    symbols = list(weights)

    adapter = None
    snapshot = None
    broker_connected = False
    broker_display_name = broker_name if not no_broker else "offline"
    if not no_broker:
        try:
            adapter = create_broker_adapter(
                role=broker_role,
                broker_name=broker_name,
                mode=BrokerMode.READ_ONLY,
                assets=symbols,
            )
            broker_display_name = adapter.broker_name
            broker_connected = bool(adapter.connect())
            if broker_connected:
                try:
                    snapshot = adapter.get_account_snapshot()
                except Exception as exc:
                    warnings.append(f"broker account snapshot unavailable: {exc}")
        except Exception as exc:
            warnings.append(f"broker read-only adapter unavailable: {exc}")

    quotes: dict[str, QuoteObservation] = {}
    for symbol in symbols:
        broker_quote_used = False
        if adapter is not None and broker_connected:
            try:
                quotes[symbol] = observe_quote(symbol, adapter.get_quote(symbol), source=adapter.broker_name)
                broker_quote_used = True
            except Exception as exc:
                warnings.append(f"{symbol} broker quote unavailable, using local adjusted close: {exc}")
        if not broker_quote_used:
            quotes[symbol] = load_local_quote(symbol, data_dir=data_dir)

    current_positions = load_current_positions(current_json)
    if current_positions is None:
        current_positions = positions_from_snapshot(snapshot)
    if current_positions is None:
        warnings.append("current positions unavailable; report contains target notionals but no shadow orders")

    shadow_nav = float(snapshot.total_value) if snapshot is not None and snapshot.total_value > 0 else float(aum)
    orders = build_shadow_orders(current_positions, weights, shadow_nav, quotes)

    quote_payload = {symbol: asdict(obs) for symbol, obs in quotes.items()}
    market_dates = sorted({obs.as_of for obs in quotes.values() if obs.as_of})
    max_slippage = max(
        (obs.estimated_one_way_slippage_bps or 0.0 for obs in quotes.values()),
        default=0.0,
    )
    status = "OK"
    if warnings:
        status = "WARNING"
    if any(obs.mid <= 0 for obs in quotes.values()):
        status = "FAIL_CLOSED"
        warnings.append("non-positive observed quote detected")

    report: dict[str, Any] = {
        "timestamp": timestamp,
        "portfolio_id": portfolio_id,
        "dry_run": True,
        "trading_disabled": True,
        "aum": float(aum),
        "shadow_nav": shadow_nav,
        "candidate_policy": {
            "name": f"90pct_fixed_defensive_10pct_{satellite}",
            "defensive_weight": DEFENSIVE_WEIGHT,
            "satellite_weight": SATELLITE_WEIGHT,
            "satellite": satellite,
            "backtest_gate": "Research PASS / Production design APPROVED / Live leveraged execution NOT YET APPROVED",
        },
        "target_weights": weights,
        "target_notionals": target_notionals(weights, shadow_nav),
        "quotes": quote_payload,
        "market_dates": market_dates,
        "broker": {
            "name": broker_display_name,
            "role": broker_role,
            "mode": BrokerMode.READ_ONLY.value,
            "connected": broker_connected,
            "account_id": snapshot.account_id if snapshot else None,
        },
        "current_positions": current_positions or {},
        "shadow_orders": orders,
        "slippage_audit": {
            "backtest_panic_assumption_bps": 50.0,
            "max_observed_half_spread_bps": round(float(max_slippage), 4),
            "status": "OBSERVED" if broker_connected else "LOCAL_PRICE_ONLY",
        },
        "warnings": warnings,
        "requires_attention": bool(warnings or status == "FAIL_CLOSED"),
        "status": status,
    }

    path = _persist_json_report(report, output_dir, "shadow_sync")
    report["output_path"] = str(path)
    if persist_db:
        try:
            _save_shadow_report_to_db(report)
        except Exception as exc:
            report["warnings"].append(f"database persistence skipped: {exc}")
            report["requires_attention"] = True
            report["status"] = "WARNING"
            _persist_json_report(report, output_dir, "shadow_sync")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Stage 9.5 read-only shadow sync.")
    parser.add_argument("--aum", type=float, default=2_000_000.0)
    parser.add_argument("--satellite", choices=sorted(SATELLITE_SCENARIOS), default="qqq_spy_gld")
    parser.add_argument("--broker", default="ibkr")
    parser.add_argument("--broker-role", default="primary")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--current-json", type=Path)
    parser.add_argument("--portfolio-id", default="shadow-90-10")
    parser.add_argument("--no-broker", action="store_true")
    parser.add_argument("--skip-db", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = run_shadow_sync(
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
        f"Shadow sync {report['status']} | orders={len(report['shadow_orders'])} | "
        f"output={report['output_path']}"
    )
    if report["warnings"]:
        print("Warnings:")
        for warning in report["warnings"]:
            print(f"- {warning}")


if __name__ == "__main__":
    main()

